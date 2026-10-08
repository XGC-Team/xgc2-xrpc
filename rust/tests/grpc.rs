#![cfg(feature = "grpc")]
use std::{
    convert::Infallible,
    future::Future,
    io,
    os::unix::fs::PermissionsExt,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tokio_stream::{Stream, StreamExt};
use tonic::{
    body::BoxBody,
    codec::ProstCodec,
    server::{NamedService, ServerStreamingService, UnaryService},
    transport::{Channel, Endpoint},
    Code, Request, Response, Status,
};
use tower::Service;
use xgc2_xrpc::{
    grpc::{GrpcClient, GrpcHost, GrpcLimits},
    Context, Runtime, RuntimeOptions, UnixLease,
};

#[derive(Clone, PartialEq, prost::Message)]
struct Message {
    #[prost(string, tag = "1")]
    text: String,
}
#[derive(Default)]
struct Work {
    calls: AtomicUsize,
    received_budget: Mutex<Option<Duration>>,
    entered: Mutex<bool>,
    released: Mutex<bool>,
    changed: Condvar,
}
impl Work {
    fn enter_and_wait(&self) {
        *self.entered.lock().unwrap() = true;
        self.changed.notify_all();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.changed.wait(released).unwrap();
        }
    }
    fn wait_entered(&self) {
        let entered = self.entered.lock().unwrap();
        let (entered, _) = self
            .changed
            .wait_timeout_while(entered, Duration::from_secs(2), |v| !*v)
            .unwrap();
        assert!(*entered, "native blocking work must have started");
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
#[derive(Clone)]
struct Echo(Arc<Work>);
impl NamedService for Echo {
    const NAME: &'static str = "test.Echo";
}
struct Unary(Echo);
impl UnaryService<Message> for Unary {
    type Response = Message;
    type Future = Pin<Box<dyn Future<Output = Result<Response<Message>, Status>> + Send>>;
    fn call(&mut self, request: Request<Message>) -> Self::Future {
        let work = self.0 .0.clone();
        Box::pin(async move {
            work.calls.fetch_add(1, Ordering::SeqCst);
            let context = request
                .extensions()
                .get::<Context>()
                .expect("guarded native context")
                .clone();
            assert_eq!(context.peer_uid, Some(unsafe { libc::geteuid() }));
            assert_eq!(
                context.request_id,
                request
                    .metadata()
                    .get("x-request-id")
                    .unwrap()
                    .to_str()
                    .unwrap()
            );
            if request.get_ref().text == "remaining-budget" {
                *work.received_budget.lock().unwrap() = Some(context.remaining());
            }
            if request.get_ref().text == "starve" {
                *work.entered.lock().unwrap() = true;
                work.changed.notify_all();
                std::thread::sleep(Duration::from_millis(600));
            }
            if request.get_ref().text == "late-unary" {
                std::thread::sleep(Duration::from_millis(150));
            }
            if request.get_ref().text == "block" {
                context
                    .blocking(move || work.enter_and_wait())
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
            if request.get_ref().text == "large" {
                return Ok(Response::new(Message {
                    text: "x".repeat(2048),
                }));
            }
            Ok(Response::new(request.into_inner()))
        })
    }
}
struct Streaming(Echo);
impl ServerStreamingService<Message> for Streaming {
    type Response = Message;
    type ResponseStream = Pin<Box<dyn Stream<Item = Result<Message, Status>> + Send>>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Response<Self::ResponseStream>, Status>> + Send>>;
    fn call(&mut self, request: Request<Message>) -> Self::Future {
        self.0 .0.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if request.get_ref().text.starts_with("postpoll-") {
                let end = request.get_ref().text == "postpoll-eof";
                let stream: Self::ResponseStream = Box::pin(
                    tokio_stream::once(Ok(Message {
                        text: "first".into(),
                    }))
                    .chain(LatePoll {
                        delay: Box::pin(tokio::time::sleep(Duration::from_millis(10))),
                        end,
                        done: false,
                    }),
                );
                return Ok(Response::new(stream));
            }
            if request.get_ref().text == "finite" {
                let stream: Self::ResponseStream =
                    Box::pin(tokio_stream::once(Ok(request.into_inner())));
                return Ok(Response::new(stream));
            }
            let stream: Self::ResponseStream = Box::pin(
                tokio_stream::once(Ok(request.into_inner())).chain(tokio_stream::pending()),
            );
            Ok(Response::new(stream))
        })
    }
}

#[test]
fn completed_stream_releases_admission_while_consumer_retains_stream_object() {
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions {
        max_calls: 1,
        ..RuntimeOptions::default()
    })
    .unwrap();
    let mut limits = GrpcLimits::default();
    limits.rpc.in_flight = 1;
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        limits.clone(),
        false,
        Echo(Arc::new(Work::default())),
    )
    .unwrap();
    let client =
        GrpcClient::unix_with_limits(&runtime.handle(), &path, "instance.1", limits).unwrap();
    native_runtime().block_on(async {
        let mut grpc = tonic::client::Grpc::new(client.clone());
        grpc.ready().await.unwrap();
        let mut response: Response<tonic::Streaming<Message>> = grpc
            .server_streaming(
                request("finite", Some(Duration::from_secs(1))),
                tonic::codegen::http::uri::PathAndQuery::from_static("/test.Echo/Stream"),
                ProstCodec::default(),
            )
            .await
            .unwrap();
        assert!(response.get_mut().message().await.unwrap().is_some());
        assert!(response.get_mut().message().await.unwrap().is_none());
        unary(
            client.clone(),
            request("after-complete", Some(Duration::from_secs(1))),
        )
        .await
        .unwrap();
        assert!(response.get_mut().message().await.unwrap().is_none());
    });
    drop(client);
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
}

#[derive(Debug)]
struct OversizedEncode;
impl prost::Message for OversizedEncode {
    fn encode_raw(&self, _: &mut impl bytes::BufMut) {
        panic!("oversized message must be rejected before native encoding");
    }
    fn merge_field(
        &mut self,
        _: u32,
        _: prost::encoding::WireType,
        _: &mut impl bytes::Buf,
        _: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        unreachable!()
    }
    fn encoded_len(&self) -> usize {
        1024 * 1024
    }
    fn clear(&mut self) {}
}
#[test]
fn bounded_prost_codec_rejects_before_native_encode_allocates() {
    use http_body_util::BodyExt;
    use tonic::codec::Codec;
    assert_eq!(
        xgc2_xrpc::grpc::check_message_size(&OversizedEncode, 64)
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    native_runtime().block_on(async {
        let mut codec =
            xgc2_xrpc::grpc::BoundedProstCodec::<OversizedEncode, Message>::new(64, 64).unwrap();
        let mut body = tonic::codec::EncodeBody::new_client(
            codec.encoder(),
            tokio_stream::once(Ok(OversizedEncode)),
            None,
            Some(64),
        );
        assert!(body.frame().await.unwrap().is_err());
    });
}

#[test]
fn native_client_caller_deadline_remains_finite_when_selected_loop_is_paused() {
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let work = Arc::new(Work::default());
    let mut limits = GrpcLimits::default();
    limits.rpc.shutdown_timeout = Duration::from_millis(20);
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        limits,
        false,
        Echo(work.clone()),
    )
    .unwrap();
    let client = GrpcClient::unix(&runtime.handle(), &path, "instance.1").unwrap();
    native_runtime().block_on(async {
        unary(
            client.clone(),
            request("warm", Some(Duration::from_secs(1))),
        )
        .await
        .unwrap();
        let started = std::time::Instant::now();
        assert!(unary(
            client.clone(),
            request("starve", Some(Duration::from_millis(150)))
        )
        .await
        .is_err());
        assert!(
            started.elapsed() < Duration::from_millis(450),
            "caller loop must enforce deadline while SDK loop is paused"
        );
    });
    assert!(*work.entered.lock().unwrap());
    assert_eq!(host.close().unwrap_err().kind(), io::ErrorKind::TimedOut);
    assert!(path.exists());
    drop(client);
    runtime.close(Duration::from_secs(2)).unwrap();
    host.close().unwrap();
    assert_eq!(work.calls.load(Ordering::SeqCst), 2);
}

#[test]
fn native_stream_receive_deadline_runs_on_consumer_after_first_frame_and_sdk_pause() {
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        GrpcLimits::default(),
        false,
        Echo(Arc::new(Work::default())),
    )
    .unwrap();
    let client = GrpcClient::unix(&runtime.handle(), &path, "instance.1").unwrap();
    let handle = runtime.handle();
    native_runtime().block_on(async {
        let mut grpc = tonic::client::Grpc::new(client.clone());
        grpc.ready().await.unwrap();
        let mut response: Response<tonic::Streaming<Message>> = grpc
            .server_streaming(
                request("stream", Some(Duration::from_millis(100))),
                tonic::codegen::http::uri::PathAndQuery::from_static("/test.Echo/Stream"),
                ProstCodec::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.get_mut().message().await.unwrap().unwrap().text,
            "stream"
        );
        let (entered, received) = tokio::sync::oneshot::channel();
        let pauser = tokio::spawn({
            let handle = handle.clone();
            async move {
                handle
                    .execute(
                        async move {
                            entered.send(()).unwrap();
                            std::thread::sleep(Duration::from_millis(350));
                        },
                        Duration::from_secs(1),
                    )
                    .await
            }
        });
        received.await.unwrap();
        let started = std::time::Instant::now();
        let error = tokio::time::timeout(Duration::from_secs(1), response.get_mut().message())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code(), Code::DeadlineExceeded);
        assert!(
            started.elapsed() < Duration::from_millis(150),
            "consumer timer must retain only the original 100 ms budget"
        );
        assert_eq!(
            handle.stats().executions,
            1,
            "actually paused native work remains owned"
        );
        assert_eq!(handle.stats().hosts, 1);
        assert!(path.exists());
        pauser.await.unwrap().unwrap();
    });
    drop(client);
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
}

struct RoutedDialer {
    path: std::path::PathBuf,
    connections: Arc<AtomicUsize>,
}

struct InvalidBootstrapDialer {
    key: String,
    reads: Arc<AtomicUsize>,
}
impl xgc2_xrpc::Dialer for InvalidBootstrapDialer {
    fn pool_key(&self) -> String {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.key.clone()
    }
    fn connect(
        &self,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn xgc2_xrpc::AsyncIo>>> + Send>> {
        panic!("invalid pool identity must be rejected before connector execution");
    }
}
#[test]
fn native_client_rejects_empty_or_oversized_pool_identity_after_one_snapshot() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let limits = GrpcLimits::default();
    for key in [String::new(), "x".repeat(limits.rpc.header_bytes + 1)] {
        let reads = Arc::new(AtomicUsize::new(0));
        let dialer = Arc::new(InvalidBootstrapDialer {
            key,
            reads: reads.clone(),
        });
        let error = GrpcClient::with_dialer(
            &runtime.handle(),
            "instance.1".into(),
            dialer,
            limits.clone(),
        )
        .err()
        .unwrap();
        assert_eq!(error.disposition, xgc2_xrpc::Disposition::NotSent);
        assert_eq!(reads.load(Ordering::SeqCst), 1);
    }
    runtime.close(Duration::from_secs(2)).unwrap();
}

#[test]
fn closing_runtime_rejects_native_dispatch_client_calls_and_new_binds() {
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let work = Arc::new(Work::default());
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        GrpcLimits::default(),
        false,
        Echo(work.clone()),
    )
    .unwrap();
    let mut client = GrpcClient::unix(&runtime.handle(), &path, "instance.1").unwrap();
    let foreign = native_runtime();
    let channel = foreign.block_on(native_channel(path.clone()));
    assert_eq!(
        runtime.close(Duration::from_millis(20)).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    foreign.block_on(async {
        assert_eq!(
            unary(
                channel.clone(),
                request("closing", Some(Duration::from_millis(500)))
            )
            .await
            .unwrap_err()
            .code(),
            Code::Unavailable
        );
        let request = hyper::Request::builder()
            .uri("http://[::]:50051/test.Echo/Unary")
            .method("POST")
            .header("content-type", "application/grpc")
            .body(tonic::body::empty_body())
            .unwrap();
        let error = client.call(request).await.unwrap_err();
        assert_eq!(error.disposition, xgc2_xrpc::Disposition::NotSent);
    });
    let rejected_path = dir.path().join("rejected.sock");
    let error = GrpcHost::bind(
        &runtime,
        &rejected_path,
        "instance.1".into(),
        GrpcLimits::default(),
        false,
        Echo(work.clone()),
    )
    .err()
    .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    assert!(!rejected_path.exists());
    assert_eq!(work.calls.load(Ordering::SeqCst), 0);
    drop(channel);
    drop(client);
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
}

#[test]
fn native_grpc_service_reference_factories_validate_before_resource_admission() {
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let work = Arc::new(Work::default());
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        GrpcLimits::default(),
        false,
        Echo(work.clone()),
    )
    .unwrap();
    let service = xgc2_xrpc::ServiceRef {
        target_id: "local-target".into(),
        service: "test.Echo".into(),
        api_version: "v1".into(),
        instance_id: "instance.1".into(),
        profile: "grpc.v1".into(),
        endpoint: xgc2_xrpc::Endpoint {
            kind: "unix".into(),
            address: path.to_str().unwrap().into(),
        },
    };
    let mut bad = Vec::new();
    for target in ["", "remote-target", "local-target ", "local-target\0"] {
        let mut reference = service.clone();
        reference.target_id = target.into();
        bad.push(reference);
    }
    for name in ["", "test.Echo ", "test\nEcho"] {
        let mut reference = service.clone();
        reference.service = name.into();
        bad.push(reference);
    }
    for version in ["", " v1", "v1\r"] {
        let mut reference = service.clone();
        reference.api_version = version.into();
        bad.push(reference);
    }
    for instance in ["", "instance/1", "instance.1 "] {
        let mut reference = service.clone();
        reference.instance_id = instance.into();
        bad.push(reference);
    }
    for profile in ["http.v1", "grpc.v2"] {
        let mut reference = service.clone();
        reference.profile = profile.into();
        bad.push(reference);
    }
    let mut wrong_kind = service.clone();
    wrong_kind.endpoint.kind = "tcp".into();
    bad.push(wrong_kind);
    for endpoint in [
        "relative.sock".into(),
        "/tmp/../missing.sock".into(),
        format!("/{}", "x".repeat(107)),
        "/tmp/socket\0".into(),
    ] {
        let mut reference = service.clone();
        reference.endpoint.address = endpoint;
        bad.push(reference);
    }
    let mut limits = GrpcLimits::default();
    limits.rpc.call_timeout = Duration::from_millis(300);
    for reference in bad {
        assert_eq!(
            GrpcClient::from_service(&runtime.handle(), &reference, "local-target")
                .err()
                .unwrap()
                .disposition,
            xgc2_xrpc::Disposition::NotSent
        );
        assert_eq!(
            GrpcClient::from_service_with_limits(
                &runtime.handle(),
                &reference,
                "local-target",
                limits.clone()
            )
            .err()
            .unwrap()
            .disposition,
            xgc2_xrpc::Disposition::NotSent
        );
        let stats = runtime.handle().stats();
        assert_eq!(stats.references, 0);
        assert_eq!(stats.outbound_connections, 0);
        assert_eq!(stats.outbound_calls, 0);
    }
    assert_eq!(host.stats.accepted.load(Ordering::SeqCst), 0);
    let configured =
        GrpcClient::from_service_with_limits(&runtime.handle(), &service, "local-target", limits)
            .unwrap();
    let default = GrpcClient::from_service(&runtime.handle(), &service, "local-target").unwrap();
    native_runtime().block_on(async {
        assert_eq!(
            unary(
                configured.clone(),
                request("configured-ref", Some(Duration::from_secs(1)))
            )
            .await
            .unwrap()
            .get_ref()
            .text,
            "configured-ref"
        );
        assert_eq!(
            unary(
                default.clone(),
                request("default-ref", Some(Duration::from_secs(1)))
            )
            .await
            .unwrap()
            .get_ref()
            .text,
            "default-ref"
        );
    });
    assert_eq!(work.calls.load(Ordering::SeqCst), 2);
    drop(configured);
    drop(default);
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
}
impl xgc2_xrpc::Dialer for RoutedDialer {
    fn pool_key(&self) -> String {
        "authenticated-agent-route:test-identity".into()
    }
    fn connect(
        &self,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn xgc2_xrpc::AsyncIo>>> + Send>> {
        let path = self.path.clone();
        let connections = self.connections.clone();
        Box::pin(async move {
            connections.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(tokio::net::UnixStream::connect(path).await?)
                as Box<dyn xgc2_xrpc::AsyncIo>)
        })
    }
}
#[test]
fn native_client_injected_route_uses_shared_authenticated_pool_identity() {
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions {
        max_sessions: 1,
        ..RuntimeOptions::default()
    })
    .unwrap();
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        GrpcLimits::default(),
        false,
        Echo(Arc::new(Work::default())),
    )
    .unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let route = Arc::new(RoutedDialer {
        path,
        connections: connections.clone(),
    });
    let client = GrpcClient::with_dialer(
        &runtime.handle(),
        "instance.1".into(),
        route.clone(),
        GrpcLimits::default(),
    )
    .unwrap();
    let shared = GrpcClient::with_dialer(
        &runtime.handle(),
        "instance.1".into(),
        route,
        GrpcLimits::default(),
    )
    .unwrap();
    native_runtime().block_on(async {
        unary(
            client.clone(),
            request("routed-first", Some(Duration::from_secs(1))),
        )
        .await
        .unwrap();
        unary(
            shared.clone(),
            request("routed-second", Some(Duration::from_secs(1))),
        )
        .await
        .unwrap();
    });
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    drop(client);
    drop(shared);
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
}
impl Service<hyper::Request<BoxBody>> for Echo {
    type Response = hyper::Response<BoxBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, _: &mut TaskContext<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: hyper::Request<BoxBody>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(ProstCodec::default())
                .max_decoding_message_size(4096)
                .max_encoding_message_size(4096);
            let response = match request.uri().path() {
                "/test.Echo/Unary" => grpc.unary(Unary(service), request).await,
                "/test.Echo/Stream" => grpc.server_streaming(Streaming(service), request).await,
                _ => Status::unimplemented("unknown method").into_http(),
            };
            Ok(response)
        })
    }
}
fn native_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
fn private_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}
async fn native_channel(path: std::path::PathBuf) -> Channel {
    Endpoint::from_static("http://[::]:50051")
        .connect_with_connector(tower::service_fn(move |_uri| {
            let path = path.clone();
            async move {
                tokio::net::UnixStream::connect(path)
                    .await
                    .map(hyper_util::rt::TokioIo::new)
            }
        }))
        .await
        .unwrap()
}
fn request(text: &str, deadline: Option<Duration>) -> Request<Message> {
    let mut request = Request::new(Message {
        text: text.to_owned(),
    });
    request
        .metadata_mut()
        .insert("x-request-id", "request.1".parse().unwrap());
    request
        .metadata_mut()
        .insert("x-xrpc-instance-id", "instance.1".parse().unwrap());
    if let Some(deadline) = deadline {
        request.set_timeout(deadline);
    }
    request
}
async fn unary<S>(transport: S, request: Request<Message>) -> Result<Response<Message>, Status>
where
    S: tonic::client::GrpcService<BoxBody>,
    S::ResponseBody: hyper::body::Body<Data = bytes::Bytes> + Send + 'static,
    <S::ResponseBody as hyper::body::Body>::Error: Into<tonic::codegen::StdError>,
{
    let mut grpc = tonic::client::Grpc::new(transport)
        .max_decoding_message_size(4096)
        .max_encoding_message_size(4096);
    grpc.ready()
        .await
        .map_err(|e| Status::unknown(e.into().to_string()))?;
    grpc.unary(
        request,
        tonic::codegen::http::uri::PathAndQuery::from_static("/test.Echo/Unary"),
        ProstCodec::default(),
    )
    .await
}

#[test]
fn native_unary_and_stream_use_guarded_transport_on_foreign_loop() {
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let work = Arc::new(Work::default());
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        GrpcLimits::default(),
        false,
        Echo(work.clone()),
    )
    .unwrap();
    let client = GrpcClient::unix(&runtime.handle(), &path, "instance.1").unwrap();
    native_runtime().block_on(async {
        let response = unary(
            client.clone(),
            Request::new(Message {
                text: "hello".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(response.get_ref().text, "hello");
        assert_eq!(
            response.metadata().get("x-xrpc-instance-id").unwrap(),
            "instance.1"
        );
        let mut grpc = tonic::client::Grpc::new(client.clone());
        grpc.ready().await.unwrap();
        let mut stream: tonic::Streaming<Message> = grpc
            .server_streaming(
                request("stream", Some(Duration::from_millis(120))),
                tonic::codegen::http::uri::PathAndQuery::from_static("/test.Echo/Stream"),
                ProstCodec::default(),
            )
            .await
            .unwrap()
            .into_inner();
        assert_eq!(stream.message().await.unwrap().unwrap().text, "stream");
        assert!(
            tokio::time::timeout(Duration::from_secs(1), stream.message())
                .await
                .unwrap()
                .is_err()
        );
    });
    drop(client);
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
    assert!(!path.exists());
    assert_eq!(work.calls.load(Ordering::SeqCst), 2);
}

#[test]
fn native_deadlines_metadata_and_instance_are_rejected_before_dispatch() {
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let work = Arc::new(Work::default());
    let mut limits = GrpcLimits::default();
    limits.rpc.call_timeout = Duration::from_millis(500);
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        limits,
        false,
        Echo(work.clone()),
    )
    .unwrap();
    native_runtime().block_on(async {
        let channel = native_channel(path.clone()).await;
        for request in [
            request("missing", None),
            request("long", Some(Duration::from_secs(1))),
        ] {
            assert_eq!(
                unary(channel.clone(), request).await.unwrap_err().code(),
                Code::InvalidArgument
            );
        }
        // tonic's native deadline layer can cancel zero-budget requests before
        // the application metadata guard runs. Both native races reject them;
        // the zero dispatch count below is the admission invariant.
        let zero = unary(channel.clone(), request("zero", Some(Duration::ZERO)))
            .await
            .unwrap_err();
        assert!(matches!(
            zero.code(),
            Code::Cancelled | Code::InvalidArgument
        ));
        for key in ["x-request-id", "x-xrpc-instance-id", "grpc-timeout"] {
            let mut request = request("duplicate", Some(Duration::from_millis(300)));
            let value = request.metadata().get(key).unwrap().clone();
            request.metadata_mut().append(key, value);
            assert_eq!(
                unary(channel.clone(), request).await.unwrap_err().code(),
                Code::InvalidArgument
            );
        }
        for key in ["x-request-id", "x-xrpc-instance-id"] {
            let mut request = request("empty", Some(Duration::from_millis(300)));
            request.metadata_mut().insert(key, "".parse().unwrap());
            let expected = if key == "x-request-id" {
                Code::InvalidArgument
            } else {
                Code::FailedPrecondition
            };
            assert_eq!(
                unary(channel.clone(), request).await.unwrap_err().code(),
                expected
            );
        }
        let mut request = request("compressed", Some(Duration::from_millis(300)));
        request
            .metadata_mut()
            .insert("grpc-encoding", "gzip".parse().unwrap());
        assert_eq!(
            unary(channel.clone(), request).await.unwrap_err().code(),
            Code::Unimplemented
        );
    });
    assert_eq!(work.calls.load(Ordering::SeqCst), 0);
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
}

#[test]
fn blocked_native_work_retains_shared_call_capacity_and_lease_after_cancel() {
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let path2 = dir.path().join("other.sock");
    let mut runtime = Runtime::new(RuntimeOptions {
        max_calls: 1,
        ..RuntimeOptions::default()
    })
    .unwrap();
    let work = Arc::new(Work::default());
    let mut limits = GrpcLimits::default();
    limits.rpc.call_timeout = Duration::from_secs(2);
    limits.rpc.shutdown_timeout = Duration::from_millis(40);
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        limits.clone(),
        false,
        Echo(work.clone()),
    )
    .unwrap();
    let other_work = Arc::new(Work::default());
    let mut other = GrpcHost::bind(
        &runtime,
        &path2,
        "instance.1".into(),
        limits,
        false,
        Echo(other_work.clone()),
    )
    .unwrap();
    let loop_runtime = native_runtime();
    let call = loop_runtime.block_on(async {
        let channel = native_channel(path.clone()).await;
        tokio::spawn(unary(
            channel,
            request("block", Some(Duration::from_millis(600))),
        ))
    });
    // Drive the foreign request while synchronizing with actual native work entry.
    loop_runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !*work.entered.lock().unwrap() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("actual native work entry");
    });
    call.abort();
    loop_runtime.block_on(async {
        let _ = call.await;
    });
    loop_runtime.block_on(async {
        let channel = native_channel(path2.clone()).await;
        assert_eq!(
            unary(
                channel,
                request("overload", Some(Duration::from_millis(300)))
            )
            .await
            .unwrap_err()
            .code(),
            Code::ResourceExhausted
        );
    });
    assert_eq!(other_work.calls.load(Ordering::SeqCst), 0);
    assert_eq!(host.close().unwrap_err().kind(), io::ErrorKind::TimedOut);
    assert!(path.exists());
    assert!(UnixLease::reserve(&path, true).is_err());
    assert_eq!(
        runtime.close(Duration::from_millis(40)).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    work.release();
    host.close().unwrap();
    other.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
    assert!(!path.exists());
    assert_eq!(work.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn native_streams_and_bytes_have_finite_admission() {
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let work = Arc::new(Work::default());
    let mut limits = GrpcLimits::default();
    limits.rpc.in_flight = 1;
    limits.rpc.body_bytes = 128;
    limits.rpc.response_bytes = 128;
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        limits,
        false,
        Echo(work.clone()),
    )
    .unwrap();
    native_runtime().block_on(async {
        let channel = native_channel(path.clone()).await;
        let mut grpc = tonic::client::Grpc::new(channel.clone());
        grpc.ready().await.unwrap();
        let stream: Response<tonic::Streaming<Message>> = grpc
            .server_streaming(
                request("stream", Some(Duration::from_secs(1))),
                tonic::codegen::http::uri::PathAndQuery::from_static("/test.Echo/Stream"),
                ProstCodec::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            unary(
                channel.clone(),
                request("overload", Some(Duration::from_millis(400)))
            )
            .await
            .unwrap_err()
            .code(),
            Code::ResourceExhausted
        );
        drop(stream);
        // The peer observes the HTTP/2 stream reset before the next admission.
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(unary(
            channel.clone(),
            request(&"x".repeat(1024), Some(Duration::from_millis(400)))
        )
        .await
        .is_err());
        assert!(
            unary(channel, request("large", Some(Duration::from_millis(400))))
                .await
                .is_err()
        );
    });
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
}

#[test]
fn guarded_native_client_does_not_replay_a_lost_receipt_and_shares_reference_cap() {
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions {
        max_sessions: 1,
        ..RuntimeOptions::default()
    })
    .unwrap();
    let work = Arc::new(Work::default());
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        GrpcLimits::default(),
        false,
        Echo(work.clone()),
    )
    .unwrap();
    let client = GrpcClient::unix(&runtime.handle(), &path, "instance.1").unwrap();
    let shared = GrpcClient::unix(&runtime.handle(), &path, "instance.1").unwrap();
    assert!(xgc2_xrpc::Client::unix(&runtime.handle(), &path, "instance.1").is_err());
    assert!(GrpcClient::unix(
        &runtime.handle(),
        dir.path().join("another.sock"),
        "instance.1"
    )
    .is_err());
    let caller = std::thread::spawn({
        let client = client.clone();
        move || {
            native_runtime().block_on(unary(
                client,
                request("block", Some(Duration::from_secs(1))),
            ))
        }
    });
    work.wait_entered();
    host.stop();
    assert!(caller.join().unwrap().is_err());
    assert_eq!(work.calls.load(Ordering::SeqCst), 1);
    work.release();
    drop(shared);
    drop(client);
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
}

#[test]
fn native_client_pool_reuses_connections_then_reclaims_idle_io() {
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        GrpcLimits::default(),
        false,
        Echo(Arc::new(Work::default())),
    )
    .unwrap();
    let mut limits = GrpcLimits::default();
    limits.rpc.idle_timeout = Duration::from_millis(120);
    limits.rpc.client_reference_idle_timeout = Duration::from_millis(80);
    let client =
        GrpcClient::unix_with_limits(&runtime.handle(), &path, "instance.1", limits.clone())
            .unwrap();
    let shared =
        GrpcClient::unix_with_limits(&runtime.handle(), &path, "instance.1", limits).unwrap();
    native_runtime().block_on(async {
        unary(
            client.clone(),
            request("first", Some(Duration::from_secs(1))),
        )
        .await
        .unwrap();
        unary(
            shared.clone(),
            request("second", Some(Duration::from_secs(1))),
        )
        .await
        .unwrap();
        assert_eq!(host.stats.accepted.load(Ordering::SeqCst), 1);
        tokio::time::timeout(Duration::from_secs(2), async {
            while host.stats.active.load(Ordering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("client reference idle timeout closes actual IO");
        unary(
            shared.clone(),
            request("after-idle", Some(Duration::from_secs(1))),
        )
        .await
        .unwrap();
        assert_eq!(host.stats.accepted.load(Ordering::SeqCst), 2);
    });
    drop(client);
    drop(shared);
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
}

#[test]
fn native_multiplexed_short_call_does_not_shorten_long_stream_deadline() {
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let mut limits = GrpcLimits::default();
    limits.rpc.idle_timeout = Duration::from_millis(60);
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        limits,
        false,
        Echo(Arc::new(Work::default())),
    )
    .unwrap();
    native_runtime().block_on(async {
        let channel = native_channel(path.clone()).await;
        let mut grpc = tonic::client::Grpc::new(channel.clone());
        grpc.ready().await.unwrap();
        let started = std::time::Instant::now();
        let mut long: tonic::Streaming<Message> = grpc
            .server_streaming(
                request("stream", Some(Duration::from_secs(2))),
                tonic::codegen::http::uri::PathAndQuery::from_static("/test.Echo/Stream"),
                ProstCodec::default(),
            )
            .await
            .unwrap()
            .into_inner();
        assert_eq!(long.message().await.unwrap().unwrap().text, "stream");
        unary(
            channel.clone(),
            request("short", Some(Duration::from_millis(100))),
        )
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(250), long.message())
                .await
                .is_err(),
            "short-call and idle deadlines must not end the long stream"
        );
        assert_eq!(host.stats.active.load(Ordering::SeqCst), 1);
        unary(
            channel.clone(),
            request("short-later", Some(Duration::from_millis(100))),
        )
        .await
        .unwrap();
        assert_eq!(host.stats.accepted.load(Ordering::SeqCst), 1);
        assert!(tokio::time::timeout(Duration::from_secs(3), long.message())
            .await
            .unwrap()
            .is_err());
        assert!(
            started.elapsed() >= Duration::from_millis(1800),
            "long stream must retain its own native deadline"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while host.stats.active.load(Ordering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("idle expiry follows the last completed stream");
    });
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
}

#[test]
fn native_completed_stream_restarts_host_idle_expiry_before_its_call_deadline() {
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let mut limits = GrpcLimits::default();
    limits.rpc.idle_timeout = Duration::from_millis(60);
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        limits,
        false,
        Echo(Arc::new(Work::default())),
    )
    .unwrap();
    native_runtime().block_on(async {
        let channel = native_channel(path.clone()).await;
        let mut grpc = tonic::client::Grpc::new(channel.clone());
        grpc.ready().await.unwrap();
        let mut completed: tonic::Streaming<Message> = grpc
            .server_streaming(
                request("finite", Some(Duration::from_secs(2))),
                tonic::codegen::http::uri::PathAndQuery::from_static("/test.Echo/Stream"),
                ProstCodec::default(),
            )
            .await
            .unwrap()
            .into_inner();
        assert!(completed.message().await.unwrap().is_some());
        assert!(completed.message().await.unwrap().is_none());
        let stopped = std::time::Instant::now();
        tokio::time::timeout(Duration::from_millis(500), async {
            while host.stats.active.load(Ordering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("host idle timer must wake actual IO after early stream completion");
        assert!(stopped.elapsed() < Duration::from_millis(500));
        assert!(completed.message().await.unwrap().is_none());
    });
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
}

#[test]
fn unstarted_native_http2_headers_expire_and_global_connections_are_bounded() {
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions {
        max_connections: 1,
        ..RuntimeOptions::default()
    })
    .unwrap();
    let mut limits = GrpcLimits::default();
    limits.rpc.header_timeout = Duration::from_millis(80);
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        limits,
        false,
        Echo(Arc::new(Work::default())),
    )
    .unwrap();
    let unstarted = std::os::unix::net::UnixStream::connect(&path).unwrap();
    native_runtime().block_on(async {
        tokio::time::timeout(Duration::from_secs(1), async {
            while host.stats.active.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let rejected = std::os::unix::net::UnixStream::connect(&path).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while host.stats.rejected.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        drop(rejected);
        tokio::time::timeout(Duration::from_secs(1), async {
            while host.stats.active.load(Ordering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("initial HTTP2 headers expire without application dispatch");
        let channel = native_channel(path.clone()).await;
        unary(
            channel,
            request("healthy", Some(Duration::from_millis(500))),
        )
        .await
        .unwrap();
    });
    drop(unstarted);
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
}

#[derive(Clone)]
struct Unfenced;
impl NamedService for Unfenced {
    const NAME: &'static str = "test.Echo";
}
impl Service<hyper::Request<BoxBody>> for Unfenced {
    type Response = hyper::Response<BoxBody>;
    type Error = Infallible;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, _: &mut TaskContext<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: hyper::Request<BoxBody>) -> Self::Future {
        std::future::ready(Ok(
            Status::permission_denied("no identity receipt").into_http()
        ))
    }
}
#[test]
fn guarded_native_client_checks_identity_before_exposing_response_body() {
    let dir = private_dir();
    let path = dir.path().join("raw.sock");
    let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let raw_server = std::thread::spawn(move || {
        native_runtime().block_on(async {
            let listener = tokio::net::UnixListener::from_std(listener).unwrap();
            tonic::transport::Server::builder()
                .add_service(Unfenced)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::UnixListenerStream::new(listener),
                    async {
                        let _ = stopped.await;
                    },
                )
                .await
                .unwrap();
        })
    });
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let mut client = GrpcClient::unix(&runtime.handle(), &path, "instance.1").unwrap();
    native_runtime().block_on(async {
        let request = hyper::Request::builder()
            .uri("http://[::]:50051/test.Echo/Unary")
            .method("POST")
            .header("content-type", "application/grpc")
            .body(tonic::body::empty_body())
            .unwrap();
        let error = client.call(request).await.unwrap_err();
        assert_eq!(error.disposition, xgc2_xrpc::Disposition::ResponseReceived);
        assert!(error.message.contains("identity fence"));
    });
    drop(client);
    runtime.close(Duration::from_secs(2)).unwrap();
    stop.send(()).unwrap();
    raw_server.join().unwrap();
}

struct LatePoll {
    delay: Pin<Box<tokio::time::Sleep>>,
    end: bool,
    done: bool,
}
impl Stream for LatePoll {
    type Item = Result<Message, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        if self.delay.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        self.done = true;
        std::thread::sleep(Duration::from_millis(150));
        if self.end {
            Poll::Ready(None)
        } else {
            Poll::Ready(Some(Ok(Message {
                text: "late".into(),
            })))
        }
    }
}
#[test]
fn native_client_propagates_remaining_budget_after_queued_future() {
    use http_body_util::BodyExt;
    use prost::Message as _;
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let work = Arc::new(Work::default());
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        GrpcLimits::default(),
        false,
        Echo(work.clone()),
    )
    .unwrap();
    let mut client = GrpcClient::unix(&runtime.handle(), &path, "instance.1").unwrap();
    native_runtime().block_on(async {
        std::future::poll_fn(|cx| client.poll_ready(cx))
            .await
            .unwrap();
        let payload = Message {
            text: "remaining-budget".into(),
        }
        .encode_to_vec();
        let mut wire = vec![0];
        wire.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        wire.extend(payload);
        let body = http_body_util::Full::new(bytes::Bytes::from(wire))
            .map_err(|never| -> Status { match never {} })
            .boxed_unsync();
        let request = hyper::Request::builder()
            .method("POST")
            .uri("http://localhost/test.Echo/Unary")
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .header("x-request-id", "remaining.1")
            .header("x-xrpc-instance-id", "instance.1")
            .header("grpc-timeout", "200m")
            .body(body)
            .unwrap();
        let future = client.call(request);
        tokio::time::sleep(Duration::from_millis(120)).await;
        let response = future.await.unwrap();
        let initial = response.headers().get("grpc-status").cloned();
        let body = response.into_body().collect().await.unwrap();
        let status = initial.or_else(|| {
            body.trailers()
                .and_then(|trailers| trailers.get("grpc-status").cloned())
        });
        assert_eq!(status.unwrap().to_str().unwrap(), "0");
        assert_eq!(work.calls.load(Ordering::SeqCst), 1);
        assert!(
            work.received_budget.lock().unwrap().unwrap() <= Duration::from_millis(100),
            "the server must receive only the original call's remaining budget"
        );
    });
    drop(client);
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
}

#[test]
fn native_client_rechecks_metadata_cap_when_remaining_timeout_encoding_grows() {
    use http_body_util::BodyExt;
    use prost::Message as _;
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let work = Arc::new(Work::default());
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        GrpcLimits::default(),
        false,
        Echo(work.clone()),
    )
    .unwrap();
    let mut limits = GrpcLimits::default();
    limits.rpc.header_bytes = 8192;
    let mut client =
        GrpcClient::unix_with_limits(&runtime.handle(), &path, "instance.1", limits).unwrap();
    native_runtime().block_on(async {
        std::future::poll_fn(|cx| client.poll_ready(cx))
            .await
            .unwrap();
        let payload = Message {
            text: "bounded-metadata".into(),
        }
        .encode_to_vec();
        let mut wire = vec![0];
        wire.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        wire.extend(payload);
        let body = http_body_util::Full::new(bytes::Bytes::from(wire))
            .map_err(|never| -> Status { match never {} })
            .boxed_unsync();
        let mut request = hyper::Request::builder()
            .method("POST")
            .uri("http://localhost/test.Echo/Unary")
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .header("x-request-id", "metadata-grow.1")
            .header("x-xrpc-instance-id", "instance.1")
            .header("grpc-timeout", "100000u")
            .body(body)
            .unwrap();
        let initial_bytes = request
            .headers()
            .iter()
            .map(|(key, value)| key.as_str().len() + value.as_bytes().len() + 32)
            .sum::<usize>();
        let padding = "x".repeat(8192 - initial_bytes - "x-budget-pad".len() - 32);
        request
            .headers_mut()
            .insert("x-budget-pad", padding.parse().unwrap());
        assert_eq!(
            request
                .headers()
                .iter()
                .map(|(key, value)| key.as_str().len() + value.as_bytes().len() + 32)
                .sum::<usize>(),
            8192
        );
        let future = client.call(request);
        tokio::time::sleep(Duration::from_millis(20)).await;
        let error = future.await.unwrap_err();
        assert_eq!(error.disposition, xgc2_xrpc::Disposition::NotSent);
        assert!(error.message.contains("after native timeout encoding"));
        assert_eq!(work.calls.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.handle().stats().outbound_calls, 0);
    });
    drop(client);
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
}

#[test]
fn native_client_call_entry_budget_includes_time_before_future_first_poll() {
    use http_body_util::BodyExt;
    use prost::Message as _;
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let work = Arc::new(Work::default());
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        GrpcLimits::default(),
        false,
        Echo(work.clone()),
    )
    .unwrap();
    let mut client = GrpcClient::unix(&runtime.handle(), &path, "instance.1").unwrap();
    native_runtime().block_on(async {
        unary(
            client.clone(),
            request("entry-positive", Some(Duration::from_millis(200))),
        )
        .await
        .unwrap();
        assert_eq!(work.calls.load(Ordering::SeqCst), 1);
        std::future::poll_fn(|cx| client.poll_ready(cx))
            .await
            .unwrap();
        let payload = Message {
            text: "expired-before-poll".into(),
        }
        .encode_to_vec();
        let mut wire = vec![0];
        wire.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        wire.extend(payload);
        let body = http_body_util::Full::new(bytes::Bytes::from(wire))
            .map_err(|never| -> Status { match never {} })
            .boxed_unsync();
        let request = hyper::Request::builder()
            .method("POST")
            .uri("http://localhost/test.Echo/Unary")
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .header("x-request-id", "entry-expired.1")
            .header("x-xrpc-instance-id", "instance.1")
            .header("grpc-timeout", "20m")
            .body(body)
            .unwrap();
        let future = client.call(request);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let error = future.await.unwrap_err();
        assert_eq!(error.disposition, xgc2_xrpc::Disposition::NotSent);
        assert!(error.message.contains("before admission"));
        assert_eq!(work.calls.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.handle().stats().outbound_calls, 0);
    });
    drop(client);
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
}

#[test]
fn native_stream_does_not_publish_late_frame_or_successful_eof() {
    for text in ["postpoll-frame", "postpoll-eof"] {
        let dir = private_dir();
        let path = dir.path().join("grpc.sock");
        let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
        let work = Arc::new(Work::default());
        let mut host = GrpcHost::bind(
            &runtime,
            &path,
            "instance.1".into(),
            GrpcLimits::default(),
            false,
            Echo(work),
        )
        .unwrap();
        native_runtime().block_on(async {
            let channel = native_channel(path.clone()).await;
            let mut grpc = tonic::client::Grpc::new(channel);
            grpc.ready().await.unwrap();
            let response: Response<tonic::Streaming<Message>> = grpc
                .server_streaming(
                    request(text, Some(Duration::from_millis(40))),
                    tonic::codegen::http::uri::PathAndQuery::from_static("/test.Echo/Stream"),
                    ProstCodec::default(),
                )
                .await
                .unwrap();
            let mut stream = response.into_inner();
            assert_eq!(stream.message().await.unwrap().unwrap().text, "first");
            let start = std::time::Instant::now();
            let next = stream.message().await;
            assert!(start.elapsed() >= Duration::from_millis(100));
            assert!(
                next.is_err(),
                "a non-yielding stream poll must not return a late frame or successful EOF"
            );
        });
        host.close().unwrap();
        runtime.close(Duration::from_secs(2)).unwrap();
    }
}
#[test]
fn native_unary_does_not_publish_success_after_noncooperative_poll() {
    use http_body_util::BodyExt;
    use prost::Message as _;
    let dir = private_dir();
    let path = dir.path().join("grpc.sock");
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let work = Arc::new(Work::default());
    let mut host = GrpcHost::bind(
        &runtime,
        &path,
        "instance.1".into(),
        GrpcLimits::default(),
        false,
        Echo(work.clone()),
    )
    .unwrap();
    native_runtime().block_on(async {
        let socket = tokio::net::UnixStream::connect(&path).await.unwrap();
        let (mut sender, connection) = hyper::client::conn::http2::handshake(
            hyper_util::rt::TokioExecutor::new(),
            hyper_util::rt::TokioIo::new(socket),
        )
        .await
        .unwrap();
        let connection = tokio::spawn(connection);
        let payload = Message {
            text: "late-unary".into(),
        }
        .encode_to_vec();
        let mut wire = vec![0];
        wire.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        wire.extend_from_slice(&payload);
        let request = hyper::Request::builder()
            .method("POST")
            .uri("http://localhost/test.Echo/Unary")
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .header("x-request-id", "request.1")
            .header("x-xrpc-instance-id", "instance.1")
            .header("grpc-timeout", "20m")
            .body(http_body_util::Full::new(bytes::Bytes::from(wire)))
            .unwrap();
        let start = std::time::Instant::now();
        let response = sender.send_request(request).await;
        match response {
            Err(error) => assert!(start.elapsed() >= Duration::from_millis(100), "{error}"),
            Ok(response) => {
                let initial = response.headers().get("grpc-status").cloned();
                let body = response.into_body().collect().await;
                match body {
                    Err(error) => assert!(start.elapsed() >= Duration::from_millis(100), "{error}"),
                    Ok(body) => {
                        let status = initial.or_else(|| {
                            body.trailers()
                                .and_then(|trailers| trailers.get("grpc-status").cloned())
                        });
                        assert!(
                            body.to_bytes().is_empty(),
                            "late response must not expose a successful payload"
                        );
                        assert_eq!(
                            status.as_ref().and_then(|value| value.to_str().ok()),
                            Some("4"),
                            "late native unary completion must not claim success"
                        );
                    }
                }
            }
        }
        connection.abort();
    });
    assert_eq!(work.calls.load(Ordering::SeqCst), 1);
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
}
