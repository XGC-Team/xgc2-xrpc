//! Explicit process/component owner. Tokio owns IO, task cancellation and its
//! bounded blocking executor; the SDK only reserves capacity before dispatch.
use crate::client::Session;
use std::{
    collections::HashMap,
    io,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex, Weak,
    },
    thread,
    time::Duration,
};
use tokio::{
    runtime::Handle,
    sync::{Notify, Semaphore},
};

/// Tokio cannot preempt a future's poll, and a ready inner future can win over
/// its elapsed timer. Check the original deadline before polling and again
/// after completion so a non-yielding poll cannot report a late success.
pub(crate) async fn timeout_at_checked<F>(
    deadline: tokio::time::Instant,
    future: F,
) -> Result<F::Output, ()>
where
    F: std::future::Future,
{
    tokio::pin!(future);
    let checked = std::future::poll_fn(|cx| {
        if tokio::time::Instant::now() >= deadline {
            return std::task::Poll::Ready(Err(()));
        }
        let result = future.as_mut().poll(cx);
        if tokio::time::Instant::now() >= deadline {
            std::task::Poll::Ready(Err(()))
        } else {
            result.map(Ok)
        }
    });
    tokio::time::timeout_at(deadline, checked)
        .await
        .map_err(|_| ())?
}

#[derive(Clone, Debug)]
pub struct RuntimeOptions {
    pub max_connections: usize,
    pub max_calls: usize,
    pub max_sessions: usize,
    pub blocking_workers: usize,
}
impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            max_connections: crate::policy::default_policy()
                .integer("HOST_MAX_CONNECTIONS")
                .expect("generated policy") as usize,
            max_calls: crate::policy::default_policy()
                .integer("HOST_MAX_IN_FLIGHT")
                .expect("generated policy") as usize,
            max_sessions: crate::policy::default_policy()
                .integer("CLIENT_MAX_REFERENCES")
                .expect("generated policy") as usize,
            blocking_workers: 4,
        }
    }
}
impl RuntimeOptions {
    /// Process-owned fixed blocking pool remains an explicit option. Other
    /// limits consume the shared policy resolved once by the composition root.
    pub fn from_policy(policy: &crate::RuntimePolicy) -> io::Result<Self> {
        let number = |name| {
            if policy.fields().contains_key(name) {
                policy.integer(name)
            } else {
                crate::policy::default_policy().integer(name)
            }
            .map(|n| n as usize)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
        };
        Ok(Self {
            max_connections: number("HOST_MAX_CONNECTIONS")?,
            max_calls: number("HOST_MAX_IN_FLIGHT")?,
            max_sessions: number("CLIENT_MAX_REFERENCES")?,
            ..Self::default()
        })
    }
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct RuntimeStats {
    pub hosts: usize,
    pub references: usize,
    pub inbound_connections: usize,
    pub outbound_connections: usize,
    pub in_flight: usize,
    pub outbound_calls: usize,
    pub blocking_jobs: usize,
    pub executions: usize,
    pub closing: bool,
}
#[derive(Default)]
pub(crate) struct Drained {
    done: Mutex<bool>,
    changed: Condvar,
}
impl Drained {
    pub fn finish(&self) {
        *self.done.lock().unwrap() = true;
        self.changed.notify_all();
    }
    pub fn wait(&self, budget: Duration) -> bool {
        let done = self.done.lock().unwrap();
        *self
            .changed
            .wait_timeout_while(done, budget, |v| !*v)
            .unwrap()
            .0
    }
}
pub(crate) struct Shared {
    pub handle: Handle,
    pub options: RuntimeOptions,
    pub incoming: Arc<Semaphore>,
    pub outgoing: Arc<Semaphore>,
    pub calls: Arc<Semaphore>,
    pub outbound_calls: Arc<Semaphore>,
    pub executions: Arc<Semaphore>,
    pub blocking: Arc<Semaphore>,
    pub sessions: Mutex<HashMap<String, Weak<Session>>>,
    pub hosts: AtomicUsize,
    pub session_count: AtomicUsize,
    pub closing: AtomicBool,
    pub changed: Notify,
}
impl Shared {
    fn quiescent(&self) -> bool {
        self.hosts.load(Ordering::Acquire) == 0
            && self.session_count.load(Ordering::Acquire) == 0
            && self.blocking.available_permits() == self.options.blocking_workers
            && self.outgoing.available_permits() == self.options.max_connections
            && self.outbound_calls.available_permits() == self.options.max_calls
            && self.calls.available_permits() == self.options.max_calls
            && self.executions.available_permits() == self.options.max_calls
    }
    pub fn ensure_open(&self) -> io::Result<()> {
        if self.closing.load(Ordering::Acquire) {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "runtime is closing",
            ))
        } else {
            Ok(())
        }
    }
}
#[derive(Clone)]
pub struct RuntimeHandle(pub(crate) Arc<Shared>);
struct ExecutionOwner {
    runtime: RuntimeHandle,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}
impl Drop for ExecutionOwner {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.runtime.0.changed.notify_one();
    }
}
impl RuntimeHandle {
    /// Finite, bounded execution on this owner from any running Tokio loop.
    /// Cancellation aborts the submitted future and retains its admission
    /// until Tokio actually drops it. Blocking domain work uses Context::blocking.
    pub async fn execute<F, T>(&self, future: F, budget: Duration) -> io::Result<T>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.0.ensure_open()?;
        let deadline = crate::client::caller_deadline(budget)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let permit = self.0.executions.clone().try_acquire_owned().map_err(|_| {
            io::Error::new(io::ErrorKind::WouldBlock, "runtime call admission full")
        })?;
        self.0.ensure_open()?;
        let owner = ExecutionOwner {
            runtime: self.clone(),
            permit: Some(permit),
        };
        let task = crate::client::AbortOnDrop(self.0.handle.spawn(async move {
            let _owner = owner;
            timeout_at_checked(deadline, future).await
        }));
        tokio::pin!(task);
        // A stopped/noncooperative SDK loop cannot postpone the caller loop's
        // own timeout. The owner is retained until the submitted task ends.
        match timeout_at_checked(deadline, &mut task.0).await {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Err(_)) => Err(io::Error::other("runtime execution task panicked")),
            _ => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "runtime execution deadline exceeded",
            )),
        }
    }
    pub fn stats(&self) -> RuntimeStats {
        let shared = &self.0;
        RuntimeStats {
            hosts: shared.hosts.load(Ordering::Acquire),
            references: shared.session_count.load(Ordering::Acquire),
            inbound_connections: shared.options.max_connections
                - shared.incoming.available_permits(),
            outbound_connections: shared.options.max_connections
                - shared.outgoing.available_permits(),
            in_flight: shared.options.max_calls - shared.calls.available_permits(),
            outbound_calls: shared.options.max_calls - shared.outbound_calls.available_permits(),
            blocking_jobs: shared.options.blocking_workers - shared.blocking.available_permits(),
            executions: shared.options.max_calls - shared.executions.available_permits(),
            closing: shared.closing.load(Ordering::Acquire),
        }
    }
}
pub struct Runtime {
    shared: Arc<Shared>,
    drained: Arc<Drained>,
    worker: Option<thread::JoinHandle<()>>,
}
impl Runtime {
    pub fn new(options: RuntimeOptions) -> io::Result<Self> {
        if [
            options.max_connections,
            options.max_calls,
            options.max_sessions,
            options.blocking_workers,
        ]
        .iter()
        .any(|n| *n == 0 || *n > i32::MAX as usize)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "positive runtime limits required",
            ));
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(options.blocking_workers)
            .build()?;
        let shared = Arc::new(Shared {
            handle: runtime.handle().clone(),
            incoming: Arc::new(Semaphore::new(options.max_connections)),
            outgoing: Arc::new(Semaphore::new(options.max_connections)),
            calls: Arc::new(Semaphore::new(options.max_calls)),
            outbound_calls: Arc::new(Semaphore::new(options.max_calls)),
            executions: Arc::new(Semaphore::new(options.max_calls)),
            blocking: Arc::new(Semaphore::new(options.blocking_workers)),
            options,
            sessions: Mutex::new(HashMap::new()),
            hosts: AtomicUsize::new(0),
            session_count: AtomicUsize::new(0),
            closing: AtomicBool::new(false),
            changed: Notify::new(),
        });
        let drained = Arc::new(Drained::default());
        let exit = drained.clone();
        let run = shared.clone();
        let worker = thread::Builder::new()
            .name("xrpc-io".into())
            .spawn(move || {
                runtime.block_on(async {
                    loop {
                        let changed = run.changed.notified();
                        if run.closing.load(Ordering::Acquire) && run.quiescent() {
                            break;
                        }
                        changed.await;
                    }
                });
                // Blocking slots remain reserved until native closures really end.
                // A live closure prevents this branch rather than extending shutdown.
                drop(runtime);
                exit.finish();
            })?;
        Ok(Self {
            shared,
            drained,
            worker: Some(worker),
        })
    }
    pub fn handle(&self) -> RuntimeHandle {
        RuntimeHandle(self.shared.clone())
    }
    pub fn close(&mut self, budget: Duration) -> io::Result<()> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "blocking close cannot run in an async runtime",
            ));
        }
        self.shared.closing.store(true, Ordering::Release);
        self.shared.changed.notify_one();
        if !self.drained.wait(budget) {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "runtime owners/work have not quiesced; resources retained",
            ));
        }
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| io::Error::other("runtime worker panicked"))?;
        }
        Ok(())
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        self.shared.closing.store(true, Ordering::Release);
        self.shared.changed.notify_one();
        // Dropping a JoinHandle detaches, never joins a non-cooperative handler.
        // The worker retains ownership until hosts, sessions and jobs quiesce.
    }
}
