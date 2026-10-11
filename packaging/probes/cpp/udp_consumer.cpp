// Installed-consumer probe of the udp component: only its public headers and
// library. A server and a client talk over loopback with a programmatic key; the
// server's methods come from a MethodRouter (method.hpp and method_udp.hpp are
// installed with this component).
#include <xgc2/xrpc/method_udp.hpp>
#include <xgc2/xrpc/udp.hpp>
#include <chrono>
#include <string>

int main() {
  using namespace xgc2::xrpc;
  udp::KeyRing keys;
  keys.add(1, std::string(udp::key_bytes, 'k'));
  udp::ServerOptions options;
  options.bind_address = "127.0.0.1";
  udp::Server server(options, keys);
  MethodRouter router;
  router.add("probe.v1/Echo", [](MethodRequest request, MethodReply reply) { reply.complete(request.body); });
  add_methods(server, router);
  server.start();
  udp::Client client(keys);
  const auto response = client.call("127.0.0.1:" + std::to_string(server.port()), 1, "probe.v1/Echo",
                                    "{\"probe\":true}", std::chrono::steady_clock::now() + std::chrono::seconds(2),
                                    server.instance());
  if (response.delivery != udp::Delivery::ResponseReceived || response.status != udp::Status::Ok) return 2;
  if (response.body != "{\"probe\":true}" || response.instance != server.instance()) return 3;
  if (udp::instance_from_hex(server.instance_hex()) != server.instance()) return 4;
  return server.shutdown() ? 0 : 5;
}
