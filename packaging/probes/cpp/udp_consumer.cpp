// Installed-consumer probe of the udp component: only its public header and
// library. A server and a client talk over loopback with a programmatic key.
#include <xgc2/xrpc/udp.hpp>
#include <chrono>
#include <string>

int main() {
  using namespace xgc2::xrpc::udp;
  KeyRing keys;
  keys.add(1, std::string(key_bytes, 'k'));
  ServerOptions options;
  options.bind_address = "127.0.0.1";
  Server server(options, keys);
  server.add_method("probe.v1/Echo", [](Request request, Reply reply) {
    reply.complete(Status::Ok, request.body);
  });
  server.start();
  Client client(keys);
  const auto response = client.call("127.0.0.1:" + std::to_string(server.port()), 1, "probe.v1/Echo",
                                    "{\"probe\":true}", std::chrono::steady_clock::now() + std::chrono::seconds(2),
                                    server.instance());
  if (response.delivery != Delivery::ResponseReceived || response.status != Status::Ok) return 2;
  if (response.body != "{\"probe\":true}" || response.instance != server.instance()) return 3;
  if (instance_from_hex(server.instance_hex()) != server.instance()) return 4;
  return server.shutdown() ? 0 : 5;
}
