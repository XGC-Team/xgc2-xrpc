#include "xgc2/xrpc/http.hpp"
#include "xgc2/xrpc/diagnostics.hpp"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <thread>
#include <unistd.h>
using namespace xgc2::xrpc;
using namespace std::chrono;
bool capture(void *state, std::string_view record) {
  static_cast<std::string *>(state)->append(record);
  return true;
}
int main() {
  DiagnosticsOptions settings;
  settings.level = LogSeverity::Debug;
  Diagnostics diagnostics(settings);
  char pattern[] = "/tmp/xrpc-diagnostic-transport-XXXXXX";
  const auto value = ::mkdtemp(pattern);
  assert(value);
  const std::string dir = value;
  UnixOptions options;
  options.path = dir + "/http.sock";
  HttpServer server(options, [](HttpRequest, HttpReply reply) {
    HttpResponse response;
    response.body = "response-secret-should-never-be-logged";
    reply.complete(std::move(response));
  }, {}, {"diagnostic-boot", {}});
  server.set_diagnostics(&diagnostics, "xgc2.transport-test");
  std::thread owner([&] { server.run(); });
  HttpClient client(options.path, {}, "diagnostic-boot");
  HttpRequest request;
  request.method = "POST";
  request.target = "/payload-secret-should-never-be-logged";
  request.request_id = "public-request-id";
  request.body = "request-secret-should-never-be-logged";
  request.headers.emplace_back("Authorization", "header-secret-should-never-be-logged");
  assert(client.call(request, Clock::now() + seconds(1)).status == 200);
  server.request_stop();
  owner.join();
  std::string records;
  assert(diagnostics.drain(64, capture, &records) >= 3);
  assert(records.find(diagnostic_code_name(DiagnosticCode::CallStarted)) != std::string::npos);
  assert(records.find(diagnostic_code_name(DiagnosticCode::CallCompleted)) != std::string::npos);
  assert(records.find("public-request-id") != std::string::npos);
  assert(records.find("diagnostic-boot") != std::string::npos);
  assert(records.find("xgc2.transport-test") != std::string::npos);
  assert(records.find("secret") == std::string::npos);
  assert(records.find("Authorization") == std::string::npos);
  std::filesystem::remove_all(dir);
  std::cout << "real HTTP lifecycle events and payload/header/path exclusion passed\n";
}
