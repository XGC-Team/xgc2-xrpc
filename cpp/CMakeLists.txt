# C++20 standard-library baseline, also used by binary-safe bounded output.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 11)
  message(FATAL_ERROR "XgcXrpc C++20 requires GCC >= 11 (including its libstdc++ runtime)")
endif()
find_package(Threads REQUIRED)
find_package(Boost 1.70 REQUIRED)
find_package(OpenSSL 1.1.1 REQUIRED COMPONENTS Crypto)
find_package(nlohmann_json 3.7 CONFIG REQUIRED)
include(CMakePackageConfigHelpers)
include(CheckCXXSourceCompiles)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(_xrpc_required_libraries "${CMAKE_REQUIRED_LIBRARIES}")
list(APPEND CMAKE_REQUIRED_LIBRARIES Threads::Threads)
check_cxx_source_compiles("#include <stop_token>
#include <condition_variable>
#include <chrono>
#include <mutex>
#include <span>
#include <map>
#include <string_view>
int main() {
  std::stop_source stop;
  std::stop_callback callback(stop.get_token(), [] {});
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  std::condition_variable_any changed;
  changed.wait_until(lock, stop.get_token(), std::chrono::steady_clock::now(), [] { return true; });
  int data[1] = {0}; std::span<int> view(data);
  std::map<int, int> table;
  return view.size() != 1 || table.contains(0) || !std::string_view(\"xrpc\").starts_with(\"x\");
}" XGC2_XRPC_CXX20_RUNTIME)
set(CMAKE_REQUIRED_LIBRARIES "${_xrpc_required_libraries}")
unset(_xrpc_required_libraries)
if(NOT XGC2_XRPC_CXX20_RUNTIME)
  message(FATAL_ERROR "XgcXrpc needs C++20 stop_callback, cancellable condition-variable waits, span and matching standard-library headers/runtime")
endif()

add_library(xgc2_xrpc_unix SHARED src/unix.cpp)
add_library(xgc2_xrpc_policy SHARED src/runtime_policy.cpp)
add_library(xgc2_xrpc_diagnostics SHARED src/diagnostics.cpp)
add_library(xgc2_xrpc_http SHARED src/http.cpp)
add_library(xgc2_xrpc_bootstrap SHARED src/bootstrap.cpp)
add_library(xgc2_xrpc_json_http SHARED src/json_http.cpp)
add_library(XgcXrpc::unix ALIAS xgc2_xrpc_unix)
add_library(XgcXrpc::policy ALIAS xgc2_xrpc_policy)
add_library(XgcXrpc::diagnostics ALIAS xgc2_xrpc_diagnostics)
add_library(XgcXrpc::http ALIAS xgc2_xrpc_http)
add_library(XgcXrpc::bootstrap ALIAS xgc2_xrpc_bootstrap)
add_library(XgcXrpc::json_http ALIAS xgc2_xrpc_json_http)
set_target_properties(xgc2_xrpc_unix PROPERTIES EXPORT_NAME unix)
set_target_properties(xgc2_xrpc_policy PROPERTIES EXPORT_NAME policy)
set_target_properties(xgc2_xrpc_diagnostics PROPERTIES EXPORT_NAME diagnostics INSTALL_RPATH "$ORIGIN")
set_target_properties(xgc2_xrpc_http PROPERTIES EXPORT_NAME http INSTALL_RPATH "$ORIGIN")
set_target_properties(xgc2_xrpc_bootstrap PROPERTIES EXPORT_NAME bootstrap INSTALL_RPATH "$ORIGIN")
set_target_properties(xgc2_xrpc_json_http PROPERTIES EXPORT_NAME json_http INSTALL_RPATH "$ORIGIN")
foreach(target xgc2_xrpc_unix xgc2_xrpc_policy xgc2_xrpc_diagnostics xgc2_xrpc_http xgc2_xrpc_bootstrap xgc2_xrpc_json_http)
  target_compile_features(${target} PUBLIC cxx_std_20)
  target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
  target_include_directories(${target} PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
  set_target_properties(${target} PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION 1)
endforeach()
target_include_directories(xgc2_xrpc_http SYSTEM PRIVATE ${Boost_INCLUDE_DIRS})
target_include_directories(xgc2_xrpc_policy SYSTEM PRIVATE ${Boost_INCLUDE_DIRS})
target_compile_definitions(xgc2_xrpc_http PRIVATE BOOST_ERROR_CODE_HEADER_ONLY)
target_link_libraries(xgc2_xrpc_diagnostics PUBLIC xgc2_xrpc_policy)
target_link_libraries(xgc2_xrpc_bootstrap PUBLIC xgc2_xrpc_unix PRIVATE OpenSSL::Crypto Threads::Threads)
target_link_libraries(xgc2_xrpc_http PUBLIC xgc2_xrpc_unix xgc2_xrpc_policy
  xgc2_xrpc_diagnostics xgc2_xrpc_bootstrap PRIVATE Threads::Threads)
target_link_libraries(xgc2_xrpc_json_http PUBLIC xgc2_xrpc_http nlohmann_json::nlohmann_json)

option(XGC2_XRPC_ENABLE_GRPC "Build the optional native gRPC C++ SDK" OFF)
if(XGC2_XRPC_ENABLE_GRPC)
  find_package(Protobuf REQUIRED)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(XgcXrpcNativeGrpc REQUIRED IMPORTED_TARGET grpc++>=1.16)
  add_library(xgc2_xrpc_grpc SHARED src/grpc.cpp)
  add_library(XgcXrpc::grpc ALIAS xgc2_xrpc_grpc)
  set_target_properties(xgc2_xrpc_grpc PROPERTIES EXPORT_NAME grpc
    VERSION ${PROJECT_VERSION} SOVERSION 1 INSTALL_RPATH "$ORIGIN")
  target_compile_features(xgc2_xrpc_grpc PUBLIC cxx_std_20)
  target_compile_options(xgc2_xrpc_grpc PRIVATE -Wall -Wextra -Wpedantic)
  target_include_directories(xgc2_xrpc_grpc PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
  target_link_libraries(xgc2_xrpc_grpc PUBLIC xgc2_xrpc_unix xgc2_xrpc_policy
    xgc2_xrpc_diagnostics PkgConfig::XgcXrpcNativeGrpc Threads::Threads)
  install(TARGETS xgc2_xrpc_grpc EXPORT XgcXrpcGrpcTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  install(EXPORT XgcXrpcGrpcTargets NAMESPACE XgcXrpc::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/XgcXrpc)
endif()

foreach(component Unix Policy Diagnostics Bootstrap Http JsonHttp)
  string(TOLOWER "${component}" target_component)
  if(component STREQUAL "JsonHttp")
    set(target_component json_http)
  endif()
  install(TARGETS xgc2_xrpc_${target_component} EXPORT XgcXrpc${component}Targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  install(EXPORT XgcXrpc${component}Targets NAMESPACE XgcXrpc::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/XgcXrpc)
endforeach()
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
configure_package_config_file(cmake/XgcXrpcConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/XgcXrpcConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/XgcXrpc)
write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/XgcXrpcConfigVersion.cmake
  VERSION ${PROJECT_VERSION} COMPATIBILITY SameMajorVersion)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/XgcXrpcConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/XgcXrpcConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/XgcXrpc)

if(BUILD_TESTING)
  add_executable(xrpc_cpp_json_test tests/json_http_test.cpp)
  target_link_libraries(xrpc_cpp_json_test PRIVATE XgcXrpc::json_http Threads::Threads)
  target_compile_options(xrpc_cpp_json_test PRIVATE -Wall -Wextra -Wpedantic -UNDEBUG)
  add_test(NAME xrpc_cpp_json COMMAND xrpc_cpp_json_test)
  set_tests_properties(xrpc_cpp_json PROPERTIES TIMEOUT 30)
  add_executable(xrpc_cpp_bootstrap_test tests/bootstrap_test.cpp)
  target_compile_features(xrpc_cpp_bootstrap_test PRIVATE cxx_std_20)
  target_compile_options(xrpc_cpp_bootstrap_test PRIVATE -Wall -Wextra -Wpedantic -UNDEBUG)
  target_include_directories(xrpc_cpp_bootstrap_test SYSTEM PRIVATE ${Boost_INCLUDE_DIRS})
  target_compile_definitions(xrpc_cpp_bootstrap_test PRIVATE
    XRPC_BOOTSTRAP_FIXTURE="${CMAKE_CURRENT_SOURCE_DIR}/../contracts/fixtures/bootstrap.json")
  target_link_libraries(xrpc_cpp_bootstrap_test PRIVATE XgcXrpc::bootstrap OpenSSL::Crypto)
  add_test(NAME xrpc_cpp_bootstrap COMMAND xrpc_cpp_bootstrap_test)
  set_tests_properties(xrpc_cpp_bootstrap PROPERTIES TIMEOUT 30)
  foreach(case runtime policy resources wire environment fault diagnostics diagnostic_transport)
    if(case STREQUAL "runtime")
      set(test_source tests/runtime_test.cpp)
    else()
      set(test_source tests/${case}_test.cpp)
    endif()
    add_executable(xrpc_cpp_${case}_test ${test_source})
    target_compile_features(xrpc_cpp_${case}_test PRIVATE cxx_std_20)
    target_compile_options(xrpc_cpp_${case}_test PRIVATE -Wall -Wextra -Wpedantic -UNDEBUG)
    target_link_libraries(xrpc_cpp_${case}_test PRIVATE XgcXrpc::http Threads::Threads)
    if(case STREQUAL "wire" OR case STREQUAL "environment" OR case STREQUAL "diagnostics")
      target_include_directories(xrpc_cpp_${case}_test SYSTEM PRIVATE ${Boost_INCLUDE_DIRS})
      target_compile_definitions(xrpc_cpp_${case}_test PRIVATE BOOST_ERROR_CODE_HEADER_ONLY
        XRPC_WIRE_FIXTURE="${CMAKE_CURRENT_SOURCE_DIR}/../contracts/fixtures/wire.json")
    endif()
    add_test(NAME xrpc_cpp_${case} COMMAND xrpc_cpp_${case}_test)
    set_tests_properties(xrpc_cpp_${case} PROPERTIES TIMEOUT 30)
  endforeach()
  if(XGC2_XRPC_ENABLE_GRPC)
    find_program(XgcXrpcGrpcPlugin grpc_cpp_plugin)
    if(NOT XgcXrpcGrpcPlugin)
      message(FATAL_ERROR "gRPC fixture generation requires grpc_cpp_plugin")
    endif()
    set(grpc_fixture_dir "${CMAKE_CURRENT_BINARY_DIR}/grpc-fixture")
    file(MAKE_DIRECTORY "${grpc_fixture_dir}")
    add_custom_command(OUTPUT
      "${grpc_fixture_dir}/grpc_fixture.pb.cc" "${grpc_fixture_dir}/grpc_fixture.pb.h"
      "${grpc_fixture_dir}/grpc_fixture.grpc.pb.cc" "${grpc_fixture_dir}/grpc_fixture.grpc.pb.h"
      COMMAND protobuf::protoc --cpp_out=${grpc_fixture_dir} --grpc_out=${grpc_fixture_dir}
        --plugin=protoc-gen-grpc=${XgcXrpcGrpcPlugin}
        --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/grpc_fixture.proto
      DEPENDS tests/grpc_fixture.proto VERBATIM)
    add_executable(xrpc_cpp_grpc_test tests/grpc_test.cpp
      "${grpc_fixture_dir}/grpc_fixture.pb.cc" "${grpc_fixture_dir}/grpc_fixture.grpc.pb.cc")
    target_compile_features(xrpc_cpp_grpc_test PRIVATE cxx_std_20)
    target_compile_options(xrpc_cpp_grpc_test PRIVATE -Wall -Wextra -Wpedantic -UNDEBUG)
    target_include_directories(xrpc_cpp_grpc_test PRIVATE "${grpc_fixture_dir}")
    target_link_libraries(xrpc_cpp_grpc_test PRIVATE XgcXrpc::grpc protobuf::libprotobuf)
    add_test(NAME xrpc_cpp_grpc COMMAND xrpc_cpp_grpc_test)
    set_tests_properties(xrpc_cpp_grpc PROPERTIES TIMEOUT 30)
  endif()
endif()
