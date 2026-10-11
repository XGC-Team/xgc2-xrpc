# XgcXrpc, C++17 (GNU 7.5 and newer, clang 10 and newer).
#
# XGC2_XRPC_COMPONENTS selects the shared libraries to build. A component pulls
# in the ones it links against, and only the dependencies of the selected
# components are searched for:
#
#   unix       POSIX only                  diagnostics  C++ standard library only
#   bootstrap  OpenSSL (Crypto)            http         Boost >= 1.70 (Asio, Beast), OpenSSL
#   json_http  nlohmann_json >= 3.7        grpc         gRPC++ >= 1.16, Protobuf, pkg-config
#   udp        OpenSSL (Crypto) only, nothing else of this SDK (GNU 7.5 is enough)
#
# `-DXGC2_XRPC_COMPONENTS=udp` builds and installs the udp.v1 library alone.
# grpc is not built by default because it needs the gRPC development packages.
#
# SOVERSION is the ABI generation of one library, never the product version: it
# changes when a release breaks that library's C++ ABI. 0.2.0 broke the ABI of
# every library 0.1.0 shipped (generation 2); udp is new in 0.2.0 (generation 1).
# Header-only code (method.hpp, method_udp.hpp, method_http.hpp, stop.hpp,
# delivery.hpp) has no ABI and rides with the component that installs it.
set(XGC2_XRPC_COMPONENTS "unix;diagnostics;bootstrap;http;json_http;udp" CACHE STRING
  "Components to build: unix diagnostics bootstrap http json_http grpc udp")
set(_xrpc_known unix diagnostics bootstrap http json_http grpc udp)
set(_xrpc_components ${XGC2_XRPC_COMPONENTS})
foreach(_xrpc_name IN LISTS _xrpc_components)
  if(NOT _xrpc_name IN_LIST _xrpc_known)
    message(FATAL_ERROR "Unknown XGC2_XRPC_COMPONENTS entry '${_xrpc_name}'; known: ${_xrpc_known}")
  endif()
endforeach()
if("json_http" IN_LIST _xrpc_components)
  list(APPEND _xrpc_components http)
endif()
if("http" IN_LIST _xrpc_components)
  list(APPEND _xrpc_components unix diagnostics bootstrap)
endif()
if("grpc" IN_LIST _xrpc_components)
  list(APPEND _xrpc_components unix diagnostics)
endif()
if("bootstrap" IN_LIST _xrpc_components)
  list(APPEND _xrpc_components unix)
endif()
list(REMOVE_DUPLICATES _xrpc_components)
if(NOT _xrpc_components)
  message(FATAL_ERROR "XGC2_XRPC_COMPONENTS selects nothing")
endif()
message(STATUS "XgcXrpc components: ${_xrpc_components}")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
include(CMakePackageConfigHelpers)

find_package(Threads REQUIRED)
if("bootstrap" IN_LIST _xrpc_components OR "udp" IN_LIST _xrpc_components)
  find_package(OpenSSL 1.1.1 REQUIRED COMPONENTS Crypto)
endif()
# Only http uses Boost; the bootstrap and diagnostics tests read JSON fixtures with it.
if("http" IN_LIST _xrpc_components OR (BUILD_TESTING AND
    ("bootstrap" IN_LIST _xrpc_components OR "diagnostics" IN_LIST _xrpc_components)))
  find_package(Boost 1.70 REQUIRED)
endif()
if("json_http" IN_LIST _xrpc_components)
  find_package(nlohmann_json 3.7 CONFIG REQUIRED)
endif()
if("grpc" IN_LIST _xrpc_components)
  find_package(Protobuf REQUIRED)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(XgcXrpcNativeGrpc REQUIRED IMPORTED_TARGET grpc++>=1.16)
endif()

# One shared library per component: target xgc2_xrpc_<component>, alias
# XgcXrpc::<component>, export file XgcXrpc<Export>Targets.cmake. Everything a
# component installs carries its name as the install COMPONENT, which is how the
# packaging splits one build into Debian packages.
function(xgc2_xrpc_component component export_name)
  cmake_parse_arguments(ARG "" "SOVERSION" "SOURCES;HEADERS" ${ARGN})
  if(NOT ARG_SOVERSION)
    message(FATAL_ERROR "xgc2_xrpc_component(${component}) needs a SOVERSION")
  endif()
  set(target xgc2_xrpc_${component})
  add_library(${target} SHARED ${ARG_SOURCES})
  add_library(XgcXrpc::${component} ALIAS ${target})
  set_target_properties(${target} PROPERTIES EXPORT_NAME ${component}
    VERSION ${PROJECT_VERSION} SOVERSION ${ARG_SOVERSION} INSTALL_RPATH "$ORIGIN")
  target_compile_features(${target} PUBLIC cxx_std_17)
  target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
  target_include_directories(${target} PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
  install(TARGETS ${target} EXPORT XgcXrpc${export_name}Targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT ${component}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT ${component}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT ${component})
  install(EXPORT XgcXrpc${export_name}Targets NAMESPACE XgcXrpc::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/XgcXrpc COMPONENT ${component})
  set(headers "")
  foreach(header IN LISTS ARG_HEADERS)
    list(APPEND headers include/xgc2/xrpc/${header})
  endforeach()
  install(FILES ${headers} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/xgc2/xrpc COMPONENT ${component})
endfunction()

# stop.hpp and delivery.hpp are header-only vocabulary of the unix base.
if("unix" IN_LIST _xrpc_components)
  xgc2_xrpc_component(unix Unix SOVERSION 2 SOURCES src/unix.cpp HEADERS unix.hpp stop.hpp delivery.hpp)
endif()
if("diagnostics" IN_LIST _xrpc_components)
  xgc2_xrpc_component(diagnostics Diagnostics SOVERSION 2 SOURCES src/diagnostics.cpp HEADERS diagnostics.hpp)
endif()
if("bootstrap" IN_LIST _xrpc_components)
  xgc2_xrpc_component(bootstrap Bootstrap SOVERSION 2 SOURCES src/bootstrap.cpp HEADERS bootstrap.hpp)
  target_link_libraries(xgc2_xrpc_bootstrap PUBLIC xgc2_xrpc_unix PRIVATE OpenSSL::Crypto Threads::Threads)
endif()
if("http" IN_LIST _xrpc_components)
  set(_xrpc_http_headers http.hpp bounded_output.hpp)
  # method_http.hpp serves the router of method.hpp, which the udp component installs.
  if("udp" IN_LIST _xrpc_components)
    list(APPEND _xrpc_http_headers method_http.hpp)
  endif()
  xgc2_xrpc_component(http Http SOVERSION 2 SOURCES src/http.cpp HEADERS ${_xrpc_http_headers})
  target_include_directories(xgc2_xrpc_http SYSTEM PRIVATE ${Boost_INCLUDE_DIRS})
  target_compile_definitions(xgc2_xrpc_http PRIVATE BOOST_ERROR_CODE_HEADER_ONLY)
  target_link_libraries(xgc2_xrpc_http PUBLIC xgc2_xrpc_unix xgc2_xrpc_diagnostics
    xgc2_xrpc_bootstrap PRIVATE Threads::Threads)
endif()
if("json_http" IN_LIST _xrpc_components)
  xgc2_xrpc_component(json_http JsonHttp SOVERSION 2 SOURCES src/json_http.cpp HEADERS json_http.hpp)
  target_link_libraries(xgc2_xrpc_json_http PUBLIC xgc2_xrpc_http nlohmann_json::nlohmann_json)
endif()
if("grpc" IN_LIST _xrpc_components)
  xgc2_xrpc_component(grpc Grpc SOVERSION 2 SOURCES src/grpc.cpp HEADERS grpc.hpp)
  target_link_libraries(xgc2_xrpc_grpc PUBLIC xgc2_xrpc_unix xgc2_xrpc_diagnostics
    PkgConfig::XgcXrpcNativeGrpc Threads::Threads)
endif()
if("udp" IN_LIST _xrpc_components)
  # method.hpp (the router, standard library only) and method_udp.hpp (the router on a
  # udp.v1 server) are header-only and travel with the library that every
  # distribution, Bionic included, ships.
  xgc2_xrpc_component(udp Udp SOVERSION 1 SOURCES src/udp_wire.cpp src/udp_keys.cpp src/udp_net.cpp
    src/udp_server.cpp src/udp_client.cpp HEADERS udp.hpp method.hpp method_udp.hpp)
  target_link_libraries(xgc2_xrpc_udp PRIVATE OpenSSL::Crypto Threads::Threads)
endif()

configure_package_config_file(cmake/XgcXrpcConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/XgcXrpcConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/XgcXrpc)
write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/XgcXrpcConfigVersion.cmake
  VERSION ${PROJECT_VERSION} COMPATIBILITY SameMajorVersion)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/XgcXrpcConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/XgcXrpcConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/XgcXrpc COMPONENT config)

if(BUILD_TESTING)
  set(_xrpc_test_flags -Wall -Wextra -Wpedantic -UNDEBUG)
  # xgc2_xrpc_test(<name> <source> LINK <libraries>...): one executable and one
  # ctest of the same name (xrpc_cpp_<name>), 30 s limit unless TIMEOUT is given.
  function(xgc2_xrpc_test name source)
    cmake_parse_arguments(ARG "" "TIMEOUT" "LINK;SOURCES;ARGS" ${ARGN})
    add_executable(xrpc_cpp_${name}_test ${source} ${ARG_SOURCES})
    target_compile_features(xrpc_cpp_${name}_test PRIVATE cxx_std_17)
    target_compile_options(xrpc_cpp_${name}_test PRIVATE ${_xrpc_test_flags})
    target_link_libraries(xrpc_cpp_${name}_test PRIVATE ${ARG_LINK} Threads::Threads)
    add_test(NAME xrpc_cpp_${name} COMMAND xrpc_cpp_${name}_test ${ARG_ARGS})
    set_tests_properties(xrpc_cpp_${name} PROPERTIES TIMEOUT 30)
  endfunction()
  function(xgc2_xrpc_boost_json_test name)
    target_include_directories(xrpc_cpp_${name}_test SYSTEM PRIVATE ${Boost_INCLUDE_DIRS})
    target_compile_definitions(xrpc_cpp_${name}_test PRIVATE BOOST_ERROR_CODE_HEADER_ONLY)
  endfunction()

  if("unix" IN_LIST _xrpc_components)
    xgc2_xrpc_test(stop tests/stop_test.cpp LINK XgcXrpc::unix)
  endif()
  if("diagnostics" IN_LIST _xrpc_components)
    xgc2_xrpc_test(diagnostics tests/diagnostics_test.cpp LINK XgcXrpc::diagnostics)
    xgc2_xrpc_boost_json_test(diagnostics)
  endif()
  if("bootstrap" IN_LIST _xrpc_components)
    xgc2_xrpc_test(bootstrap tests/bootstrap_test.cpp LINK XgcXrpc::bootstrap OpenSSL::Crypto)
    xgc2_xrpc_boost_json_test(bootstrap)
    target_compile_definitions(xrpc_cpp_bootstrap_test PRIVATE
      XRPC_BOOTSTRAP_FIXTURE="${CMAKE_CURRENT_SOURCE_DIR}/../contracts/fixtures/bootstrap.json")
  endif()
  if("http" IN_LIST _xrpc_components)
    foreach(case runtime http_host resources fault diagnostic_transport)
      xgc2_xrpc_test(${case} tests/${case}_test.cpp LINK XgcXrpc::http)
    endforeach()
    xgc2_xrpc_test(wire tests/wire_test.cpp LINK XgcXrpc::http)
    xgc2_xrpc_boost_json_test(wire)
    target_compile_definitions(xrpc_cpp_wire_test PRIVATE
      XRPC_WIRE_FIXTURE="${CMAKE_CURRENT_SOURCE_DIR}/../contracts/fixtures/wire.json")
  endif()
  if("json_http" IN_LIST _xrpc_components)
    xgc2_xrpc_test(json_http tests/json_http_test.cpp LINK XgcXrpc::json_http)
  endif()
  if("udp" IN_LIST _xrpc_components)
    # The tests reach the library's internals (codec, address helpers) through
    # its exported symbols and the private headers in src/.
    xgc2_xrpc_test(method tests/method_test.cpp LINK XgcXrpc::udp)
    xgc2_xrpc_test(method_udp tests/method_udp_test.cpp LINK XgcXrpc::udp)
    xgc2_xrpc_test(udp_wire tests/udp_wire_test.cpp LINK XgcXrpc::udp)
    xgc2_xrpc_test(udp_server tests/udp_server_test.cpp LINK XgcXrpc::udp ARGS 127.0.0.1)
    # IPv6 loopback, or a private one made in a user+network namespace; exit 77
    # (skipped) where neither exists. See the comment in the test.
    if(CMAKE_VERSION VERSION_LESS 3.16)
      # CMake before 3.16 has no SKIP_RETURN_CODE: report "skipped" as a pass.
      add_test(NAME xrpc_cpp_udp_server_ipv6 COMMAND sh -c
        "\"$0\" ::1; status=$?; if [ $status -eq 77 ]; then exit 0; fi; exit $status"
        $<TARGET_FILE:xrpc_cpp_udp_server_test>)
      set_tests_properties(xrpc_cpp_udp_server_ipv6 PROPERTIES TIMEOUT 60)
    else()
      add_test(NAME xrpc_cpp_udp_server_ipv6 COMMAND xrpc_cpp_udp_server_test ::1)
      set_tests_properties(xrpc_cpp_udp_server_ipv6 PROPERTIES TIMEOUT 60 SKIP_RETURN_CODE 77)
    endif()
    set_tests_properties(xrpc_cpp_udp_server PROPERTIES TIMEOUT 60)
    # The peer of the cross-language tests: a fixed command line and READY line
    # (see the header of the source); the Go interop test starts this binary.
    add_executable(xgc2-xrpc-udp-interop-server tests/udp_interop_server.cpp)
    target_compile_features(xgc2-xrpc-udp-interop-server PRIVATE cxx_std_17)
    target_compile_options(xgc2-xrpc-udp-interop-server PRIVATE ${_xrpc_test_flags})
    target_link_libraries(xgc2-xrpc-udp-interop-server PRIVATE XgcXrpc::udp Threads::Threads)
    xgc2_xrpc_test(udp_interop tests/udp_interop_test.cpp LINK XgcXrpc::udp
      ARGS $<TARGET_FILE:xgc2-xrpc-udp-interop-server>)
    add_dependencies(xrpc_cpp_udp_interop_test xgc2-xrpc-udp-interop-server)
  endif()
  if("http" IN_LIST _xrpc_components AND "udp" IN_LIST _xrpc_components)
    xgc2_xrpc_test(method_http tests/method_http_test.cpp LINK XgcXrpc::http)
    # One router on both transports, as a separate process: the peer of the
    # cross-language method tests (see the header of the source).
    add_executable(xgc2-xrpc-method-interop-server tests/method_interop_server.cpp)
    target_compile_features(xgc2-xrpc-method-interop-server PRIVATE cxx_std_17)
    target_compile_options(xgc2-xrpc-method-interop-server PRIVATE ${_xrpc_test_flags})
    target_link_libraries(xgc2-xrpc-method-interop-server PRIVATE XgcXrpc::http XgcXrpc::udp Threads::Threads)
    xgc2_xrpc_test(method_interop tests/method_interop_test.cpp LINK XgcXrpc::http XgcXrpc::udp
      ARGS $<TARGET_FILE:xgc2-xrpc-method-interop-server>)
    add_dependencies(xrpc_cpp_method_interop_test xgc2-xrpc-method-interop-server)
  endif()
  if("grpc" IN_LIST _xrpc_components)
    find_program(XgcXrpcGrpcPlugin grpc_cpp_plugin)
    if(NOT XgcXrpcGrpcPlugin)
      message(FATAL_ERROR "gRPC fixture generation requires grpc_cpp_plugin")
    endif()
    set(grpc_fixture_dir "${CMAKE_CURRENT_BINARY_DIR}/grpc-fixture")
    file(MAKE_DIRECTORY "${grpc_fixture_dir}/google/rpc")
    add_custom_command(OUTPUT
      "${grpc_fixture_dir}/grpc_fixture.pb.cc" "${grpc_fixture_dir}/grpc_fixture.pb.h"
      "${grpc_fixture_dir}/grpc_fixture.grpc.pb.cc" "${grpc_fixture_dir}/grpc_fixture.grpc.pb.h"
      COMMAND protobuf::protoc --cpp_out=${grpc_fixture_dir} --grpc_out=${grpc_fixture_dir}
        --plugin=protoc-gen-grpc=${XgcXrpcGrpcPlugin}
        --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/grpc_fixture.proto
      DEPENDS tests/grpc_fixture.proto VERBATIM)
    # Vendored googleapis messages: the test decodes the hand-written
    # application-error marker with generated code. Never part of the SDK.
    add_custom_command(OUTPUT
      "${grpc_fixture_dir}/google/rpc/status.pb.cc" "${grpc_fixture_dir}/google/rpc/status.pb.h"
      "${grpc_fixture_dir}/google/rpc/error_details.pb.cc" "${grpc_fixture_dir}/google/rpc/error_details.pb.h"
      COMMAND protobuf::protoc --cpp_out=${grpc_fixture_dir}
        --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/tests/proto
        --proto_path=${Protobuf_INCLUDE_DIRS}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/proto/google/rpc/status.proto
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/proto/google/rpc/error_details.proto
      DEPENDS tests/proto/google/rpc/status.proto tests/proto/google/rpc/error_details.proto VERBATIM)
    xgc2_xrpc_test(grpc tests/grpc_test.cpp LINK XgcXrpc::grpc protobuf::libprotobuf
      SOURCES
        "${grpc_fixture_dir}/grpc_fixture.pb.cc" "${grpc_fixture_dir}/grpc_fixture.grpc.pb.cc"
        "${grpc_fixture_dir}/google/rpc/status.pb.cc" "${grpc_fixture_dir}/google/rpc/error_details.pb.cc")
    # Generated code is not held to the SDK's warning level.
    target_include_directories(xrpc_cpp_grpc_test SYSTEM PRIVATE "${grpc_fixture_dir}")
    set_source_files_properties(
      "${grpc_fixture_dir}/grpc_fixture.pb.cc" "${grpc_fixture_dir}/grpc_fixture.grpc.pb.cc"
      "${grpc_fixture_dir}/google/rpc/status.pb.cc" "${grpc_fixture_dir}/google/rpc/error_details.pb.cc"
      PROPERTIES COMPILE_OPTIONS -w)
  endif()
endif()
