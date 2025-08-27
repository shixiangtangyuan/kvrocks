cmake_minimum_required(VERSION 3.16)
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
set(CMAKE_POLICY_DEFAULT_CMP0074 NEW)

set(FETCHCONTENT_QUIET OFF)
set(ABSL_PROPAGATE_CXX_STD ON)
set(ABSL_ENABLE_INSTALL ON)
FetchContent_Declare(
        protobuf
        GIT_REPOSITORY https://github.com/google/protobuf.git
        GIT_TAG        v25.0
        GIT_PROGRESS   TRUE
        GIT_SHALLOW    TRUE
        USES_TERMINAL_DOWNLOAD TRUE
        GIT_SUBMODULES_RECURSE FALSE
        GIT_SUBMODULES ""
)
set(protobuf_BUILD_TESTS OFF)
set(protobuf_BUILD_CONFORMANCE OFF)
set(protobuf_BUILD_EXAMPLES OFF)
set(protobuf_BUILD_PROTOC_BINARIES ON)
set(protobuf_DISABLE_RTTI ON)
set(protobuf_MSVC_STATIC_RUNTIME ON)
set(protobuf_WITH_ZLIB ON CACHE BOOL "" FORCE)
FetchContent_GetProperties(protobuf)
if(NOT protobuf_POPULATED)
    FetchContent_Populate(protobuf)
    set(PROTOBUF_ROOT_DIR "${protobuf_SOURCE_DIR}")
endif()

FetchContent_Declare(
        grpc
        GIT_REPOSITORY https://chongxin.mao:ASQLB8MAYRx6BP2QPFhF@git.garena.com/shopee/platform/kv/grpc.git
        GIT_TAG        kv1.60.1
        GIT_PROGRESS   TRUE
        GIT_SHALLOW    TRUE
        USES_TERMINAL_DOWNLOAD TRUE
        GIT_SUBMODULES_RECURSE FALSE
        GIT_SUBMODULES
            "third_party/cares"
            "third_party/boringssl-with-bazel"
            "third_party/re2"
            "third_party/abseil-cpp"
            "third_party/zlib"
)
set(gRPC_BUILD_TESTS OFF)
set(gRPC_BUILD_CODEGEN ON) # for grpc_cpp_plugin
set(gRPC_BUILD_GRPC_CPP_PLUGIN ON) # we want to use only C++ plugin
set(gRPC_BUILD_CSHARP_EXT OFF)
set(gRPC_BUILD_GRPC_CSHARP_PLUGIN OFF)
set(gRPC_BUILD_GRPC_NODE_PLUGIN OFF)
set(gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN OFF)
set(gRPC_BUILD_GRPC_PHP_PLUGIN OFF)
set(gRPC_BUILD_GRPC_PYTHON_PLUGIN OFF)
set(gRPC_BUILD_GRPC_RUBY_PLUGIN OFF)
set(gRPC_BENCHMARK_PROVIDER "none" CACHE STRING "" FORCE)
set(gRPC_PROTOBUF_PROVIDER "module" CACHE STRING "" FORCE)
set(gRPC_ZLIB_PROVIDER "module" CACHE STRING "" FORCE)
# use lite protobuf version, unless we start using features
# that require full protobuf
set(gRPC_USE_PROTO_LITE ON CACHE BOOL "" FORCE)
FetchContent_GetProperties(grpc)
if(NOT grpc_POPULATED)
    FetchContent_Populate(grpc)
    add_subdirectory(${grpc_SOURCE_DIR} ${grpc_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

if(NOT TARGET protoc)
    message(FATAL_ERROR "Can not find target protoc")
endif()
set(_gRPC_PROTOBUF_PROTOC_EXECUTABLE $<TARGET_FILE:protoc>)
function(target_add_protobuf target)
    if(NOT TARGET ${target})
        message(FATAL_ERROR "Target ${target} doesn't exist")
    endif()
    if(NOT ARGN)
        message(SEND_ERROR "Error: PROTOBUF_GENERATE_GRPC_CPP() called without any proto files")
        return()
    endif()
    if(NOT TARGET grpc_cpp_plugin)
      message(FATAL_ERROR "Can not find target grpc_cpp_plugin")
    endif()
    set(_gRPC_CPP_PLUGIN $<TARGET_FILE:grpc_cpp_plugin>)

    foreach(FIL ${ARGN})
      get_filename_component(FIL_WE ${FIL} NAME_WE)
      get_filename_component(FIL_DIR ${FIL} DIRECTORY)
      if (FIL_DIR)
        set(FIL_WE "${FIL_DIR}/${FIL_WE}")
      endif()
      add_custom_command(
      OUTPUT  "${_gRPC_PROTO_GENS_DIR}/${FIL_WE}.grpc.pb.cc"
              "${_gRPC_PROTO_GENS_DIR}/${FIL_WE}.grpc.pb.h"
              "${_gRPC_PROTO_GENS_DIR}/${FIL_WE}_mock.grpc.pb.h"
              "${_gRPC_PROTO_GENS_DIR}/${FIL_WE}.pb.cc"
              "${_gRPC_PROTO_GENS_DIR}/${FIL_WE}.pb.h"
      COMMAND ${_gRPC_PROTOBUF_PROTOC_EXECUTABLE}
      ARGS --grpc_out=generate_mock_code=true:${_gRPC_PROTO_GENS_DIR}
          --cpp_out=${_gRPC_PROTO_GENS_DIR}
          --plugin=protoc-gen-grpc=${_gRPC_CPP_PLUGIN}
          ${_protobuf_include_path}
          ${FIL}
      DEPENDS ${_gRPC_PROTOBUF_PROTOC} ${_gRPC_CPP_PLUGIN}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT "Running gRPC C++ protocol buffer compiler on ${FIL}"
      VERBATIM)
      target_sources(${target} PRIVATE
        "${_gRPC_PROTO_GENS_DIR}/${FIL_WE}.grpc.pb.cc"
        "${_gRPC_PROTO_GENS_DIR}/${FIL_WE}.grpc.pb.h"
        "${_gRPC_PROTO_GENS_DIR}/${FIL_WE}_mock.grpc.pb.h"
        "${_gRPC_PROTO_GENS_DIR}/${FIL_WE}.pb.cc"
        "${_gRPC_PROTO_GENS_DIR}/${FIL_WE}.pb.h"
        )
      target_include_directories(${target} PRIVATE
        $<BUILD_INTERFACE:${_gRPC_PROTO_GENS_DIR}>
        $<BUILD_INTERFACE:${_gRPC_PROTOBUF_WELLKNOWN_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${grpc_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${grpc_SOURCE_DIR}/third_party/abseil-cpp>
      )
  endforeach()
endfunction()

# download googleapis
FetchContent_Declare(
  googleapis
  GIT_REPOSITORY https://github.com/googleapis/googleapis.git
  GIT_TAG        2f9af29)
FetchContent_MakeAvailable(googleapis)
# prepare for generate
set(_gRPC_PROTO_GENS_DIR ${CMAKE_BINARY_DIR}/gens)
file(MAKE_DIRECTORY ${_gRPC_PROTO_GENS_DIR})
set(_gRPC_PROTOBUF_WELLKNOWN_INCLUDE_DIR "${protobuf_SOURCE_DIR}/src")
set(_protobuf_include_path -I ${_gRPC_PROTOBUF_WELLKNOWN_INCLUDE_DIR} -I "${googleapis_SOURCE_DIR}")
# build rpc-status lib
add_library(rpc-status)
set_target_properties(rpc-status PROPERTIES
  ARCHIVE_OUTPUT_DIRECTORY ${_gRPC_PROTO_GENS_DIR})
target_add_protobuf(rpc-status google/rpc/status.proto)
include_directories("${_gRPC_PROTO_GENS_DIR}")
# export to build kvproto lib
set(_PROTOBUF_LIBPROTOBUF libprotobuf)
set(_REFLECTION grpc++_reflection)
set(_GRPC_GRPCPP grpc++)
