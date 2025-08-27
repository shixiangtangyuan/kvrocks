# Copyright (c) 2024 Shopee. All rights reserved.
# This source code is licensed under Apache 2.0 License.

include_guard()

if (NOT DEFINED KVPROTO_VERSION)
  set(KVPROTO_VERSION v1.24.0)
else ()
  set(KVPROTO_VERSION ${KVPROTO_VERSION})
endif ()
message(STATUS "Using kvproto ${KVPROTO_VERSION}")

FetchContent_Declare(
  kvproto
  GIT_REPOSITORY https://chongxin.mao:ASQLB8MAYRx6BP2QPFhF@git.garena.com/shopee/platform/kv/proto.git
  GIT_TAG        ${KVPROTO_VERSION})
FetchContent_MakeAvailable(kvproto)
