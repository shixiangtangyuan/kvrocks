# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

include_guard()

include(cmake/utils.cmake)

FetchContent_DeclareGitHubWithMirror(benchmark
  google/benchmark v1.9.2
  MD5=4db9899da54b77d4836a5e45bca3c4ef
)

FetchContent_MakeAvailableWithArgs(benchmark
  CMAKE_MODULE_PATH=${PROJECT_SOURCE_DIR}/cmake/modules
  BENCHMARK_ENABLE_TESTING=OFF
  BENCHMARK_ENABLE_GTEST_TESTS=OFF
)
