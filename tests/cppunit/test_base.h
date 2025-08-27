/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
#ifndef KVROCKS_TEST_BASE_H
#define KVROCKS_TEST_BASE_H

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "storage/redis_db.h"
#include "types/redis_hash.h"

class TestBase : public ::testing::TestWithParam<bool> {  // NOLINT
 protected:
  explicit TestBase() : config_(new Config()), root_dir_("datanode-unittest.XXXXXX"), db_dir_("111") {
    const char *path = "test.conf";
    unlink(path);
    std::ofstream output_file(path, std::ios::out);
    output_file << "";

    if (!mkdtemp(root_dir_.data())) {
      std::cerr << "Create root dir failed, pattern:" << root_dir_ << std::endl;
      exit(1);
    }
    db_dir_ = root_dir_ + "/" + db_dir_;

    auto s = config_->Load(CLIOptions(path));
    config_->rocks_db.compression = rocksdb::CompressionType::kNoCompression;
    config_->rocks_db.write_buffer_size = 1;
    config_->rocks_db.block_size = 100;
    storage_ = new engine::Storage(config_);
    s = storage_->Open(db_dir_);
    if (!s.IsOK()) {
      std::cout << "Failed to open the storage, encounter error: " << s.Msg() << std::endl;
      assert(s.IsOK());
    }
  }
  ~TestBase() override {
    delete storage_;
    delete config_;

    std::error_code ec;
    std::filesystem::remove_all(root_dir_, ec);
    if (ec) {
      std::cout << "Encounter filesystem error: " << ec << std::endl;
    }
    const char *path = "test.conf";
    unlink(path);
  }

  engine::Storage *storage_;
  Config *config_ = nullptr;
  std::string root_dir_;
  std::string db_dir_;
  std::string key_;
  std::vector<Slice> fields_;
  std::vector<Slice> values_;
};
#endif  // KVROCKS_TEST_BASE_H
