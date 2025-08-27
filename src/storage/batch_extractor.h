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

#pragma once

#include <map>
#include <string>
#include <vector>

#include "redis_db.h"
#include "redis_metadata.h"
#include "status.h"
#include "storage.h"

// An extractor to extract update from raw write batch
class WriteBatchExtractor : public rocksdb::WriteBatch::Handler {
 public:
  WriteBatchExtractor() = default;

  class CMDDataInterface {
   public:
    virtual ~CMDDataInterface() = default;
    virtual Status ParseAndCheckLogArgs() = 0;
    virtual rocksdb::Status ParsePutData(std::string &key, std::string &sub_key, const Slice &value) = 0;
    virtual rocksdb::Status ParseDelData(std::string &key, std::string &sub_key) = 0;
    virtual rocksdb::Status ParseDelRangeData(const Slice &begin_key, const Slice &end_key) = 0;
    virtual void ConstructCmd() = 0;
    virtual void Clear() = 0;

    void SetCmdTypeVal(uint8_t cmd_type_val) { cmd_type_val_ = cmd_type_val; }
    void SetCmdArgs(const std::vector<std::string> &args) { args_ = args; }
    const std::vector<std::string> &GetArgs() const { return args_; }
    std::vector<std::vector<std::string>> &GetCommands() { return commands_; }

   protected:
    uint8_t cmd_type_val_ = 0;
    std::vector<std::string> args_;
    // parsed commands
    std::vector<std::vector<std::string>> commands_;
  };

  // cmd data factory
  static std::unique_ptr<CMDDataInterface> CreateDataHandler(RedisType type);

  void LogData(const rocksdb::Slice &blob) override;
  rocksdb::Status PutCF(uint32_t column_family_id, const Slice &key, const Slice &value) override;
  rocksdb::Status DeleteCF(uint32_t column_family_id, const Slice &key) override;
  rocksdb::Status DeleteRangeCF(uint32_t column_family_id, const Slice &begin_key, const Slice &end_key) override;
  const std::vector<std::vector<std::string>> &GetCommands() { return commands_; }

 private:
  Status parseAndCheckLogArgs();
  void constructCmd();
  void clearPrevParsedInfo();

  redis::WriteBatchLogData log_data_;
  bool is_slot_id_encoded_ = true;
  // NOTE(mingfo): Data parsing depends on the log data, but WriteBatch::Iterate won't
  // stop even if it failed to parse log data. Add flag to specify the result of parsing log data.
  bool can_parse_data_ = true;
  bool has_log_data_ = false;

  // output results
  std::vector<std::string> del_tokens_;
  std::vector<std::vector<std::string>> commands_;

  // data handlers
  std::unique_ptr<CMDDataInterface> cmd_data_handler_;
};

class HashCMDDataHandler : public WriteBatchExtractor::CMDDataInterface {
 public:
  Status ParseAndCheckLogArgs() override;
  rocksdb::Status ParsePutData(std::string &key, std::string &sub_key, const Slice &value) override;
  rocksdb::Status ParseDelData(std::string &key, std::string &sub_key) override;
  rocksdb::Status ParseDelRangeData(const Slice &begin_key, const Slice &end_key) override;
  void ConstructCmd() override;
  void Clear() override;

 private:
  std::string key_;
  std::vector<std::pair<std::string, std::string>> fv_pairs_;
  std::vector<uint64_t> fields_exat_;
  std::vector<std::string> del_fields_;
  RedisHashCodec hash_codec_ = RedisHashCodec::kNoFieldTTL;
};

class StringCMDDataHandler : public WriteBatchExtractor::CMDDataInterface {
 public:
  Status ParseAndCheckLogArgs() override { return Status::OK(); }
  rocksdb::Status ParsePutData(std::string &key, std::string &sub_key, const Slice &value) override;
  rocksdb::Status ParseDelData(std::string &key, std::string &sub_key) override { return rocksdb::Status::OK(); }
  rocksdb::Status ParseDelRangeData(const Slice &begin_key, const Slice &end_key) override {
    return rocksdb::Status::OK();
  }
  void ConstructCmd() override { commands_.emplace_back(std::move(string_tokens_)); }
  void Clear() override {
    string_tokens_.clear();
    expire_at_ = 0;
    cmd_type_val_ = 0;
    args_.clear();
    commands_.clear();
  }

  void SetExpireAt(uint64_t ex) { expire_at_ = ex; }

 private:
  uint64_t expire_at_ = 0;
  std::vector<std::string> string_tokens_;
};

class ListCMDDataHandler : public WriteBatchExtractor::CMDDataInterface {
 public:
  Status ParseAndCheckLogArgs() override { return Status::OK(); }
  rocksdb::Status ParsePutData(std::string &key, std::string &sub_key, const Slice &value) override;
  rocksdb::Status ParseDelData(std::string &key, std::string &sub_key) override;
  rocksdb::Status ParseDelRangeData(const Slice &begin_key, const Slice &end_key) override {
    return rocksdb::Status::OK();
  }
  void ConstructCmd() override;
  void Clear() override;

 private:
  bool first_seen_ = true;
  uint32_t list_count_ = 0;
  std::vector<std::string> list_tokens_;
};

class SetCMDDataHandler : public WriteBatchExtractor::CMDDataInterface {
 public:
  Status ParseAndCheckLogArgs() override { return Status::OK(); }
  rocksdb::Status ParsePutData(std::string &key, std::string &sub_key, const Slice &value) override;
  rocksdb::Status ParseDelData(std::string &key, std::string &sub_key) override;
  rocksdb::Status ParseDelRangeData(const Slice &begin_key, const Slice &end_key) override {
    return rocksdb::Status::OK();
  }
  void ConstructCmd() override { commands_.emplace_back(std::move(set_tokens_)); }
  void Clear() override {
    set_tokens_.clear();
    args_.clear();
    cmd_type_val_ = 0;
    commands_.clear();
  }

 private:
  std::vector<std::string> set_tokens_;
};

class ZSetCMDDataHandler : public WriteBatchExtractor::CMDDataInterface {
 public:
  Status ParseAndCheckLogArgs() override { return Status::OK(); }
  rocksdb::Status ParsePutData(std::string &key, std::string &sub_key, const Slice &value) override;
  rocksdb::Status ParseDelData(std::string &key, std::string &sub_key) override;
  rocksdb::Status ParseDelRangeData(const Slice &begin_key, const Slice &end_key) override {
    return rocksdb::Status::OK();
  }
  void ConstructCmd() override { commands_.emplace_back(std::move(zset_tokens_)); }
  void Clear() override {
    zset_tokens_.clear();
    args_.clear();
    cmd_type_val_ = 0;
    commands_.clear();
  }

 private:
  std::vector<std::string> zset_tokens_;
};
