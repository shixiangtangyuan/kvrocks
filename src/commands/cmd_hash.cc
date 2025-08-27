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

#include <cstdint>
#include <iostream>
#include <vector>

#include "commander.h"
#include "commands/command_parser.h"
#include "common/scope_exit.h"
#include "error_constants.h"
#include "parse_util.h"
#include "scan_base.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "status.h"
#include "storage/redis_metadata.h"
#include "time_util.h"
#include "types/redis_hash.h"

namespace redis {

class CommandHGet : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    redis::Hash hash_db(storage, conn->GetNamespace());
    std::string value;
    auto s = hash_db.Get(args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? redis::NilString() : redis::BulkString(value);
    return Status::OK();
  }
};

class CommandHSetNX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    for (size_t i = 2; i < args_.size(); i += 2) {
      field_values_.emplace_back(args_[i], args_[i + 1]);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    uint64_t ret = 0;
    redis::Hash hash_db(storage, conn->GetNamespace());
    auto s = hash_db.MSet(args_[1], std::move(field_values_), true, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  std::vector<FieldValue> field_values_;
};

class CommandHStrlen : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    redis::Hash hash_db(storage, conn->GetNamespace());
    std::string value;
    auto s = hash_db.Get(args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(static_cast<int>(value.size()));
    return Status::OK();
  }
};

class CommandHDel : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    std::vector<Slice> fields;
    for (size_t i = 2; i < args_.size(); i++) {
      fields.emplace_back(args_[i]);
    }
    estimated_subkey_count_ = static_cast<int64_t>(fields.size());
    uint64_t ret = 0;
    redis::Hash hash_db(storage, conn->GetNamespace());
    auto s = hash_db.Delete(args_[1], fields, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandHExists : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    redis::Hash hash_db(storage, conn->GetNamespace());
    std::string value;
    auto s = hash_db.Get(args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? redis::Integer(0) : redis::Integer(1);
    return Status::OK();
  }
};

class CommandHLen : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    uint64_t count = 0;
    redis::Hash hash_db(storage, conn->GetNamespace());
    auto s = hash_db.Size(args_[1], &count);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? redis::Integer(0) : redis::Integer(count);
    return Status::OK();
  }
};

class CommandHIncrBy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int64_t>(args[3], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    increment_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    int64_t ret = 0;
    redis::Hash hash_db(storage, conn->GetNamespace());
    auto s = hash_db.IncrBy(args_[1], args_[2], increment_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  int64_t increment_ = 0;
};

class CommandHIncrByFloat : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto increment = ParseFloat(args[3]);
    if (!increment) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    increment_ = *increment;
    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    double ret = 0;
    redis::Hash hash_db(storage, conn->GetNamespace());
    auto s = hash_db.IncrByFloat(args_[1], args_[2], increment_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::BulkString(util::Float2String(ret));
    return Status::OK();
  }

 private:
  double increment_ = 0;
};

class CommandHMGet : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    std::vector<Slice> fields;
    for (size_t i = 2; i < args_.size(); i++) {
      fields.emplace_back(args_[i]);
    }
    estimated_subkey_count_ = static_cast<int64_t>(fields.size());
    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    redis::Hash hash_db(storage, conn->GetNamespace());
    auto s = hash_db.MGet(args_[1], fields, &values, &statuses);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      estimated_subkey_count_ = 0;
      values.resize(fields.size(), "");
      *output = redis::MultiBulkString(values);
    } else {
      *output = redis::MultiBulkString(values, statuses);
    }
    return Status::OK();
  }
};

class CommandHMSet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    field_values_.clear();
    for (size_t i = 2; i < args_.size(); i += 2) {
      field_values_.emplace_back(args_[i], args_[i + 1]);
    }
    estimated_subkey_count_ = static_cast<int64_t>(field_values_.size());
    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    uint64_t ret = 0;
    redis::Hash hash_db(storage, conn->GetNamespace());
    auto s = hash_db.MSet(args_[1], std::move(field_values_), false, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (GetAttributes()->name == "hset") {
      *output = redis::Integer(ret);
    } else {
      *output = redis::SimpleString("OK");
    }
    return Status::OK();
  }

 private:
  std::vector<FieldValue> field_values_;
};

class CommandHKeys : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    redis::Hash hash_db(storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    auto s = hash_db.GetAll(args_[1], &field_values, HashFetchType::kOnlyKey, &estimated_subkey_count_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> keys;
    keys.reserve(field_values.size());
    for (const auto &fv : field_values) {
      keys.emplace_back(fv.field);
    }
    *output = redis::MultiBulkString(keys);
    return Status::OK();
  }
};

class CommandHVals : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    redis::Hash hash_db(storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    auto s = hash_db.GetAll(args_[1], &field_values, HashFetchType::kOnlyValue, &estimated_subkey_count_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> values;
    values.reserve(field_values.size());
    for (const auto &p : field_values) {
      values.emplace_back(p.value);
    }
    *output = MultiBulkString(values, false);
    return Status::OK();
  }
};

class CommandHGetAll : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    redis::Hash hash_db(storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    auto s = hash_db.GetAll(args_[1], &field_values, HashFetchType::kAll, &estimated_subkey_count_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> kv_pairs;
    kv_pairs.reserve(field_values.size());
    for (const auto &p : field_values) {
      kv_pairs.emplace_back(p.field);
      kv_pairs.emplace_back(p.value);
    }
    *output = MultiBulkString(kv_pairs, false);

    return Status::OK();
  }
};

class CommandHRangeByLex : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 4);
    while (parser.Good()) {
      if (parser.EatEqICase("REV")) {
        spec_.reversed = true;
      } else if (parser.EatEqICase("LIMIT")) {
        spec_.offset = GET_OR_RET(parser.TakeInt());
        spec_.count = GET_OR_RET(parser.TakeInt());
      } else {
        return parser.InvalidSyntax();
      }
    }
    Status s;
    if (spec_.reversed) {
      s = ParseRangeLexSpec(args[3], args[2], &spec_);
    } else {
      s = ParseRangeLexSpec(args[2], args[3], &spec_);
    }
    if (!s.IsOK()) {
      return {Status::RedisParseErr, s.Msg()};
    }
    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    redis::Hash hash_db(storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    rocksdb::Status s = hash_db.RangeByLex(args_[1], spec_, &field_values, &estimated_subkey_count_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    std::vector<std::string> kv_pairs;
    for (const auto &p : field_values) {
      kv_pairs.emplace_back(p.field);
      kv_pairs.emplace_back(p.value);
    }
    *output = MultiBulkString(kv_pairs, false);

    return Status::OK();
  }

 private:
  RangeLexSpec spec_;
};

class CommandHScan : public CommandSubkeyScanBaseV1 {
 public:
  CommandHScan() = default;
  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    CursorPair cursor_pair;
    auto ret = srv->xscan_lru_cache->GetScanSession(redis_cursor_, -1, -1, &cursor_pair, CursorType::kTypeHash, key_);
    if (!ret.IsOK()) {
      return ret.ToStatus();
    }
    auto session = ret.GetValue();
    auto scope_exit = MakeScopeExit([&session] { session->ResetInUsing(); });
    std::string store_cursor;
    if (redis_cursor_ == cursor_pair.client_cursor) {
      store_cursor = std::move(cursor_pair.store_cursor);
    }

    redis::Hash hash_db(storage, conn->GetNamespace());
    if (hscan_no_values_) {
      std::vector<std::string> fields;
      auto s = hash_db.Scan(key_, &store_cursor, limit_, pattern_, &fields, nullptr, &estimated_subkey_count_);
      if (!s.ok() && !s.IsNotFound()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      auto new_cursor = session->Update(store_cursor);
      *output = GenerateOutput(srv, new_cursor, fields);
      return Status::OK();
    }

    std::vector<std::string> fields, values;
    auto s = hash_db.Scan(key_, &store_cursor, limit_, pattern_, &fields, &values, &estimated_subkey_count_);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    auto new_cursor = session->Update(store_cursor);
    *output = GenerateOutput(srv, new_cursor, fields, values);
    return Status::OK();
  }
};

class CommandHScanV2 : public CommandSubkeyScanBaseV2 {
 public:
  CommandHScanV2() = default;
  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    redis::Hash hash_db(storage, conn->GetNamespace());
    std::vector<std::string> fields;
    std::vector<std::string> values;
    auto s = hash_db.Scan(key_, &cursor_, limit_, pattern_, &fields, &values, &estimated_subkey_count_);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = GenerateOutput(srv, fields, values);
    return Status::OK();
  }
};

class CommandHRandField : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() >= 3) {
      no_parameters_ = false;
      auto parse_result = ParseInt<int64_t>(args[2], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }
      command_count_ = *parse_result;
      if (args.size() > 4 || (args.size() == 4 && !util::EqualICase(args[3], "withvalues"))) {
        return {Status::RedisParseErr, errInvalidSyntax};
      } else if (args.size() == 4) {
        withvalues_ = true;
      }
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    redis::Hash hash_db(storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;

    auto s = hash_db.RandField(args_[1], command_count_, &field_values,
                               withvalues_ ? HashFetchType::kAll : HashFetchType::kOnlyKey, &estimated_subkey_count_);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> result_entries;
    result_entries.reserve(field_values.size());
    for (const auto &p : field_values) {
      result_entries.emplace_back(p.field);
      if (withvalues_) result_entries.emplace_back(p.value);
    }
    if (no_parameters_)
      *output = s.IsNotFound() ? redis::NilString() : redis::BulkString(result_entries[0]);
    else
      *output = redis::MultiBulkString(result_entries, false);
    return Status::OK();
  }

 private:
  bool withvalues_ = false;
  int64_t command_count_ = 1;
  bool no_parameters_ = true;
};

class CommandHPExpireAt : public Commander {
 public:
  // HPEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
  Status Parse(const std::vector<std::string> &args) override {
    if (!encode_hash_sub_flag) {
      return {Status::CmdDisabled, errCmdDisabled};
    }
    CommandParser parser(args, 1);
    uint64_t idx = 1;
    // user key
    GET_OR_RET(parser.TakeStr());
    key_ = args[idx++];
    // unix-time-milliseconds
    StatusOr<int64_t> take_int_ret = parser.TakeInt<>();
    if (!take_int_ret.IsOK()) {
      return {Status::RedisParseErr, errValueNotInteger};
    }
    int64_t input = take_int_ret.GetValue();
    if (input < 0) {
      return {Status::RedisParseErr, errNegativeExpireTime};
    }
    const auto now_timestamp_ms = util::GetTimeStampMS();
    const auto &cmd_name = GetAttributes()->name;
    if (cmd_name == "hexpireat") {
      // check input < max uint64_t/1000
      if (static_cast<uint64_t>(input) > std::numeric_limits<uint64_t>::max() / 1000) {
        return {Status::RedisParseErr, std::string(errInvalidExpireTime) + " in '" + cmd_name + "' command"};
      }
      timestamp_ = input * 1000;
    } else if (cmd_name == "hpexpireat") {
      timestamp_ = input;
    } else if (cmd_name == "hexpire") {
      // check input * 1000 + current_time < max uint64_t
      if (static_cast<uint64_t>(input) > (std::numeric_limits<uint64_t>::max() - now_timestamp_ms) / 1000) {
        return {Status::RedisParseErr,
                std::string(errInvalidExpireTime) + " in '" + GetAttributes()->name + "' command"};
      }
      timestamp_ = input * 1000 + now_timestamp_ms;
    } else if (cmd_name == "hpexpire") {
      // check input + current_time < max uint64_t
      if (static_cast<uint64_t>(input) > std::numeric_limits<uint64_t>::max() - now_timestamp_ms) {
        return {Status::RedisParseErr,
                std::string(errInvalidExpireTime) + " in '" + GetAttributes()->name + "' command"};
      }
      timestamp_ = input + now_timestamp_ms;
    } else {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    if (timestamp_ > kHashFieldMaxAbsTimeMS) {
      return {Status::RedisParseErr, std::string(errInvalidExpireTime) + " in '" + GetAttributes()->name + "' command"};
    }
    idx++;
    // [NX | XX | GT | LT]
    if (parser.EatEqICase("NX")) {
      idx++;
      option_ = ExpireSetCond::NX;
    } else if (parser.EatEqICase("XX")) {
      idx++;
      option_ = ExpireSetCond::XX;
    } else if (parser.EatEqICase("GT")) {
      idx++;
      option_ = ExpireSetCond::GT;
    } else if (parser.EatEqICase("LT")) {
      idx++;
      option_ = ExpireSetCond::LT;
    }
    // FIELDS numfields field [field ...]
    if (!parser.EatEqICase("FIELDS")) {
      return {Status::RedisParseErr, errMissKeyWordField};
    }
    auto s = parser.TakeInt<int64_t>();
    if (!s.IsOK()) {
      return {Status::RedisParseErr, errNegativeNumFields};
    }
    idx += 2;
    auto num_fields = s.GetValue();
    if (num_fields <= 0) {
      return {Status::RedisParseErr, errNegativeNumFields};
    }
    size_t fields_count = num_fields;
    fields_.clear();
    for (int i = 0; i < num_fields; i++) {
      if (!parser.Good()) {
        return {Status::RedisParseErr, errNumFieldsNotMatch};
      }
      parser.RawNext();
      fields_count--;
      fields_.emplace_back(args[idx++]);
    }

    if (fields_count != 0 || idx != args.size()) {
      return {Status::RedisParseErr, errNumFieldsNotMatch};
    }
    estimated_subkey_count_ = static_cast<int64_t>(fields_.size());

    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    redis::Hash hash_db(storage, conn->GetNamespace());
    std::vector<SetExRes> ret;
    auto s = hash_db.HExpireAt(key_, fields_, timestamp_, ret, option_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    std::vector<int64_t> reply;
    reply.reserve(ret.size());
    for (const auto &r : ret) {
      reply.emplace_back(static_cast<int64_t>(r));
    }
    *output = redis::MultiInteger(reply);
    return Status::OK();
  }

 private:
  Slice key_;
  uint64_t timestamp_ = 0;
  std::vector<Slice> fields_;
  ExpireSetCond option_ = ExpireSetCond::NONE;
};

class CommandHPExpireTime : public Commander {
 public:
  // HPEXPIRETIME key FIELDS numfields field [field ...]
  Status Parse(const std::vector<std::string> &args) override {
    if (!encode_hash_sub_flag) {
      return {Status::CmdDisabled, errCmdDisabled};
    }
    CommandParser parser(args, 1);
    uint64_t idx = 1;
    // user key
    GET_OR_RET(parser.TakeStr());
    key_ = args[idx++];
    // FIELDS numfields field [field ...]
    if (!parser.EatEqICase("FIELDS")) {
      return {Status::RedisParseErr, errMissKeyWordField};
    }
    auto s = parser.TakeInt<int64_t>();
    if (!s.IsOK()) {
      return {Status::RedisParseErr, errNeedPositiveInteger};
    }
    auto num_fields = s.GetValue();
    if (num_fields <= 0) {
      return {Status::RedisParseErr, errNeedPositiveInteger};
    }
    idx += 2;
    size_t fields_count = num_fields;
    for (int i = 0; i < num_fields; i++) {
      if (!parser.Good()) {
        return {Status::RedisParseErr, errNumFieldsNotMatch};
      }
      parser.RawNext();
      fields_count--;
      fields_.emplace_back(args[idx++]);
    }

    if (fields_count != 0 || idx != args.size()) {
      return {Status::RedisParseErr, errNumFieldsNotMatch};
    }
    estimated_subkey_count_ = static_cast<int64_t>(fields_.size());
    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    redis::Hash hash_db(storage, conn->GetNamespace());
    std::vector<std::pair<GetTTLStatus, uint64_t>> status_ret;
    auto s = hash_db.HExpireTime(key_, fields_, status_ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    const auto &cmd_name = GetAttributes()->name;
    auto now_ms = util::GetTimeStampMS();
    std::vector<int64_t> ret;
    for (const auto &p : status_ret) {
      if (p.first == GetTTLStatus::HGETEX_OK) {
        if (cmd_name == "hpexpiretime") {
          ret.emplace_back(p.second);
        } else if (cmd_name == "hexpiretime") {
          ret.emplace_back((p.second + 999) / 1000);
        } else if (cmd_name == "hpttl") {
          ret.emplace_back(p.second - now_ms);
        } else if (cmd_name == "httl") {
          ret.emplace_back((p.second + 999 - now_ms) / 1000);
        } else {
          return {Status::RedisExecErr, errInvalidSyntax};
        }
      } else if (p.first == GetTTLStatus::HGETEX_GET_NO_TTL) {
        ret.emplace_back(-1);
      } else {
        ret.emplace_back(-2);
      }
    }
    *output = redis::MultiInteger(ret);
    return Status::OK();
  }

 private:
  Slice key_;
  std::vector<Slice> fields_;
};

class CommandHPersist : public Commander {
 public:
  // HPERSIST key FIELDS numfields field [field ...]
  Status Parse(const std::vector<std::string> &args) override {
    if (!encode_hash_sub_flag) {
      return {Status::CmdDisabled, errCmdDisabled};
    }
    CommandParser parser(args, 1);
    uint64_t idx = 1;
    // user key
    GET_OR_RET(parser.TakeStr());
    key_ = args[idx++];
    // FIELDS numfields field [field ...]
    if (!parser.EatEqICase("FIELDS")) {
      return {Status::RedisParseErr, errMissKeyWordField};
    }
    auto s = parser.TakeInt<int64_t>();
    if (!s.IsOK()) {
      return {Status::RedisParseErr, errNeedPositiveInteger};
    }
    auto num_fields = s.GetValue();
    if (num_fields <= 0) {
      return {Status::RedisParseErr, errNeedPositiveInteger};
    }
    idx += 2;
    size_t fields_count = num_fields;
    fields_.clear();
    for (int i = 0; i < num_fields; i++) {
      if (!parser.Good()) {
        return {Status::RedisParseErr, errNumFieldsNotMatch};
      }
      parser.RawNext();
      fields_count--;
      fields_.emplace_back(args[idx++]);
    }

    if (fields_count != 0 || idx != args.size()) {
      return {Status::RedisParseErr, errNumFieldsNotMatch};
    }
    estimated_subkey_count_ = static_cast<int64_t>(fields_.size());
    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    redis::Hash hash_db(storage, conn->GetNamespace());
    std::vector<SetPersistRes> ret;
    auto s = hash_db.HPersist(key_, fields_, ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    std::vector<int64_t> reply;
    reply.reserve(ret.size());
    for (const auto &r : ret) {
      reply.emplace_back(static_cast<int64_t>(r));
    }
    *output = redis::MultiInteger(reply);
    return Status::OK();
  }

 private:
  Slice key_;
  std::vector<Slice> fields_;
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandHGet>("hget", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHIncrBy>("hincrby", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHIncrByFloat>("hincrbyfloat", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHMSet>("hset", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHSetNX>("hsetnx", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHDel>("hdel", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHStrlen>("hstrlen", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHExists>("hexists", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHLen>("hlen", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHMGet>("hmget", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHMSet>("hmset", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHKeys>("hkeys", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHVals>("hvals", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHGetAll>("hgetall", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHScan>("hscan", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHScanV2>("hscanv2", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHRangeByLex>("hrangebylex", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHRandField>("hrandfield", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHPExpireAt>("hexpire", -6, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHPExpireAt>("hpexpire", -6, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHPExpireAt>("hexpireat", -6, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHPExpireAt>("hpexpireat", -6, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHPersist>("hpersist", -5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHPExpireTime>("httl", -5, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHPExpireTime>("hpttl", -5, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHPExpireTime>("hexpiretime", -5, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHPExpireTime>("hpexpiretime", -5, "read-only", 1, 1, 1), )

}  // namespace redis
