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

#include <fmt/format.h>

#include "commander.h"
#include "common/status.h"
#include "error_constants.h"
#include "parse_util.h"
#include "server/server.h"
#include "storage/redis_metadata.h"

namespace redis {

// inline constexpr const char *kCursorPrefix = "_";

class CommandScanBase : public Commander {
 public:
  Status ParseMatchAndCountParam(const std::string &type, std::string value) {
    if (type == "match") {
      pattern_ = std::move(value);
      return Status::OK();
    } else if (type == "count") {
      auto parse_result = ParseInt<int64_t>(value, 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      if (*parse_result <= 0) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      limit_ = *parse_result;
    } else if (type == "type") {
      if (GetAttributes()->name != "nodescan") {
        return {Status::RedisParseErr, "TYPE option can only be used in NODESCAN"};
      }
      type_ = GetRedisTypeByName(value);
      if (type_ == RedisType::kRedisNone) {
        return {Status::RedisParseErr, fmt::format("Type '{}' is not a redis type", value)};
      }
    }

    return Status::OK();
  }

 protected:
  std::string cursor_;
  std::string pattern_;
  int64_t limit_ = 10;
  RedisType type_ = RedisType::kRedisNone;
};

class CommandSubkeyScanBaseV2 : public CommandScanBase {
 public:
  CommandSubkeyScanBaseV2() : CommandScanBase() {}

  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 == 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    key_ = args[1];
    cursor_ = args[2];
    if (args.size() >= 5) {
      Status s = ParseMatchAndCountParam(util::ToLower(args[3]), args_[4]);
      if (!s.IsOK()) {
        return s;
      }
    }

    if (args.size() >= 7) {
      Status s = ParseMatchAndCountParam(util::ToLower(args[5]), args_[6]);
      if (!s.IsOK()) {
        return s;
      }
    }
    return Commander::Parse(args);
  }

  std::string GenerateOutput(Server *srv, const std::vector<std::string> &keys) const {
    auto cursor_str = cursor_.empty() ? redis::NilString() : redis::BulkString(cursor_);
    return redis::Array({cursor_str, redis::MultiBulkString(keys, false)});
  }

  std::string GenerateOutput(Server *srv, const std::vector<std::string> &fields,
                             const std::vector<std::string> &values) const {
    std::vector<std::string> fvs;
    if (auto count = fields.size(); count > 0) {
      fvs.reserve(2 * count);
      for (size_t i = 0; i < count; i++) {
        fvs.emplace_back(fields[i]);
        fvs.emplace_back(values[i]);
      }
    }
    return GenerateOutput(srv, fvs);
  }

 protected:
  std::string key_;
};

class CommandSubkeyScanBaseV1 : public CommandScanBase {
 public:
  CommandSubkeyScanBaseV1() : CommandScanBase() {}

  Status Parse(const std::vector<std::string> &args) override {
    if (util::EqualICase(args[args.size() - 1], "novalues")) {
      if (GetAttributes()->name != "hscan") {
        return {Status::RedisParseErr, "NOVALUES option can only be used in HSCAN"};
      }
      if (args.size() % 2 == 1) {
        return {Status::RedisParseErr, errWrongNumOfArguments};
      }
      hscan_no_values_ = true;
    } else {
      if (args.size() % 2 == 0) {
        return {Status::RedisParseErr, errWrongNumOfArguments};
      }
      hscan_no_values_ = false;
    }

    key_ = args[1];
    auto ret = ParseInt<uint64_t>(args[2], 10);
    if (!ret) {
      return {Status::NotOK, "invalid cursor"};
    }
    redis_cursor_ = ret.GetValue();

    if (args.size() >= 5) {
      Status s = ParseMatchAndCountParam(util::ToLower(args[3]), args_[4]);
      if (!s.IsOK()) {
        return s;
      }
    }

    if (args.size() >= 7) {
      Status s = ParseMatchAndCountParam(util::ToLower(args[5]), args_[6]);
      if (!s.IsOK()) {
        return s;
      }
    }

    return Commander::Parse(args);
  }

  static std::string GenerateOutput(Server *srv, const uint64_t cursor, const std::vector<std::string> &fields) {
    std::vector<std::string> list;
    list.emplace_back(redis::BulkString(std::to_string(cursor)));
    list.emplace_back(redis::MultiBulkString(fields, false));
    return redis::Array(list);
  }

  static std::string GenerateOutput(Server *srv, const uint64_t cursor, const std::vector<std::string> &fields,
                                    const std::vector<std::string> &values) {
    std::vector<std::string> fvs;
    if (auto count = fields.size(); count > 0) {
      fvs.reserve(2 * count);
      for (size_t i = 0; i < count; i++) {
        fvs.emplace_back(fields[i]);
        fvs.emplace_back(values[i]);
      }
    }
    return GenerateOutput(srv, cursor, fvs);
  }

 protected:
  std::string key_;
  uint64_t redis_cursor_;
  bool hscan_no_values_;
};

}  // namespace redis
