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

#include <event2/bufferevent.h>
#include <event2/event.h>
#include <glog/logging.h>
#include <rocksdb/types.h>
#include <rocksdb/utilities/backup_engine.h>

#include <cstdint>
#include <deque>
#include <initializer_list>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cluster/cluster_defs.h"
#include "parse_util.h"
#include "server/redis_reply.h"
#include "status.h"
#include "storage/storage.h"
#include "string_util.h"

class Server;

extern std::atomic<bool> enable_kkv_cmd_flag;
namespace redis {

class Connection;
struct CommandAttributes;

enum CommandFlags : uint64_t {
  kCmdWrite = 1ULL << 0,        // "write" flag
  kCmdReadOnly = 1ULL << 1,     // "read-only" flag
  kCmdReplication = 1ULL << 2,  // "replication" flag
  kCmdPubSub = 1ULL << 3,       // "pub-sub" flag
  kCmdScript = 1ULL << 4,       // "script" flag for command SCRIPT
  kCmdLoading = 1ULL << 5,      // "ok-loading" flag
  kCmdMulti = 1ULL << 6,        // "multi" flag
  kCmdExclusive = 1ULL << 7,    // "exclusive" flag
  kCmdNoMulti = 1ULL << 8,      // "no-multi" flag
  kCmdNoScript = 1ULL << 9,     // "no-script" flag
  kCmdROScript = 1ULL << 10,    // "ro-script" flag for read-only script commands
  kCmdCluster = 1ULL << 11,     // "cluster" flag
};

class Commander {
 public:
  void SetAttributes(const CommandAttributes *attributes) { attributes_ = attributes; }
  const CommandAttributes *GetAttributes() const { return attributes_; }
  void SetArgs(const std::vector<std::string> &args) {
    // TODO: why need to copy args?
    args_ = args;
    for (const auto &arg : args) {
      estimated_subkey_size_bytes_ += arg.size();
    }
  }
  virtual Status Parse() { return Parse(args_); }
  virtual Status Parse(const std::vector<std::string> &args) { return Status::OK(); }
  virtual Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) {
    return {Status::RedisExecErr, "not implemented"};
  }

  virtual ~Commander() = default;

  int64_t GetEstimatedSubkeyNum() const { return estimated_subkey_count_; }
  uint64_t GetEstimatedSubkeySize() const { return estimated_subkey_size_bytes_; }

 protected:
  std::vector<std::string> args_;
  // 1. estimated_subkey_count_ records the number of subkey operations per command to reflect its time complexity.
  // Commands that operate on a single key are excluded.
  // 2. For point-read commands (e.g. HMGET), count the number of subkeys passed; if metadata is absent, record as zero.
  // 3. For write commands (e.g. HMSET, HSET, SADD, ZADD), count all passed subkeys or metakeys without deduplication;
  // writes that don’t change data are still counted.
  // 4. For scan- or range-trim commands (e.g. SCAN, ZREMRANGEBYRANK), count the number of RocksDB iterator steps
  // performed.
  // 5. For delete commands (e.g. DEL, HDEL), count the number of subkeys passed; deletions that don’t actually remove
  // data are still counted.
  // 6. For pop commands (e.g. ZPOP, LPOP), count the number of subkeys returned.
  // 7. For commands combining read, write, and scan actions, count the RocksDB iterator steps performed during the
  // operation.
  int64_t estimated_subkey_count_ = -1;
  uint64_t estimated_subkey_size_bytes_ = 0;

  const CommandAttributes *attributes_ = nullptr;
};

class CommanderWithParseMove : Commander {
 public:
  Status Parse() override { return ParseMove(std::move(args_)); }
  virtual Status ParseMove(std::vector<std::string> &&args) { return Status::OK(); }
};

using CommanderFactory = std::function<std::unique_ptr<Commander>()>;

struct CommandKeyRange {
  // index of the first key in command tokens
  // 0 stands for no key, since the first index of command arguments is command name
  int first_key;

  // index of the last key in command tokens
  // in normal one-key commands, first key and last key index are both 1
  // -n stands for the n-th last index of the sequence, i.e. args.size() - n
  int last_key;

  // step length of key position
  // e.g. key step 2 means "key other key other ..." sequence
  int key_step;

  CommandKeyRange &operator=(const CommandKeyRange &range) {
    this->first_key = range.first_key;
    this->last_key = range.last_key;
    this->key_step = range.key_step;
    return *this;
  }
};

using CommandKeyRangeGen = std::function<CommandKeyRange(const std::vector<std::string> &)>;

using CommandKeyRangeVecGen = std::function<std::vector<CommandKeyRange>(const std::vector<std::string> &)>;

using AdditionalFlagGen = std::function<uint64_t(const std::vector<std::string> &)>;

struct CommandAttributes {
  // command name
  std::string name;

  // number of command arguments
  // positive number n means number of arguments is equal to n
  // negative number -n means number of arguments is equal to or large than n
  int arity;

  // space-separated flag strings to initialize flags
  std::string description;

  // bitmap of enum CommandFlags
  uint64_t flags;

  // additional flags regarding to dynamic command arguments
  AdditionalFlagGen flag_gen;

  // static determined key range
  CommandKeyRange key_range;

  // if key_range.first_key == -1, key_range_gen is used instead
  CommandKeyRangeGen key_range_gen = nullptr;

  // if key_range.first_key == -2, key_range_vec_gen is used instead
  CommandKeyRangeVecGen key_range_vec_gen;

  // commander object generator
  CommanderFactory factory;

  auto GenerateFlags(const std::vector<std::string> &args) const {
    uint64_t res = flags;
    if (flag_gen) res |= flag_gen(args);
    return res;
  }

  const CommandKeyRange GetKeyRange(const std::vector<std::string> &args) const {
    if (key_range_gen != nullptr) return key_range_gen(args);
    return key_range;
  }
};

using CommandMap = std::map<std::string, const CommandAttributes *>;

inline uint64_t ParseCommandFlags(const std::string &description, const std::string &cmd_name) {
  uint64_t flags = 0;

  for (const auto &flag : util::Split(description, " ")) {
    if (flag == "write")
      flags |= kCmdWrite;
    else if (flag == "read-only")
      flags |= kCmdReadOnly;
    else if (flag == "replication")
      flags |= kCmdReplication;
    else if (flag == "pub-sub")
      flags |= kCmdPubSub;
    else if (flag == "ok-loading")
      flags |= kCmdLoading;
    else if (flag == "exclusive")
      flags |= kCmdExclusive;
    else if (flag == "multi")
      flags |= kCmdMulti;
    else if (flag == "no-multi")
      flags |= kCmdNoMulti;
    else if (flag == "no-script")
      flags |= kCmdNoScript;
    else if (flag == "ro-script")
      flags |= kCmdROScript;
    else if (flag == "script")
      flags |= kCmdScript;
    else if (flag == "cluster")
      flags |= kCmdCluster;
    else {
      std::cout << fmt::format("Encountered non-existent flag '{}' in command {} in command attribute parsing", flag,
                               cmd_name)
                << std::endl;
      std::abort();
    }
  }

  return flags;
}

template <typename T>
auto MakeCmdAttr(const std::string &name, int arity, const std::string &description, int first_key, int last_key,
                 int key_step, const AdditionalFlagGen &flag_gen = {}) {
  CommandAttributes attr{name,
                         arity,
                         description,
                         ParseCommandFlags(description, name),
                         flag_gen,
                         {first_key, last_key, key_step},
                         {},
                         {},
                         []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new T()); }};

  if ((first_key > 0 && key_step <= 0) || (first_key > 0 && last_key >= 0 && last_key < first_key)) {
    std::cout << fmt::format("Encountered invalid key range in command {}", name) << std::endl;
    std::abort();
  }

  return attr;
}

template <typename T>
auto MakeCmdAttr(const std::string &name, int arity, const std::string &description, const CommandKeyRangeGen &gen,
                 const AdditionalFlagGen &flag_gen = {}) {
  CommandAttributes attr{name,
                         arity,
                         description,
                         ParseCommandFlags(description, name),
                         flag_gen,
                         {-1, 0, 0},
                         gen,
                         {},
                         []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new T()); }};

  return attr;
}

template <typename T>
auto MakeCmdAttr(const std::string &name, int arity, const std::string &description,
                 const CommandKeyRangeVecGen &vec_gen, const AdditionalFlagGen &flag_gen = {}) {
  CommandAttributes attr{name,
                         arity,
                         description,
                         ParseCommandFlags(description, name),
                         flag_gen,
                         {-2, 0, 0},
                         {},
                         vec_gen,
                         []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new T()); }};

  return attr;
}

struct RegisterToCommandTable {
  RegisterToCommandTable(std::initializer_list<CommandAttributes> list);
};

struct CommandTable {
 public:
  CommandTable() = delete;

  static CommandMap *Get();
  static const CommandMap *GetOriginal();
  static void Reset();

  static void GetAllCommandsInfo(std::string *info);
  static void GetCommandsInfo(std::string *info, const std::vector<std::string> &cmd_names);
  static std::string GetCommandInfo(const CommandAttributes *command_attributes);
  static Status GetKeysFromCommand(const CommandAttributes *attributes, const std::vector<std::string> &cmd_tokens,
                                   std::vector<int> *keys_indexes);

  static size_t Size();
  static bool IsExists(const std::string &name);

 private:
  static inline std::deque<CommandAttributes> redis_command_table;

  // Original Command table before rename-command directive
  static inline CommandMap original_commands;

  // Command table after rename-command directive
  static inline CommandMap commands;

  friend struct RegisterToCommandTable;
};

#define KVROCKS_CONCAT(a, b) a##b                   // NOLINT
#define KVROCKS_CONCAT2(a, b) KVROCKS_CONCAT(a, b)  // NOLINT

// NOLINTNEXTLINE
#define REDIS_REGISTER_COMMANDS(...) \
  static RegisterToCommandTable KVROCKS_CONCAT2(register_to_command_table_, __LINE__){__VA_ARGS__};

}  // namespace redis
