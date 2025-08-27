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

#include "batch_extractor.h"

#include <glog/logging.h>

#include "cluster/redis_slot.h"
#include "common/string_util.h"
#include "parse_util.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "types/redis_bitmap.h"

std::unique_ptr<WriteBatchExtractor::CMDDataInterface> WriteBatchExtractor::CreateDataHandler(RedisType type) {
  switch (type) {
    case kRedisString:
      return std::make_unique<StringCMDDataHandler>();
    case kRedisHash:
      return std::make_unique<HashCMDDataHandler>();
    case kRedisList:
      return std::make_unique<ListCMDDataHandler>();
    case kRedisSet:
      return std::make_unique<SetCMDDataHandler>();
    case kRedisZSet:
      return std::make_unique<ZSetCMDDataHandler>();
    default:
      return nullptr;
  }
}

void WriteBatchExtractor::LogData(const rocksdb::Slice &blob) {
  // Currently, we only have two kinds of log data
  if (ServerLogData::IsServerLogData(blob.data())) {
    ServerLogData server_log;
    if (server_log.Decode(blob).IsOK()) {
      // We don't handle server log currently
    } else {
      LOG(WARNING) << "[batch_extractor] Failed to decode server log data";
    }

    // construct previous parsed cmd
    constructCmd();
    // clear previous parsed info
    clearPrevParsedInfo();
  } else {
    if (has_log_data_) {
      LOG(ERROR) << "[batch_extractor] Wrong writebatch structure, Err: no replId";
      can_parse_data_ = false;
      return;
    }
    has_log_data_ = true;

    // Redis type log data
    auto s = log_data_.Decode(blob);
    if (s.IsOK()) {
      can_parse_data_ = true;
      cmd_data_handler_ = CreateDataHandler(log_data_.GetRedisType());
      if (cmd_data_handler_ == nullptr && log_data_.GetRedisType() != kRedisNone) {
        LOG(ERROR) << "[batch_extractor] Failed to create data handler for Redis type: "
                   << EnumToString(log_data_.GetRedisType());
        can_parse_data_ = false;
        return;
      }
      if (cmd_data_handler_) {
        cmd_data_handler_->SetCmdTypeVal(log_data_.GetCmdLogDataObj().GetCmdTypeVal());
        cmd_data_handler_->SetCmdArgs(log_data_.GetCmdLogDataObj().GetCmdArgs());
      }

      s = parseAndCheckLogArgs();
      if (!s.IsOK()) {
        LOG(ERROR) << "[batch_extractor] Failed to decode command log data, Err: " << s.Msg();
        can_parse_data_ = false;
      }
    } else {
      LOG(ERROR) << "[batch_extractor] Failed to decode Redis log data, Err: " << s.Msg();
      can_parse_data_ = false;
    }
  }
}

rocksdb::Status WriteBatchExtractor::PutCF(uint32_t column_family_id, const Slice &key, const Slice &value) {
  if (column_family_id == kColumnFamilyIDZSetScore) {
    return rocksdb::Status::OK();
  }

  std::string ns, user_key;
  std::vector<std::string> command_args;

  if (column_family_id == kColumnFamilyIDMetadata) {
    std::tie(ns, user_key) = ExtractNamespaceKey<std::string>(key, is_slot_id_encoded_);

    if (!can_parse_data_) {
      LOG(ERROR) << "[batch_extractor] Log data err, skip key: " << user_key;
      return rocksdb::Status::OK();
    }

    Metadata metadata(kRedisNone);
    auto s = metadata.Decode(value);
    if (!s.ok()) return s;

    if (log_data_.GetRedisType() == kRedisString) {
      auto data_handler = dynamic_cast<StringCMDDataHandler *>(cmd_data_handler_.get());
      if (data_handler == nullptr) {
        LOG(ERROR) << "[batch_extractor] Failed to get StringCMDDataHandler";
        return rocksdb::Status::InvalidArgument("Failed to get StringCMDDataHandler");
      }
      if (metadata.expire > 0) {
        data_handler->SetExpireAt(metadata.expire);
      }
      std::string sub_key;
      return data_handler->ParsePutData(user_key, sub_key, value);
    } else if (metadata.expire > 0) {
      auto cmd_type = static_cast<RedisKeyCommand>(log_data_.GetCmdLogDataObj().GetCmdTypeVal());
      if (cmd_type == RedisKeyCommand::kCmdExpire) {
        command_args = {"PEXPIREAT", user_key, std::to_string(metadata.expire)};
        commands_.emplace_back(std::move(command_args));
      }
    }

    return rocksdb::Status::OK();
  }

  if (column_family_id == kColumnFamilyIDDefault) {
    InternalKey ikey(key, is_slot_id_encoded_);
    user_key = ikey.GetKey().ToString();

    std::string sub_key = ikey.GetSubKey().ToString();
    ns = ikey.GetNamespace().ToString();

    if (!can_parse_data_) {
      LOG(ERROR) << "[batch_extractor] Log data err, skip key: " << user_key << ", subkey: " << sub_key;
      return rocksdb::Status::OK();
    }

    switch (log_data_.GetRedisType()) {
      case kRedisHash: {
        auto data_handler = dynamic_cast<HashCMDDataHandler *>(cmd_data_handler_.get());
        if (data_handler == nullptr) {
          LOG(ERROR) << "[batch_extractor] Failed to get HashCMDDataHandler";
          return rocksdb::Status::InvalidArgument("Failed to get HashCMDDataHandler");
        }
        return data_handler->ParsePutData(user_key, sub_key, value);
      } break;
      case kRedisList: {
        auto data_handler = dynamic_cast<ListCMDDataHandler *>(cmd_data_handler_.get());
        if (data_handler == nullptr) {
          LOG(ERROR) << "[batch_extractor] Failed to get ListCMDDataHandler";
          return rocksdb::Status::InvalidArgument("Failed to get ListCMDDataHandler");
        }
        return data_handler->ParsePutData(user_key, sub_key, value);
      }
      case kRedisSet: {
        auto data_handler = dynamic_cast<SetCMDDataHandler *>(cmd_data_handler_.get());
        if (data_handler == nullptr) {
          LOG(ERROR) << "[batch_extractor] Failed to get SetCMDDataHandler";
          return rocksdb::Status::InvalidArgument("Failed to get SetCMDDataHandler");
        }
        return data_handler->ParsePutData(user_key, sub_key, value);
      }
      case kRedisZSet: {
        auto data_handler = dynamic_cast<ZSetCMDDataHandler *>(cmd_data_handler_.get());
        if (data_handler == nullptr) {
          LOG(ERROR) << "[batch_extractor] Failed to get ZSetCMDDataHandler";
          return rocksdb::Status::InvalidArgument("Failed to get ZSetCMDDataHandler");
        }
        return data_handler->ParsePutData(user_key, sub_key, value);
      }

      default: {
        // NOTE(mingfo): currently, we only support to parse 5 types(string/hash/list/set/zset).
        // Return error for other types.
        std::string type_str;
        if (log_data_.GetRedisType() >= RedisTypeNames.size()) type_str = "Unknown";
        type_str = RedisTypeNames[log_data_.GetRedisType()];
        LOG(ERROR) << "[batch_extractor] PutCf Wrong key type: " << type_str << ", key: " << user_key
                   << ", sub_key: " << sub_key;
        return rocksdb::Status::NotSupported("Unsupported type: " + type_str + " of key: " + user_key);
      }
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status WriteBatchExtractor::DeleteCF(uint32_t column_family_id, const Slice &key) {
  if (column_family_id == kColumnFamilyIDZSetScore) {
    return rocksdb::Status::OK();
  }

  std::vector<std::string> command_args;
  std::string ns;

  if (column_family_id == kColumnFamilyIDMetadata) {
    std::string user_key;
    std::tie(ns, user_key) = ExtractNamespaceKey<std::string>(key, is_slot_id_encoded_);

    if (!can_parse_data_) {
      LOG(ERROR) << "[batch_extractor] Log data err, skip key: " << user_key;
      return rocksdb::Status::OK();
    }

    if (del_tokens_.empty()) {
      del_tokens_.emplace_back("DEL");
      del_tokens_.emplace_back(std::move(user_key));
    } else {
      del_tokens_.emplace_back(std::move(user_key));
    }
  } else if (column_family_id == kColumnFamilyIDDefault) {
    InternalKey ikey(key, is_slot_id_encoded_);
    std::string user_key = ikey.GetKey().ToString();
    std::string sub_key = ikey.GetSubKey().ToString();
    ns = ikey.GetNamespace().ToString();

    if (!can_parse_data_) {
      LOG(ERROR) << "[batch_extractor] Log data err, skip key: " << user_key << ", subkey: " << sub_key;
      return rocksdb::Status::OK();
    }

    switch (log_data_.GetRedisType()) {
      case kRedisHash: {
        auto data_handler = dynamic_cast<HashCMDDataHandler *>(cmd_data_handler_.get());
        if (data_handler == nullptr) {
          LOG(ERROR) << "[batch_extractor] Failed to get HashCMDDataHandler";
          return rocksdb::Status::InvalidArgument("Failed to get HashCMDDataHandler");
        }
        return data_handler->ParseDelData(user_key, sub_key);
      }
      case kRedisSet: {
        auto data_handler = dynamic_cast<SetCMDDataHandler *>(cmd_data_handler_.get());
        if (data_handler == nullptr) {
          LOG(ERROR) << "[batch_extractor] Failed to get SetCMDDataHandler";
          return rocksdb::Status::InvalidArgument("Failed to get SetCMDDataHandler");
        }
        return data_handler->ParseDelData(user_key, sub_key);
      }
      case kRedisZSet: {
        auto data_handler = dynamic_cast<ZSetCMDDataHandler *>(cmd_data_handler_.get());
        if (data_handler == nullptr) {
          LOG(ERROR) << "[batch_extractor] Failed to get ZSetCMDDataHandler";
          return rocksdb::Status::InvalidArgument("Failed to get ZSetCMDDataHandler");
        }
        return data_handler->ParseDelData(user_key, sub_key);
      }
      case kRedisList: {
        auto data_handler = dynamic_cast<ListCMDDataHandler *>(cmd_data_handler_.get());
        if (data_handler == nullptr) {
          LOG(ERROR) << "[batch_extractor] Failed to get ListCMDDataHandler";
          return rocksdb::Status::InvalidArgument("Failed to get ListCMDDataHandler");
        }
        return data_handler->ParseDelData(user_key, sub_key);
      }
      default: {
        // Return error for other types.
        std::string type_str;
        if (log_data_.GetRedisType() >= RedisTypeNames.size()) type_str = "Unknown";
        type_str = RedisTypeNames[log_data_.GetRedisType()];
        LOG(ERROR) << "[batch_extractor] DeleteCf Wrong key type: " << type_str << ", key: " << user_key
                   << ", sub_key: " << sub_key;
        return rocksdb::Status::NotSupported("Unsupported type: " + type_str + " of key: " + user_key);
      }
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status WriteBatchExtractor::DeleteRangeCF(uint32_t column_family_id, const Slice &begin_key,
                                                   const Slice &end_key) {
  if (log_data_.GetRedisType() == kRedisHash) {
    auto data_handler = dynamic_cast<HashCMDDataHandler *>(cmd_data_handler_.get());
    if (data_handler == nullptr) {
      LOG(ERROR) << "[batch_extractor] Failed to get HashCMDDataHandler";
      return rocksdb::Status::InvalidArgument("Failed to get HashCMDDataHandler");
    }
    data_handler->ParseDelRangeData(begin_key, end_key);
  }

  return rocksdb::Status::OK();
}

Status WriteBatchExtractor::parseAndCheckLogArgs() {
  switch (log_data_.GetRedisType()) {
    case kRedisString: {
      auto data_handler = dynamic_cast<StringCMDDataHandler *>(cmd_data_handler_.get());
      if (data_handler == nullptr) {
        return {Status::NotOK, "Failed to get StringCMDDataHandler"};
      }
      return data_handler->ParseAndCheckLogArgs();
    } break;
    case kRedisHash: {
      auto data_handler = dynamic_cast<HashCMDDataHandler *>(cmd_data_handler_.get());
      if (data_handler == nullptr) {
        return {Status::NotOK, "Failed to get HashCMDDataHandler"};
      }
      return data_handler->ParseAndCheckLogArgs();
    } break;
    case kRedisList: {
      auto data_handler = dynamic_cast<ListCMDDataHandler *>(cmd_data_handler_.get());
      if (data_handler == nullptr) {
        return {Status::NotOK, "Failed to get ListCMDDataHandler"};
      }
      return data_handler->ParseAndCheckLogArgs();
    } break;
    case kRedisSet: {
      auto data_handler = dynamic_cast<SetCMDDataHandler *>(cmd_data_handler_.get());
      if (data_handler == nullptr) {
        return {Status::NotOK, "Failed to get SetCMDDataHandler"};
      }
      return data_handler->ParseAndCheckLogArgs();
    } break;
    case kRedisZSet: {
      auto data_handler = dynamic_cast<ZSetCMDDataHandler *>(cmd_data_handler_.get());
      if (data_handler == nullptr) {
        return {Status::NotOK, "Failed to get ZSetCMDDataHandler"};
      }
      return data_handler->ParseAndCheckLogArgs();
    } break;
    case kRedisNone:
      return Status::OK();
    default:
      return {Status::NotOK, fmt::format("Unknown redis type: {}", std::to_string(log_data_.GetRedisType()))};
  }
  return Status::OK();
}

void WriteBatchExtractor::constructCmd() {
  if (!can_parse_data_) {
    LOG(WARNING) << "[batch_extractor] Can't construct command when failed to parse log data";
    return;
  }

  switch (log_data_.GetRedisType()) {
    case RedisType::kRedisString: {
      auto data_handler = dynamic_cast<StringCMDDataHandler *>(cmd_data_handler_.get());
      data_handler->ConstructCmd();
    } break;
    case RedisType::kRedisList: {
      auto data_handler = dynamic_cast<ListCMDDataHandler *>(cmd_data_handler_.get());
      data_handler->ConstructCmd();
    } break;
    case RedisType::kRedisHash: {
      auto data_handler = dynamic_cast<HashCMDDataHandler *>(cmd_data_handler_.get());
      data_handler->ConstructCmd();
    } break;
    case RedisType::kRedisSet: {
      auto data_handler = dynamic_cast<SetCMDDataHandler *>(cmd_data_handler_.get());
      data_handler->ConstructCmd();
    } break;
    case RedisType::kRedisZSet: {
      auto data_handler = dynamic_cast<ZSetCMDDataHandler *>(cmd_data_handler_.get());
      data_handler->ConstructCmd();
    } break;
    case RedisType::kRedisNone:
      break;
    default:
      LOG(ERROR) << "[batch_extractor] Can't construct command for unknown redis type: " << log_data_.GetRedisType();
      return;
  }

  if (cmd_data_handler_) {
    for (auto &cmd : cmd_data_handler_->GetCommands()) {
      commands_.emplace_back(std::move(cmd));
    }
  } else {
    if (!del_tokens_.empty()) commands_.emplace_back(std::move(del_tokens_));
  }
}

void WriteBatchExtractor::clearPrevParsedInfo() {
  // clear log data
  log_data_.Clear();
  can_parse_data_ = true;
  has_log_data_ = false;

  // clear data handlers
  cmd_data_handler_.reset(nullptr);
  del_tokens_.clear();
}

Status HashCMDDataHandler::ParseAndCheckLogArgs() {
  auto cmd = static_cast<RedisHashCommand>(cmd_type_val_);
  // check args in LogData
  switch (cmd) {
    case RedisHashCommand::kCmdHExpire:
    case RedisHashCommand::kCmdHExpireAt:
    case RedisHashCommand::kCmdHPExpire:
    case RedisHashCommand::kCmdHPExpireAt:
      if (args_.size() != 1) {
        return {Status::NotOK,
                fmt::format("Hash cmd: {}, log args size: {}, expected: 1", EnumToString(cmd), args_.size())};
      }
      break;
    case RedisHashCommand::kCmdHIncrby:
    case RedisHashCommand::kCmdHIncrbyFloat:
    case RedisHashCommand::kCmdHSet:
    case RedisHashCommand::kCmdHSetNX:
    case RedisHashCommand::kCmdHMSet:
    case RedisHashCommand::kCmdHDel: {
      if (args_.size() != 1) {
        return {Status::NotOK,
                fmt::format("Hash cmd: {}, log args size: {}, expected: 1", EnumToString(cmd), args_.size())};
      }
      if (args_[0] == "0")
        hash_codec_ = RedisHashCodec::kNoFieldTTL;
      else if (args_[0] == "1")
        hash_codec_ = RedisHashCodec::kHasFieldTTL;
      else
        return {Status::NotOK, fmt::format("Wrong hash codec type: {}", args_[0])};
    } break;
    case RedisHashCommand::kCmdHPersist:
      if (args_.size() != 0) {
        return {Status::NotOK,
                fmt::format("Hash cmd: {}, log args size: {}, expected: 0", EnumToString(cmd), args_.size())};
      }
      break;

    // kkv cmds
    case RedisHashCommand::kCmdKKVHSet:
    case RedisHashCommand::kCmdKKVHCAD:
      if (args_.size() != 0) {
        return {Status::NotOK,
                fmt::format("Hash cmd: {}, log args size: {}, expected: 0", EnumToString(cmd), args_.size())};
      }
      break;
    case RedisHashCommand::kCmdKKVHSetNX:
    case RedisHashCommand::kCmdKKVHCAS:
      if (args_.size() != 1) {
        return {Status::NotOK,
                fmt::format("Hash cmd: {}, log args size: {}, expected: 1", EnumToString(cmd), args_.size())};
      }
      break;
    case RedisHashCommand::kCmdKKVHRemRangeByLex:
      if (args_.size() != 2) {
        return {Status::NotOK,
                fmt::format("Hash cmd: {}, log args size: {}, expected: 2", EnumToString(cmd), args_.size())};
      }
      break;

    default:
      return {Status::NotOK, fmt::format("Unknown hash cmd: {}", EnumToString(cmd))};
      break;
  }

  return Status::OK();
}

rocksdb::Status HashCMDDataHandler::ParsePutData(std::string &key, std::string &sub_key, const Slice &value) {
  key_ = std::move(key);

  auto cmd = static_cast<RedisHashCommand>(cmd_type_val_);
  // RedisHashCodec::kHasFieldTTL
  switch (cmd) {
    case RedisHashCommand::kCmdHExpire:
    case RedisHashCommand::kCmdHExpireAt:
    case RedisHashCommand::kCmdHPExpire:
    case RedisHashCommand::kCmdHPExpireAt:
    case RedisHashCommand::kCmdHPersist:
      // value is unnecessary for HExpirexx cmds
      fv_pairs_.emplace_back(std::move(sub_key), "");
      break;
    case RedisHashCommand::kCmdHIncrby:
    case RedisHashCommand::kCmdHIncrbyFloat:
    case RedisHashCommand::kCmdHSet:
    case RedisHashCommand::kCmdHSetNX:
    case RedisHashCommand::kCmdHMSet: {
      // NOTE(mingfo): We have to check the coding mode while parsing cmds. Because these cmds may code data
      // in different mode depends on the configuration 'enable-hfe-cmd'.
      if (hash_codec_ == RedisHashCodec::kNoFieldTTL) {
        fv_pairs_.emplace_back(std::move(sub_key), value.ToString());
        return rocksdb::Status::OK();
      }

      HashSubData sub_data;
      auto s = sub_data.Decode(true, rocksdb::Slice(value.data(), value.size()));
      if (s.ok()) {
        fv_pairs_.emplace_back(std::move(sub_key), sub_data.value.ToString());
      } else if (s.IsNotFound()) {
        // NOTE(mingfo): Skip the expired field. These cmds will be parsed into HSET. If the field is expired,
        // HSET will write a new one, which will cause data inconsistency.
        return rocksdb::Status::OK();
      } else {
        LOG(ERROR) << "[batch_extractor] Failed to parse hash data for cmd: " << EnumToString(cmd)
                   << ", err: " << s.ToString();
        return s;
      }
    } break;

    // kkv cmds
    case RedisHashCommand::kCmdKKVHSet:
    case RedisHashCommand::kCmdKKVHSetNX:
    case RedisHashCommand::kCmdKKVHCAS: {
      // parse field,val
      HashSubData sub_data;
      auto s = sub_data.Decode(true, rocksdb::Slice(value.data(), value.size()), 0, true);
      if (!s.ok() && !s.IsNotFound()) {
        LOG(ERROR) << "[batch_extractor] Failed to parse hash data for cmd: " << EnumToString(cmd)
                   << ", err: " << s.ToString();
        return s;
      }
      // NOTE(mingfo): Can't skip the expired fields for data consistency.
      // These cmds will be parsed into KKVHSET which will delete expired fields directly.
      // If these fields haven't set ttl in dst node, they should be set ttl by cmd KKVHSET.
      fv_pairs_.emplace_back(std::move(sub_key), sub_data.value.ToString());
      // get expireat for kCmdKKVHSet
      // NOTE(mingfo): Only for cmd KKVHSET, because other cmds have written expireat in log data.
      if (cmd == RedisHashCommand::kCmdKKVHSet) {
        fields_exat_.emplace_back(sub_data.expire);
      }
    } break;

    default:
      return rocksdb::Status::InvalidArgument(fmt::format("Wrong hash cmd: {}", EnumToString(cmd)));
      break;
  }

  return rocksdb::Status::OK();
}

rocksdb::Status HashCMDDataHandler::ParseDelData(std::string &key, std::string &sub_key) {
  key_ = std::move(key);
  del_fields_.emplace_back(std::move(sub_key));
  return rocksdb::Status::OK();
}

rocksdb::Status HashCMDDataHandler::ParseDelRangeData(const Slice &begin_key, const Slice &end_key) {
  if (static_cast<RedisHashCommand>(cmd_type_val_) == RedisHashCommand::kCmdKKVHRemRangeByLex) {
    // get hash key
    InternalKey ikey(begin_key, true);
    key_ = ikey.GetKey().ToString();
    // reuse 'del_fields_' to record the range [min,max)
    del_fields_.emplace_back(std::move(args_[0]));
    del_fields_.emplace_back(std::move(args_[1]));
  }
  return rocksdb::Status::OK();
}

void HashCMDDataHandler::ConstructCmd() {
  if (fv_pairs_.empty() && del_fields_.empty()) return;

  std::vector<std::string> cmd_tokens;
  auto cmd = static_cast<RedisHashCommand>(cmd_type_val_);
  switch (cmd) {
    case RedisHashCommand::kCmdHExpire:
    case RedisHashCommand::kCmdHExpireAt:
    case RedisHashCommand::kCmdHPExpire:
    case RedisHashCommand::kCmdHPExpireAt: {
      // HPEXPIREAT key exat-ms [NX|XX|GT|LT] FIELDS $numfields $field ...
      // NOTE(mingfo): During the execution of the hexpirexxx commands, if the 'expireat' set by the command expires,
      // the fields will be deleted directly. During parsing, these fields should also be included in the final
      // 'HPEXPIREAT ...' command to avoid skipping these fields, causing the fields in the destination node
      // keep existing if they nerver set ttl.
      cmd_tokens.reserve(5 + fv_pairs_.size() + del_fields_.size());
      cmd_tokens = {"HPEXPIREAT", std::move(key_), args_[0], "FIELDS",
                    std::to_string(fv_pairs_.size() + del_fields_.size())};
      for (auto &[field, _] : fv_pairs_) {
        cmd_tokens.emplace_back(std::move(field));
      }
      for (auto &field : del_fields_) {
        cmd_tokens.emplace_back(std::move(field));
      }
    } break;
    case RedisHashCommand::kCmdHPersist: {
      // HPERSIST key FIELDS $numfields $field ...
      cmd_tokens.reserve(4 + fv_pairs_.size());
      cmd_tokens = {"HPERSIST", std::move(key_), "FIELDS", std::to_string(fv_pairs_.size())};
      for (auto &[field, _] : fv_pairs_) {
        cmd_tokens.emplace_back(std::move(field));
      }
    } break;
    case RedisHashCommand::kCmdHIncrby:
    case RedisHashCommand::kCmdHIncrbyFloat:
    case RedisHashCommand::kCmdHSet:
    case RedisHashCommand::kCmdHSetNX:
    case RedisHashCommand::kCmdHMSet: {
      cmd_tokens.reserve(2 + fv_pairs_.size() * 2);
      cmd_tokens = {"HSET", std::move(key_)};
      for (auto &[field, val] : fv_pairs_) {
        cmd_tokens.emplace_back(std::move(field));
        cmd_tokens.emplace_back(std::move(val));
      }
    } break;
    case RedisHashCommand::kCmdHDel: {
      cmd_tokens.reserve(2 + del_fields_.size());
      cmd_tokens = {"HDEL", std::move(key_)};
      for (auto &field : del_fields_) {
        cmd_tokens.emplace_back(std::move(field));
      }
    } break;

    // kkv cmds
    case RedisHashCommand::kCmdKKVHSet: {
      // NOTE(mingfo): KKVHSET may be parsed into KKVHSET and HDEL.
      // cmd syntax: KKVHSET key field value EX | PX | EXAT | PXAT | PERSIST second [field value EX ...]
      //   - Every field has independent ttl
      // Exec cmd: If the expiration time set for the field by the command has elapsed,
      //  the field will be deleted directly; otherwise, it will be updated.
      // Parse cmd:
      //   - If the field operation is PUT, it needs to be parsed as a KKVHSET command,
      //      even if the field has expired during parsing.
      //   - If the field operation is DELETE, it needs to be parsed as a HDEL command.
      auto fv_cnt = fv_pairs_.size();
      auto exat_cnt = fields_exat_.size();
      CHECK(fv_cnt == exat_cnt) << "KKVHSET fields_cnt: " << fv_cnt << " != expireat_cnt: " << exat_cnt;
      // construct KKVHSET
      if (!fv_pairs_.empty()) {
        cmd_tokens = {"KKVHSET", key_};
        for (size_t i = 0; i < fv_cnt; i++) {
          cmd_tokens.emplace_back(std::move(fv_pairs_[i].first));
          cmd_tokens.emplace_back(std::move(fv_pairs_[i].second));
          if (fields_exat_[i] == 0)
            cmd_tokens.emplace_back("PERSIST");
          else {
            cmd_tokens.emplace_back("PXAT");
            cmd_tokens.emplace_back(std::to_string(fields_exat_[i]));
          }
        }
        commands_.emplace_back(std::move(cmd_tokens));
        cmd_tokens.clear();
      }
      // construct HDEL
      if (!del_fields_.empty()) {
        cmd_tokens = {"HDEL", key_};
        for (auto &field : del_fields_) {
          cmd_tokens.emplace_back(std::move(field));
        }
        commands_.emplace_back(std::move(cmd_tokens));
        cmd_tokens.clear();
      }
    } break;
    case RedisHashCommand::kCmdKKVHSetNX: {
      auto fv = fv_pairs_.front();
      cmd_tokens = {"KKVHSETNX", std::move(key_), std::move(fv.first), std::move(fv.second)};
      if (args_[0] == "0") {
        cmd_tokens.emplace_back("PERSIST");
      } else {
        cmd_tokens.emplace_back("PXAT");
        cmd_tokens.emplace_back(std::move(args_[0]));
      }
    } break;
    case RedisHashCommand::kCmdKKVHCAS: {
      // NOTE(mingfo): KKVHCAS may be parsed into KKVHSET or HDEL.
      // cmd syntax: KKVHCAS key field old_value new_value [EX | PX | EXAT | PXAT | PERSIST time]
      // Exec cmd: If the expiration time set for the field by the command has elapsed,
      //  the field will be deleted directly; otherwise, it will be updated.
      // Parse cmd:
      //   - If the field operation is PUT, it needs to be parsed as a KKVHSET command,
      //      even if the field has expired during parsing.
      //   - If the field operation is DELETE, it needs to be parsed as a HDEL command.
      if (!fv_pairs_.empty()) {
        auto fv = fv_pairs_.front();
        cmd_tokens = {"KKVHSET", std::move(key_), std::move(fv.first), std::move(fv.second)};
        if (args_[0] == "0") {
          cmd_tokens.emplace_back("PERSIST");
        } else {
          cmd_tokens.emplace_back("PXAT");
          cmd_tokens.emplace_back(std::move(args_[0]));
        }
      } else if (!del_fields_.empty()) {
        cmd_tokens = {"HDEL", std::move(key_), std::move(del_fields_[0])};
      }
    } break;
    case RedisHashCommand::kCmdKKVHCAD:
      cmd_tokens = {"HDEL", std::move(key_), std::move(del_fields_[0])};
      break;
    case RedisHashCommand::kCmdKKVHRemRangeByLex:
      cmd_tokens = {"KKVHREMRANGEBYLEX", std::move(key_), std::move(del_fields_[0]), std::move(del_fields_[1])};
      break;

    default:
      LOG(ERROR) << "[batch_extractor] Unknown hash command: " << EnumToString(cmd);
      return;
  }

  // record cmd
  if (!cmd_tokens.empty()) commands_.emplace_back(std::move(cmd_tokens));
}

void HashCMDDataHandler::Clear() {
  key_.clear();
  fv_pairs_.clear();
  fields_exat_.clear();
  del_fields_.clear();
  hash_codec_ = RedisHashCodec::kNoFieldTTL;
}

rocksdb::Status StringCMDDataHandler::ParsePutData(std::string &key, std::string &sub_key, const Slice &value) {
  auto cmd_type = static_cast<RedisStringCommand>(cmd_type_val_);
  switch (cmd_type) {
    case RedisStringCommand::kCmdSet:
    case RedisStringCommand::kCmdSetNX:
    case RedisStringCommand::kCmdIncr:
    case RedisStringCommand::kCmdIncrBy:
    case RedisStringCommand::kCmdIncrByFloat:
    case RedisStringCommand::kCmdDecr:
    case RedisStringCommand::kCmdDecrBy: {
      string_tokens_.emplace_back("SET");
      string_tokens_.emplace_back(std::move(key));
      string_tokens_.emplace_back(value.ToString().substr(Metadata::GetOffsetAfterExpire(value[0])));
    } break;
    case RedisStringCommand::kCmdSetEX: {
      string_tokens_.emplace_back("SET");
      string_tokens_.emplace_back(std::move(key));
      string_tokens_.emplace_back(value.ToString().substr(Metadata::GetOffsetAfterExpire(value[0])));
      string_tokens_.emplace_back("PXAT");
      string_tokens_.emplace_back(std::to_string(expire_at_));
    } break;
    case RedisStringCommand::kCmdMSet: {
      if (string_tokens_.empty()) {
        string_tokens_.emplace_back("MSET");
      }
      string_tokens_.emplace_back(std::move(key));
      string_tokens_.emplace_back(value.ToString().substr(Metadata::GetOffsetAfterExpire(value[0])));
    } break;
    default:
      LOG(ERROR) << "[batch_extractor] Unsupported string cmd: " << EnumToString(cmd_type);
      return rocksdb::Status::InvalidArgument(fmt::format("Unsupported string cmd: {}", EnumToString(cmd_type)));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ListCMDDataHandler::ParsePutData(std::string &key, std::string &sub_key, const Slice &value) {
  auto cmd = static_cast<RedisListCommand>(cmd_type_val_);
  switch (cmd) {
    case RedisListCommand::kCmdLSet: {
      if (args_.size() < 1) {
        LOG(ERROR) << "[batch_extractor] Failed to parse write_batch in PutCF. Command=LSET: no enough arguments, at "
                      "least should "
                      "contain an index";
        return rocksdb::Status::OK();
      }
      list_tokens_.emplace_back("LSET");
      list_tokens_.emplace_back(std::move(key));
      list_tokens_.emplace_back(args_[0]);
      list_tokens_.emplace_back(std::move(value.ToString()));
    } break;
    case RedisListCommand::kCmdLInsert:
      if (first_seen_) {
        if (args_.size() < 3) {
          LOG(ERROR)
              << "[batch_extractor] Failed to parse write_batch in PutCF. Command=LINSERT: no enough arguments, should "
                 "contain before pivot value";
          return rocksdb::Status::OK();
        }

        list_tokens_.emplace_back("LINSERT");
        list_tokens_.emplace_back(std::move(key));
        list_tokens_.emplace_back(args_[0] == "1" ? "BEFORE" : "AFTER");
        list_tokens_.emplace_back(args_[1]);
        list_tokens_.emplace_back(args_[2]);

        first_seen_ = false;
      }
      break;
    case RedisListCommand::kCmdLPush: {
      if (list_tokens_.empty()) {
        list_tokens_.emplace_back("LPUSH");
        list_tokens_.emplace_back(std::move(key));
      }
      list_tokens_.emplace_back(std::move(value.ToString()));
    } break;
    case RedisListCommand::kCmdRPush: {
      if (list_tokens_.empty()) {
        list_tokens_.emplace_back("RPUSH");
        list_tokens_.emplace_back(std::move(key));
      }
      list_tokens_.emplace_back(std::move(value.ToString()));
    } break;
    case RedisListCommand::kCmdLRem:
      // LREM will be parsed in DeleteCF, so ignore it here
      break;
    default:
      LOG(ERROR) << "[batch_extractor] Failed to parse write_batch in PutCF. Type=List: unhandled command with code "
                 << EnumToString(cmd);
      return rocksdb::Status::InvalidArgument(fmt::format("Wrong list cmd: {}", EnumToString(cmd)));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ListCMDDataHandler::ParseDelData(std::string &key, std::string &sub_key) {
  auto cmd = static_cast<RedisListCommand>(cmd_type_val_);
  switch (cmd) {
    case RedisListCommand::kCmdLTrim: {
      if (first_seen_) {
        if (args_.size() < 2) {
          LOG(ERROR) << "[batch_extractor] Failed to parse write_batch in DeleteCF. Command=LTRIM: no enough "
                        "arguments, should "
                        "contain start and end";
          return rocksdb::Status::OK();
        }
        list_tokens_.emplace_back("LTRIM");
        list_tokens_.emplace_back(std::move(key));
        list_tokens_.emplace_back(args_[0]);
        list_tokens_.emplace_back(args_[1]);

        first_seen_ = false;
      }
    } break;
    case RedisListCommand::kCmdLRem: {
      if (first_seen_) {
        if (args_.size() < 2) {
          LOG(ERROR)
              << "[batch_extractor] Failed to parse write_batch in DeleteCF. Command=LREM: no enough arguments, should "
                 "contain count and value";
          return rocksdb::Status::OK();
        }
        list_tokens_.emplace_back("LREM");
        list_tokens_.emplace_back(std::move(key));
        list_tokens_.emplace_back(args_[0]);
        list_tokens_.emplace_back(args_[1]);

        first_seen_ = false;
      }
    } break;
    case RedisListCommand::kCmdLPop: {
      if (list_tokens_.empty()) {
        list_tokens_.emplace_back("LPOP");
        list_tokens_.emplace_back(std::move(key));
      }
      list_count_++;
    } break;
    case RedisListCommand::kCmdRPop: {
      if (list_tokens_.empty()) {
        list_tokens_.emplace_back("RPOP");
        list_tokens_.emplace_back(std::move(key));
      }
      list_count_++;
    } break;
    default:
      LOG(ERROR) << "[batch_extractor] Failed to parse write_batch in DeleteCF. Type=List: unhandled command with code "
                 << EnumToString(cmd);
      return rocksdb::Status::InvalidArgument(fmt::format("Wrong list cmd: {}", EnumToString(cmd)));
  }
  return rocksdb::Status::OK();
}

void ListCMDDataHandler::ConstructCmd() {
  auto cmd = static_cast<RedisListCommand>(cmd_type_val_);
  if (cmd == RedisListCommand::kCmdLPop || cmd == RedisListCommand::kCmdRPop) {
    if (list_count_ > 1) list_tokens_.emplace_back(std::to_string(list_count_));
  }
  commands_.emplace_back(std::move(list_tokens_));
}

void ListCMDDataHandler::Clear() {
  list_tokens_.clear();
  first_seen_ = true;
  list_count_ = 0;
  args_.clear();
  cmd_type_val_ = 0;
  commands_.clear();
}

rocksdb::Status SetCMDDataHandler::ParsePutData(std::string &key, std::string &sub_key, const Slice &value) {
  if (set_tokens_.empty()) {
    set_tokens_.emplace_back("SADD");
    set_tokens_.emplace_back(std::move(key));
  }
  set_tokens_.emplace_back(std::move(sub_key));

  return rocksdb::Status::OK();
}

rocksdb::Status SetCMDDataHandler::ParseDelData(std::string &key, std::string &sub_key) {
  if (set_tokens_.empty()) {
    set_tokens_.emplace_back("SREM");
    set_tokens_.emplace_back(std::move(key));
  }
  set_tokens_.emplace_back(std::move(sub_key));
  return rocksdb::Status::OK();
}

rocksdb::Status ZSetCMDDataHandler::ParsePutData(std::string &key, std::string &sub_key, const Slice &value) {
  if (zset_tokens_.empty()) {
    zset_tokens_.emplace_back("ZADD");
    zset_tokens_.emplace_back(std::move(key));
  }
  double score = DecodeDouble(value.data());
  zset_tokens_.emplace_back(util::Float2String(score));
  zset_tokens_.emplace_back(std::move(sub_key));

  return rocksdb::Status::OK();
}

rocksdb::Status ZSetCMDDataHandler::ParseDelData(std::string &key, std::string &sub_key) {
  if (zset_tokens_.empty()) {
    zset_tokens_.emplace_back("ZREM");
    zset_tokens_.emplace_back(std::move(key));
  }
  zset_tokens_.emplace_back(std::move(sub_key));
  return rocksdb::Status::OK();
}
