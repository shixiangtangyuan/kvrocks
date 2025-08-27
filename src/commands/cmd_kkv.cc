#include "commands/command_parser.h"
#include "commands/commander.h"
#include "commands/error_constants.h"
#include "common/status.h"
#include "server/redis_connection.h"
#include "server/server.h"
#include "types/redis_kkv.h"

namespace redis {

class CommandKKVHSet : public Commander {
 public:
  // KKVHSET key field value EX second | PX millisecond | EXAT timestamp | PXAT millisecond timestamp | PERSIST [field
  // value EX ...]
  Status Parse(const std::vector<std::string> &args) override {
    if (!encode_hash_sub_flag || !enable_kkv_cmd_flag) {
      return {Status::CmdDisabled, errCmdDisabled};
    }
    uint64_t expire_ts = 0;
    CommandParser parser(args, 2);
    uint64_t curr_ts = util::GetTimeStampMS();
    for (size_t idx = 2, delta = 0; parser.Good(); idx += delta) {
      parser.Skip(1);
      if (!parser.Good()) {
        return parser.InvalidSyntax();
      }
      parser.Skip(1);
      std::string_view ttl_type;
      if (parser.EatEqICaseFlag("ex", ttl_type)) {
        expire_ts = GET_OR_RET(parser.TakeInt<uint64_t>());
        if (expire_ts > kHashFieldMaxAbsTimeMS) {
          return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
        }
        expire_ts = expire_ts * 1000 + curr_ts;
        if (expire_ts > kHashFieldMaxAbsTimeMS) {
          return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
        }
        delta = 4;
      } else if (parser.EatEqICaseFlag("px", ttl_type)) {
        expire_ts = GET_OR_RET(parser.TakeInt<uint64_t>());
        if (expire_ts > kHashFieldMaxAbsTimeMS) {
          return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
        }
        expire_ts = expire_ts + curr_ts;
        if (expire_ts > kHashFieldMaxAbsTimeMS) {
          return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
        }
        delta = 4;
      } else if (parser.EatEqICaseFlag("exat", ttl_type)) {
        expire_ts = GET_OR_RET(parser.TakeInt<uint64_t>());
        if (expire_ts > kHashFieldMaxAbsTimeMS) {
          return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
        }
        expire_ts = expire_ts * 1000;
        if (expire_ts > kHashFieldMaxAbsTimeMS) {
          return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
        }
        delta = 4;
      } else if (parser.EatEqICaseFlag("pxat", ttl_type)) {
        expire_ts = GET_OR_RET(parser.TakeInt<uint64_t>());
        if (expire_ts > kHashFieldMaxAbsTimeMS) {
          return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
        }
        delta = 4;
      } else if (parser.EatEqICaseFlag("persist", ttl_type)) {
        expire_ts = 0;
        delta = 3;
      } else {
        return parser.InvalidSyntax();
      }

      HashSubData sub_data;
      sub_data.SetValue(args[idx + 1]);
      sub_data.SetExpire(expire_ts);
      field_values_.emplace_back(args[idx], sub_data);
    }
    estimated_subkey_count_ = static_cast<int64_t>(field_values_.size());
    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    redis::KKV kkv_db(storage, conn->GetNamespace());
    auto s = kkv_db.Set(args_[1], field_values_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  std::vector<KKVFieldValue> field_values_;
};

class CommandKKVHSetNX : public Commander {
 public:
  // KKVHSETNX key field value EX second | PX millisecond | EXAT timestamp | PXAT millisecond timestamp | PERSIST
  Status Parse(const std::vector<std::string> &args) override {
    if (!encode_hash_sub_flag || !enable_kkv_cmd_flag) {
      return {Status::CmdDisabled, errCmdDisabled};
    }
    CommandParser parser(args, 4);
    std::string_view ttl_type;
    if (parser.EatEqICaseFlag("ex", ttl_type)) {
      expire_at_ = GET_OR_RET(parser.TakeInt<uint64_t>());
      if (expire_at_ > kHashFieldMaxAbsTimeMS) {
        return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
      }
      expire_at_ = expire_at_ * 1000 + util::GetTimeStampMS();
      if (expire_at_ > kHashFieldMaxAbsTimeMS) {
        return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
      }
    } else if (parser.EatEqICaseFlag("px", ttl_type)) {
      expire_at_ = GET_OR_RET(parser.TakeInt<uint64_t>());
      if (expire_at_ > kHashFieldMaxAbsTimeMS) {
        return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
      }
      expire_at_ = expire_at_ + util::GetTimeStampMS();
      if (expire_at_ > kHashFieldMaxAbsTimeMS) {
        return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
      }
    } else if (parser.EatEqICaseFlag("exat", ttl_type)) {
      expire_at_ = GET_OR_RET(parser.TakeInt<uint64_t>());
      if (expire_at_ > kHashFieldMaxAbsTimeMS) {
        return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
      }
      expire_at_ = expire_at_ * 1000;
      if (expire_at_ > kHashFieldMaxAbsTimeMS) {
        return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
      }
    } else if (parser.EatEqICaseFlag("pxat", ttl_type)) {
      expire_at_ = GET_OR_RET(parser.TakeInt<uint64_t>());
      if (expire_at_ > kHashFieldMaxAbsTimeMS) {
        return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
      }
    } else if (!parser.EatEqICaseFlag("persist", ttl_type)) {
      return parser.InvalidSyntax();
    }
    return parser.Good() ? parser.InvalidSyntax() : Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    bool flag = false;
    HashSubData sub_data;
    sub_data.SetValue(args_[3]);
    sub_data.SetExpire(expire_at_);
    std::optional<std::string> actual_val;
    redis::KKV kkv_db(storage, conn->GetNamespace());
    auto s = kkv_db.SetNX(args_[1], args_[2], sub_data, actual_val, &flag);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = KKVCASResp(actual_val, flag);
    return Status::OK();
  }

 private:
  uint64_t expire_at_ = 0;
};

class CommandKKVHCAS : public Commander {
 public:
  // KKVHCAS key field old_value new_value [EX second | PX millisecond | EXAT timestamp | PXAT millisecond timestamp |
  // PERSIST]
  Status Parse(const std::vector<std::string> &args) override {
    if (!encode_hash_sub_flag || !enable_kkv_cmd_flag) {
      return {Status::CmdDisabled, errCmdDisabled};
    }
    if (args.size() == 5) {
      return Status::OK();
    }
    CommandParser parser(args, 5);
    std::string_view ttl_type;
    if (parser.EatEqICaseFlag("ex", ttl_type)) {
      swap_ts_ = GET_OR_RET(parser.TakeInt<uint64_t>());
      if (swap_ts_ > kHashFieldMaxAbsTimeMS) {
        return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
      }
      swap_ts_ = swap_ts_ * 1000 + util::GetTimeStampMS();
      if (swap_ts_ > kHashFieldMaxAbsTimeMS) {
        return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
      }
      do_swap_ts_ = true;
    } else if (parser.EatEqICaseFlag("px", ttl_type)) {
      swap_ts_ = GET_OR_RET(parser.TakeInt<uint64_t>());
      if (swap_ts_ > kHashFieldMaxAbsTimeMS) {
        return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
      }
      swap_ts_ = swap_ts_ + util::GetTimeStampMS();
      if (swap_ts_ > kHashFieldMaxAbsTimeMS) {
        return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
      }
      do_swap_ts_ = true;
    } else if (parser.EatEqICaseFlag("exat", ttl_type)) {
      swap_ts_ = GET_OR_RET(parser.TakeInt<uint64_t>());
      if (swap_ts_ > kHashFieldMaxAbsTimeMS) {
        return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
      }
      swap_ts_ = swap_ts_ * 1000;
      if (swap_ts_ > kHashFieldMaxAbsTimeMS) {
        return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
      }
      do_swap_ts_ = true;
    } else if (parser.EatEqICaseFlag("pxat", ttl_type)) {
      swap_ts_ = GET_OR_RET(parser.TakeInt<uint64_t>());
      if (swap_ts_ > kHashFieldMaxAbsTimeMS) {
        return {Status::ExpireTSExceedRedisLimit, std::string(errInvalidExpireTime)};
      }
      do_swap_ts_ = true;
    } else if (parser.EatEqICaseFlag("persist", ttl_type)) {
      do_swap_ts_ = true;
    } else {
      return parser.InvalidSyntax();
    }
    return parser.Good() ? parser.InvalidSyntax() : Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    bool flag = false;
    std::optional<std::string> actual_val;
    redis::KKV kkv_db(storage, conn->GetNamespace());
    uint64_t *swap_ts = do_swap_ts_ ? &swap_ts_ : nullptr;
    auto s = kkv_db.CAS(args_[1], args_[2], args_[3], args_[4], swap_ts, actual_val, &flag);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = KKVCASResp(actual_val, flag);
    return Status::OK();
  }

 private:
  bool do_swap_ts_ = false;
  uint64_t swap_ts_ = 0;
};

class CommandKKVHCAD : public Commander {
 public:
  // KKVHCAD key field old_value
  Status Parse(const std::vector<std::string> &args) override {
    if (!encode_hash_sub_flag || !enable_kkv_cmd_flag) {
      return {Status::CmdDisabled, errCmdDisabled};
    }
    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    bool flag = false;
    std::optional<std::string> actual_val;
    redis::KKV kkv_db(storage, conn->GetNamespace());
    auto s = kkv_db.CAD(args_[1], args_[2], args_[3], actual_val, &flag);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = KKVCASResp(actual_val, flag);
    return Status::OK();
  }
};

class CommandKKVHRemRangeByLex : public Commander {
 public:
  // KKVHREMRANGEBYLEX key [lower (upper
  Status Parse(const std::vector<std::string> &args) override {
    if (!encode_hash_sub_flag || !enable_kkv_cmd_flag) {
      return {Status::CmdDisabled, errCmdDisabled};
    }
    auto s = ParseRangeLexSpec(args[2], args[3], &spec_, true);
    if (!s.IsOK()) {
      return {Status::RedisParseErr, s.Msg()};
    }
    if (spec_.minex || !spec_.maxex || spec_.max_infinite) {
      return {Status::RedisParseErr, "invalid lower or upper bound to delete"};
    }
    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    redis::KKV kkv_db(storage, conn->GetNamespace());
    auto s = kkv_db.RemRange(args_[1], spec_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  RangeLexSpec spec_;
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandKKVHSet>("kkvhset", -5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandKKVHSetNX>("kkvhsetnx", -5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandKKVHCAS>("kkvhcas", -5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandKKVHCAD>("kkvhcad", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandKKVHRemRangeByLex>("kkvhremrangebylex", 4, "write", 1, 1, 1))

}  // namespace redis
