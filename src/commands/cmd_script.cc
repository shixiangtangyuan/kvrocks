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

#include "commander.h"
#include "error_constants.h"
#include "parse_util.h"
#include "script/lua_state.h"
#include "server/server.h"

namespace redis {

template <bool evalsha>
class CommandEvalImpl : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (evalsha && args[1].size() != 40) {
      return {Status::NotOK, errNoMatchingScript};
    }

    numkeys_ = GET_OR_RET(ParseInt<int64_t>(args[2], 10));
    if (numkeys_ > int64_t(args.size() - 3)) {
      return {Status::NotOK, "Number of keys can't be greater than number of args"};
    } else if (numkeys_ < 0) {
      return {Status::NotOK, "Number of keys can't be negative"};
    }

    return Status::OK();
  }

 public:
  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    return srv->script_mgr->EvalGenericCommand(
        conn, args_[1], std::vector<std::string>(args_.begin() + 3, args_.begin() + 3 + numkeys_),
        std::vector<std::string>(args_.begin() + 3 + numkeys_, args_.end()), evalsha, output, storage);
  }

 private:
  int64_t numkeys_ = 0;
};

class CommandEval : public CommandEvalImpl<false> {};

class CommandEvalSHA : public CommandEvalImpl<true> {};

class CommandEvalRO : public CommandEvalImpl<false> {};

class CommandEvalSHARO : public CommandEvalImpl<true> {};

class CommandScript : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = util::ToLower(args[1]);
    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override {
    if (args_.size() == 2 && subcommand_ == "flush") {
      auto s = srv->script_mgr->ScriptFlush();
      if (!s) {
        LOG(ERROR) << "Failed to flush scripts: " << s.Msg();
        return s;
      }
      *output = redis::SimpleString("OK");
    } else if (args_.size() >= 3 && subcommand_ == "exists") {
      *output = redis::MultiLen(args_.size() - 2);
      for (size_t j = 2; j < args_.size(); j++) {
        if (srv->script_mgr->ScriptExists(args_[j]).IsOK()) {
          *output += redis::Integer(1);
        } else {
          *output += redis::Integer(0);
        }
      }
    } else if (args_.size() == 3 && subcommand_ == "load") {
      std::string sha;
      auto s = srv->script_mgr->LoadScript(args_[2], &sha);
      if (!s.IsOK()) {
        return s;
      }

      *output = redis::BulkString(sha);
    } else {
      return {Status::NotOK, "Unknown SCRIPT subcommand or wrong number of arguments, subcommand: LOAD|EXISTS|FLUSH"};
    }
    return Status::OK();
  }

 private:
  std::string subcommand_;
};

CommandKeyRange GetScriptEvalKeyRange(const std::vector<std::string> &args) {
  auto numkeys = ParseInt<int>(args[2], 10).ValueOr(0);

  return {3, 2 + numkeys, 1};
}

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandEval>("eval", -3, "script write no-script", GetScriptEvalKeyRange),
                        MakeCmdAttr<CommandEvalSHA>("evalsha", -3, "script write no-script", GetScriptEvalKeyRange),
                        // MakeCmdAttr<CommandEvalRO>("eval_ro", -3, "read-only no-script ro-script",
                        //                            GetScriptEvalKeyRange),
                        // MakeCmdAttr<CommandEvalSHARO>("evalsha_ro", -3, "read-only no-script ro-script",
                        //                               GetScriptEvalKeyRange),
                        MakeCmdAttr<CommandScript>("script", -2, "script no-script", 0, 0, 0), )

}  // namespace redis
