#pragma once

#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "lua.hpp"
#include "server/redis_connection.h"
#include "storage/storage.h"

class Server;

namespace lua {

class LuaState;

class ScriptManager {
 public:
  ScriptManager() = delete;
  ScriptManager(Server *svr) : svr_(svr) {}
  ~ScriptManager() {}

  Status EvalGenericCommand(redis::Connection *conn, const std::string &body_or_sha,
                            const std::vector<std::string> &keys, const std::vector<std::string> &argv, bool evalsha,
                            std::string *output, engine::Storage *storage);

  std::shared_ptr<LuaState> GetLuaStateOfThisThread();

  Status ScriptGet(const std::string &sha, std::string *body, engine::Storage *storage) const;
  Status ScriptExists(const std::string &sha) const;
  Status ScriptExists(const std::string &sha, engine::Storage *storage) const;
  Status ScriptFlush();
  Status ScriptSet(const std::string &sha, const std::string &body, engine::Storage *storage);
  void ScriptReset();
  void EraseLuaState(const std::string &thread_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    lua_states_map_.erase(thread_id);
  }
  Status LoadScript(const std::string &body, std::string *sha);
  Status CreateFunction(Server *srv, const std::string &body, std::string *sha, lua_State *lua,
                        engine::Storage *storage, bool need_to_store);

  void SortArray(lua_State *lua);
  void PushArray(lua_State *lua, const std::vector<std::string> &elems);
  void SetGlobalArray(lua_State *lua, const std::string &var, const std::vector<std::string> &elems);

 private:
  Server *svr_;
  std::shared_mutex mutex_;
  std::map<std::string, std::shared_ptr<LuaState>> lua_states_map_;
};

}  // namespace lua
