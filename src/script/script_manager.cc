#include "script_manager.h"

#include "common/scope_exit.h"
#include "lock/lock.h"
#include "lua_state.h"
#include "script_util.h"
#include "server/server.h"

namespace lua {

Status ScriptManager::EvalGenericCommand(redis::Connection *conn, const std::string &body_or_sha,
                                         const std::vector<std::string> &keys, const std::vector<std::string> &argv,
                                         bool evalsha, std::string *output, engine::Storage *storage) {
  Server *srv = conn->GetServer();
  // Get lua of this thread
  auto ls = GetLuaStateOfThisThread();
  ls->conn = conn;
  ls->srv = conn->GetServer();
  ls->storage = storage;
  lua_State *lua = ls->lua;

  auto exit = MakeScopeExit([&ls]() {
    ls->conn = nullptr;
    ls->srv = nullptr;
    ls->storage = nullptr;
    ls->slot = -1;
  });

  /* We obtain the script SHA1, then check if this function is already
   * defined into the Lua state */
  char funcname[2 + 40 + 1] = {};
  memcpy(funcname, REDIS_LUA_FUNC_SHA_PREFIX, sizeof(REDIS_LUA_FUNC_SHA_PREFIX));

  if (!evalsha) {
    SHA1Hex(funcname + 2, body_or_sha.c_str(), body_or_sha.size());
  } else {
    for (int j = 0; j < 40; j++) {
      funcname[j + 2] = static_cast<char>(tolower(body_or_sha[j]));
    }
  }

  /* Push the pcall error handler function on the stack. */
  lua_getglobal(lua, "__redis__err__handler");

  /* Try to lookup the Lua function */
  lua_getglobal(lua, funcname);
  if (lua_isnil(lua, -1)) {
    lua_pop(lua, 1); /* remove the nil from the stack */
    std::string body;
    if (evalsha) {
      if (storage == nullptr) {
        redis::Context ctx;
        auto [name, store] = ls->srv->cluster->GetOneSlotRangeAndStorage();
        auto res = redis::SlotRangeLock::AcquireSlotRangeLock(name, redis::mgl::LockMode::LOCK_IS, &ctx,
                                                              ls->srv->GetMGLockMgr());
        if (!res.IsOK()) {
          lua_pop(lua, 1); /* remove the error handler from the stack. */
          return {Status::NotOK, res.Msg()};
        }
        auto s = ScriptGet(funcname + 2, &body, store.get());
        if (!s.IsOK()) {
          lua_pop(lua, 1); /* remove the error handler from the stack. */
          return {Status::NotOK, "NOSCRIPT No matching script. Please use EVAL"};
        }
      } else {
        auto s = ScriptGet(funcname + 2, &body, storage);
        if (!s.IsOK()) {
          lua_pop(lua, 1); /* remove the error handler from the stack. */
          return {Status::NotOK, "NOSCRIPT No matching script. Please use EVAL"};
        }
      }
    } else {
      body = body_or_sha;
    }

    std::string sha = funcname + 2;
    auto s = CreateFunction(srv, body, &sha, lua, storage, false);
    if (!s.IsOK()) {
      lua_pop(lua, 1); /* remove the error handler from the stack. */
      return s;
    }
    /* Now the following is guaranteed to return non nil */
    lua_getglobal(lua, funcname);
  }

  /* Populate the argv and keys table accordingly to the arguments that
   * EVAL received. */
  SetGlobalArray(lua, "KEYS", keys);
  SetGlobalArray(lua, "ARGV", argv);

  if (lua_pcall(lua, 0, 1, -2)) {
    auto msg = fmt::format("ERR running script (call to {}): {}", funcname, lua_tostring(lua, -1));
    *output = redis::Error(msg);
    lua_pop(lua, 2);
  } else {
    *output = ReplyToRedisReply(lua);
    lua_pop(lua, 2);
  }

  // clean global variables to prevent information leak in function commands
  lua_pushnil(lua);
  lua_setglobal(lua, "KEYS");
  lua_pushnil(lua);
  lua_setglobal(lua, "ARGV");

  // Call Lua garbage collector from time to time to avoid
  // a full cycle performed by Lua, which adds too latency.
  ls->LuaGC();

  return Status::OK();
}

std::shared_ptr<LuaState> ScriptManager::GetLuaStateOfThisThread() {
  std::shared_ptr<LuaState> lua_state = nullptr;
  std::string thread_id = util::GetCurThreadId();
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = lua_states_map_.find(thread_id);
    if (it != lua_states_map_.end()) {
      lua_state = it->second;
    }
  }

  if (lua_state == nullptr) {
    lua_state = std::make_shared<LuaState>(thread_id);
    std::unique_lock<std::shared_mutex> lock(mutex_);
    lua_states_map_.emplace(thread_id, lua_state);
  }

  return lua_state;
}

Status ScriptManager::ScriptGet(const std::string &sha, std::string *body, engine::Storage *storage) const {
  if (storage == nullptr) {
    return Status{Status::NotOK, "Reading null storage"};
  }
  std::string func_name = engine::kLuaFuncSHAPrefix + sha;
  auto cf = storage->GetCFHandle(engine::kPropagateColumnFamilyName);
  auto s = storage->Get(rocksdb::ReadOptions(), cf, func_name, body);
  if (!s.ok()) {
    return {s.IsNotFound() ? Status::NotFound : Status::NotOK, s.ToString()};
  }
  return Status::OK();
}

Status ScriptManager::ScriptExists(const std::string &sha) const {
  auto all_storage = svr_->storage_mgr->GetAllStorage();
  for (auto &[id, storage] : all_storage) {
    auto s = ScriptExists(sha, storage.get());
    if (!s.IsOK()) return s;
  }
  return Status::OK();
}

Status ScriptManager::ScriptExists(const std::string &sha, engine::Storage *storage) const {
  std::string body;
  return ScriptGet(sha, &body, storage);
}

Status ScriptManager::ScriptSet(const std::string &sha, const std::string &body, engine::Storage *storage) {
  if (storage == nullptr) {
    return Status{Status::NotOK, "Writing null storage"};
  }
  std::string func_name = engine::kLuaFuncSHAPrefix + sha;
  return storage->WriteToPropagateCF(func_name, body);
}

void ScriptManager::ScriptReset() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  lua_states_map_.clear();
}

Status ScriptManager::ScriptFlush() {
  auto all_storage = svr_->storage_mgr->GetAllStorage();
  for (auto &[id, storage] : all_storage) {
    auto cf = storage->GetCFHandle(engine::kPropagateColumnFamilyName);
    auto s = storage->FlushScripts(storage->DefaultWriteOptions(), cf);
    if (!s.ok()) return {Status::NotOK, s.ToString()};
  }
  ScriptReset();
  return Status::OK();
}

Status ScriptManager::LoadScript(const std::string &body, std::string *sha) {
  auto all_storage = svr_->storage_mgr->GetAllStorage();
  auto sl = GetLuaStateOfThisThread();
  auto s = CreateFunction(svr_, body, sha, sl->lua, nullptr, false);
  if (!s.IsOK()) return s;
  for (auto &[id, storage] : all_storage) {
    auto s = ScriptSet(*sha, body, storage.get());
    if (!s.IsOK()) return s;
  }
  return Status::OK();
}

/* Define a Lua function with the specified body.
 * The function name will be generated in the following form:
 *
 *   f_<hex sha1 sum>
 *
 * The function increments the reference count of the 'body' object as a
 * side effect of a successful call.
 *
 * On success a pointer to an SDS string representing the function SHA1 of the
 * just added function is returned (and will be valid until the next call
 * to scriptingReset() function), otherwise NULL is returned.
 *
 * The function handles the fact of being called with a script that already
 * exists, and in such a case, it behaves like in the success case.
 *
 * If 'c' is not NULL, on error the client is informed with an appropriate
 * error describing the nature of the problem and the Lua interpreter error. */
Status ScriptManager::CreateFunction(Server *srv, const std::string &body, std::string *sha, lua_State *lua,
                                     engine::Storage *storage, bool need_to_store) {
  char funcname[2 + 40 + 1] = {};
  memcpy(funcname, REDIS_LUA_FUNC_SHA_PREFIX, sizeof(REDIS_LUA_FUNC_SHA_PREFIX));

  if (sha->empty()) {
    SHA1Hex(funcname + 2, body.c_str(), body.size());
    *sha = funcname + 2;
  } else {
    std::copy(sha->begin(), sha->end(), funcname + 2);
  }

  if (luaL_loadbuffer(lua, body.c_str(), body.size(), "@user_script")) {
    std::string err_msg = lua_tostring(lua, -1);
    lua_pop(lua, 1);
    return {Status::NotOK, "Error compiling script: " + err_msg};
  }
  lua_setglobal(lua, funcname);

  // would store lua function into propagate column family and propagate those scripts to slaves
  return need_to_store ? ScriptSet(*sha, body, storage) : Status::OK();
}

/* Sort the array currently in the stack. We do this to make the output
 * of commands like KEYS or SMEMBERS something deterministic when called
 * from Lua (to play well with AOf/replication).
 *
 * The array is sorted using table.sort itself, and assuming all the
 * list elements are strings. */
void ScriptManager::SortArray(lua_State *lua) {
  /* Initial Stack: array */
  lua_getglobal(lua, "table");
  lua_pushstring(lua, "sort");
  lua_gettable(lua, -2);  /* Stack: array, table, table.sort */
  lua_pushvalue(lua, -3); /* Stack: array, table, table.sort, array */
  if (lua_pcall(lua, 1, 0, 0)) {
    /* Stack: array, table, error */

    /* We are not interested in the error, we assume that the problem is
     * that there are 'false' elements inside the array, so we try
     * again with a slower function but able to handle this case, that
     * is: table.sort(table, __redis__compare_helper) */
    lua_pop(lua, 1);             /* Stack: array, table */
    lua_pushstring(lua, "sort"); /* Stack: array, table, sort */
    lua_gettable(lua, -2);       /* Stack: array, table, table.sort */
    lua_pushvalue(lua, -3);      /* Stack: array, table, table.sort, array */
    lua_getglobal(lua, "__redis__compare_helper");
    /* Stack: array, table, table.sort, array, __redis__compare_helper */
    lua_call(lua, 2, 0);
  }
  /* Stack: array (sorted), table */
  lua_pop(lua, 1); /* Stack: array (sorted) */
}

void ScriptManager::PushArray(lua_State *lua, const std::vector<std::string> &elems) {
  lua_newtable(lua);
  for (size_t i = 0; i < elems.size(); i++) {
    lua_pushlstring(lua, elems[i].c_str(), elems[i].size());
    lua_rawseti(lua, -2, static_cast<int>(i) + 1);
  }
}

void ScriptManager::SetGlobalArray(lua_State *lua, const std::string &var, const std::vector<std::string> &elems) {
  PushArray(lua, elems);
  lua_setglobal(lua, var.c_str());
}

}  // namespace lua
