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

// This file is modified from several source code files about lua scripting of Redis.
// See the original code at https://github.com/redis/redis.

#include "lua_state.h"

#include <math.h>

#include <algorithm>
#include <cctype>
#include <string>

#include "commands/commander.h"
#include "db_util.h"
#include "fmt/format.h"
#include "parse_util.h"
#include "rand.h"
#include "scope_exit.h"
#include "script_util.h"
#include "server/redis_connection.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "sha1.h"
#include "stats/stats.h"
#include "storage/storage.h"

namespace lua {

constexpr int64_t LUA_GC_CYCLE_PERIOD = 50;

lua_State *LuaState::CreateState(LuaState *lua_state, bool read_only) {
  lua_State *lua = lua_open();
  LoadLibraries(lua);
  RemoveUnsupportedFunctions(lua);
  LoadFuncs(lua, read_only);

  lua_pushlightuserdata(lua, lua_state);
  lua_setglobal(lua, REDIS_LUA_STATE_PTR);

  EnableGlobalsProtection(lua);
  return lua;
}

void LuaState::DestroyState() {
  lua_gc(lua, LUA_GCCOLLECT, 0);
  lua_close(lua);
}

LuaState *LuaState::GetLuaState(lua_State *lua) {
  lua_getglobal(lua, REDIS_LUA_STATE_PTR);
  auto ls = reinterpret_cast<LuaState *>(lua_touserdata(lua, -1));
  lua_pop(lua, 1);

  return ls;
}

void LuaState::LoadFuncs(lua_State *lua, bool read_only) {
  lua_newtable(lua);

  /* redis.call */
  lua_pushstring(lua, "call");
  lua_pushcfunction(lua, RedisCallCommand);
  lua_settable(lua, -3);

  /* redis.pcall */
  lua_pushstring(lua, "pcall");
  lua_pushcfunction(lua, RedisPCallCommand);
  lua_settable(lua, -3);

  /* redis.log and log levels. */
  lua_pushstring(lua, "log");
  lua_pushcfunction(lua, RedisLogCommand);
  lua_settable(lua, -3);

  lua_pushstring(lua, "LOG_DEBUG");
  lua_pushnumber(lua, LL_DEBUG);
  lua_settable(lua, -3);

  lua_pushstring(lua, "LOG_VERBOSE");
  lua_pushnumber(lua, LL_VERBOSE);
  lua_settable(lua, -3);

  lua_pushstring(lua, "LOG_NOTICE");
  lua_pushnumber(lua, LL_NOTICE);
  lua_settable(lua, -3);

  lua_pushstring(lua, "LOG_WARNING");
  lua_pushnumber(lua, LL_WARNING);
  lua_settable(lua, -3);

  /* redis.sha1hex */
  lua_pushstring(lua, "sha1hex");
  lua_pushcfunction(lua, RedisSha1hexCommand);
  lua_settable(lua, -3);

  /* redis.error_reply and redis.status_reply */
  lua_pushstring(lua, "error_reply");
  lua_pushcfunction(lua, RedisErrorReplyCommand);
  lua_settable(lua, -3);
  lua_pushstring(lua, "status_reply");
  lua_pushcfunction(lua, RedisStatusReplyCommand);
  lua_settable(lua, -3);

  /* redis.read_only */
  lua_pushstring(lua, "read_only");
  lua_pushboolean(lua, read_only);
  lua_settable(lua, -3);

  // /* redis.register_function */
  // lua_pushstring(lua, "register_function");
  // lua_pushcfunction(lua, RedisRegisterFunction);
  // lua_settable(lua, -3);

  lua_setglobal(lua, "redis");

  /* Replace math.random and math.randomseed with our implementations. */
  lua_getglobal(lua, "math");

  lua_pushstring(lua, "random");
  lua_pushcfunction(lua, RedisMathRandom);
  lua_settable(lua, -3);

  lua_pushstring(lua, "randomseed");
  lua_pushcfunction(lua, RedisMathRandomSeed);
  lua_settable(lua, -3);

  lua_setglobal(lua, "math");

  /* Add a helper function we use for pcall error reporting.
   * Note that when the error is in the C function we want to report the
   * information about the caller, that's what makes sense from the point
   * of view of the user debugging a script. */
  const char *err_func =
      "local dbg = debug\n"
      "function __redis__err__handler(err)\n"
      "  local i = dbg.getinfo(2,'nSl')\n"
      "  if i and i.what == 'C' then\n"
      "    i = dbg.getinfo(3,'nSl')\n"
      "  end\n"
      "  if i then\n"
      "    return i.source .. ':' .. i.currentline .. ': ' .. err\n"
      "  else\n"
      "    return err\n"
      "  end\n"
      "end\n";
  luaL_loadbuffer(lua, err_func, strlen(err_func), "@err_handler_def");
  lua_pcall(lua, 0, 0, 0);

  const char *compare_func =
      "function __redis__compare_helper(a,b)\n"
      "  if a == false then a = '' end\n"
      "  if b == false then b = '' end\n"
      "  return a<b\n"
      "end\n";
  luaL_loadbuffer(lua, compare_func, strlen(compare_func), "@cmp_func_def");
  lua_pcall(lua, 0, 0, 0);
}

void LuaState::LoadLibraries(lua_State *lua) {
  auto load_lib = [](lua_State *lua, const char *libname, lua_CFunction func) {
    lua_pushcfunction(lua, func);
    lua_pushstring(lua, libname);
    lua_call(lua, 1, 0);
  };

  load_lib(lua, "", luaopen_base);
  load_lib(lua, LUA_TABLIBNAME, luaopen_table);
  load_lib(lua, LUA_STRLIBNAME, luaopen_string);
  load_lib(lua, LUA_MATHLIBNAME, luaopen_math);
  load_lib(lua, LUA_DBLIBNAME, luaopen_debug);
  load_lib(lua, "cjson", luaopen_cjson);
  load_lib(lua, "struct", luaopen_struct);
  load_lib(lua, "cmsgpack", luaopen_cmsgpack);
  load_lib(lua, "bit", luaopen_bit);
}

void LuaState::RemoveUnsupportedFunctions(lua_State *lua) {
  lua_pushnil(lua);
  lua_setglobal(lua, "loadfile");
  lua_pushnil(lua);
  lua_setglobal(lua, "dofile");
}

void LuaState::EnableGlobalsProtection(lua_State *lua) {
  const char *code =
      "local dbg=debug\n"
      "local mt = {}\n"
      "setmetatable(_G, mt)\n"
      "mt.__newindex = function (t, n, v)\n"
      "  if dbg.getinfo(2) then\n"
      "    local w = dbg.getinfo(2, \"S\").what\n"
      "    if w ~= \"user_script\" and w ~= \"C\" then\n"
      "      error(\"Script attempted to create global variable '\"..tostring(n)..\"'\", 2)\n"
      "    end\n"
      "  end\n"
      "  rawset(t, n, v)\n"
      "end\n"
      "mt.__index = function (t, n)\n"
      "  if dbg.getinfo(2) and dbg.getinfo(2, \"S\").what ~= \"C\" then\n"
      "    error(\"Script attempted to access nonexistent global variable '\"..tostring(n)..\"'\", 2)\n"
      "  end\n"
      "  return rawget(t, n)\n"
      "end\n"
      "debug = nil\n";

  luaL_loadbuffer(lua, code, strlen(code), "@enable_strict_lua");
  lua_pcall(lua, 0, 0, 0);
}

void LuaState::LuaGC() {
  /* Call the Lua garbage collector from time to time to avoid a
   * full cycle performed by Lua, which adds too latency.
   *
   * The call is performed every LUA_GC_CYCLE_PERIOD executed commands
   * (and for LUA_GC_CYCLE_PERIOD collection steps) because calling it
   * for every command uses too much CPU. */
  gc_count_++;
  if (gc_count_ == LUA_GC_CYCLE_PERIOD) {
    lua_gc(lua, LUA_GCSTEP, LUA_GC_CYCLE_PERIOD);
    gc_count_ = 0;
  }
}

int LuaState::RedisCallCommand(lua_State *lua) { return RedisGenericCommand(lua, 1); }

int LuaState::RedisPCallCommand(lua_State *lua) { return RedisGenericCommand(lua, 0); }

int LuaState::RedisGenericCommand(lua_State *lua, int raise_error) {
  lua_getglobal(lua, "redis");
  lua_getfield(lua, -1, "read_only");
  int read_only = lua_toboolean(lua, -1);
  lua_pop(lua, 2);

  auto ls = GetLuaState(lua);
  auto srv = ls->srv;

  int argc = lua_gettop(lua);
  if (argc == 0) {
    PushError(lua, "Please specify at least one argument for redis.call()");
    GlobalStatsInstance().IncrRequests(GlobalStats::FAIL_USAGE_ERROR);
    return raise_error ? RaiseError(lua) : 1;
  }

  std::vector<std::string> args;
  for (int j = 1; j <= argc; j++) {
    if (lua_type(lua, j) == LUA_TNUMBER) {
      lua_Number num = lua_tonumber(lua, j);
      args.emplace_back(fmt::format("{:.17g}", static_cast<double>(num)));
    } else {
      size_t obj_len = 0;
      const char *obj_s = lua_tolstring(lua, j, &obj_len);
      if (obj_s == nullptr) {
        PushError(lua, "Lua redis.call() command arguments must be strings or integers");
        GlobalStatsInstance().IncrRequests(GlobalStats::FAIL_USAGE_ERROR);
        return raise_error ? RaiseError(lua) : 1;
      }
      args.emplace_back(obj_s, obj_len);
    }
  }

  auto conn = ls->conn;
  auto storage = ls->storage;

  auto commands = redis::CommandTable::Get();
  auto cmd_iter = commands->find(util::ToLower(args[0]));
  if (cmd_iter == commands->end()) {
    LOG(WARNING) << "[lua] Calling unknown Redis command: " << args[0];
    PushError(lua, "Unknown Redis command called from Lua script");
    GlobalStatsInstance().IncrRequests(GlobalStats::FAIL_USAGE_ERROR);
    return raise_error ? RaiseError(lua) : 1;
  }

  auto redis_cmd = cmd_iter->second;
  if (read_only && !(redis_cmd->flags & redis::kCmdReadOnly)) {
    PushError(lua, "Write commands are not allowed from read-only scripts");
    GlobalStatsInstance().IncrRequests(GlobalStats::FAIL_USAGE_ERROR);
    return raise_error ? RaiseError(lua) : 1;
  }

  auto cmd = redis_cmd->factory();
  cmd->SetAttributes(redis_cmd);
  cmd->SetArgs(args);

  int arity = cmd->GetAttributes()->arity;
  if (((arity > 0 && argc != arity) || (arity < 0 && argc < -arity))) {
    PushError(lua, "Wrong number of args calling Redis command From Lua script");
    GlobalStatsInstance().IncrRequests(GlobalStats::FAIL_USAGE_ERROR);
    return raise_error ? RaiseError(lua) : 1;
  }
  auto attributes = cmd->GetAttributes();
  auto cmd_flags = attributes->GenerateFlags(args);
  if (cmd_flags & redis::kCmdNoScript) {
    PushError(lua, "This Redis command is not allowed from scripts");
    GlobalStatsInstance().IncrRequests(GlobalStats::FAIL_USAGE_ERROR);
    return raise_error ? RaiseError(lua) : 1;
  }

  std::string slotrange_name = "unknown";
  auto key_range = attributes->GetKeyRange(args);
  // command has key
  if (key_range.first_key > 0) {
    // check for eval/evalsha has no explicit keys, like: eval "return redis.call('get', 'key')" 0
    // NOTE(mingfo): Server commands without keys, which will read/write data, should be handled specificlly.
    if (storage == nullptr) {
      LOG(WARNING) << "Lua get in script data operating cmd: " << attributes->name;
      PushError(lua, "This lua command has no explicit keys");
      GlobalStatsInstance().IncrRequests(GlobalStats::FAIL_USAGE_ERROR);
      return raise_error ? RaiseError(lua) : 1;
    }

    // NOTE(mingfo): make sure all keys of script in same slot
    // Check for the senario that some keys in script, like:
    //   eval "redis.call('get', KEYS[1]); redis.call('set', 'key2', 'val')" 1 key1
    // 1. If keys in EVAL command and in script are in same slot,
    //    it can be executed but the atomicity can't be guaranteed.
    // 2. If keys in EVAL command and in script are in same slot, return CROSSSLOT error.
    auto slot = GetSlotIdFromKey(args[key_range.first_key]);
    if (ls->slot < 0) {
      ls->slot = static_cast<int16_t>(slot);
    } else if (slot != ls->slot) {
      LOG(WARNING) << "[script] CROSSSLOT target_slot: " << ls->slot << ", key_slot: " << slot
                   << ", key: " << args[key_range.first_key];
      PushError(lua, "CROSSSLOT Attempted to access keys that don't hash to the same slot");
      GlobalStatsInstance().IncrRequests(GlobalStats::FAIL_USAGE_ERROR);
      return raise_error ? RaiseError(lua) : 1;
    }
    slotrange_name = ls->conn->GetServer()->cluster->GetSlotRangeNameBySlotId(ls->slot);
    // check keys in same slot of this command
    auto s = conn->CheckKeysInSameSlot(attributes, args);
    if (!s.IsOK()) {
      PushError(lua, s.Msg().data());
      GlobalStatsInstance().IncrRequests(GlobalStats::FAIL_USAGE_ERROR);
      return raise_error ? RaiseError(lua) : 1;
    }
  }

  auto s = cmd->Parse(args);
  if (!s) {
    PushError(lua, s.Msg().data());
    return raise_error ? RaiseError(lua) : 1;
  }

  std::string cmd_name = attributes->name;
  auto start = std::chrono::high_resolution_clock::now();
  bool is_profiling = conn->IsProfilingEnabled(cmd_name);

  std::string output;
  s = cmd->Execute(srv, conn, &output, storage);
  auto end = std::chrono::high_resolution_clock::now();
  uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  std::optional<std::pair<std::string, std::string>> perf_io_context;
  srv->SlowlogPushEntryIfNeeded(&args, duration, conn, is_profiling, perf_io_context, -1,
                                static_cast<int64_t>(cmd->GetEstimatedSubkeyNum()));
  if (is_profiling) conn->RecordProfilingSampleIfNeed(cmd_name, duration, std::move(perf_io_context));
  srv->FeedMonitorConns(conn, args);
  if (!s) {
    GlobalStatsInstance().IncrCalls(cmd_name, false);
    PushError(lua, s.Msg().data());
    GlobalStatsInstance().IncrRequests(GlobalStats::FAIL_UNCONCERNED_ERROR);
    return raise_error ? RaiseError(lua) : 1;
  }
  thread_local_metric_array.RecordCommadLatency(cmd_name, slotrange_name, duration);
  thread_local_metric_array.RecordCommadSize(cmd_name, slotrange_name, cmd->GetEstimatedSubkeyNum(),
                                             cmd->GetEstimatedSubkeySize());
  thread_local_metric_array.RecordReplySize(cmd_name, slotrange_name, output.size());

  GlobalStatsInstance().IncrCalls(cmd_name, true);
  RedisProtocolToLuaType(lua, output.data());
  return 1;
}

/* This adds redis.sha1hex(string) to Lua scripts using the same hashing
 * function used for sha1ing lua scripts. */
int LuaState::RedisSha1hexCommand(lua_State *lua) {
  int argc = lua_gettop(lua);

  if (argc != 1) {
    lua_pushstring(lua, "wrong number of arguments");
    return lua_error(lua);
  }

  size_t len = 0;
  const char *s = static_cast<const char *>(lua_tolstring(lua, 1, &len));

  char digest[41];
  SHA1Hex(digest, s, len);
  lua_pushstring(lua, digest);
  return 1;
}

/* redis.error_reply() */
int LuaState::RedisErrorReplyCommand(lua_State *lua) { return RedisReturnSingleFieldTable(lua, "err"); }

/* redis.status_reply() */
int LuaState::RedisStatusReplyCommand(lua_State *lua) { return RedisReturnSingleFieldTable(lua, "ok"); }

int LuaState::RedisLogCommand(lua_State *lua) {
  int argc = lua_gettop(lua);

  if (argc < 2) {
    lua_pushstring(lua, "redis.log() requires two arguments or more.");
    return lua_error(lua);
  }
  if (!lua_isnumber(lua, -argc)) {
    lua_pushstring(lua, "First argument must be a number (log level).");
    return lua_error(lua);
  }
  int level = static_cast<int>(lua_tonumber(lua, -argc));
  if (level < LL_DEBUG || level > LL_WARNING) {
    lua_pushstring(lua, "Invalid debug level.");
    return lua_error(lua);
  }

  std::string log_message;
  for (int j = 1; j < argc; j++) {
    size_t len = 0;
    if (const char *s = lua_tolstring(lua, j - argc, &len)) {
      if (j != 1) {
        log_message += " ";
      }
      log_message += std::string(s, len);
    }
  }

  // The min log level was INFO, DEBUG would never take effect
  switch (level) {
    case LL_VERBOSE:  // also regard VERBOSE as INFO here since no VERBOSE level
    case LL_NOTICE:
      LOG(INFO) << "[Lua] " << log_message;
      break;
    case LL_WARNING:
      LOG(WARNING) << "[Lua] " << log_message;
      break;
  }
  return 0;
}

/* Returns a table with a single field 'field' set to the string value
 * passed as argument. This helper function is handy when returning
 * a Redis Protocol error or status reply from Lua:
 *
 * return redis.error_reply("ERR Some Error")
 * return redis.status_reply("ERR Some Error")
 */
int LuaState::RedisReturnSingleFieldTable(lua_State *lua, const char *field) {
  if (lua_gettop(lua) != 1 || lua_type(lua, -1) != LUA_TSTRING) {
    PushError(lua, "wrong number or type of arguments");
    return 1;
  }

  lua_newtable(lua);
  lua_pushstring(lua, field);
  lua_pushvalue(lua, -3);
  lua_settable(lua, -3);
  return 1;
}

/* ---------------------------------------------------------------------------
 * Redis provided math.random
 * ------------------------------------------------------------------------- */

/* We replace math.random() with our implementation that is not affected
 * by specific libc random() implementations and will output the same sequence
 * (for the same seed) in every arch. */

/* The following implementation is the one shipped with Lua itself but with
 * rand() replaced by redisLrand48(). */
int LuaState::RedisMathRandom(lua_State *lua) {
  /* the `%' avoids the (rare) case of r==1, and is needed also because on
     some systems (SunOS!) `rand()' may return a value larger than RAND_MAX */
  lua_Number r = (lua_Number)(RedisLrand48() % REDIS_LRAND48_MAX) / (lua_Number)REDIS_LRAND48_MAX;
  switch (lua_gettop(lua)) {  /* check number of arguments */
    case 0: {                 /* no arguments */
      lua_pushnumber(lua, r); /* Number between 0 and 1 */
      break;
    }
    case 1: { /* only upper limit */
      int u = luaL_checkint(lua, 1);
      luaL_argcheck(lua, 1 <= u, 1, "interval is empty");
      lua_pushnumber(lua, floor(r * u) + 1); /* int between 1 and `u' */
      break;
    }
    case 2: { /* lower and upper limits */
      int l = luaL_checkint(lua, 1);
      int u = luaL_checkint(lua, 2);
      luaL_argcheck(lua, l <= u, 2, "interval is empty");
      lua_pushnumber(lua, floor(r * (u - l + 1)) + l); /* int between `l' and `u' */
      break;
    }
    default:
      return luaL_error(lua, "wrong number of arguments");
  }
  return 1;
}

int LuaState::RedisMathRandomSeed(lua_State *lua) {
  RedisSrand48(luaL_checkint(lua, 1));
  return 0;
}

}  // namespace lua
