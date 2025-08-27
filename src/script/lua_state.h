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

#include <string>
#include <vector>

#include "lua.hpp"
#include "server/redis_connection.h"
#include "status.h"

namespace lua {

class LuaState {
 public:
  LuaState() = delete;
  LuaState(const std::string &id) : thread_id(id), srv(nullptr), conn(nullptr), storage(nullptr) {
    lua = CreateState(this);
  }
  LuaState(const std::string &id, Server *s, redis::Connection *c, engine::Storage *store)
      : thread_id(id), srv(s), conn(c), storage(store) {
    lua = CreateState(this);
  }
  ~LuaState() { DestroyState(); }

  lua_State *CreateState(LuaState *lua_state, bool read_only = false);
  void DestroyState();

  void LoadFuncs(lua_State *lua, bool read_only = false);
  void LoadLibraries(lua_State *lua);
  void RemoveUnsupportedFunctions(lua_State *lua);
  void EnableGlobalsProtection(lua_State *lua);

  void LuaGC();

  static LuaState *GetLuaState(lua_State *lua);
  static int RedisCallCommand(lua_State *lua);
  static int RedisPCallCommand(lua_State *lua);
  static int RedisGenericCommand(lua_State *lua, int raise_error);
  static int RedisSha1hexCommand(lua_State *lua);
  static int RedisStatusReplyCommand(lua_State *lua);
  static int RedisErrorReplyCommand(lua_State *lua);
  static int RedisLogCommand(lua_State *lua);
  // static int RedisRegisterFunction(lua_State *lua);
  static int RedisReturnSingleFieldTable(lua_State *lua, const char *field);

  static int RedisMathRandom(lua_State *l);
  static int RedisMathRandomSeed(lua_State *l);

 public:
  std::string thread_id;
  lua_State *lua;
  Server *srv;
  redis::Connection *conn;
  engine::Storage *storage;
  int16_t slot = -1;

 private:
  int64_t gc_count_ = 0;
};

}  // namespace lua
