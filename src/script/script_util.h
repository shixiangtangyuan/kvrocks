#pragma once

#include <math.h>

#include <algorithm>
#include <cctype>
#include <string>

#include "lua.hpp"

/* The maximum number of characters needed to represent a long double
 * as a string (long double has a huge range).
 * This should be the size of the buffer given to doule to string */
constexpr size_t MAX_LONG_DOUBLE_CHARS = 5 * 1024;

enum {
  LL_DEBUG = 0,
  LL_VERBOSE,
  LL_NOTICE,
  LL_WARNING,
};

inline constexpr const char REDIS_LUA_FUNC_SHA_PREFIX[] = "f_";
inline constexpr const char REDIS_LUA_REGISTER_FUNC_PREFIX[] = "__redis_registered_";
inline constexpr const char REDIS_LUA_SERVER_PTR[] = "__server_ptr";
inline constexpr const char REDIS_LUA_STATE_PTR[] = "__lua_state_ptr";
inline constexpr const char REDIS_FUNCTION_LIBNAME[] = "REDIS_FUNCTION_LIBNAME";
inline constexpr const char REDIS_FUNCTION_NEEDSTORE[] = "REDIS_FUNCTION_NEEDSTORE";
inline constexpr const char REDIS_FUNCTION_LIBRARIES[] = "REDIS_FUNCTION_LIBRARIES";

namespace lua {

const char *RedisProtocolToLuaType(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeInt(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeBulk(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeStatus(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeError(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeAggregate(lua_State *lua, const char *reply, int atype);
const char *RedisProtocolToLuaTypeNull(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeBool(lua_State *lua, const char *reply, int tf);
const char *RedisProtocolToLuaTypeDouble(lua_State *lua, const char *reply);

std::string ReplyToRedisReply(lua_State *lua);
void PushError(lua_State *lua, const char *err);
[[noreturn]] int RaiseError(lua_State *lua);

void SHA1Hex(char *digest, const char *script, size_t len);

}  // namespace lua
