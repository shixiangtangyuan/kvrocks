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

#include <rocksdb/status.h>

#include <atomic>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "common/string_util.h"
#include "encoding.h"
#include "types/redis_stream_base.h"

#ifdef USE_PINNABLE_SLICE
#define StringOrPinSlice rocksdb::PinnableSlice
#else
#define StringOrPinSlice std::string
#endif

// We write enum integer value of every datatype
// explicitly since it cannot be changed once confirmed
// Note that if you want to add a new redis type in `RedisType`
// you should also add a type name to the `RedisTypeNames` below
enum RedisType {
  kRedisNone = 0,
  kRedisString = 1,
  kRedisHash = 2,
  kRedisList = 3,
  kRedisSet = 4,
  kRedisZSet = 5,
  kRedisBitmap = 6,
  kRedisSortedint = 7,
  kRedisStream = 8,
  kRedisBloomFilter = 9,
  kRedisJson = 10,
};

enum class RedisKeyCommand : uint8_t {
  kCmdNone = 0,
  kCmdExpire = 1,
  kCmdExpireAt = 2,
  kCmdPExpire = 3,
  kCmdPExpireAt = 4,
  kCmdDel = 5,
  kCmdUnlink = 6,

  kCmdProtect = 7,
};

const std::vector<std::string> RedisKeyCmdNames = {"none",      "expire", "expireat", "pexpire",
                                                   "pexpireat", "del",    "unlink"};

enum class RedisStringCommand : uint8_t {
  kCmdNone = 0,
  kCmdSet = 1,
  kCmdSetEX = 2,
  kCmdSetNX = 3,
  kCmdIncr = 4,
  kCmdIncrBy = 5,
  kCmdIncrByFloat = 6,
  kCmdDecr = 7,
  kCmdDecrBy = 8,
  kCmdMSet = 9,

  kCmdProtect = 10,
};

const std::vector<std::string> RedisStringCmdNames = {"none",   "set",         "setex", "setnx",  "incr",
                                                      "incrby", "incrbyfloat", "decr",  "decrby", "mset"};

enum class RedisListCommand : uint8_t {
  kCmdNone = 0,
  kCmdLPush = 1,
  kCmdRPush = 2,
  kCmdLPushX = 3,
  kCmdRPushX = 4,
  kCmdLPop = 5,
  kCmdRPop = 6,
  kCmdLRem = 7,
  kCmdLTrim = 8,
  kCmdLSet = 9,
  kCmdLInsert = 10,
  kCmdLMove = 11,

  kCmdProtect = 12,
};

const std::vector<std::string> RedisListCmdNames = {"none", "lpush", "rpush", "lpushx", "rpushx", "lpop",
                                                    "rpop", "lrem",  "ltrim", "lset",   "linsert"};

enum class RedisSetCommand : uint8_t {
  kCmdNone = 0,
  kCmdSAdd = 1,
  kCmdSRem = 2,
  kCmdSPop = 3,

  kCmdProtect = 4,
};

const std::vector<std::string> RedisSetCmdNames = {"none", "sadd", "srem", "spop"};

enum class RedisZSetCommand : uint8_t {
  kCmdNone = 0,
  kCmdZAdd = 1,
  kCmdZRem = 2,
  kCmdZRemRangeByRank = 3,
  kCmdZRemRangeByScore = 4,
  kCmdZPopMin = 5,
  kCmdZPopMax = 6,

  kCmdProtect = 7,
};

const std::vector<std::string> RedisZSetCmdNames = {"none",    "zadd",   "zrem", "zremrangebyrank", "zremrangebyscore",
                                                    "zpopmin", "zpopmax"};

enum class RedisBitmapCommand : uint8_t {
  kCmdNone = 0,
  kCmdSetBit = 1,
  kCmdBitOp = 2,
  kCmdBitfield = 3,

  kCmdProtect = 4,
};

const std::vector<std::string> RedisBitmapCmdNames = {"none", "setbit", "bitop", "bitfield"};

enum class RedisHashCommand : uint8_t {
  // NOTE(mingfo): Can't change the order of items or insert new item
  kCmdNone = 0,
  kCmdHIncrby = 1,
  kCmdHIncrbyFloat = 2,
  kCmdHSet = 3,
  kCmdHSetNX = 4,
  kCmdHMSet = 5,
  kCmdHDel = 6,
  // httl cmds
  kCmdHExpire = 7,
  kCmdHExpireAt = 8,
  kCmdHPExpire = 9,
  kCmdHPExpireAt = 10,
  kCmdHPersist = 11,
  // kkv cmds
  kCmdKKVHSet = 12,
  kCmdKKVHSetNX = 13,
  kCmdKKVHCAS = 14,
  kCmdKKVHCAD = 15,
  kCmdKKVHRemRangeByLex = 16,

  kCmdProtect = 17,
};

const std::vector<std::string> RedisHashCmdNames = {
    "none",    "hincrby",   "hincrbyfloat", "hset",     "hsetnx",           "hmset",
    "hdel",    "hexpire",   "hexpireat",    "hpexpire", "hpexpireat",       "hpersist",
    "kkvhset", "kkvhsetnx", "kkvhcas",      "kkvhcad",  "kkvhremrangebylex"};

RedisHashCommand GetHashCmdEqType(RedisHashCommand cmd_type);
RedisStringCommand GetStringCmdEqType(RedisStringCommand cmd_type);
RedisListCommand GetListCmdEqType(RedisListCommand cmd_type);
RedisSetCommand GetSetCmdEqType(RedisSetCommand cmd_type);
RedisZSetCommand GetZSetCmdEqType(RedisZSetCommand cmd_type);
RedisBitmapCommand GetBitmapCmdEqType(RedisBitmapCommand cmd_type);
RedisKeyCommand GetKeyCmdEqType(RedisKeyCommand cmd_type);

template <typename T>
inline const std::string EnumToString(T type) {
  return std::to_string(static_cast<uint8_t>(type));
}

enum RedisHashCodec {
  kNoFieldTTL = 0,
  kHasFieldTTL = 1,
};

extern std::atomic<bool> encode_hash_sub_flag;

const std::vector<std::string> RedisTypeNames = {"none",   "string",    "hash",   "list",      "set",      "zset",
                                                 "bitmap", "sortedint", "stream", "MBbloom--", "ReJSON-RL"};

constexpr const char *kErrMsgWrongType = "WRONGTYPE Operation against a key holding the wrong kind of value";
constexpr const char *kErrMsgKeyExpired = "the key was expired";

using rocksdb::Slice;

struct KeyNumStats {
  uint64_t n_key = 0;
  uint64_t n_expires = 0;
  uint64_t n_expired = 0;
  uint64_t avg_ttl = 0;
};

template <typename T = Slice>
[[nodiscard]] std::tuple<T, T> ExtractNamespaceKey(Slice ns_key, bool slot_id_encoded);
[[nodiscard]] std::string ComposeNamespaceKey(const Slice &ns, const Slice &key, bool slot_id_encoded);
[[nodiscard]] std::string ComposeSlotKeyPrefix(const Slice &ns, int slotid);

[[nodiscard]] RedisType GetRedisTypeByName(const std::string &type_str);

class InternalKey {
 public:
  explicit InternalKey(Slice ns_key, Slice sub_key, uint64_t version, bool slot_id_encoded);
  explicit InternalKey(Slice input, bool slot_id_encoded);
  ~InternalKey() = default;

  Slice GetNamespace() const;
  Slice GetKey() const;
  Slice GetSubKey() const;
  uint64_t GetVersion() const;
  [[nodiscard]] std::string Encode() const;
  bool operator==(const InternalKey &that) const;

 private:
  Slice key_;
  Slice sub_key_;
  uint64_t version_;
  uint16_t slotid_;
  bool slot_id_encoded_;
};

constexpr uint8_t METADATA_TTL_MASK = 0x80;
constexpr uint8_t METADATA_TYPE_MASK = 0x0f;
constexpr uint8_t METADATA_HAS_SUB_FLAG_MASK = 0x40;
constexpr uint8_t METADATA_SUB_TTL_SET_MASK = 0x20;
constexpr uint8_t HASH_SUB_KEY_HAS_TTL_MASK = 0x80;
constexpr uint8_t CONFLICT_TTL_AND_SUB_TTL_MASK = 0xa0;

class Metadata {
 public:
  // metadata flags
  // <(1-bit) has-ttl> <(1-bit) has-sub-flag> <(1-bit) sub-ttl-set> 0 <(4-bit) redis-type>
  // 'expire' will take 8 bytes in metadata, it can be encoded selectively with 'has-ttl'
  // if redis-type is hash, has-sub-flag will be set, and sub-ttl-set will be set if sub-ttl is set
  // redis-type: RedisType for the key-value
  uint8_t flags;

  // expire timestamp, in milliseconds
  uint64_t expire;

  // the current version: 53bit timestamp + 11bit counter
  uint64_t version;

  // element size of the key-value
  uint64_t size;

  // for hash type, the size of the hash nottl field
  uint64_t persist_field_size;
  // for hash type, the max expire timestamp of the hash fields
  uint64_t fields_max_expire_at;

  explicit Metadata(RedisType type, bool generate_version = true);

  static void InitVersionCounter();

  static size_t GetOffsetAfterExpire(uint8_t flags);
  static size_t GetOffsetAfterSize(uint8_t flags);
  static uint64_t ExpireMsToS(uint64_t ms);

  bool HasSubFlag() const;
  bool IsSubTTLSet() const;

  bool Is64BitEncoded() const;
  bool GetFixedCommon(rocksdb::Slice *input, uint64_t *value) const;
  bool GetExpire(rocksdb::Slice *input);
  void PutFixedCommon(std::string *dst, uint64_t value) const;
  void PutExpire(std::string *dst) const;

  RedisType Type() const;
  size_t CommonEncodedSize() const;
  static bool HasTTL(uint8_t flags);
  bool HasTTL() const;
  int64_t TTL() const;
  timeval Time() const;
  virtual bool Expired() const;
  bool ExpireAt(uint64_t expired_ts) const;

  // return whether for this type, the metadata itself is the whole data,
  // no other key-values.
  // this means that the metadata of these types do NOT have
  // `version` and `size` field.
  // e.g. RedisString, RedisJson
  bool IsSingleKVType() const;

  // return whether the `size` field of this type can be zero.
  // if a type is NOT an emptyable type,
  // any key of this type is regarded as expired if `size` equals to 0.
  // e.g. any SingleKVType, RedisStream, RedisBloomFilter
  bool IsEmptyableType() const;

  virtual void Encode(std::string *dst);
  [[nodiscard]] virtual rocksdb::Status Decode(Slice *input);
  [[nodiscard]] rocksdb::Status Decode(Slice input);

  bool operator==(const Metadata &that) const;
  virtual ~Metadata() = default;

 private:
  static uint64_t generateVersion();
};

class HashMetadata : public Metadata {
 public:
  explicit HashMetadata(bool generate_version = true);
  rocksdb::Status SetSubTTL(uint64_t persist_field_size, uint64_t fields_max_expire_at);
  void ResetSubTTL() {
    persist_field_size = 0;
    fields_max_expire_at = 0;
    flags &= ~METADATA_SUB_TTL_SET_MASK;
  }
};

class HashSubData {
 public:
  uint8_t flags = 0;
  uint64_t expire = 0;
  Slice value;

  void Encode(Metadata *metadata, std::string *dst) const;
  [[nodiscard]] rocksdb::Status Decode(Metadata *metadata, rocksdb::PinnableSlice *input, uint64_t curr_ts = 0);
  [[nodiscard]] rocksdb::Status Decode(Metadata *metadata, rocksdb::Slice input, uint64_t curr_ts = 0);
  [[nodiscard]] rocksdb::Status Decode(Metadata *metadata, std::string *input, uint64_t curr_ts = 0);
  [[nodiscard]] rocksdb::Status Decode(bool has_sub_flag, rocksdb::Slice input, uint64_t curr_ts = 0,
                                       bool keep_result = false);
  bool HasTTL() const;
  bool Expired(uint64_t curr_ts = 0) const;
  bool operator==(const HashSubData &rhs) const;
  bool operator!=(const HashSubData &rhs) const;
  void SetExpire(uint64_t expire);
  void ClearExpire();
  void Reset();
  void SetValue(Slice value);

  HashSubData() = default;
  explicit HashSubData(Slice value);
};

class SetMetadata : public Metadata {
 public:
  explicit SetMetadata(bool generate_version = true) : Metadata(kRedisSet, generate_version) {}
};

class ZSetMetadata : public Metadata {
 public:
  explicit ZSetMetadata(bool generate_version = true) : Metadata(kRedisZSet, generate_version) {}
};

class BitmapMetadata : public Metadata {
 public:
  explicit BitmapMetadata(bool generate_version = true) : Metadata(kRedisBitmap, generate_version) {}
};

class SortedintMetadata : public Metadata {
 public:
  explicit SortedintMetadata(bool generate_version = true) : Metadata(kRedisSortedint, generate_version) {}
};

class ListMetadata : public Metadata {
 public:
  uint64_t head;
  uint64_t tail;
  explicit ListMetadata(bool generate_version = true);

  void Encode(std::string *dst) override;
  using Metadata::Decode;
  rocksdb::Status Decode(Slice *input) override;
};

class StreamMetadata : public Metadata {
 public:
  redis::StreamEntryID last_generated_id;
  redis::StreamEntryID recorded_first_entry_id;
  redis::StreamEntryID max_deleted_entry_id;
  redis::StreamEntryID first_entry_id;
  redis::StreamEntryID last_entry_id;
  uint64_t entries_added = 0;
  uint64_t group_number = 0;

  explicit StreamMetadata(bool generate_version = true) : Metadata(kRedisStream, generate_version) {}

  void Encode(std::string *dst) override;
  using Metadata::Decode;
  rocksdb::Status Decode(Slice *input) override;
};

class BloomChainMetadata : public Metadata {
 public:
  /// The number of sub-filters
  uint16_t n_filters;

  /// Adding an element to a Bloom filter never fails due to the data structure "filling up". Instead the error rate
  /// starts to grow. To keep the error close to the one set on filter initialisation - the bloom filter will
  /// auto-scale, meaning when capacity is reached an additional sub-filter will be created.
  ///
  /// The capacity of the new sub-filter is the capacity of the last sub-filter multiplied by expansion.
  ///
  /// The default expansion value is 2.
  ///
  /// For non-scaling, expansion should be set to 0
  uint16_t expansion;

  /// The number of entries intended to be added to the filter. If your filter allows scaling, the capacity of the last
  /// sub-filter should be: base_capacity -> base_capacity * expansion -> base_capacity * expansion^2...
  ///
  /// The default base_capacity value is 100.
  uint32_t base_capacity;

  /// The desired probability for false positives.
  ///
  /// The rate is a decimal value between 0 and 1. For example, for a desired false positive rate of 0.1% (1 in 1000),
  /// error_rate should be set to 0.001.
  ///
  /// The default error_rate value is 0.01.
  double error_rate;

  /// The total number of bytes allocated for all sub-filters.
  uint32_t bloom_bytes;

  explicit BloomChainMetadata(bool generate_version = true) : Metadata(kRedisBloomFilter, generate_version) {}

  void Encode(std::string *dst) override;
  using Metadata::Decode;
  rocksdb::Status Decode(Slice *bytes) override;

  uint32_t GetCapacity() const;

  bool IsScaling() const { return expansion != 0; };
};

enum class JsonStorageFormat : uint8_t {
  JSON = 0,
  CBOR = 1,
};

class JsonMetadata : public Metadata {
 public:
  // to make JSON type more extensible,
  // we add a field to indicate the format of stored data
  JsonStorageFormat format = JsonStorageFormat::JSON;

  explicit JsonMetadata(bool generate_version = true) : Metadata(kRedisJson, generate_version) {}

  void Encode(std::string *dst) override;
  rocksdb::Status Decode(Slice *input) override;
};
