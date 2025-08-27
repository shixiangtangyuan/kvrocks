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

#include "redis_metadata.h"

#include <rocksdb/env.h>
#include <sys/time.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <string_view>

#include "cluster/redis_slot.h"
#include "encoding.h"
#include "glog/logging.h"
#include "time_util.h"

// 52 bit for microseconds and 11 bit for counter
const int VersionCounterBits = 11;

static std::atomic<uint64_t> version_counter_ = 0;

constexpr const char *kErrMetadataTooShort = "metadata is too short";
constexpr const char *kErrHashSubKVTooShort = "hash subkey value is too short";

std::atomic<bool> encode_hash_sub_flag{false};

InternalKey::InternalKey(Slice input, bool slot_id_encoded) : slot_id_encoded_(slot_id_encoded) {
  uint32_t key_size = 0;
  if (slot_id_encoded_) {
    GetFixed16(&input, &slotid_);
  }
  GetFixed32(&input, &key_size);
  key_ = Slice(input.data(), key_size);
  input.remove_prefix(key_size);
  GetFixed64(&input, &version_);
  sub_key_ = Slice(input.data(), input.size());
}

InternalKey::InternalKey(Slice ns_key, Slice sub_key, uint64_t version, bool slot_id_encoded)
    : sub_key_(sub_key), version_(version), slot_id_encoded_(slot_id_encoded) {
  if (slot_id_encoded_) {
    GetFixed16(&ns_key, &slotid_);
  }
  key_ = ns_key;
}

Slice InternalKey::GetNamespace() const { return "__namespace"; }

Slice InternalKey::GetKey() const { return key_; }

Slice InternalKey::GetSubKey() const { return sub_key_; }

uint64_t InternalKey::GetVersion() const { return version_; }

std::string InternalKey::Encode() const {
  std::string out;
  size_t total = 4 + key_.size() + 8 + sub_key_.size();
  if (slot_id_encoded_) {
    total += 2;
  }
  out.resize(total);
  auto buf = out.data();
  if (slot_id_encoded_) {
    buf = EncodeFixed16(buf, slotid_);
  }
  buf = EncodeFixed32(buf, static_cast<uint32_t>(key_.size()));
  buf = EncodeBuffer(buf, key_);
  buf = EncodeFixed64(buf, version_);
  EncodeBuffer(buf, sub_key_);
  return out;
}

bool InternalKey::operator==(const InternalKey &that) const {
  if (key_ != that.key_) return false;
  if (sub_key_ != that.sub_key_) return false;
  return version_ == that.version_;
}

template <typename T>
std::tuple<T, T> ExtractNamespaceKey(Slice ns_key, bool slot_id_encoded) {
  if (slot_id_encoded) {
    uint16_t slot_id = 0;
    GetFixed16(&ns_key, &slot_id);
  }

  T key = {ns_key.data(), ns_key.size()};
  return {"__namespace", key};
}

template std::tuple<Slice, Slice> ExtractNamespaceKey<Slice>(Slice ns_key, bool slot_id_encoded);
template std::tuple<std::string, std::string> ExtractNamespaceKey<std::string>(Slice ns_key, bool slot_id_encoded);

std::string ComposeNamespaceKey(const Slice &ns, const Slice &key, bool slot_id_encoded) {
  std::string ns_key;

  if (slot_id_encoded) {
    auto slot_id = GetSlotIdFromKey(key.ToStringView());
    PutFixed16(&ns_key, slot_id);
  }

  ns_key.append(key.data(), key.size());

  return ns_key;
}

std::string ComposeSlotKeyPrefix(const Slice &ns, int slotid) {
  std::string output;
  PutFixed16(&output, static_cast<uint16_t>(slotid));

  return output;
}

RedisType GetRedisTypeByName(const std::string &type_str) {
  std::string name = util::ToLower(type_str);
  if (!strcasecmp(type_str.c_str(), "string")) return RedisType::kRedisString;
  if (!strcasecmp(name.c_str(), "hash")) return RedisType::kRedisHash;
  if (!strcasecmp(name.c_str(), "list")) return RedisType::kRedisList;
  if (!strcasecmp(name.c_str(), "set")) return RedisType::kRedisSet;
  if (!strcasecmp(name.c_str(), "zset")) return RedisType::kRedisZSet;
  if (!strcasecmp(name.c_str(), "bitmap")) return RedisType::kRedisBitmap;
  if (!strcasecmp(name.c_str(), "sortedint")) return RedisType::kRedisSortedint;
  if (!strcasecmp(name.c_str(), "stream")) return RedisType::kRedisStream;
  if (!strcasecmp(name.c_str(), "MBbloom--")) return RedisType::kRedisBloomFilter;
  if (!strcasecmp(name.c_str(), "ReJSON-RL")) return RedisType::kRedisJson;
  return RedisType::kRedisNone;
}

Metadata::Metadata(RedisType type, bool generate_version)
    : flags(METADATA_TYPE_MASK & type),
      expire(0),
      version(generate_version ? generateVersion() : 0),
      size(0),
      persist_field_size(0),
      fields_max_expire_at(0) {
  if (type == kRedisHash && encode_hash_sub_flag.load()) {
    this->flags |= METADATA_HAS_SUB_FLAG_MASK;
  }
}

rocksdb::Status Metadata::Decode(Slice *input) {
  if (!GetFixed8(input, &flags)) {
    return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
  }

  if (HasTTL()) {
    if (!GetExpire(input)) {
      return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
    }
  } else {
    expire = 0;
  }

  if (!IsSingleKVType()) {
    if (input->size() < 8 + CommonEncodedSize()) {  // sizeof(version) + sizeof(size)
      return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
    }
    GetFixed64(input, &version);
    GetFixedCommon(input, &size);
  }

  if (Type() == kRedisHash) {
    if (IsSubTTLSet()) {
      if (!GetFixed64(input, &persist_field_size)) {
        return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
      }
      if (!GetFixed64(input, &fields_max_expire_at)) {
        return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
      }
    } else {
      persist_field_size = 0;
      fields_max_expire_at = 0;
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Metadata::Decode(Slice input) { return Decode(&input); }

void Metadata::Encode(std::string *dst) {
  if (expire > 0) {
    flags |= METADATA_TTL_MASK;
    PutFixed8(dst, flags);
    PutExpire(dst);
  } else {
    flags &= ~METADATA_TTL_MASK;
    PutFixed8(dst, flags);
  }

  if (!IsSingleKVType()) {
    PutFixed64(dst, version);
    PutFixedCommon(dst, size);
  }
  if (Type() == kRedisHash) {
    if (HasSubFlag() && IsSubTTLSet()) {
      PutFixed64(dst, persist_field_size);
      PutFixed64(dst, fields_max_expire_at);
    }
  }
  DCHECK_NE(flags & CONFLICT_TTL_AND_SUB_TTL_MASK, CONFLICT_TTL_AND_SUB_TTL_MASK);
}

void Metadata::InitVersionCounter() {
  // use random position for initial counter to avoid conflicts,
  // when the slave was promoted as master and the system clock may backoff
  version_counter_ = static_cast<uint64_t>(std::rand());
}

uint64_t Metadata::generateVersion() {
  uint64_t timestamp = util::GetTimeStampUS();
  uint64_t counter = version_counter_.fetch_add(1);
  return (timestamp << VersionCounterBits) + (counter % (1 << VersionCounterBits));
}

bool Metadata::operator==(const Metadata &that) const {
  if (flags != that.flags) return false;
  if (expire != that.expire) return false;
  if (!IsSingleKVType()) {
    if (size != that.size) return false;
    if (version != that.version) return false;
  }
  if (Type() == kRedisHash) {
    if (persist_field_size != that.persist_field_size) return false;
    if (fields_max_expire_at != that.fields_max_expire_at) return false;
  }
  return true;
}

RedisType Metadata::Type() const { return static_cast<RedisType>(flags & METADATA_TYPE_MASK); }

size_t Metadata::GetOffsetAfterExpire(uint8_t flags) {
  if (Metadata::HasTTL(flags)) {
    return 1 + 8;
  }
  return 1;
}

size_t Metadata::GetOffsetAfterSize(uint8_t flags) {
  if (Metadata::HasTTL(flags)) {
    return 1 + 8 + 8;
  }
  return 1 + 8;
}

uint64_t Metadata::ExpireMsToS(uint64_t ms) {
  if (ms == 0) {
    return 0;
  }

  if (ms < 1000) {
    return 1;
  }

  // We use rounding to get closer to the original value
  return (ms + 499) / 1000;
}

bool Metadata::Is64BitEncoded() const { return true; }

size_t Metadata::CommonEncodedSize() const { return Is64BitEncoded() ? 8 : 4; }

bool Metadata::GetFixedCommon(rocksdb::Slice *input, uint64_t *value) const {
  if (Is64BitEncoded()) {
    return GetFixed64(input, value);
  } else {
    uint32_t v = 0;
    bool res = GetFixed32(input, &v);
    *value = v;
    return res;
  }
}

bool Metadata::GetExpire(rocksdb::Slice *input) {
  uint64_t v = 0;

  if (!GetFixedCommon(input, &v)) {
    return false;
  }

  if (Is64BitEncoded()) {
    expire = v;
  } else {
    expire = v * 1000;
  }

  return true;
}

void Metadata::PutFixedCommon(std::string *dst, uint64_t value) const {
  if (Is64BitEncoded()) {
    PutFixed64(dst, value);
  } else {
    PutFixed32(dst, value);
  }
}

void Metadata::PutExpire(std::string *dst) const {
  if (Is64BitEncoded()) {
    PutFixed64(dst, expire);
  } else {
    PutFixed32(dst, ExpireMsToS(expire));
  }
}

bool Metadata::HasTTL(uint8_t flags) { return (flags & METADATA_TTL_MASK); }
bool Metadata::HasTTL() const { return (flags & METADATA_TTL_MASK); }

int64_t Metadata::TTL() const {
  if (expire == 0) {
    return -1;
  }

  auto now = util::GetTimeStampMS();
  if (expire < now) {
    return -2;
  }

  return int64_t(expire - now);
}

timeval Metadata::Time() const {
  auto t = version >> VersionCounterBits;
  timeval created_at{static_cast<uint32_t>(t / 1000000), static_cast<int32_t>(t % 1000000)};
  return created_at;
}

bool Metadata::ExpireAt(uint64_t expired_ts) const {
  if (!IsEmptyableType() && size == 0) {
    return true;
  }
  if (expire == 0) {
    return false;
  }

  return expire < expired_ts;
}

bool Metadata::IsSingleKVType() const { return Type() == kRedisString || Type() == kRedisJson; }

bool Metadata::IsEmptyableType() const {
  return IsSingleKVType() || Type() == kRedisStream || Type() == kRedisBloomFilter;
}

bool Metadata::Expired() const {
  if (Type() == kRedisHash && IsSubTTLSet()) {
    return persist_field_size == 0 && fields_max_expire_at < util::GetTimeStampMS();
  }
  return ExpireAt(util::GetTimeStampMS());
}

bool Metadata::HasSubFlag() const { return flags & METADATA_HAS_SUB_FLAG_MASK; }

bool Metadata::IsSubTTLSet() const { return flags & METADATA_SUB_TTL_SET_MASK; }

ListMetadata::ListMetadata(bool generate_version)
    : Metadata(kRedisList, generate_version), head(UINT64_MAX / 2), tail(head) {}

void ListMetadata::Encode(std::string *dst) {
  Metadata::Encode(dst);
  PutFixed64(dst, head);
  PutFixed64(dst, tail);
}

rocksdb::Status ListMetadata::Decode(Slice *input) {
  if (auto s = Metadata::Decode(input); !s.ok()) {
    return s;
  }
  if (Type() == kRedisList) {
    if (input->size() < 8 + 8) {
      return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
    }
    GetFixed64(input, &head);
    GetFixed64(input, &tail);
  }
  return rocksdb::Status::OK();
}

HashMetadata::HashMetadata(bool generate_version) : Metadata(kRedisHash, generate_version) {}

rocksdb::Status HashMetadata::SetSubTTL(uint64_t persist_field_size, uint64_t fields_max_expire_at) {
  if (HasTTL()) {
    return rocksdb::Status::InvalidArgument("cannot set sub ttl when meta key has ttl");
  }
  // old version, no sub key flag
  if (!HasSubFlag()) {
    return rocksdb::Status::InvalidArgument("cannot set sub ttl when meta key has no sub flag");
  }
  flags |= METADATA_SUB_TTL_SET_MASK;
  this->persist_field_size = persist_field_size;
  this->fields_max_expire_at = fields_max_expire_at;
  return rocksdb::Status::OK();
}

HashSubData::HashSubData(Slice value) : flags(0), expire(0), value(value) {}

bool HashSubData::HasTTL() const { return (flags & HASH_SUB_KEY_HAS_TTL_MASK); }

bool HashSubData::Expired(uint64_t curr_ts) const {
  if (HasTTL()) {
    return curr_ts == 0 ? expire < util::GetTimeStampMS() : expire < curr_ts;
  }
  return false;
}

bool HashSubData::operator==(const HashSubData &rhs) const {
  return flags == rhs.flags && expire == rhs.expire && value == rhs.value;
}

bool HashSubData::operator!=(const HashSubData &rhs) const { return !((*this) == rhs); }

void HashSubData::Encode(Metadata *metadata, std::string *dst) const {
  // old version, no sub key flag
  if (!metadata->HasSubFlag()) {
    dst->append(value.data(), value.size());
    return;
  }
  PutFixed8(dst, flags);
  if (HasTTL()) {
    PutFixed64(dst, expire);
  }
  dst->append(value.data(), value.size());
}

rocksdb::Status HashSubData::Decode(Metadata *metadata, rocksdb::PinnableSlice *input, uint64_t curr_ts) {
  DCHECK(metadata != nullptr);
  DCHECK(input != nullptr);
  rocksdb::Slice s = rocksdb::Slice(input->data(), input->size());
  return Decode(metadata->HasSubFlag(), s, curr_ts);
}

rocksdb::Status HashSubData::Decode(Metadata *metadata, std::string *input, uint64_t curr_ts) {
  DCHECK(metadata != nullptr);
  DCHECK(input != nullptr);
  rocksdb::Slice s = rocksdb::Slice(input->data(), input->size());
  return Decode(metadata->HasSubFlag(), s, curr_ts);
}

rocksdb::Status HashSubData::Decode(Metadata *metadata, rocksdb::Slice input, uint64_t curr_ts) {
  DCHECK(metadata != nullptr);
  DCHECK(input != nullptr);
  return Decode(metadata->HasSubFlag(), input, curr_ts);
}

rocksdb::Status HashSubData::Decode(bool has_sub_flag, rocksdb::Slice input, uint64_t curr_ts, bool keep_result) {
  DCHECK(input != nullptr);
  // old version, no sub key flag
  value = input;
  if (!has_sub_flag) {
    flags = 0;
    expire = 0;
    return rocksdb::Status::OK();
  }

  if (!GetFixed8(&value, &flags)) {
    return rocksdb::Status::InvalidArgument(kErrHashSubKVTooShort);
  }

  if (HasTTL()) {
    if (!GetFixed64(&value, &expire)) {
      return rocksdb::Status::InvalidArgument(kErrHashSubKVTooShort);
    }
  }
  if (Expired(curr_ts)) {
    // NOTE(mingfo): The decoded data may be used during parsing the WriteBatch in WriteBatchExtractor,
    // here the keep_result flag determines whether to retain it.
    if (!keep_result) Reset();
    return rocksdb::Status::NotFound("hash subkey expired");
  }
  return rocksdb::Status::OK();
}

void HashSubData::SetExpire(uint64_t expire) {
  if (expire == 0) {
    flags &= ~HASH_SUB_KEY_HAS_TTL_MASK;
  } else {
    flags |= HASH_SUB_KEY_HAS_TTL_MASK;
  }
  this->expire = expire;
}

void HashSubData::ClearExpire() {
  flags &= ~HASH_SUB_KEY_HAS_TTL_MASK;
  expire = 0;
}

void HashSubData::Reset() {
  flags = 0;
  expire = 0;
  value = Slice();
}

void HashSubData::SetValue(Slice value) { this->value = value; }

void StreamMetadata::Encode(std::string *dst) {
  Metadata::Encode(dst);

  PutFixed64(dst, last_generated_id.ms);
  PutFixed64(dst, last_generated_id.seq);

  PutFixed64(dst, recorded_first_entry_id.ms);
  PutFixed64(dst, recorded_first_entry_id.seq);

  PutFixed64(dst, max_deleted_entry_id.ms);
  PutFixed64(dst, max_deleted_entry_id.seq);

  PutFixed64(dst, first_entry_id.ms);
  PutFixed64(dst, first_entry_id.seq);

  PutFixed64(dst, last_entry_id.ms);
  PutFixed64(dst, last_entry_id.seq);

  PutFixed64(dst, entries_added);
  PutFixed64(dst, group_number);
}

rocksdb::Status StreamMetadata::Decode(Slice *input) {
  if (auto s = Metadata::Decode(input); !s.ok()) {
    return s;
  }

  if (input->size() < 8 * 11) {
    return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
  }

  GetFixed64(input, &last_generated_id.ms);
  GetFixed64(input, &last_generated_id.seq);

  GetFixed64(input, &recorded_first_entry_id.ms);
  GetFixed64(input, &recorded_first_entry_id.seq);

  GetFixed64(input, &max_deleted_entry_id.ms);
  GetFixed64(input, &max_deleted_entry_id.seq);

  GetFixed64(input, &first_entry_id.ms);
  GetFixed64(input, &first_entry_id.seq);

  GetFixed64(input, &last_entry_id.ms);
  GetFixed64(input, &last_entry_id.seq);

  GetFixed64(input, &entries_added);

  if (input->size() >= 8) {
    GetFixed64(input, &group_number);
  }

  return rocksdb::Status::OK();
}

void BloomChainMetadata::Encode(std::string *dst) {
  Metadata::Encode(dst);

  PutFixed16(dst, n_filters);
  PutFixed16(dst, expansion);

  PutFixed32(dst, base_capacity);
  PutDouble(dst, error_rate);
  PutFixed32(dst, bloom_bytes);
}

rocksdb::Status BloomChainMetadata::Decode(Slice *input) {
  if (auto s = Metadata::Decode(input); !s.ok()) {
    return s;
  }

  if (input->size() < 20) {
    return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
  }

  GetFixed16(input, &n_filters);
  GetFixed16(input, &expansion);

  GetFixed32(input, &base_capacity);
  GetDouble(input, &error_rate);
  GetFixed32(input, &bloom_bytes);

  return rocksdb::Status::OK();
}

uint32_t BloomChainMetadata::GetCapacity() const {
  // non-scaling
  if (expansion == 0) {
    return base_capacity;
  }

  // the sum of Geometric progression
  if (expansion == 1) {
    return base_capacity * n_filters;
  }
  return static_cast<uint32_t>(base_capacity * (1 - pow(expansion, n_filters)) / (1 - expansion));
}

void JsonMetadata::Encode(std::string *dst) {
  Metadata::Encode(dst);

  PutFixed8(dst, uint8_t(format));
}

rocksdb::Status JsonMetadata::Decode(Slice *input) {
  if (auto s = Metadata::Decode(input); !s.ok()) {
    return s;
  }

  if (!GetFixed8(input, reinterpret_cast<uint8_t *>(&format))) {
    return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
  }

  return rocksdb::Status::OK();
}

// Batch extractor funcs
RedisHashCommand GetHashCmdEqType(RedisHashCommand cmd_type) {
  switch (cmd_type) {
    case RedisHashCommand::kCmdHIncrby:
    case RedisHashCommand::kCmdHIncrbyFloat:
    case RedisHashCommand::kCmdHSet:
    case RedisHashCommand::kCmdHSetNX:
    case RedisHashCommand::kCmdHMSet:
      return RedisHashCommand::kCmdHSet;
    case RedisHashCommand::kCmdHDel:
      return RedisHashCommand::kCmdHDel;
    case RedisHashCommand::kCmdHExpire:
    case RedisHashCommand::kCmdHExpireAt:
    case RedisHashCommand::kCmdHPExpire:
    case RedisHashCommand::kCmdHPExpireAt:
      return RedisHashCommand::kCmdHPExpireAt;
    case RedisHashCommand::kCmdHPersist:
      return RedisHashCommand::kCmdHPersist;
    // kkv cmds
    case RedisHashCommand::kCmdKKVHSet:
    case RedisHashCommand::kCmdKKVHCAS:
      return RedisHashCommand::kCmdKKVHSet;
    case RedisHashCommand::kCmdKKVHSetNX:
      return RedisHashCommand::kCmdKKVHSetNX;
    case RedisHashCommand::kCmdKKVHCAD:
      return RedisHashCommand::kCmdHDel;
    case RedisHashCommand::kCmdKKVHRemRangeByLex:
      return RedisHashCommand::kCmdKKVHRemRangeByLex;

    default:
      return RedisHashCommand::kCmdNone;
  }
  return RedisHashCommand::kCmdNone;
}

RedisStringCommand GetStringCmdEqType(RedisStringCommand cmd_type) {
  switch (cmd_type) {
    case RedisStringCommand::kCmdSet:
    case RedisStringCommand::kCmdSetEX:
    case RedisStringCommand::kCmdSetNX:
    case RedisStringCommand::kCmdMSet:
    case RedisStringCommand::kCmdIncr:
    case RedisStringCommand::kCmdIncrBy:
    case RedisStringCommand::kCmdIncrByFloat:
    case RedisStringCommand::kCmdDecr:
    case RedisStringCommand::kCmdDecrBy:
      return RedisStringCommand::kCmdSet;
    default:
      break;
  }
  return RedisStringCommand::kCmdNone;
}

RedisListCommand GetListCmdEqType(RedisListCommand cmd_type) {
  switch (cmd_type) {
    case RedisListCommand::kCmdLPush:
    case RedisListCommand::kCmdLPushX:
      return RedisListCommand::kCmdLPush;
    case RedisListCommand::kCmdRPush:
    case RedisListCommand::kCmdRPushX:
      return RedisListCommand::kCmdRPush;
    case RedisListCommand::kCmdLPop:
      return RedisListCommand::kCmdLPop;
    case RedisListCommand::kCmdRPop:
      return RedisListCommand::kCmdRPop;
    case RedisListCommand::kCmdLRem:
      return RedisListCommand::kCmdLRem;
    case RedisListCommand::kCmdLTrim:
      return RedisListCommand::kCmdLTrim;
    case RedisListCommand::kCmdLSet:
      return RedisListCommand::kCmdLSet;
    case RedisListCommand::kCmdLInsert:
      return RedisListCommand::kCmdLInsert;
    default:
      break;
  }

  return RedisListCommand::kCmdNone;
}

RedisSetCommand GetSetCmdEqType(RedisSetCommand cmd_type) {
  switch (cmd_type) {
    case RedisSetCommand::kCmdSAdd:
      return RedisSetCommand::kCmdSAdd;
    case RedisSetCommand::kCmdSRem:
      return RedisSetCommand::kCmdSRem;
    case RedisSetCommand::kCmdSPop:
      return RedisSetCommand::kCmdSPop;
    default:
      break;
  }

  return RedisSetCommand::kCmdNone;
}

RedisZSetCommand GetZSetCmdEqType(RedisZSetCommand cmd_type) {
  switch (cmd_type) {
    case RedisZSetCommand::kCmdZAdd:
      return RedisZSetCommand::kCmdZAdd;
    case RedisZSetCommand::kCmdZRem:
    case RedisZSetCommand::kCmdZRemRangeByRank:
    case RedisZSetCommand::kCmdZRemRangeByScore:
    case RedisZSetCommand::kCmdZPopMin:
    case RedisZSetCommand::kCmdZPopMax:
      return RedisZSetCommand::kCmdZRem;

    default:
      break;
  }

  return RedisZSetCommand::kCmdNone;
}

RedisBitmapCommand GetBitmapCmdEqType(RedisBitmapCommand cmd_type) {
  // TODO: currently, bitmap cmds are not supported
  return RedisBitmapCommand::kCmdNone;
}

RedisKeyCommand GetKeyCmdEqType(RedisKeyCommand cmd_type) {
  switch (cmd_type) {
    case RedisKeyCommand::kCmdExpire:
    case RedisKeyCommand::kCmdExpireAt:
    case RedisKeyCommand::kCmdPExpire:
    case RedisKeyCommand::kCmdPExpireAt:
      return RedisKeyCommand::kCmdPExpireAt;
    case RedisKeyCommand::kCmdDel:
    case RedisKeyCommand::kCmdUnlink:
      return RedisKeyCommand::kCmdDel;
    default:
      break;
  }

  return RedisKeyCommand::kCmdNone;
}
