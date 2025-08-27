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

#include <rocksdb/status.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <random>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "db_util.h"
#include "glog/logging.h"
#include "parse_util.h"
#include "redis_hash.h"
#include "rocksdb/slice.h"
#include "storage/redis_metadata.h"
#include "time_util.h"

namespace redis {

rocksdb::Status Hash::getEx(HashMetadata &metadata, std::string &ns_key, const Slice &user_key, const Slice &field,
                            std::string *value) {
  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  StringOrPinSlice pin_value;
  rocksdb::Status s = storage_->Get(read_options, sub_key, &pin_value);
  if (!s.ok()) return s;
  HashSubData sub_data;
  s = sub_data.Decode(&metadata, &pin_value);
  if (!s.ok()) return s;
  *value = sub_data.value.ToString();
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::incrByEx(rocksdb::Status &s, HashMetadata &metadata, const std::string &ns_key,
                               const Slice &user_key, const Slice &field, int64_t increment, int64_t *new_value) {
  HashSubData sub_data;
  int64_t old_value = 0;
  bool update_meta = false;
  std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  if (s.ok()) {
    StringOrPinSlice read_bytes;
    s = storage_->Get(rocksdb::ReadOptions(), sub_key, &read_bytes);
    if (s.ok()) {
      s = sub_data.Decode(&metadata, rocksdb::Slice(read_bytes.data(), read_bytes.size()));
      if (s.ok()) {
        auto val = sub_data.value;
        auto parse_result = SimpleAtoi(val.data(), val.size());
        if (!parse_result) {
          return rocksdb::Status::InvalidArgument("hash value is not an integer");
        }
        if (isspace(val[0])) {
          return rocksdb::Status::InvalidArgument("hash value is not an integer");
        }
        old_value = *parse_result;
      } else if (s.IsNotFound()) {
        // create when subkey expired
        if (metadata.IsSubTTLSet()) {
          ++metadata.persist_field_size;
          update_meta = true;
        }
      } else {
        return s;
      }
    } else if (s.IsNotFound()) {
      // create when subkey not exist
      if (metadata.IsSubTTLSet()) {
        ++metadata.persist_field_size;
      }
      ++metadata.size;
      update_meta = true;
    } else {
      return s;
    }
  } else {
    // create when subkey not exist
    if (metadata.IsSubTTLSet()) {
      ++metadata.persist_field_size;
    }
    ++metadata.size;
    update_meta = true;
  }
  if ((increment < 0 && old_value < 0 && increment < (LLONG_MIN - old_value)) ||
      (increment > 0 && old_value > 0 && increment > (LLONG_MAX - old_value))) {
    return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
  }

  *new_value = old_value + increment;
  auto value_str = std::to_string(*new_value);
  sub_data.value = value_str;
  std::string subdata_str;
  sub_data.Encode(&metadata, &subdata_str);
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(
      kRedisHash, {EnumToString(RedisHashCommand::kCmdHIncrby), std::to_string(RedisHashCodec::kHasFieldTTL)});
  batch->PutLogData(log_data.Encode());
  batch->Put(sub_key, subdata_str);
  if (update_meta) {
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::incrByFloatEx(rocksdb::Status &s, HashMetadata &metadata, const std::string &ns_key,
                                    const Slice &user_key, const Slice &field, double increment, double *new_value) {
  HashSubData sub_data;
  double old_value = 0;
  bool update_meta = false;
  std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  if (s.ok()) {
    StringOrPinSlice read_bytes;
    s = storage_->Get(rocksdb::ReadOptions(), sub_key, &read_bytes);
    if (s.ok()) {
      s = sub_data.Decode(&metadata, rocksdb::Slice(read_bytes.data(), read_bytes.size()));
      if (s.ok()) {
        auto val = sub_data.value;
        auto parse_result = SimpleAtod(val.data(), val.size());
        if (!parse_result || isspace(val[0])) {
          return rocksdb::Status::InvalidArgument("hash value is not a float");
        }
        old_value = *parse_result;
      } else if (s.IsNotFound()) {
        // create when subkey expired
        if (metadata.IsSubTTLSet()) {
          ++metadata.persist_field_size;
          update_meta = true;
        }
      } else {
        return s;
      }
    } else if (s.IsNotFound()) {
      // create when subkey not exist
      if (metadata.IsSubTTLSet()) {
        ++metadata.persist_field_size;
      }
      ++metadata.size;
      update_meta = true;
    } else {
      return s;
    }
  } else {
    // create when subkey not exist
    if (metadata.IsSubTTLSet()) {
      ++metadata.persist_field_size;
    }
    ++metadata.size;
    update_meta = true;
  }
  double n = old_value + increment;
  if (std::isinf(n) || std::isnan(n)) {
    return rocksdb::Status::InvalidArgument("increment would produce NaN or Infinity");
  }

  *new_value = n;
  auto value_str = std::to_string(*new_value);
  sub_data.value = value_str;
  std::string subdata_str;
  sub_data.Encode(&metadata, &subdata_str);
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(
      kRedisHash, {EnumToString(RedisHashCommand::kCmdHIncrbyFloat), std::to_string(RedisHashCodec::kHasFieldTTL)});
  batch->PutLogData(log_data.Encode());
  batch->Put(sub_key, subdata_str);
  if (update_meta) {
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::mGetEx(HashMetadata &metadata, std::vector<std::string> *values,
                             std::vector<rocksdb::Slice> &keys, std::vector<rocksdb::PinnableSlice> &values_vector,
                             std::vector<rocksdb::Status> &statuses_vector, std::vector<rocksdb::Status> *statuses) {
  for (size_t i = 0; i < keys.size(); i++) {
    if (!statuses_vector[i].ok() && !statuses_vector[i].IsNotFound()) return statuses_vector[i];
    if (statuses_vector[i].IsNotFound()) {
      values->emplace_back(values_vector[i].ToString());
      statuses->emplace_back(statuses_vector[i]);
      continue;
    }
    HashSubData sub_data;
    rocksdb::Status s = sub_data.Decode(&metadata, &values_vector[i]);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.IsNotFound()) {
      values->emplace_back("");
      statuses->emplace_back(rocksdb::Status::NotFound("key has expired"));
      continue;
    }
    values->emplace_back(sub_data.value.ToString());
    statuses->emplace_back(statuses_vector[i]);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::deleteEx(HashMetadata &metadata, const std::string &ns_key, const Slice &user_key,
                               const std::vector<Slice> &fields, uint64_t *deleted_cnt) {
  uint64_t delta_persist_field_cnt = 0;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash,
                             {EnumToString(RedisHashCommand::kCmdHDel), std::to_string(RedisHashCodec::kHasFieldTTL)});
  batch->PutLogData(log_data.Encode());

  std::unordered_set<std::string_view> field_set;
  for (const auto &field : fields) {
    if (!field_set.emplace(field.ToStringView()).second) {
      continue;
    }
    std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    StringOrPinSlice pin_value;
    rocksdb::Status s = storage_->Get(rocksdb::ReadOptions(), sub_key, &pin_value);
    if (s.ok()) {
      if (metadata.IsSubTTLSet() && metadata.HasSubFlag()) {
        HashSubData sub_data;
        s = sub_data.Decode(&metadata, &pin_value);
        if (s.ok()) {
          // update persist_field_size
          if (!sub_data.HasTTL()) {
            delta_persist_field_cnt--;
          }
        } else if (s.IsNotFound()) {  // expired, do nothing
          continue;
        } else {
          return s;
        }
      }
      *deleted_cnt += 1;
      batch->Delete(sub_key);
    } else if (s.IsNotFound()) {
      continue;
    } else {
      // error
      return s;
    }
  }
  if (*deleted_cnt == 0) {
    return rocksdb::Status::OK();
  }
  metadata.size -= *deleted_cnt;
  if (metadata.IsSubTTLSet()) {
    metadata.persist_field_size += delta_persist_field_cnt;
  }
  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::mSetEx(HashMetadata &metadata, std::string &ns_key, const Slice &user_key,
                             const std::vector<FieldValue> &field_values, bool nx, uint64_t *added_cnt) {
  int added = 0;
  uint64_t delta_persist = 0;
  uint64_t delta_size = 0;
  uint32_t changed = 0;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash,
                             {EnumToString(RedisHashCommand::kCmdHSet), std::to_string(RedisHashCodec::kHasFieldTTL)});
  batch->PutLogData(log_data.Encode());
  std::unordered_set<std::string_view> field_set;
  for (auto it = field_values.rbegin(); it != field_values.rend(); it++) {
    if (!field_set.insert(it->field).second) {
      continue;
    }

    std::string sub_key = InternalKey(ns_key, it->field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    HashSubData input_sub_data;
    input_sub_data.value = it->value;
    if (metadata.size > 0) {
      StringOrPinSlice pin_value;
      rocksdb::Status s = storage_->Get(rocksdb::ReadOptions(), sub_key, &pin_value);
      if (s.ok()) {
        HashSubData old_sub_data;
        s = old_sub_data.Decode(&metadata, &pin_value);
        if (s.ok()) {
          // Conditions for not updating:
          // 1. `nx` is true and the key exists.
          // 2. The old value is equal to the new value, and the old value has no TTL.
          if (nx || (!old_sub_data.HasTTL() && old_sub_data.value == input_sub_data.value)) continue;
          if (old_sub_data.HasTTL()) {
            delta_persist++;
          }
        } else if (s.IsNotFound()) {  // notfound, bytes exist but expired, not add size
          added++;
          delta_persist++;
        } else {
          return s;
        }
      } else if (s.IsNotFound()) {  // bytes not found
        delta_size++;
        delta_persist++;
        added++;
      } else {
        return s;
      }
    } else {
      delta_size++;
      delta_persist++;
      added++;
    }

    std::string in_str;
    input_sub_data.Encode(&metadata, &in_str);
    batch->Put(sub_key, in_str);
    changed++;
  }

  // if no changes, return immediately
  if (changed == 0) {
    return rocksdb::Status::OK();
  }

  if (added > 0) {
    *added_cnt = added;
  }
  if (delta_size > 0 || delta_persist > 0) {
    metadata.size += delta_size;
    if (metadata.IsSubTTLSet()) {
      metadata.persist_field_size += delta_persist;
    }
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::getAllEx(HashMetadata &metadata, util::UniqueIterator &iter, const std::string &prefix_key,
                               std::string &ns_key, const Slice &user_key, std::vector<FieldValue> *field_values,
                               HashFetchType type, int64_t *seek_count) {
  auto curr_ts = util::GetTimeStampMS();
  *seek_count = 1;
  for (iter->Seek(prefix_key); iter->Valid() && iter->key().starts_with(prefix_key); iter->Next(), *seek_count += 1) {
    HashSubData sub_data;
    rocksdb::Status s = sub_data.Decode(&metadata, iter->value(), curr_ts);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        continue;
      } else {
        return s;
      }
    }
    if (type == HashFetchType::kOnlyKey) {
      InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
      field_values->emplace_back(ikey.GetSubKey().ToString(), "");
    } else if (type == HashFetchType::kOnlyValue) {
      field_values->emplace_back("", sub_data.value.ToString());
    } else {
      InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
      field_values->emplace_back(ikey.GetSubKey().ToString(), sub_data.value.ToString());
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::rangeByLexEx(HashMetadata &metadata, util::UniqueIterator &iter, const std::string &prefix_key,
                                   const RangeLexSpec &spec, std::vector<FieldValue> *field_values,
                                   int64_t *seek_count) {
  int64_t op_count = 1;
  if (!seek_count) {
    seek_count = &op_count;
  }
  int64_t pos = 0;
  auto curr_ts = util::GetTimeStampMS();
  for (; iter->Valid() && iter->key().starts_with(prefix_key);
       (!spec.reversed ? (iter->Next(), *seek_count += 1) : (iter->Prev(), *seek_count += 1))) {
    HashSubData sub_data;
    auto s = sub_data.Decode(&metadata, iter->value(), curr_ts);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        continue;
      } else {
        return s;
      }
    }
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    if (spec.reversed) {
      if (ikey.GetSubKey().ToString() < spec.min || (spec.minex && ikey.GetSubKey().ToString() == spec.min)) {
        break;
      }
      if ((spec.maxex && ikey.GetSubKey().ToString() == spec.max) ||
          (!spec.max_infinite && ikey.GetSubKey().ToString() > spec.max)) {
        continue;
      }
    } else {
      if (spec.minex && ikey.GetSubKey().ToString() == spec.min) continue;  // the min member was exclusive
      if ((spec.maxex && ikey.GetSubKey().ToString() == spec.max) ||
          (!spec.max_infinite && ikey.GetSubKey().ToString() > spec.max))
        break;
    }
    if (spec.offset >= 0 && pos++ < spec.offset) continue;

    field_values->emplace_back(ikey.GetSubKey().ToString(), sub_data.value.ToString());
    if (spec.count > 0 && field_values->size() >= static_cast<unsigned>(spec.count)) break;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::HExpireAt(const Slice &user_key, const std::vector<Slice> &fields, uint64_t expire_at,
                                std::vector<SetExRes> &ret, ExpireSetCond option) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  // metadata not found
  if (s.IsNotFound()) {
    for (size_t i = 0; i < fields.size(); i++) {
      ret.emplace_back(SetExRes::HSETEX_NO_FIELD);
    }
    DCHECK(ret.size() == fields.size());
    return rocksdb::Status::OK();
  }
  // metadata has ttl
  if (metadata.HasTTL()) {
    return rocksdb::Status::InvalidArgument("cannot set sub ttl when meta key has ttl");
  }
  // old version
  if (!metadata.HasSubFlag()) {
    return rocksdb::Status::InvalidArgument("cannot set sub ttl when meta key has no sub flag");
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash, {EnumToString(RedisHashCommand::kCmdHPExpireAt), std::to_string(expire_at)});
  batch->PutLogData(log_data.Encode());

  int64_t changed = 0;
  int64_t delta_persist_field = 0;
  int64_t delta_size = 0;
  auto now_timestamp_ms = util::GetTimeStampMS();
  bool delete_field = false;
  bool update_meta = false;
  if (expire_at <= now_timestamp_ms) {
    delete_field = true;
  }
  bool sub_ttl_updated = false;

  std::unordered_map<std::string_view, SetExRes> repeated_fields_ret(fields.size());

  for (const auto &field : fields) {
    auto it = repeated_fields_ret.find(field.ToStringView());
    if (it != repeated_fields_ret.end()) {
      ret.emplace_back(it->second);
      continue;
    }
    std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    StringOrPinSlice pin_value;
    s = storage_->Get(rocksdb::ReadOptions(), sub_key, &pin_value);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.IsNotFound()) {
      ret.emplace_back(SetExRes::HSETEX_NO_FIELD);
      repeated_fields_ret.emplace(field.ToStringView(), SetExRes::HSETEX_NO_FIELD);
      continue;
    }
    HashSubData sub_data;
    s = sub_data.Decode(&metadata, &pin_value);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.IsNotFound()) {
      // field expire
      ret.emplace_back(SetExRes::HSETEX_NO_FIELD);
      repeated_fields_ret.emplace(field.ToStringView(), SetExRes::HSETEX_NO_FIELD);
      continue;
    }
    if (!IsHexpireOptionSatisfied(sub_data, expire_at, option)) {
      // not satisfy user condition
      ret.emplace_back(SetExRes::HSETEX_NO_CONDITION_MET);
      repeated_fields_ret.emplace(field.ToStringView(), SetExRes::HSETEX_NO_CONDITION_MET);
      continue;
    }
    if (!sub_data.HasTTL()) {
      delta_persist_field--;
    }

    if (delete_field) {
      delta_size--;
      batch->Delete(sub_key);
      changed++;
      ret.emplace_back(SetExRes::HSETEX_DELETED);
      // Cause the field was deleted, next repeated field not exist
      repeated_fields_ret.emplace(field.ToStringView(), SetExRes::HSETEX_NO_FIELD);
    } else {
      sub_ttl_updated = true;
      sub_data.SetExpire(expire_at);
      std::string in_str;
      sub_data.Encode(&metadata, &in_str);
      batch->Put(sub_key, in_str);
      changed++;
      ret.emplace_back(SetExRes::HSETEX_OK);
      switch (option) {
        case ExpireSetCond::NONE:
        case ExpireSetCond::XX:
          repeated_fields_ret.emplace(field.ToStringView(), SetExRes::HSETEX_OK);
          break;
        default:
          // Because the field's TTL was updated, the NX, LT, and GT conditions no longer hold for the next repeated
          // field.
          repeated_fields_ret.emplace(field.ToStringView(), SetExRes::HSETEX_NO_CONDITION_MET);
          break;
      }
    }
  }

  if (changed == 0) {
    return rocksdb::Status::OK();
  }

  if (!delete_field) {
    if (sub_ttl_updated && !metadata.IsSubTTLSet()) {
      update_meta = true;
      metadata.SetSubTTL(metadata.size, expire_at);
    }
    if (sub_ttl_updated && delta_persist_field != 0) {
      update_meta = true;
      metadata.persist_field_size += delta_persist_field;
    }
    if (sub_ttl_updated && metadata.fields_max_expire_at < expire_at) {
      update_meta = true;
      metadata.fields_max_expire_at = expire_at;
    }
  } else {
    update_meta = true;
    metadata.size += delta_size;
    if (metadata.IsSubTTLSet()) {
      metadata.persist_field_size += delta_persist_field;
    }
  }
  if (update_meta) {
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }
  DCHECK(ret.size() == fields.size());
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::HExpireTime(const Slice &user_key, const std::vector<Slice> &fields,
                                  std::vector<std::pair<GetTTLStatus, uint64_t>> &expire_time) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  // metadata not found
  if (s.IsNotFound()) {
    for (size_t i = 0; i < fields.size(); i++) {
      expire_time.emplace_back(GetTTLStatus::HGETEX_GET_NO_FIELD, 0);
    }
    DCHECK(expire_time.size() == fields.size());
    return rocksdb::Status::OK();
  }

  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options = storage_->DefaultMultiGetOptions();
  read_options.snapshot = ss.GetSnapShot();
  std::vector<rocksdb::Slice> keys;

  std::vector<std::string> sub_keys;
  sub_keys.resize(fields.size());
  for (size_t i = 0; i < fields.size(); i++) {
    auto &field = fields[i];
    sub_keys[i] = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    keys.emplace_back(sub_keys[i]);
  }

  std::vector<rocksdb::PinnableSlice> values_vector(keys.size());
  std::vector<rocksdb::Status> statuses_vector(keys.size());
  storage_->MultiGet(read_options, storage_->GetDB()->DefaultColumnFamily(), keys.size(), keys.data(),
                     values_vector.data(), statuses_vector.data());
  for (size_t i = 0; i < keys.size(); i++) {
    if (!statuses_vector[i].ok() && !statuses_vector[i].IsNotFound()) return statuses_vector[i];
    if (statuses_vector[i].IsNotFound()) {
      expire_time.emplace_back(GetTTLStatus::HGETEX_GET_NO_FIELD, 0);
      continue;
    }
    HashSubData sub_data;
    s = sub_data.Decode(&metadata, &values_vector[i]);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.IsNotFound()) {
      // field expire
      expire_time.emplace_back(GetTTLStatus::HGETEX_GET_NO_FIELD, 0);
      continue;
    }
    if (!sub_data.HasTTL()) {
      expire_time.emplace_back(GetTTLStatus::HGETEX_GET_NO_TTL, 0);
      continue;
    }
    expire_time.emplace_back(GetTTLStatus::HGETEX_OK, sub_data.expire);
  }

  DCHECK(expire_time.size() == fields.size());
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::HPersist(const Slice &user_key, const std::vector<Slice> &fields,
                               std::vector<SetPersistRes> &ret) {
  uint64_t persist_field_add = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  // metadata not found
  if (s.IsNotFound()) {
    for (size_t i = 0; i < fields.size(); i++) {
      ret.emplace_back(SetPersistRes::HFE_PERSIST_NO_FIELD);
    }
    DCHECK(ret.size() == fields.size());
    return rocksdb::Status::OK();
  }

  // metadata has ttl
  if (metadata.HasTTL()) {
    return rocksdb::Status::InvalidArgument("cannot set sub ttl when meta key has ttl");
  }
  // old version
  if (!metadata.HasSubFlag()) {
    return rocksdb::Status::InvalidArgument("cannot set sub ttl when meta key has no sub flag");
  }

  // metadata not set sub ttl
  if (!metadata.IsSubTTLSet()) {
    for (size_t i = 0; i < fields.size(); i++) {
      ret.emplace_back(SetPersistRes::HFE_PERSIST_NO_TTL);
    }
    DCHECK(ret.size() == fields.size());
    return rocksdb::Status::OK();
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash, {EnumToString(RedisHashCommand::kCmdHPersist)});
  batch->PutLogData(log_data.Encode());

  std::unordered_map<std::string_view, SetPersistRes> repeated_fields_ret(fields.size());
  for (const auto &field : fields) {
    auto it = repeated_fields_ret.find(field.ToStringView());
    if (it != repeated_fields_ret.end()) {
      ret.emplace_back(it->second);
      continue;
    }
    std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    StringOrPinSlice pin_value;
    s = storage_->Get(rocksdb::ReadOptions(), sub_key, &pin_value);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.IsNotFound()) {
      ret.emplace_back(SetPersistRes::HFE_PERSIST_NO_FIELD);
      repeated_fields_ret.emplace(field.ToStringView(), SetPersistRes::HFE_PERSIST_NO_FIELD);
      continue;
    }
    HashSubData sub_data;
    s = sub_data.Decode(&metadata, &pin_value);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.IsNotFound()) {
      // field expire
      ret.emplace_back(SetPersistRes::HFE_PERSIST_NO_FIELD);
      repeated_fields_ret.emplace(field.ToStringView(), SetPersistRes::HFE_PERSIST_NO_FIELD);
      continue;
    }
    if (!sub_data.HasTTL()) {
      ret.emplace_back(SetPersistRes::HFE_PERSIST_NO_TTL);
      repeated_fields_ret.emplace(field.ToStringView(), SetPersistRes::HFE_PERSIST_NO_TTL);
      continue;
    }

    sub_data.ClearExpire();
    persist_field_add++;
    std::string in_str;
    sub_data.Encode(&metadata, &in_str);
    batch->Put(sub_key, in_str);
    ret.emplace_back(SetPersistRes::HFE_PERSIST_OK);
    // The next repeated field no longer has the TTL.
    repeated_fields_ret.emplace(field.ToStringView(), SetPersistRes::HFE_PERSIST_NO_TTL);
  }

  if (persist_field_add > 0) {
    metadata.persist_field_size += persist_field_add;
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  } else {
    return rocksdb::Status::OK();
  }

  DCHECK(ret.size() == fields.size());
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::randFieldEx(const Slice &user_key, HashMetadata &metadata, uint64_t count, bool unique,
                                  std::vector<FieldValue> *field_values, HashFetchType type, int64_t *seek_count) {
  std::vector<FieldValue> samples;
  // TODO: Getting all values in Hash might be heavy, consider lazy-loading these values later
  if (count == 0) return rocksdb::Status::OK();
  rocksdb::Status s = GetAll(user_key, &samples, type, seek_count);
  if (!s.ok()) return s;
  uint64_t size = samples.size();
  auto append_field_with_index = [field_values, &samples, type](uint64_t index) {
    if (type == HashFetchType::kAll) {
      field_values->emplace_back(samples[index].field, samples[index].value);
    } else {
      field_values->emplace_back(samples[index].field, "");
    }
  };
  field_values->reserve(std::min(size, count));
  if (!unique || count == 1) {
    // Case 1: Negative count, randomly select elements or without parameter
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dis(0, size - 1);
    for (uint64_t i = 0; i < count; i++) {
      uint64_t index = dis(gen);
      append_field_with_index(index);
    }
  } else if (size <= count) {
    // Case 2: Requested count is greater than or equal to the number of elements inside the hash
    for (uint64_t i = 0; i < size; i++) {
      append_field_with_index(i);
    }
  } else {
    // Case 3: Requested count is less than the number of elements inside the hash
    std::vector<uint64_t> indices(size);
    std::iota(indices.begin(), indices.end(), 0);
    std::shuffle(indices.begin(), indices.end(),
                 std::random_device{});  // use Fisher-Yates shuffle algorithm to randomize the order
    for (uint64_t i = 0; i < count; i++) {
      uint64_t index = indices[i];
      append_field_with_index(index);
    }
  }
  return rocksdb::Status::OK();
}

}  // namespace redis
