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

#include <cstdint>
#include <string>
#include <vector>

#include "common/db_util.h"
#include "common/range_spec.h"
#include "encoding.h"
#include "status.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

struct FieldValue {
  std::string field;
  std::string value;

  FieldValue(std::string f, std::string v) : field(std::move(f)), value(std::move(v)) {}
};

struct FieldValueView {
  std::string field;
  std::string_view value;

  FieldValueView(std::string &&f, std::string_view v) : field(std::move(f)), value(v) {}
};

enum class HashFetchType { kAll = 0, kOnlyKey = 1, kOnlyValue = 2 };

// A non-volatile field is treated as an infinite TTL for the purpose of GT and LT. The NX, XX, GT, and LT options are
// mutually exclusive.
enum class ExpireSetCond {
  NONE,  // directly set the expiration time
  NX,    // For each specified field, set expiration only when the field has no expiration.
  XX,    // For each specified field, set expiration only when the field has an existing expiration.
  GT,    // For each specified field, set expiration only when the new expiration is greater than current one.
  LT     // For each specified field, set expiration only when the new expiration is less than current one.
};

enum class GetTTLStatus { HGETEX_OK, HGETEX_GET_NO_FIELD, HGETEX_GET_NO_TTL };

enum SetPersistRes {
  HFE_PERSIST_NO_FIELD = -2, /* No such hash-field */
  HFE_PERSIST_NO_TTL = -1,   /* No TTL attached to the field */
  HFE_PERSIST_OK = 1
};

enum SetExRes {
  HSETEX_NO_FIELD = -2,        /* No such hash-field */
  HSETEX_NO_CONDITION_MET = 0, /* Specified NX | XX | GT | LT condition not met */
  HSETEX_OK = 1,               /* Expiration time set/updated as expected */
  HSETEX_DELETED = 2,          /* Field deleted because the specified time is in the past */
};

namespace redis {

class Hash : public SubKeyScanner {
 public:
  Hash(engine::Storage *storage, const std::string &ns) : SubKeyScanner(storage, ns) {}

  rocksdb::Status Size(const Slice &user_key, uint64_t *size);
  rocksdb::Status Get(const Slice &user_key, const Slice &field, std::string *value);
  rocksdb::Status Set(const Slice &user_key, const Slice &field, const Slice &value, uint64_t *added_cnt);
  rocksdb::Status Delete(const Slice &user_key, const std::vector<Slice> &fields, uint64_t *deleted_cnt);
  rocksdb::Status IncrBy(const Slice &user_key, const Slice &field, int64_t increment, int64_t *new_value);
  rocksdb::Status IncrByFloat(const Slice &user_key, const Slice &field, double increment, double *new_value);
  rocksdb::Status MSet(const Slice &user_key, std::vector<FieldValue> field_values, bool nx, uint64_t *added_cnt);
  rocksdb::Status RangeByLex(const Slice &user_key, const RangeLexSpec &spec, std::vector<FieldValue> *field_values,
                             int64_t *seek_count = nullptr);
  rocksdb::Status MGet(const Slice &user_key, const std::vector<Slice> &fields, std::vector<std::string> *values,
                       std::vector<rocksdb::Status> *statuses);
  rocksdb::Status GetAll(const Slice &user_key, std::vector<FieldValue> *field_values,
                         HashFetchType type = HashFetchType::kAll, int64_t *seek_count = nullptr);
  rocksdb::Status Scan(const Slice &user_key, std::string *cursor, uint64_t limit, const std::string &field_prefix,
                       std::vector<std::string> *fields, std::vector<std::string> *values = nullptr,
                       int64_t *seek_count = nullptr);
  rocksdb::Status RandField(const Slice &user_key, int64_t command_count, std::vector<FieldValue> *field_values,
                            HashFetchType type = HashFetchType::kOnlyKey, int64_t *seek_count = nullptr);
  rocksdb::Status HExpireAt(const Slice &user_key, const std::vector<Slice> &fields, uint64_t expire_at,
                            std::vector<SetExRes> &ret, ExpireSetCond option = ExpireSetCond::NONE);

  rocksdb::Status HExpireTime(const Slice &user_key, const std::vector<Slice> &fields,
                              std::vector<std::pair<GetTTLStatus, uint64_t>> &expire_time);

  rocksdb::Status HPersist(const Slice &user_key, const std::vector<Slice> &fields, std::vector<SetPersistRes> &ret);

  inline static bool IsHexpireOptionSatisfied(const HashSubData &sub_data, uint64_t expire_at, ExpireSetCond option) {
    switch (option) {
      case ExpireSetCond::NX:
        return !sub_data.HasTTL();
      case ExpireSetCond::XX:
        return sub_data.HasTTL();
      case ExpireSetCond::GT:
        return sub_data.HasTTL() && sub_data.expire < expire_at;
      case ExpireSetCond::LT:
        return !sub_data.HasTTL() || (sub_data.HasTTL() && sub_data.expire > expire_at);
      default:
        return true;
    }
  }

 private:
  friend class KKV;
  friend class RedisHashTest;

  rocksdb::Status GetMetadata(const Slice &ns_key, HashMetadata *metadata);
  rocksdb::Status getEx(HashMetadata &metadata, std::string &ns_key, const Slice &user_key, const Slice &field,
                        std::string *value);
  rocksdb::Status deleteEx(HashMetadata &metadata, const std::string &ns_key, const Slice &user_key,
                           const std::vector<Slice> &fields, uint64_t *deleted_cnt);
  rocksdb::Status incrByEx(rocksdb::Status &s, HashMetadata &metadata, const std::string &ns_key, const Slice &user_key,
                           const Slice &field, int64_t increment, int64_t *new_value);
  rocksdb::Status incrByFloatEx(rocksdb::Status &s, HashMetadata &metadata, const std::string &ns_key,
                                const Slice &user_key, const Slice &field, double increment, double *new_value);
  rocksdb::Status mSetEx(HashMetadata &metadata, std::string &ns_key, const Slice &user_key,
                         const std::vector<FieldValue> &field_values, bool nx, uint64_t *added_cnt);
  static rocksdb::Status mGetEx(HashMetadata &metadata, std::vector<std::string> *values,
                                std::vector<rocksdb::Slice> &keys, std::vector<rocksdb::PinnableSlice> &values_vector,
                                std::vector<rocksdb::Status> &statuses_vector, std::vector<rocksdb::Status> *statuses);
  rocksdb::Status getAllEx(HashMetadata &metadata, util::UniqueIterator &iter, const std::string &prefix_key,
                           std::string &ns_key, const Slice &user_key, std::vector<FieldValue> *field_values,
                           HashFetchType type, int64_t *seek_count);
  rocksdb::Status rangeByLexEx(HashMetadata &metadata, util::UniqueIterator &iter, const std::string &prefix_key,
                               const RangeLexSpec &spec, std::vector<FieldValue> *field_values, int64_t *seek_count);
  rocksdb::Status randFieldEx(const Slice &user_key, HashMetadata &metadata, uint64_t count, bool unique,
                              std::vector<FieldValue> *field_values, HashFetchType type, int64_t *seek_count);
  std::string getLogData(RedisType type, RedisHashCommand cmd_type, std::vector<kv::datanode::v1::FieldData> &&fields);
};

}  // namespace redis
