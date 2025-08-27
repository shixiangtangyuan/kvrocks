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

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <climits>
#include <cmath>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "commands/commander.h"
#include "common/scope_exit.h"
#include "rocksdb/slice.h"
#include "status.h"
#include "storage/redis_metadata.h"
#include "test_base.h"
#include "time_util.h"
#include "types/redis_hash.h"

namespace redis {

class RedisHashTest : public TestBase {
 public:
  rocksdb::Status GetMetaData(Slice user_key, HashMetadata* meta_data) {
    auto meta_key = hash_->AppendNamespacePrefix(user_key);
    return hash_->GetMetadata(meta_key, meta_data);
  }

  void CheckMetaData(Slice user_key, HashMetadata* expect, int line_num) {
    HashMetadata actual;
    auto s = GetMetaData(user_key, &actual);
    if (!expect) {
      EXPECT_TRUE(s.IsNotFound()) << "line " << line_num;
      return;
    }
    EXPECT_TRUE(s.ok()) << "line " << line_num;
    EXPECT_EQ(actual.size, expect->size) << "line " << line_num;
    EXPECT_EQ(actual.HasSubFlag(), expect->HasSubFlag()) << "line " << line_num;
    EXPECT_EQ(actual.IsSubTTLSet(), expect->IsSubTTLSet()) << "line " << line_num;
    EXPECT_EQ(actual.fields_max_expire_at, expect->fields_max_expire_at) << "line " << line_num;
    EXPECT_EQ(actual.persist_field_size, expect->persist_field_size) << "line " << line_num;
  }

  static HashMetadata VoidHashMetaData() {
    HashMetadata exp_meta;
    if (encode_hash_sub_flag.load()) {
      EXPECT_TRUE(exp_meta.HasSubFlag());
    } else {
      EXPECT_FALSE(exp_meta.HasSubFlag());
    }
    EXPECT_FALSE(exp_meta.IsSubTTLSet());
    EXPECT_EQ(exp_meta.expire, 0);
    EXPECT_EQ(exp_meta.fields_max_expire_at, 0);
    EXPECT_EQ(exp_meta.size, 0);
    EXPECT_EQ(exp_meta.persist_field_size, 0);
    return exp_meta;
  }

 protected:
  explicit RedisHashTest() { hash_ = std::make_unique<redis::Hash>(storage_, "hash_ns"); }
  ~RedisHashTest() override = default;

  void SetUp() override {
    key_ = "test_hash->key";
    fields_ = {"test-hash-key-1", "test-hash-key-2", "test-hash-key-3"};
    values_ = {"hash-test-value-1", "hash-test-value-2", "hash-test-value-3"};
    config_->enable_hfe_cmd = true;

    config_->enable_cdc_sync = GetParam();
  }
  void TearDown() override { config_->enable_hfe_cmd = false; }

  std::unique_ptr<redis::Hash> hash_;
};

TEST_P(RedisHashTest, GetAndSet) {
  auto prev = encode_hash_sub_flag.exchange(false);
  auto exist = MakeScopeExit([&prev]() { encode_hash_sub_flag.store(prev); });
  HashMetadata exp_meta = VoidHashMetaData();
  EXPECT_FALSE(exp_meta.HasSubFlag());
  uint64_t ret = 0;
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
    exp_meta.size++;
    CheckMetaData(key_, &exp_meta, __LINE__);
  }
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string got;
    auto s = hash_->Get(key_, fields_[i], &got);
    EXPECT_EQ(s.ToString(), "OK");
    EXPECT_EQ(values_[i], got);
  }
  auto s = hash_->Delete(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = hash_->Del(key_);
}

TEST_P(RedisHashTest, MGetAndMSet) {
  auto prev = encode_hash_sub_flag.exchange(false);
  auto exist = MakeScopeExit([&prev]() { encode_hash_sub_flag.store(prev); });
  HashMetadata exp_meta = VoidHashMetaData();
  EXPECT_FALSE(exp_meta.HasSubFlag());
  uint64_t ret = 0;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < fields_.size(); i++) {
    fvs.emplace_back(fields_[i].ToString(), values_[i].ToString());
  }
  std::vector<FieldValue> fvs1(fvs);
  auto s = hash_->MSet(key_, fvs1, false, &ret);
  EXPECT_TRUE(s.ok() && fvs.size() == ret);
  exp_meta.size = fvs.size();
  CheckMetaData(key_, &exp_meta, __LINE__);
  std::vector<FieldValue> fvs2(fvs);
  s = hash_->MSet(key_, fvs2, false, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(ret, 0);
  CheckMetaData(key_, &exp_meta, __LINE__);
  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses;
  s = hash_->MGet(key_, fields_, &values, &statuses);
  EXPECT_TRUE(s.ok());
  for (size_t i = 0; i < fields_.size(); i++) {
    EXPECT_EQ(values[i], values_[i].ToString());
  }
  s = hash_->Delete(key_, fields_, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(static_cast<int>(fields_.size()), ret);
  s = hash_->Del(key_);
}

TEST_P(RedisHashTest, MSetAndDeleteRepeated) {
  auto prev = encode_hash_sub_flag.exchange(false);
  auto exist = MakeScopeExit([&prev]() { encode_hash_sub_flag.store(prev); });
  HashMetadata exp_meta = VoidHashMetaData();
  EXPECT_FALSE(exp_meta.HasSubFlag());
  std::vector<std::string> fields{"f1", "f1", "f2", "f3"};
  std::vector<std::string> values{"v1", "v11", "v2", "v3"};
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < fields.size(); i++) {
    fvs.emplace_back(fields[i], values[i]);
  }

  uint64_t ret = 0;
  rocksdb::Status s = hash_->MSet(key_, fvs, false, &ret);
  EXPECT_TRUE(s.ok() && static_cast<uint64_t>(fvs.size() - 1) == ret);
  exp_meta.size = fvs.size() - 1;
  CheckMetaData(key_, &exp_meta, __LINE__);
  std::string got;
  s = hash_->Get(key_, "f1", &got);
  EXPECT_EQ("v11", got);

  s = hash_->Size(key_, &ret);
  EXPECT_TRUE(s.ok() && ret == static_cast<uint64_t>(fvs.size() - 1));

  std::vector<rocksdb::Slice> fields_to_delete{"f1", "f2", "f2"};
  s = hash_->Delete(key_, fields_to_delete, &ret);
  EXPECT_TRUE(s.ok() && ret == static_cast<uint64_t>(fields_to_delete.size() - 1));
  s = hash_->Size(key_, &ret);
  EXPECT_TRUE(s.ok() && ret == 1);
  exp_meta.size = 1;
  CheckMetaData(key_, &exp_meta, __LINE__);
  s = hash_->Get(key_, "f3", &got);
  EXPECT_EQ("v3", got);

  s = hash_->Del(key_);
}

TEST_P(RedisHashTest, MSetSingleFieldAndNX) {
  auto prev = encode_hash_sub_flag.exchange(false);
  auto exist = MakeScopeExit([&prev]() { encode_hash_sub_flag.store(prev); });
  HashMetadata exp_meta = VoidHashMetaData();
  EXPECT_FALSE(exp_meta.HasSubFlag());

  uint64_t ret = 0;
  std::vector<FieldValue> values = {{"field-one", "value-one"}};
  std::vector<FieldValue> values1(values);
  auto s = hash_->MSet(key_, values1, true, &ret);
  EXPECT_TRUE(s.ok() && ret == 1);
  exp_meta.size = 1;
  CheckMetaData(key_, &exp_meta, __LINE__);
  std::string field2 = "field-two";
  std::string initial_value = "value-two";
  s = hash_->Set(key_, field2, initial_value, &ret);
  EXPECT_TRUE(s.ok() && ret == 1);
  exp_meta.size = 2;
  CheckMetaData(key_, &exp_meta, __LINE__);
  values = {{field2, "value-two-changed"}};
  std::vector<FieldValue> values2(values);
  s = hash_->MSet(key_, values2, true, &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  CheckMetaData(key_, &exp_meta, __LINE__);
  std::string final_value;
  s = hash_->Get(key_, field2, &final_value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(initial_value, final_value);

  s = hash_->Del(key_);
}

TEST_P(RedisHashTest, MSetMultipleFieldsAndNX) {
  auto prev = encode_hash_sub_flag.exchange(false);
  auto exist = MakeScopeExit([&prev]() { encode_hash_sub_flag.store(prev); });
  HashMetadata exp_meta = VoidHashMetaData();
  EXPECT_FALSE(exp_meta.HasSubFlag());

  uint64_t ret = 0;
  std::vector<FieldValue> values = {{"field-one", "value-one"}, {"field-two", "value-two"}};
  std::vector<FieldValue> values1(values);
  auto s = hash_->MSet(key_, values1, true, &ret);
  EXPECT_TRUE(s.ok() && ret == 2);
  exp_meta.size = 2;
  CheckMetaData(key_, &exp_meta, __LINE__);

  values = {{"field-one", "value-one"}, {"field-two", "value-two-changed"}, {"field-three", "value-three"}};
  std::vector<FieldValue> values2(values);
  s = hash_->MSet(key_, values2, true, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(ret, 1);
  exp_meta.size++;
  CheckMetaData(key_, &exp_meta, __LINE__);

  std::string value;
  s = hash_->Get(key_, "field-one", &value);
  EXPECT_TRUE(s.ok() && value == "value-one");

  s = hash_->Get(key_, "field-two", &value);
  EXPECT_TRUE(s.ok() && value == "value-two");

  s = hash_->Get(key_, "field-three", &value);
  EXPECT_TRUE(s.ok() && value == "value-three");

  s = hash_->Del(key_);
}

TEST_P(RedisHashTest, HGetAll) {
  uint64_t ret = 0;
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  std::vector<FieldValue> fvs;
  auto s = hash_->GetAll(key_, &fvs);
  EXPECT_TRUE(s.ok() && fvs.size() == fields_.size());
  s = hash_->Delete(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = hash_->Del(key_);
}

TEST_P(RedisHashTest, HGetAllWithExpireField) {
  auto prev = encode_hash_sub_flag.exchange(true);
  auto exist = MakeScopeExit([this, prev]() {
    auto s = hash_->Del(key_);
    EXPECT_TRUE(s.ok());
    encode_hash_sub_flag.store(prev);
  });
  // set fields
  uint64_t ret = 0;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < 5; i++) {
    fvs.emplace_back("field-" + std::to_string(i), "value-" + std::to_string(i));
  }
  auto s = hash_->MSet(key_, fvs, false, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(fvs.size(), ret);
  // expire partial fields
  std::vector<SetExRes> hexpire_ret;
  auto expire_ts = util::GetTimeStampMS() + 50;
  std::vector<Slice> expired_fields{fvs[0].field, fvs[2].field, fvs[4].field};
  s = hash_->HExpireAt(key_, expired_fields, expire_ts, hexpire_ret);
  EXPECT_TRUE(s.ok());
  // check fields
  usleep(60000);
  std::vector<FieldValue> result;
  s = hash_->GetAll(key_, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].field, fvs[1].field);
  EXPECT_EQ(result[0].value, fvs[1].value);
  EXPECT_EQ(result[1].field, fvs[3].field);
  EXPECT_EQ(result[1].value, fvs[3].value);
}

TEST_P(RedisHashTest, HIncr) {
  auto prev = encode_hash_sub_flag.exchange(true);
  Slice key("hincr"), field1("hincr-1"), field2("hincr-2");
  auto exist = MakeScopeExit([this, &prev, &key]() {
    auto s = this->hash_->Del(key);
    EXPECT_TRUE(s.ok());
    encode_hash_sub_flag.store(prev);
  });
  // incr key without ttl
  int64_t value = 0;
  for (int i = 0; i < 32; i++) {
    auto s = hash_->IncrBy(key, field1, 1, &value);
    EXPECT_TRUE(s.ok());
  }
  std::string bytes;
  hash_->Get(key, field1, &bytes);
  ASSERT_EQ(bytes, "32");
  HashMetadata exp_meta;
  ++exp_meta.size;
  CheckMetaData(key, &exp_meta, __LINE__);
  // incr unexpired key
  auto expire_ts = util::GetTimeStampMS() + 50;
  std::vector<SetExRes> ret;
  auto s = hash_->IncrBy(key, field2, 1, &value);
  EXPECT_TRUE(s.ok());
  s = hash_->HExpireAt(key, {field2}, expire_ts, ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ++exp_meta.size;
  exp_meta.SetSubTTL(1, expire_ts);
  CheckMetaData(key, &exp_meta, __LINE__);
  s = hash_->IncrBy(key, field2, 1, &value);
  EXPECT_TRUE(s.ok());
  bytes.clear();
  s = hash_->Get(key, field2, &bytes);
  EXPECT_TRUE(s.ok());
  ASSERT_EQ(bytes, "2");
  CheckMetaData(key, &exp_meta, __LINE__);
  // incr expired key
  usleep(60000);
  s = hash_->IncrBy(key, field2, -1, &value);
  EXPECT_TRUE(s.ok());
  bytes.clear();
  hash_->Get(key, field2, &bytes);
  ASSERT_EQ(bytes, "-1");
  ++exp_meta.persist_field_size;
  CheckMetaData(key, &exp_meta, __LINE__);
}

TEST_P(RedisHashTest, HIncrInvalid) {
  uint64_t ret = 0;
  int64_t value = 0;
  double float_value = 0;
  Slice field("hash-incrby-invalid-field");
  auto s = hash_->IncrBy(key_, field, 1, &value);
  EXPECT_TRUE(s.ok() && value == 1);
  // incr exceed upper bound
  s = hash_->IncrByFloat(key_, field, INFINITY, &float_value);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
  s = hash_->IncrBy(key_, field, LLONG_MAX, &value);
  EXPECT_TRUE(s.IsInvalidArgument());
  hash_->Set(key_, field, "abc", &ret);
  s = hash_->IncrBy(key_, field, 1, &value);
  EXPECT_TRUE(s.IsInvalidArgument());
  // incr exceed lower bound
  hash_->Set(key_, field, "-1", &ret);
  s = hash_->IncrBy(key_, field, -1, &value);
  EXPECT_TRUE(s.ok());
  s = hash_->IncrByFloat(key_, field, -1.0 * INFINITY, &float_value);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
  s = hash_->IncrBy(key_, field, LLONG_MIN, &value);
  EXPECT_TRUE(s.IsInvalidArgument());
  // incr a nan
  s = hash_->IncrByFloat(key_, field, NAN, &float_value);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
  s = hash_->IncrByFloat(key_, field, -NAN, &float_value);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
  s = hash_->Del(key_);
}

TEST_P(RedisHashTest, HIncrByFloat) {
  auto prev = encode_hash_sub_flag.exchange(true);
  Slice key("hincrbyfloat"), field1("hincrbyfloat-1"), field2("hincrbyfloat-2");
  auto exist = MakeScopeExit([this, &prev, &key]() {
    auto s = this->hash_->Del(key);
    encode_hash_sub_flag.store(prev);
  });
  // incr key without ttl
  double value = 0.0;
  for (int i = 0; i < 32; i++) {
    auto s = hash_->IncrByFloat(key, field1, 1.2, &value);
    EXPECT_TRUE(s.ok());
  }
  std::string bytes;
  hash_->Get(key, field1, &bytes);
  value = std::stof(bytes);
  EXPECT_FLOAT_EQ(32 * 1.2, value);
  HashMetadata exp_meta;
  ++exp_meta.size;
  CheckMetaData(key, &exp_meta, __LINE__);
  // incr unexpired key
  auto s = hash_->IncrByFloat(key, field2, 1.2, &value);
  EXPECT_TRUE(s.ok());
  auto expire_ts = util::GetTimeStampMS() + 50;
  std::vector<SetExRes> ret;
  s = hash_->HExpireAt(key, {field2}, expire_ts, ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ++exp_meta.size;
  exp_meta.SetSubTTL(1, expire_ts);
  CheckMetaData(key, &exp_meta, __LINE__);
  s = hash_->IncrByFloat(key, field2, 1.2, &value);
  EXPECT_TRUE(s.ok());
  bytes.clear();
  s = hash_->Get(key, field2, &bytes);
  EXPECT_TRUE(s.ok());
  ASSERT_EQ(bytes, std::to_string(1.2 * 2));
  CheckMetaData(key, &exp_meta, __LINE__);
  // incr expired key
  usleep(60000);
  s = hash_->IncrByFloat(key, field2, 1.2, &value);
  EXPECT_TRUE(s.ok());
  bytes.clear();
  hash_->Get(key, field2, &bytes);
  ASSERT_EQ(bytes, std::to_string(1.2));
  ++exp_meta.persist_field_size;
  CheckMetaData(key, &exp_meta, __LINE__);
}

TEST_P(RedisHashTest, HRangeByLex) {
  uint64_t ret = 0;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < 4; i++) {
    fvs.emplace_back("key" + std::to_string(i), "value" + std::to_string(i));
  }
  for (size_t i = 0; i < 26; i++) {
    fvs.emplace_back(std::to_string(char(i + 'a')), std::to_string(char(i + 'a')));
  }

  std::random_device rd;
  std::mt19937 g(rd());
  std::vector<FieldValue> tmp(fvs);
  for (size_t i = 0; i < 100; i++) {
    std::shuffle(tmp.begin(), tmp.end(), g);
    std::vector<FieldValue> tmp1(tmp);
    auto s = hash_->MSet(key_, tmp1, false, &ret);
    EXPECT_TRUE(s.ok() && tmp.size() == ret);
    std::vector<FieldValue> tmp2(fvs);
    s = hash_->MSet(key_, tmp2, false, &ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(ret, 0);
    std::vector<FieldValue> result;
    RangeLexSpec spec;
    spec.offset = 0;
    spec.count = INT_MAX;
    spec.min = "key0";
    spec.max = "key3";
    s = hash_->RangeByLex(key_, spec, &result);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(4, result.size());
    EXPECT_EQ("key0", result[0].field);
    EXPECT_EQ("key1", result[1].field);
    EXPECT_EQ("key2", result[2].field);
    EXPECT_EQ("key3", result[3].field);
    s = hash_->Del(key_);
  }

  std::vector<FieldValue> tmp1(tmp);
  auto s = hash_->MSet(key_, tmp1, false, &ret);
  EXPECT_TRUE(s.ok() && tmp.size() == ret);
  // use offset and count
  std::vector<FieldValue> result;
  RangeLexSpec spec;
  spec.offset = 0;
  spec.count = INT_MAX;
  spec.min = "key0";
  spec.max = "key3";
  spec.offset = 1;
  s = hash_->RangeByLex(key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("key1", result[0].field);
  EXPECT_EQ("key2", result[1].field);
  EXPECT_EQ("key3", result[2].field);

  spec.offset = 1;
  spec.count = 1;
  s = hash_->RangeByLex(key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(1, result.size());
  EXPECT_EQ("key1", result[0].field);

  spec.offset = 0;
  spec.count = 0;
  s = hash_->RangeByLex(key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(0, result.size());

  spec.offset = 1000;
  spec.count = 1000;
  s = hash_->RangeByLex(key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(0, result.size());
  // exclusive range
  spec.offset = 0;
  spec.count = -1;
  spec.minex = true;
  s = hash_->RangeByLex(key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("key1", result[0].field);
  EXPECT_EQ("key2", result[1].field);
  EXPECT_EQ("key3", result[2].field);

  spec.offset = 0;
  spec.count = -1;
  spec.maxex = true;
  spec.minex = false;
  s = hash_->RangeByLex(key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("key0", result[0].field);
  EXPECT_EQ("key1", result[1].field);
  EXPECT_EQ("key2", result[2].field);

  spec.offset = 0;
  spec.count = -1;
  spec.maxex = true;
  spec.minex = true;
  s = hash_->RangeByLex(key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(2, result.size());
  EXPECT_EQ("key1", result[0].field);
  EXPECT_EQ("key2", result[1].field);

  // inf and reversed
  spec.minex = false;
  spec.maxex = false;
  spec.min = "-";
  spec.max = "+";
  spec.max_infinite = true;
  spec.reversed = true;
  s = hash_->RangeByLex(key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(4 + 26, result.size());
  EXPECT_EQ("key3", result[0].field);
  EXPECT_EQ("key2", result[1].field);
  EXPECT_EQ("key1", result[2].field);
  EXPECT_EQ("key0", result[3].field);
  s = hash_->Del(key_);
}

TEST_P(RedisHashTest, HRangeByLexNonExistingKey) {
  std::vector<FieldValue> result;
  RangeLexSpec spec;
  spec.offset = 0;
  spec.count = INT_MAX;
  spec.min = "any-start-key";
  spec.max = "any-end-key";
  auto s = hash_->RangeByLex("non-existing-key", spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(result.size(), 0);
}

TEST_P(RedisHashTest, HRangeByLexWithExpiredSubKey) {
  auto prev = encode_hash_sub_flag.exchange(true);
  auto exist = MakeScopeExit([this, prev]() {
    auto s = hash_->Del(key_);
    EXPECT_TRUE(s.ok());
    encode_hash_sub_flag.store(prev);
  });
  // set fields
  uint64_t ret = 0;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < 5; i++) {
    fvs.emplace_back("field-" + std::to_string(i), "value-" + std::to_string(i));
  }
  auto s = hash_->MSet(key_, fvs, false, &ret);
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(fvs.size(), ret);
  // expire partial fields
  std::vector<SetExRes> hexpire_ret;
  auto expire_ts = util::GetTimeStampMS() + 50;
  std::vector<Slice> expire_fields{fvs[0].field, fvs[2].field, fvs[4].field};
  s = hash_->HExpireAt(key_, expire_fields, expire_ts, hexpire_ret);
  EXPECT_TRUE(s.ok());
  // check fields
  usleep(60000);
  RangeLexSpec spec;
  spec.min = fvs[0].field;
  spec.max = fvs[4].field;
  std::vector<FieldValue> result;
  s = hash_->RangeByLex(key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].field, fvs[1].field);
  EXPECT_EQ(result[0].value, fvs[1].value);
  EXPECT_EQ(result[1].field, fvs[3].field);
  EXPECT_EQ(result[1].value, fvs[3].value);
}

TEST_P(RedisHashTest, HRandField) {
  uint64_t ret = 0;
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  auto size = static_cast<int64_t>(fields_.size());
  std::vector<FieldValue> fvs;
  // Case 1: Negative count, randomly select elements
  fvs.clear();
  auto s = hash_->RandField(key_, -(size + 10), &fvs);
  EXPECT_TRUE(s.ok() && fvs.size() == (fields_.size() + 10));

  // Case 2: Requested count is greater than or equal to the number of elements inside the hash
  fvs.clear();
  s = hash_->RandField(key_, size + 1, &fvs);
  EXPECT_TRUE(s.ok() && fvs.size() == fields_.size());

  // Case 3: Requested count is less than the number of elements inside the hash
  fvs.clear();
  s = hash_->RandField(key_, size - 1, &fvs);
  EXPECT_TRUE(s.ok() && fvs.size() == fields_.size() - 1);

  // hrandfield key 0
  fvs.clear();
  s = hash_->RandField(key_, 0, &fvs);
  EXPECT_TRUE(s.ok() && fvs.size() == 0);

  s = hash_->Del(key_);
}

TEST_P(RedisHashTest, HRandFieldEx) {
  auto prev = encode_hash_sub_flag.exchange(true);
  auto exist = MakeScopeExit([this, prev]() {
    auto s = hash_->Del(key_);
    EXPECT_TRUE(s.ok());
    encode_hash_sub_flag.store(prev);
  });

  uint64_t ret = 0;
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  // expire a field
  auto expire_ts = util::GetTimeStampMS() + 10;
  std::vector<SetExRes> rets;
  auto s = hash_->HExpireAt(key_, {fields_[0]}, expire_ts, rets);
  EXPECT_TRUE(s.ok());
  usleep(20000);

  std::string value;
  s = hash_->Get(key_, fields_[0], &value);
  EXPECT_TRUE(s.IsNotFound());

  int64_t size = 2;
  std::vector<FieldValue> fvs;
  // Case 1: Negative count, randomly select elements
  fvs.clear();
  s = hash_->RandField(key_, -(size + 10), &fvs);

  EXPECT_TRUE(s.ok() && static_cast<int64_t>(fvs.size()) == (size + 10));
  for (const auto& fv : fvs) {
    EXPECT_NE(fv.field, fields_[0]);
  }

  // Case 2: Requested count is greater than or equal to the number of elements inside the hash
  fvs.clear();
  s = hash_->RandField(key_, size + 2, &fvs);
  EXPECT_TRUE(s.ok() && static_cast<int64_t>(fvs.size()) == size);
  for (const auto& fv : fvs) {
    EXPECT_NE(fv.field, fields_[0]);
  }

  // Case 3: Requested count is less than the number of elements inside the hash
  fvs.clear();
  s = hash_->RandField(key_, size - 1, &fvs);
  EXPECT_TRUE(s.ok() && static_cast<int64_t>(fvs.size()) == size - 1);
  for (const auto& fv : fvs) {
    EXPECT_NE(fv.field, fields_[0]);
  }

  // hrandfield key 0
  fvs.clear();
  s = hash_->RandField(key_, 0, &fvs);
  EXPECT_TRUE(s.ok() && fvs.size() == 0);
}

TEST_P(RedisHashTest, HExpireAtOption) {
  auto prev = encode_hash_sub_flag.exchange(true);
  auto exist = MakeScopeExit([this, &prev]() {
    auto s = this->hash_->Del(key_);
    EXPECT_TRUE(s.ok());
    encode_hash_sub_flag.store(prev);
  });
  HashMetadata exp_meta = VoidHashMetaData();
  uint64_t expire_at = 10 * 1000 + util::GetTimeStampMS();
  uint64_t ret = 0;
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  exp_meta.size += fields_.size();
  CheckMetaData(key_, &exp_meta, __LINE__);
  // 1.Expire XX
  std::vector<Slice> expire_fields = {fields_[0], fields_[1]};
  std::vector<SetExRes> set_ttl_ret;
  auto s = hash_->HExpireAt(key_, expire_fields, expire_at, set_ttl_ret, ExpireSetCond::XX);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(set_ttl_ret[0], 0);
  EXPECT_EQ(set_ttl_ret[1], 0);
  CheckMetaData(key_, &exp_meta, __LINE__);
  std::vector<std::pair<GetTTLStatus, uint64_t>> expire_time;
  s = hash_->HExpireTime(key_, fields_, expire_time);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(expire_time.size(), fields_.size());
  EXPECT_EQ(expire_time[0].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[0].second, 0);
  EXPECT_EQ(expire_time[1].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[1].second, 0);
  EXPECT_EQ(expire_time[2].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[2].second, 0);
  // 2.Expire GT when no ttl set
  set_ttl_ret.clear();
  s = hash_->HExpireAt(key_, expire_fields, expire_at, set_ttl_ret, ExpireSetCond::GT);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(set_ttl_ret[0], 0);
  EXPECT_EQ(set_ttl_ret[1], 0);
  CheckMetaData(key_, &exp_meta, __LINE__);
  expire_time.clear();
  s = hash_->HExpireTime(key_, fields_, expire_time);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(expire_time.size(), fields_.size());
  EXPECT_EQ(expire_time[0].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[0].second, 0);
  EXPECT_EQ(expire_time[1].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[1].second, 0);
  EXPECT_EQ(expire_time[2].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[2].second, 0);
  // 3.Expire LT when no ttl set
  set_ttl_ret.clear();
  s = hash_->HExpireAt(key_, expire_fields, expire_at, set_ttl_ret, ExpireSetCond::LT);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(set_ttl_ret[0], 1);
  EXPECT_EQ(set_ttl_ret[1], 1);
  exp_meta.flags |= METADATA_SUB_TTL_SET_MASK;
  exp_meta.fields_max_expire_at = expire_at;
  exp_meta.persist_field_size = 1;
  CheckMetaData(key_, &exp_meta, __LINE__);
  expire_time.clear();
  s = hash_->HExpireTime(key_, fields_, expire_time);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(expire_time.size(), fields_.size());
  EXPECT_EQ(expire_time[0].first, GetTTLStatus::HGETEX_OK);
  EXPECT_EQ(expire_time[0].second, expire_at);
  EXPECT_EQ(expire_time[1].first, GetTTLStatus::HGETEX_OK);
  EXPECT_EQ(expire_time[1].second, expire_at);
  EXPECT_EQ(expire_time[2].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[2].second, 0);
  // 4.Expire LT when ttl set
  set_ttl_ret.clear();
  s = hash_->HExpireAt(key_, expire_fields, expire_at + 1, set_ttl_ret, ExpireSetCond::LT);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(set_ttl_ret[0], 0);
  EXPECT_EQ(set_ttl_ret[1], 0);
  CheckMetaData(key_, &exp_meta, __LINE__);
  expire_time.clear();
  s = hash_->HExpireTime(key_, fields_, expire_time);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(expire_time.size(), fields_.size());
  EXPECT_EQ(expire_time[0].first, GetTTLStatus::HGETEX_OK);
  EXPECT_EQ(expire_time[0].second, expire_at);
  EXPECT_EQ(expire_time[1].first, GetTTLStatus::HGETEX_OK);
  EXPECT_EQ(expire_time[1].second, expire_at);
  EXPECT_EQ(expire_time[2].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[2].second, 0);
  // 5.Expire GT when ttl set
  set_ttl_ret.clear();
  s = hash_->HExpireAt(key_, expire_fields, expire_at + 1, set_ttl_ret, ExpireSetCond::GT);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(set_ttl_ret[0], 1);
  EXPECT_EQ(set_ttl_ret[1], 1);
  exp_meta.fields_max_expire_at = expire_at + 1;
  CheckMetaData(key_, &exp_meta, __LINE__);
  expire_time.clear();
  s = hash_->HExpireTime(key_, fields_, expire_time);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(expire_time.size(), fields_.size());
  EXPECT_EQ(expire_time[0].first, GetTTLStatus::HGETEX_OK);
  EXPECT_EQ(expire_time[0].second, expire_at + 1);
  EXPECT_EQ(expire_time[1].first, GetTTLStatus::HGETEX_OK);
  EXPECT_EQ(expire_time[1].second, expire_at + 1);
  EXPECT_EQ(expire_time[2].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[2].second, 0);
  // 6.Expire NX when ttl set
  set_ttl_ret.clear();
  s = hash_->HExpireAt(key_, expire_fields, expire_at, set_ttl_ret, ExpireSetCond::NX);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(set_ttl_ret[0], 0);
  EXPECT_EQ(set_ttl_ret[1], 0);
  CheckMetaData(key_, &exp_meta, __LINE__);
  expire_time.clear();
  s = hash_->HExpireTime(key_, fields_, expire_time);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(expire_time.size(), fields_.size());
  EXPECT_EQ(expire_time[0].first, GetTTLStatus::HGETEX_OK);
  EXPECT_EQ(expire_time[0].second, expire_at + 1);
  EXPECT_EQ(expire_time[1].first, GetTTLStatus::HGETEX_OK);
  EXPECT_EQ(expire_time[1].second, expire_at + 1);
  EXPECT_EQ(expire_time[2].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[2].second, 0);
  // 7.Expire NX when no ttl set
  std::vector<SetPersistRes> persist_ret;
  s = hash_->HPersist(key_, expire_fields, persist_ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(persist_ret[0], 1);
  EXPECT_EQ(persist_ret[1], 1);
  exp_meta.persist_field_size += 2;
  CheckMetaData(key_, &exp_meta, __LINE__);
  expire_time.clear();
  s = hash_->HExpireTime(key_, fields_, expire_time);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(expire_time.size(), fields_.size());
  EXPECT_EQ(expire_time[0].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[0].second, 0);
  EXPECT_EQ(expire_time[1].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[1].second, 0);
  EXPECT_EQ(expire_time[2].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[2].second, 0);
  set_ttl_ret.clear();
  s = hash_->HExpireAt(key_, expire_fields, expire_at, set_ttl_ret, ExpireSetCond::NX);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(set_ttl_ret[0], 1);
  EXPECT_EQ(set_ttl_ret[1], 1);
  exp_meta.persist_field_size -= 2;
  CheckMetaData(key_, &exp_meta, __LINE__);
  expire_time.clear();
  s = hash_->HExpireTime(key_, fields_, expire_time);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(expire_time.size(), fields_.size());
  EXPECT_EQ(expire_time[0].first, GetTTLStatus::HGETEX_OK);
  EXPECT_EQ(expire_time[0].second, expire_at);
  EXPECT_EQ(expire_time[1].first, GetTTLStatus::HGETEX_OK);
  EXPECT_EQ(expire_time[1].second, expire_at);
  EXPECT_EQ(expire_time[2].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[2].second, 0);
}

TEST_P(RedisHashTest, HExpireAtAndHPersist) {
  auto prev = encode_hash_sub_flag.exchange(true);
  auto exist = MakeScopeExit([this, &prev]() {
    auto s = this->hash_->Del(key_);
    EXPECT_TRUE(s.ok());
    encode_hash_sub_flag.store(prev);
  });
  // 1.Expire
  HashMetadata exp_meta = VoidHashMetaData();
  uint64_t expire_at = 10 * 1000 + util::GetTimeStampMS();
  uint64_t ret = 0;
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  exp_meta.size += fields_.size();
  CheckMetaData(key_, &exp_meta, __LINE__);

  std::vector<Slice> expire_fields = {fields_[0], fields_[1]};
  std::vector<SetExRes> set_ttl_ret;
  auto s = hash_->HExpireAt(key_, expire_fields, expire_at, set_ttl_ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(set_ttl_ret[0], 1);
  EXPECT_EQ(set_ttl_ret[1], 1);

  exp_meta.flags |= METADATA_SUB_TTL_SET_MASK;
  exp_meta.fields_max_expire_at = expire_at;
  exp_meta.persist_field_size = 1;
  CheckMetaData(key_, &exp_meta, __LINE__);

  std::vector<std::pair<GetTTLStatus, uint64_t>> expire_time;
  fields_.emplace_back("non-exist-field");
  s = hash_->HExpireTime(key_, fields_, expire_time);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(expire_time.size(), fields_.size());
  EXPECT_EQ(expire_time[0].first, GetTTLStatus::HGETEX_OK);
  EXPECT_EQ(expire_time[0].second, expire_at);
  EXPECT_EQ(expire_time[1].first, GetTTLStatus::HGETEX_OK);
  EXPECT_EQ(expire_time[1].second, expire_at);
  EXPECT_EQ(expire_time[2].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[2].second, 0);
  EXPECT_EQ(expire_time[3].first, GetTTLStatus::HGETEX_GET_NO_FIELD);
  EXPECT_EQ(expire_time[3].second, 0);

  // 2.Persist
  std::vector<SetPersistRes> persist_ret;
  s = hash_->HPersist(key_, fields_, persist_ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(persist_ret.size(), fields_.size());
  EXPECT_EQ(persist_ret[0], 1);
  EXPECT_EQ(persist_ret[1], 1);
  EXPECT_EQ(persist_ret[2], -1);
  EXPECT_EQ(persist_ret[3], -2);
  persist_ret.clear();
  exp_meta.persist_field_size += 2;
  CheckMetaData(key_, &exp_meta, __LINE__);

  s = hash_->HPersist(key_, fields_, persist_ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(persist_ret.size(), fields_.size());
  EXPECT_EQ(persist_ret[0], -1);
  EXPECT_EQ(persist_ret[1], -1);
  EXPECT_EQ(persist_ret[2], -1);
  EXPECT_EQ(persist_ret[3], -2);
  CheckMetaData(key_, &exp_meta, __LINE__);
  // 3. Check if the expiration time is cleared
  expire_time.clear();
  s = hash_->HExpireTime(key_, fields_, expire_time);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(expire_time.size(), fields_.size());
  EXPECT_EQ(expire_time[0].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[1].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[2].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[3].first, GetTTLStatus::HGETEX_GET_NO_FIELD);
  EXPECT_TRUE(s.ok());
}

TEST_P(RedisHashTest, HExpireAtAndSet) {
  auto prev = encode_hash_sub_flag.exchange(true);
  auto exist = MakeScopeExit([this, &prev]() {
    auto s = this->hash_->Del(key_);
    EXPECT_TRUE(s.ok());
    encode_hash_sub_flag.store(prev);
  });
  // 1.Expire
  HashMetadata exp_meta = VoidHashMetaData();
  uint64_t expire_at = 10 * 1000 + util::GetTimeStampMS();
  uint64_t ret = 0;
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  exp_meta.size += fields_.size();
  CheckMetaData(key_, &exp_meta, __LINE__);
  std::vector<Slice> expire_fields = {fields_[0], fields_[1]};
  std::vector<SetExRes> set_ttl_ret;
  auto s = hash_->HExpireAt(key_, expire_fields, expire_at, set_ttl_ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(set_ttl_ret[0], 1);
  EXPECT_EQ(set_ttl_ret[1], 1);
  exp_meta.flags |= METADATA_SUB_TTL_SET_MASK;
  exp_meta.fields_max_expire_at = expire_at;
  exp_meta.persist_field_size = 1;
  CheckMetaData(key_, &exp_meta, __LINE__);
  std::vector<std::pair<GetTTLStatus, uint64_t>> expire_time;
  fields_.emplace_back("non-exist-field");
  values_.emplace_back("non-exist-value");
  s = hash_->HExpireTime(key_, fields_, expire_time);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(expire_time.size(), fields_.size());
  EXPECT_EQ(expire_time[0].first, GetTTLStatus::HGETEX_OK);
  EXPECT_EQ(expire_time[0].second, expire_at);
  EXPECT_EQ(expire_time[1].first, GetTTLStatus::HGETEX_OK);
  EXPECT_EQ(expire_time[1].second, expire_at);
  EXPECT_EQ(expire_time[2].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[2].second, 0);
  EXPECT_EQ(expire_time[3].first, GetTTLStatus::HGETEX_GET_NO_FIELD);
  EXPECT_EQ(expire_time[3].second, 0);
  // 2.Set
  ret = 0;
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok());
  }
  exp_meta.size = 4;
  exp_meta.persist_field_size = 4;
  CheckMetaData(key_, &exp_meta, __LINE__);
  // 3. Check if the expiration time is cleared
  expire_time.clear();
  s = hash_->HExpireTime(key_, fields_, expire_time);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(expire_time.size(), fields_.size());
  EXPECT_EQ(expire_time[0].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[1].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[2].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[3].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_TRUE(s.ok());
}

TEST_P(RedisHashTest, HExpireOldVersion) {
  auto prev = encode_hash_sub_flag.exchange(false);
  auto exist = MakeScopeExit([this, &prev]() {
    auto s = this->hash_->Del(key_);
    EXPECT_TRUE(s.ok());
    encode_hash_sub_flag.store(prev);
  });
  HashMetadata exp_meta = VoidHashMetaData();
  uint64_t expire_at = 10 * 1000 + util::GetTimeStampMS();
  uint64_t ret = 0;
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  exp_meta.size += fields_.size();
  CheckMetaData(key_, &exp_meta, __LINE__);
  std::vector<Slice> expire_fields = {fields_[0], fields_[1]};
  std::vector<SetExRes> set_ttl_ret;
  auto s = hash_->HExpireAt(key_, expire_fields, expire_at, set_ttl_ret);
  std::vector<std::pair<GetTTLStatus, uint64_t>> expire_time;
  EXPECT_EQ(s.code(), rocksdb::Status::Code::kInvalidArgument);
  s = hash_->HExpireTime(key_, fields_, expire_time);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(expire_time.size(), fields_.size());
  EXPECT_EQ(expire_time[0].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[1].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  EXPECT_EQ(expire_time[2].first, GetTTLStatus::HGETEX_GET_NO_TTL);
  CheckMetaData(key_, &exp_meta, __LINE__);
}

TEST_P(RedisHashTest, HExpireUseExpiredTimeStamp) {
  auto prev = encode_hash_sub_flag.exchange(true);
  auto exist = MakeScopeExit([this, &prev]() {
    auto s = this->hash_->Del(key_);
    EXPECT_TRUE(s.ok());
    encode_hash_sub_flag.store(prev);
  });
  HashMetadata exp_meta = VoidHashMetaData();
  uint64_t expire_at = util::GetTimeStampMS();
  uint64_t ret = 0;
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  exp_meta.size += fields_.size();
  CheckMetaData(key_, &exp_meta, __LINE__);
  std::vector<Slice> expire_fields = {fields_[0], fields_[1]};
  std::vector<SetExRes> set_ttl_ret;
  // delete
  auto s = hash_->HExpireAt(key_, expire_fields, expire_at, set_ttl_ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(set_ttl_ret[0], 2);
  EXPECT_EQ(set_ttl_ret[1], 2);
  exp_meta.size = 1;
  CheckMetaData(key_, &exp_meta, __LINE__);
  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses;
  s = hash_->MGet(key_, fields_, &values, &statuses);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(statuses[0], rocksdb::Status::NotFound());
  EXPECT_EQ(statuses[1], rocksdb::Status::NotFound());
  EXPECT_EQ(statuses[2], rocksdb::Status::OK());
  EXPECT_EQ(values[2], values_[2]);
  // delete again
  set_ttl_ret.clear();
  s = hash_->HExpireAt(key_, expire_fields, expire_at, set_ttl_ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(set_ttl_ret[0], -2);
  EXPECT_EQ(set_ttl_ret[1], -2);
  exp_meta.size = 1;
  CheckMetaData(key_, &exp_meta, __LINE__);

  // reset key
  values.clear();
  statuses.clear();
  s = hash_->Del(key_);
  EXPECT_TRUE(s.ok());
  exp_meta = VoidHashMetaData();

  ret = 0;
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  exp_meta.size += fields_.size();
  CheckMetaData(key_, &exp_meta, __LINE__);
  // expire
  set_ttl_ret.clear();
  s = hash_->HExpireAt(key_, {fields_[0]}, expire_at + 1000, set_ttl_ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(set_ttl_ret[0], 1);
  exp_meta.persist_field_size = 2;
  exp_meta.flags |= METADATA_SUB_TTL_SET_MASK;
  exp_meta.fields_max_expire_at = expire_at + 1000;
  CheckMetaData(key_, &exp_meta, __LINE__);

  // delete persist field
  set_ttl_ret.clear();
  s = hash_->HExpireAt(key_, {fields_[1]}, expire_at, set_ttl_ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(set_ttl_ret[0], 2);
  exp_meta.size = 2;
  exp_meta.persist_field_size = 1;
  CheckMetaData(key_, &exp_meta, __LINE__);
  // get
  values.clear();
  statuses.clear();
  s = hash_->MGet(key_, fields_, &values, &statuses);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(statuses[0], rocksdb::Status::OK());
  EXPECT_EQ(values[0], values_[0]);
  EXPECT_EQ(statuses[1], rocksdb::Status::NotFound());
  EXPECT_EQ(statuses[2], rocksdb::Status::OK());
  EXPECT_EQ(values[2], values_[2]);

  // delete ttl field
  set_ttl_ret.clear();
  s = hash_->HExpireAt(key_, {fields_[0]}, expire_at, set_ttl_ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(set_ttl_ret[0], 2);
  exp_meta.size = 1;
  exp_meta.persist_field_size = 1;
  CheckMetaData(key_, &exp_meta, __LINE__);
  // get
  values.clear();
  statuses.clear();
  s = hash_->MGet(key_, fields_, &values, &statuses);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(statuses[0], rocksdb::Status::NotFound());
  EXPECT_EQ(statuses[1], rocksdb::Status::NotFound());
  EXPECT_EQ(statuses[2], rocksdb::Status::OK());
  EXPECT_EQ(values[2], values_[2]);
}

TEST_P(RedisHashTest, HDeleteAndHExpire) {
  auto prev = encode_hash_sub_flag.exchange(true);
  auto exist = MakeScopeExit([this, &prev]() {
    auto s = this->hash_->Del(key_);
    EXPECT_TRUE(s.ok());
    encode_hash_sub_flag.store(prev);
  });
  HashMetadata exp_meta = VoidHashMetaData();
  uint64_t expire_at = util::GetTimeStampMS();
  uint64_t ret = 0;
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  exp_meta.size += fields_.size();
  CheckMetaData(key_, &exp_meta, __LINE__);
  std::vector<Slice> expire_fields = {fields_[0], fields_[1]};
  std::vector<SetExRes> set_ttl_ret;
  auto s = hash_->HExpireAt(key_, {fields_[0]}, expire_at + 1000, set_ttl_ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(set_ttl_ret[0], 1);
  exp_meta.persist_field_size = 2;
  exp_meta.flags |= METADATA_SUB_TTL_SET_MASK;
  exp_meta.fields_max_expire_at = expire_at + 1000;
  CheckMetaData(key_, &exp_meta, __LINE__);

  // delete persist field
  ret = 0;
  s = hash_->Delete(key_, {fields_[1]}, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(ret, 1);
  exp_meta.size = 2;
  exp_meta.persist_field_size = 1;
  CheckMetaData(key_, &exp_meta, __LINE__);

  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses;
  s = hash_->MGet(key_, fields_, &values, &statuses);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(statuses[0], rocksdb::Status::OK());
  EXPECT_EQ(values[0], values_[0]);
  EXPECT_EQ(statuses[1], rocksdb::Status::NotFound());
  EXPECT_EQ(statuses[2], rocksdb::Status::OK());
  EXPECT_EQ(values[2], values_[2]);

  // delete ttl field
  ret = 0;
  s = hash_->Delete(key_, {fields_[0]}, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(ret, 1);
  exp_meta.size = 1;
  exp_meta.persist_field_size = 1;
  CheckMetaData(key_, &exp_meta, __LINE__);
  // get
  values.clear();
  statuses.clear();
  s = hash_->MGet(key_, fields_, &values, &statuses);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(statuses[0], rocksdb::Status::NotFound());
  EXPECT_EQ(statuses[1], rocksdb::Status::NotFound());
  EXPECT_EQ(statuses[2], rocksdb::Status::OK());
  EXPECT_EQ(values[2], values_[2]);
}

TEST_P(RedisHashTest, HDelAndHMget) {
  auto k = "k";
  std::vector<Slice> fields = {"f1", "f2", "f3"};
  std::vector<Slice> values = {"v1", "v2", "v3"};
  auto prev = encode_hash_sub_flag.exchange(true);
  auto exist = MakeScopeExit([this, &prev, &k]() {
    auto s = this->hash_->Del(k);
    EXPECT_TRUE(s.ok());
    encode_hash_sub_flag.store(prev);
  });
  HashMetadata exp_meta = VoidHashMetaData();
  uint64_t ret = 0;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < fields.size(); i++) {
    fvs.emplace_back(fields[i].ToString(), values[i].ToString());
  }
  auto s = hash_->MSet(k, fvs, false, &ret);
  EXPECT_TRUE(s.ok() && fvs.size() == ret);
  exp_meta.size = fvs.size();
  CheckMetaData(k, &exp_meta, __LINE__);
  uint64_t del = 0;
  s = hash_->Delete(k, {fields[0]}, &del);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(del, 1);
  exp_meta.size = 2;
  CheckMetaData(k, &exp_meta, __LINE__);
  std::vector<std::string> vals;
  std::vector<rocksdb::Status> statuses;
  s = hash_->MGet(k, fields, &vals, &statuses);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(statuses[0], rocksdb::Status::NotFound());
  EXPECT_EQ(statuses[1], rocksdb::Status::OK());
  EXPECT_EQ(vals[1], values[1]);
  EXPECT_EQ(statuses[2], rocksdb::Status::OK());
  EXPECT_EQ(vals[2], values[2]);
}

TEST_P(RedisHashTest, HSetMultiTimes) {
  auto k = "kkk";
  std::vector<Slice> fields = {"f1", "f2", "f3"};
  std::vector<Slice> values = {"v1", "v2", "v3"};
  auto prev = encode_hash_sub_flag.exchange(true);
  auto exist = MakeScopeExit([this, &prev, &k]() {
    auto s = this->hash_->Del(k);
    EXPECT_TRUE(s.ok());
    encode_hash_sub_flag.store(prev);
  });
  HashMetadata exp_meta = VoidHashMetaData();
  uint64_t ret = 0;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < fields.size(); i++) {
    fvs.emplace_back(fields[i].ToString(), values[i].ToString());
  }
  std::vector<FieldValue> fvs1(fvs);
  auto s = hash_->MSet(k, fvs1, false, &ret);
  EXPECT_TRUE(s.ok() && fvs.size() == ret);
  exp_meta.size = fvs.size();
  CheckMetaData(k, &exp_meta, __LINE__);
  fvs[2].value = "xxx";
  std::vector<FieldValue> fvs2(fvs);
  s = hash_->MSet(k, fvs2, false, &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  CheckMetaData(k, &exp_meta, __LINE__);
  // expire
  uint64_t expire_at = 10 * 1000 + util::GetTimeStampMS();
  std::vector<SetExRes> set_ttl_ret;
  s = hash_->HExpireAt(k, {fields[0]}, expire_at + 1000, set_ttl_ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(set_ttl_ret[0], 1);
  exp_meta.persist_field_size = 2;
  exp_meta.flags |= METADATA_SUB_TTL_SET_MASK;
  exp_meta.fields_max_expire_at = expire_at + 1000;
  CheckMetaData(k, &exp_meta, __LINE__);
  fvs[2].value = "3xxx";
  exp_meta.persist_field_size = 3;
  std::vector<FieldValue> fvs3(fvs);
  s = hash_->MSet(k, fvs3, false, &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  CheckMetaData(k, &exp_meta, __LINE__);
}

TEST_P(RedisHashTest, HSetUpdateDataNoTTL) {
  auto k = "kkk";
  std::vector<Slice> fields = {"f1", "f2", "f3"};
  std::vector<Slice> values = {"v1", "v2", "v3"};
  std::vector<Slice> values2 = {"v11", "v22", "v3"};
  auto prev = encode_hash_sub_flag.exchange(false);
  auto exist = MakeScopeExit([this, &prev, &k]() {
    auto s = this->hash_->Del(k);
    EXPECT_TRUE(s.ok());
    encode_hash_sub_flag.store(prev);
  });
  HashMetadata exp_meta = VoidHashMetaData();
  uint64_t ret = 0;
  std::vector<FieldValue> fvs1;
  for (size_t i = 0; i < fields.size(); i++) {
    fvs1.emplace_back(fields[i].ToString(), values[i].ToString());
  }
  std::vector<FieldValue> fvs2;
  for (size_t i = 0; i < fields.size(); i++) {
    fvs2.emplace_back(fields[i].ToString(), values2[i].ToString());
  }
  auto s = hash_->MSet(k, fvs1, false, &ret);
  EXPECT_TRUE(s.ok() && fvs1.size() == ret);
  exp_meta.size = fvs1.size();
  CheckMetaData(k, &exp_meta, __LINE__);
  // check data fvs1
  std::vector<FieldValue> fvs1_check;
  s = hash_->GetAll(k, &fvs1_check);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(fvs1_check.size(), fvs1.size());
  for (size_t i = 0; i < fvs1_check.size(); i++) {
    EXPECT_EQ(fvs1_check[i].field, fvs1[i].field);
    EXPECT_EQ(fvs1_check[i].value, fvs1[i].value);
  }

  s = hash_->MSet(k, fvs2, false, &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  CheckMetaData(k, &exp_meta, __LINE__);
  // check data fvs2
  std::vector<FieldValue> fvs2_check;
  s = hash_->GetAll(k, &fvs2_check);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(fvs2_check.size(), fvs2.size());
  for (size_t i = 0; i < fvs2_check.size(); i++) {
    EXPECT_EQ(fvs2_check[i].field, fvs2[i].field);
    EXPECT_EQ(fvs2_check[i].value, fvs2[i].value);
  }
}

TEST_P(RedisHashTest, HSetUpdateData) {
  auto k = "kkk";
  std::vector<Slice> fields = {"f1", "f2", "f3"};
  std::vector<Slice> values = {"v1", "v2", "v3"};
  std::vector<Slice> values2 = {"v11", "v22", "v3"};
  auto prev = encode_hash_sub_flag.exchange(true);
  auto exist = MakeScopeExit([this, &prev, &k]() {
    auto s = this->hash_->Del(k);
    EXPECT_TRUE(s.ok());
    encode_hash_sub_flag.store(prev);
  });
  HashMetadata exp_meta = VoidHashMetaData();
  uint64_t ret = 0;
  std::vector<FieldValue> fvs1;
  for (size_t i = 0; i < fields.size(); i++) {
    fvs1.emplace_back(fields[i].ToString(), values[i].ToString());
  }
  std::vector<FieldValue> fvs2;
  for (size_t i = 0; i < fields.size(); i++) {
    fvs2.emplace_back(fields[i].ToString(), values2[i].ToString());
  }
  auto s = hash_->MSet(k, fvs1, false, &ret);
  EXPECT_TRUE(s.ok() && fvs1.size() == ret);
  exp_meta.size = fvs1.size();
  CheckMetaData(k, &exp_meta, __LINE__);
  // check data fvs1
  std::vector<FieldValue> fvs1_check;
  s = hash_->GetAll(k, &fvs1_check);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(fvs1_check.size(), fvs1.size());
  for (size_t i = 0; i < fvs1_check.size(); i++) {
    EXPECT_EQ(fvs1_check[i].field, fvs1[i].field);
    EXPECT_EQ(fvs1_check[i].value, fvs1[i].value);
  }

  s = hash_->MSet(k, fvs2, false, &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  CheckMetaData(k, &exp_meta, __LINE__);
  // check data fvs2
  std::vector<FieldValue> fvs2_check;
  s = hash_->GetAll(k, &fvs2_check);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(fvs2_check.size(), fvs2.size());
  for (size_t i = 0; i < fvs2_check.size(); i++) {
    EXPECT_EQ(fvs2_check[i].field, fvs2[i].field);
    EXPECT_EQ(fvs2_check[i].value, fvs2[i].value);
  }
}

TEST_P(RedisHashTest, HScan) {
  // prepare data
  uint64_t added_cnt = 0;
  std::string meta_key = "RedisHashTest.HScan";
  std::vector<FieldValue> field_values{{"f1", "v1"}, {"f2", "v2"}, {"f22", "v22"}, {"f222", "v222"}, {"f3", "v3"}};
  auto s = hash_->MSet(meta_key, field_values, false, &added_cnt);
  EXPECT_TRUE(s.ok() && added_cnt == field_values.size());
  // empty cursor and pattern
  std::vector<std::string> fields, values;
  std::string cursor;
  // limit < actual
  s = hash_->Scan(meta_key, &cursor, 1, "", &fields, &values);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(cursor == field_values[1].field);
  EXPECT_EQ(fields.size(), 1);
  EXPECT_EQ(values.size(), 1);
  EXPECT_EQ(fields[0], field_values[0].field);
  EXPECT_EQ(values[0], field_values[0].value);
  // scan from pattern prefix
  auto pattern = field_values[3].field + "*";
  std::vector<std::string> cursors = {"", field_values[0].field, field_values[1].field, field_values[2].field,
                                      field_values[3].field};
  for (auto cursor : cursors) {
    fields.clear();
    values.clear();
    // limit > actual
    auto s = hash_->Scan(meta_key, &cursor, 2, pattern, &fields, &values);
    EXPECT_TRUE(s.ok());
    EXPECT_TRUE(cursor.empty());
    EXPECT_EQ(fields.size(), 1);
    EXPECT_EQ(values.size(), 1);
    EXPECT_EQ(fields[0], field_values[3].field);
    EXPECT_EQ(values[0], field_values[3].value);
  }
  // scan from cursor and cursor start with pattern prefix
  std::vector<std::string> patterns = {"", "*", "f*"};
  for (auto& pattern : patterns) {
    fields.clear();
    values.clear();
    cursor = field_values[3].field;
    // limit = actual
    auto s = hash_->Scan(meta_key, &cursor, 2, pattern, &fields, &values);
    EXPECT_TRUE(s.ok());
    EXPECT_TRUE(cursor.empty());
    EXPECT_EQ(fields.size(), 2);
    EXPECT_EQ(values.size(), 2);
    EXPECT_EQ(fields[0], field_values[3].field);
    EXPECT_EQ(values[0], field_values[3].value);
    EXPECT_EQ(fields[1], field_values[4].field);
    EXPECT_EQ(values[1], field_values[4].value);
  }
  // scan with cursor > pattern prefix but cursor not start with pattern prefix
  cursors = {field_values[1].field, field_values[4].field};
  patterns = {field_values[0].field + "*", field_values[1].field + "?"};
  for (size_t i = 0; i < cursors.size(); ++i) {
    fields.clear();
    values.clear();
    auto cursor = cursors[i];
    auto pattern = patterns[i];
    s = hash_->Scan(meta_key, &cursor, 1, pattern, &fields, &values);
    EXPECT_TRUE(s.ok());
    EXPECT_TRUE(cursor.empty());
    EXPECT_TRUE(fields.empty());
    EXPECT_TRUE(values.empty());
  }
  // clean up
  s = hash_->Del(meta_key);
  EXPECT_TRUE(s.ok());
}

INSTANTIATE_TEST_SUITE_P(CDCSyncHashTests, RedisHashTest, ::testing::Values(false, true));

}  // namespace redis
