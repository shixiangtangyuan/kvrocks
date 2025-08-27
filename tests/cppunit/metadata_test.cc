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

#include <iostream>
#include <memory>
#include <string>

#include "grpc/grpc_crl_provider.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "storage/redis_metadata.h"
#include "test_base.h"
#include "time_util.h"
#include "types/redis_hash.h"

// MetaKey encoding: | slotId(2bytes) | userKey |
// SubKey encoding:  | slotId(2bytes) | keyLen(4bytes)  | userKey | version | subKey |
// MetaData encoding: | flags(1byte) | expireAt(8bytes) | [ version(8bytes) ] | [ size(8bytes) ] |
inline size_t len_flags = 1;
inline size_t len_slotId = 2;
inline size_t len_key = 4;
inline size_t len_expire = 8;
inline size_t len_version = 8;
inline size_t len_size = 8;

inline size_t len_simple_metadata_withttl = len_flags + len_expire;
inline size_t len_simple_metadata_nottl = len_flags;
inline size_t len_complex_metadata_withttl = len_flags + len_expire + len_version + len_size;
inline size_t len_complex_metadata_nottl = len_flags + len_version + len_size;
inline size_t len_complex_metadata_set_subkey_ttl = len_flags + len_expire + len_version + len_size * 2;

TEST(MetaKey, EncodeAndDecode) {
  Slice key = "test-metadata-key";
  Slice ns = "namespace";
  std::string ns_key = ComposeNamespaceKey(ns, key, false);
  EXPECT_EQ(ns_key.size(), key.size());
  auto [tmp_ns, user_key] = ExtractNamespaceKey<Slice>(ns_key, false);
  EXPECT_EQ(user_key, key);
  EXPECT_EQ(tmp_ns, "__namespace");
}

TEST(MetaKey, EncodeAndDecodeWithSlotId) {
  Slice key = "test-metadata-key";
  Slice ns = "namespace";
  std::string ns_key = ComposeNamespaceKey(ns, key, true);
  EXPECT_EQ(ns_key.size(), key.size() + len_slotId);
  auto [tmp_ns, user_key] = ExtractNamespaceKey<Slice>(ns_key, true);
  EXPECT_EQ(user_key, key);
  EXPECT_EQ(tmp_ns, "__namespace");
}

TEST(InternalKey, EncodeAndDecode) {
  Slice key = "test-metadata-key";
  Slice sub_key = "test-metadata-sub-key";
  Slice ns = "namespace";
  uint64_t version = 12;
  std::string ns_key = ComposeNamespaceKey(ns, key, false);
  EXPECT_EQ(ns_key.size(), key.size());
  InternalKey ikey(ns_key, sub_key, version, false);
  ASSERT_EQ(ikey.GetKey(), key);
  ASSERT_EQ(ikey.GetSubKey(), sub_key);
  ASSERT_EQ(ikey.GetVersion(), version);
  std::string bytes = ikey.Encode();
  EXPECT_EQ(bytes.size(), len_key + key.size() + sub_key.size() + sizeof(version));
  InternalKey ikey1(bytes, false);
  EXPECT_EQ(ikey, ikey1);
}

TEST(InternalKey, EncodeAndDecodeWithSlotId) {
  Slice key = "test-metadata-key";
  Slice sub_key = "test-metadata-sub-key";
  Slice ns = "namespace";
  uint64_t version = 12;
  std::string ns_key = ComposeNamespaceKey(ns, key, true);
  InternalKey ikey(ns_key, sub_key, version, true);
  ASSERT_EQ(ikey.GetKey(), key);
  ASSERT_EQ(ikey.GetSubKey(), sub_key);
  ASSERT_EQ(ikey.GetVersion(), version);
  std::string bytes = ikey.Encode();
  EXPECT_EQ(bytes.size(), len_slotId + len_key + key.size() + sub_key.size() + sizeof(version));
  InternalKey ikey1(bytes, true);
  EXPECT_EQ(ikey, ikey1);
}

TEST(Metadata, EncodeAndDecode) {
  std::string string_bytes;
  Metadata string_md(kRedisString);
  string_md.expire = 123000;
  string_md.Encode(&string_bytes);
  EXPECT_EQ(string_bytes.size(), len_simple_metadata_withttl);
  Metadata string_md1(kRedisNone);
  ASSERT_TRUE(string_md1.Decode(string_bytes).ok());
  ASSERT_EQ(string_md, string_md1);
  EXPECT_TRUE(string_md1.HasTTL());

  ListMetadata list_md;
  list_md.flags = 13;
  list_md.expire = 123000;
  list_md.version = 2;
  list_md.size = 1234;
  list_md.head = 123;
  list_md.tail = 321;
  ListMetadata list_md1;
  std::string list_bytes;
  list_md.Encode(&list_bytes);
  EXPECT_EQ(list_bytes.size(), len_complex_metadata_withttl + 8 + 8);
  ASSERT_TRUE(list_md1.Decode(list_bytes).ok());
  ASSERT_EQ(list_md, list_md1);
  EXPECT_TRUE(string_md1.HasTTL());
}

TEST(Metadata, EncodeAndDecodeNoTTL) {
  std::string string_bytes;
  Metadata string_md(kRedisString);
  string_md.expire = 0;
  string_md.Encode(&string_bytes);
  EXPECT_EQ(string_bytes.size(), len_simple_metadata_nottl);
  Metadata string_md1(kRedisNone);
  ASSERT_TRUE(string_md1.Decode(string_bytes).ok());
  ASSERT_EQ(string_md, string_md1);
  EXPECT_FALSE(string_md1.HasTTL());

  ListMetadata list_md;
  list_md.flags = 13;
  list_md.expire = 0;
  list_md.version = 2;
  list_md.size = 1234;
  list_md.head = 123;
  list_md.tail = 321;
  ListMetadata list_md1;
  std::string list_bytes;
  list_md.Encode(&list_bytes);
  EXPECT_EQ(list_bytes.size(), len_complex_metadata_nottl + 8 + 8);
  ASSERT_TRUE(list_md1.Decode(list_bytes).ok());
  ASSERT_EQ(list_md, list_md1);
  EXPECT_FALSE(string_md1.HasTTL());
}

TEST(Metadata, EncodeAndDecodeTTLToNOTTL) {
  std::string string_bytes;
  Metadata string_md(kRedisString);
  string_md.expire = 100;
  string_md.Encode(&string_bytes);
  EXPECT_EQ(string_bytes.size(), len_simple_metadata_withttl);
  Metadata string_md1(kRedisNone);
  ASSERT_TRUE(string_md1.Decode(string_bytes).ok());
  ASSERT_EQ(string_md, string_md1);
  EXPECT_TRUE(string_md1.HasTTL());

  std::string string_bytes2;
  string_md1.expire = 0;
  string_md1.Encode(&string_bytes2);
  EXPECT_EQ(string_bytes2.size(), len_simple_metadata_nottl);
  Metadata string_md2(kRedisString);
  ASSERT_TRUE(string_md2.Decode(string_bytes2).ok());
  EXPECT_FALSE(string_md2.HasTTL());

  ListMetadata list_md;
  list_md.flags = 13;
  list_md.expire = 100;
  list_md.version = 2;
  list_md.size = 1234;
  list_md.head = 123;
  list_md.tail = 321;
  ListMetadata list_md1;
  std::string list_bytes;
  list_md.Encode(&list_bytes);
  EXPECT_EQ(list_bytes.size(), len_complex_metadata_withttl + 8 + 8);
  ASSERT_TRUE(list_md1.Decode(list_bytes).ok());
  ASSERT_EQ(list_md, list_md1);
  EXPECT_TRUE(list_md1.HasTTL());

  std::string list_bytes2;
  list_md1.expire = 0;
  list_md1.Encode(&list_bytes2);
  ListMetadata list_md2;
  EXPECT_EQ(list_bytes2.size(), len_complex_metadata_nottl + 8 + 8);
  ASSERT_TRUE(list_md2.Decode(list_bytes2).ok());
  EXPECT_FALSE(list_md2.HasTTL());
}

TEST(Metadata, EncodeAndDecodeTTLToNOTTLSameMetadata) {
  // test string type
  std::string string_ttl_bytes;
  Metadata string_md(kRedisString);
  string_md.expire = 100;
  string_md.Encode(&string_ttl_bytes);
  // decode new md
  Metadata string_md1(kRedisString);
  std::string old_bytes;
  string_md1.Encode(&old_bytes);
  auto s = string_md1.Decode(string_ttl_bytes);
  EXPECT_TRUE(string_md1.HasTTL());
  EXPECT_EQ(string_md1.expire, 100);
  // decode old md
  s = string_md1.Decode(old_bytes);
  EXPECT_FALSE(string_md1.HasTTL());
  EXPECT_EQ(string_md1.expire, 0);
  std::string new_bytes;
  string_md1.Encode(&new_bytes);
  EXPECT_EQ(new_bytes, old_bytes);

  // test list type
  ListMetadata list_md;
  list_md.flags = 13;
  list_md.expire = 100;
  list_md.version = 2;
  list_md.size = 1234;
  list_md.head = 123;
  list_md.tail = 321;
  std::string list_bytes;
  list_md.Encode(&list_bytes);
  // decode new md
  ListMetadata list_md1;
  std::string md_old_bytes;
  list_md1.Encode(&md_old_bytes);
  s = list_md1.Decode(list_bytes);
  ASSERT_EQ(list_md, list_md1);
  EXPECT_TRUE(list_md1.HasTTL());
  // decode old md
  s = list_md1.Decode(md_old_bytes);
  EXPECT_FALSE(list_md1.HasTTL());
  EXPECT_EQ(list_md1.expire, 0);
  std::string md_new_bytes;
  list_md1.Encode(&md_new_bytes);
  EXPECT_EQ(md_new_bytes, md_old_bytes);
}

TEST(HashMetaData, EncodeAndDecode) {
  std::string string_bytes;
  HashMetadata string_md;
  string_md.expire = 0;
  string_md.Encode(&string_bytes);
  EXPECT_EQ(string_bytes.size(), len_complex_metadata_nottl);
  HashMetadata string_md1;

  ASSERT_TRUE(string_md1.Decode(string_bytes).ok());
  ASSERT_EQ(string_md, string_md1);
  EXPECT_FALSE(string_md1.HasTTL());
  EXPECT_FALSE(string_md1.IsSubTTLSet());

  HashMetadata hash_md;
  hash_md.flags = 0x02;  // hash type
  hash_md.expire = 0;

  HashMetadata hash_md1;
  std::string hash_bytes;
  hash_md.Encode(&hash_bytes);
  EXPECT_EQ(hash_bytes.size(), len_complex_metadata_nottl);
  ASSERT_TRUE(hash_md1.Decode(hash_bytes).ok());
  ASSERT_EQ(hash_md1, hash_md);
  EXPECT_FALSE(hash_md1.HasTTL());
  EXPECT_FALSE(hash_md1.IsSubTTLSet());
  EXPECT_FALSE(hash_md1.HasSubFlag());

  encode_hash_sub_flag.store(true);

  hash_md = HashMetadata();
  hash_md.expire = 0;

  hash_md1 = HashMetadata();
  hash_bytes = "";
  hash_md.Encode(&hash_bytes);
  EXPECT_EQ(hash_bytes.size(), len_complex_metadata_nottl);
  ASSERT_TRUE(hash_md1.Decode(hash_bytes).ok());
  ASSERT_EQ(hash_md1, hash_md);
  EXPECT_FALSE(hash_md1.HasTTL());
  EXPECT_FALSE(hash_md1.IsSubTTLSet());
  EXPECT_TRUE(hash_md1.HasSubFlag());

  hash_md = HashMetadata();
  hash_md.expire = 100;

  hash_md1 = HashMetadata();
  hash_bytes = "";
  hash_md.Encode(&hash_bytes);
  EXPECT_EQ(hash_bytes.size(), len_complex_metadata_withttl);
  ASSERT_TRUE(hash_md1.Decode(hash_bytes).ok());
  ASSERT_EQ(hash_md1, hash_md);
  EXPECT_TRUE(hash_md1.HasTTL());
  EXPECT_FALSE(hash_md1.IsSubTTLSet());
  EXPECT_TRUE(hash_md1.HasSubFlag());

  encode_hash_sub_flag.store(false);
  hash_md = HashMetadata();
  hash_md.expire = 100;

  hash_md1 = HashMetadata();
  hash_bytes = "";
  hash_md.Encode(&hash_bytes);
  EXPECT_EQ(hash_bytes.size(), len_complex_metadata_withttl);
  ASSERT_TRUE(hash_md1.Decode(hash_bytes).ok());
  ASSERT_EQ(hash_md1, hash_md);
  EXPECT_TRUE(hash_md1.HasTTL());
  EXPECT_FALSE(hash_md1.IsSubTTLSet());
  EXPECT_FALSE(hash_md1.HasSubFlag());

  encode_hash_sub_flag.store(true);

  // old version, no sub key flag
  hash_md.flags = 0x02;  // hash type
  hash_md.expire = 0;
  rocksdb::Status s = hash_md.SetSubTTL(1, 100);
  ASSERT_EQ(s.code(), rocksdb::Status::kInvalidArgument);

  // if TTL is set, subTTL cannot be set
  hash_md = HashMetadata();
  hash_md.expire = 100;
  hash_md.flags |= METADATA_TTL_MASK;
  s = hash_md.SetSubTTL(1, 100);
  ASSERT_EQ(s.code(), rocksdb::Status::kInvalidArgument);

  hash_md = HashMetadata();
  s = hash_md.SetSubTTL(1, 999);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(hash_md.IsSubTTLSet());
  ASSERT_EQ(hash_md.persist_field_size, 1);
  ASSERT_EQ(hash_md.fields_max_expire_at, 999);
  ASSERT_TRUE(hash_md.HasSubFlag());

  hash_md1 = HashMetadata();
  hash_bytes = "";
  hash_md.Encode(&hash_bytes);

  EXPECT_EQ(hash_bytes.size(), len_complex_metadata_set_subkey_ttl);
  ASSERT_TRUE(hash_md1.Decode(hash_bytes).ok());
  ASSERT_EQ(hash_md1, hash_md);
  EXPECT_FALSE(hash_md1.HasTTL());
  EXPECT_TRUE(hash_md1.IsSubTTLSet());
  EXPECT_TRUE(hash_md1.HasSubFlag());
}

TEST(HashSubData, EncodeAndDecodeWithTTL) {
  encode_hash_sub_flag.store(true);
  std::string sub_val("test-hash-value");
  HashSubData sub_data;
  sub_data.expire = 0;
  sub_data.SetExpire(100);
  sub_data.value = Slice(sub_val);
  EXPECT_TRUE(sub_data.HasTTL());
  EXPECT_TRUE(sub_data.Expired());
  EXPECT_NE(sub_data.expire, 0);
  // clear ttl
  sub_data.ClearExpire();
  EXPECT_FALSE(sub_data.HasTTL());
  EXPECT_FALSE(sub_data.Expired());
  EXPECT_EQ(sub_data.expire, 0);

  // set now
  sub_data.SetExpire(util::GetTimeStampMS() + 1000);
  EXPECT_TRUE(sub_data.HasTTL());
  EXPECT_FALSE(sub_data.Expired());

  HashMetadata metadata;
  rocksdb::Status s = metadata.SetSubTTL(1, 1000);

  EXPECT_TRUE(s.ok());
  std::string raw_bytes;
  sub_data.Encode(&metadata, &raw_bytes);
  EXPECT_EQ(raw_bytes.size(), 1 + 8 + 15);
  rocksdb::PinnableSlice input;
  input.PinSlice(raw_bytes, nullptr);

  HashSubData sub_data1;
  s = sub_data1.Decode(&metadata, &input);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(sub_data, sub_data1);
  EXPECT_TRUE(sub_data1.HasTTL());
  EXPECT_FALSE(sub_data1.Expired());
}

TEST(HashSubData, EncodeAndDecodeNoTTL) {
  encode_hash_sub_flag.store(true);
  std::string sub_val = "test-hash-value";
  HashSubData sub_data;
  sub_data.expire = 0;
  sub_data.value = Slice(sub_val);
  EXPECT_FALSE(sub_data.HasTTL());
  EXPECT_FALSE(sub_data.Expired());

  HashMetadata metadata;
  std::string raw_bytes;
  sub_data.Encode(&metadata, &raw_bytes);
  EXPECT_EQ(raw_bytes.size(), 1 + 15);
  rocksdb::PinnableSlice input;
  input.PinSlice(raw_bytes, nullptr);

  HashSubData sub_data1;
  rocksdb::Status s = sub_data1.Decode(&metadata, &input);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(sub_data, sub_data1);
  EXPECT_FALSE(sub_data1.HasTTL());
  EXPECT_FALSE(sub_data1.Expired());
}

TEST(HashSubData, EncodeAndDecodeNoSubFlag) {
  encode_hash_sub_flag.store(false);
  std::string sub_val = "test-hash-value";
  HashSubData sub_data;
  sub_data.expire = 0;
  sub_data.value = Slice(sub_val);
  EXPECT_FALSE(sub_data.HasTTL());
  EXPECT_FALSE(sub_data.Expired());
  HashMetadata metadata;
  std::string raw_bytes;
  sub_data.Encode(&metadata, &raw_bytes);
  EXPECT_EQ(raw_bytes.size(), 15);
  rocksdb::PinnableSlice input;
  input.PinSlice(raw_bytes, nullptr);

  HashSubData sub_data1;
  rocksdb::Status s = sub_data1.Decode(&metadata, &input);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(sub_data, sub_data1);
  EXPECT_FALSE(sub_data1.HasTTL());
  EXPECT_FALSE(sub_data1.Expired());
}

class RedisTypeTest : public TestBase {
 public:
  RedisTypeTest() {
    redis_ = std::make_unique<redis::Database>(storage_, "default_ns");
    hash_ = std::make_unique<redis::Hash>(storage_, "default_ns");
    key_ = "test-redis-type";
    fields_ = {"test-hash-key-1", "test-hash-key-2", "test-hash-key-3"};
    values_ = {"hash-test-value-1", "hash-test-value-2", "hash-test-value-3"};
  }
  ~RedisTypeTest() override = default;

 protected:
  std::unique_ptr<redis::Database> redis_;
  std::unique_ptr<redis::Hash> hash_;
};

TEST_F(RedisTypeTest, GetMetadata) {
  uint64_t ret = 0;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < fields_.size(); i++) {
    fvs.emplace_back(fields_[i].ToString(), values_[i].ToString());
  }
  rocksdb::Status s = hash_->MSet(key_, fvs, false, &ret);
  EXPECT_TRUE(s.ok() && fvs.size() == ret);
  HashMetadata metadata;
  std::string ns_key = redis_->AppendNamespacePrefix(key_);
  s = redis_->GetMetadata(kRedisHash, ns_key, &metadata);
  EXPECT_EQ(fvs.size(), metadata.size);
  s = redis_->Del(key_);
  EXPECT_TRUE(s.ok());
}

TEST_F(RedisTypeTest, Expire) {
  uint64_t ret = 0;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < fields_.size(); i++) {
    fvs.emplace_back(fields_[i].ToString(), values_[i].ToString());
  }
  rocksdb::Status s = hash_->MSet(key_, fvs, false, &ret);
  EXPECT_TRUE(s.ok() && fvs.size() == ret);
  int64_t now = 0;
  rocksdb::Env::Default()->GetCurrentTime(&now);
  s = redis_->Expire(key_, now * 1000 + 2000);
  int64_t ttl = 0;
  s = redis_->TTL(key_, &ttl);
  ASSERT_GT(ttl, 0);
  ASSERT_LE(ttl, 2000);
  s = redis_->Del(key_);
}

TEST_F(RedisTypeTest, SetAfterExpire) {
  uint64_t ret = 0;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < fields_.size(); i++) {
    fvs.emplace_back(fields_[i].ToString(), values_[i].ToString());
  }
  std::vector<FieldValue> fvs1(fvs);
  rocksdb::Status s = hash_->MSet(key_, fvs1, false, &ret);
  EXPECT_TRUE(s.ok() && fvs.size() == ret);
  int64_t now = util::GetTimeStampMS();
  s = redis_->Expire(key_, now + 500);
  int64_t ttl = 0;
  s = redis_->TTL(key_, &ttl);
  ASSERT_GT(ttl, 0);
  ASSERT_LE(ttl, 500);
  std::this_thread::sleep_for(std::chrono::milliseconds(600));
  s = redis_->TTL(key_, &ttl);
  ASSERT_EQ(-2, ttl);
  // set again
  std::vector<FieldValue> fvs2(fvs);
  s = hash_->MSet(key_, fvs2, false, &ret);
  EXPECT_TRUE(s.ok() && fvs.size() == ret);
  s = redis_->TTL(key_, &ttl);
  ASSERT_EQ(-1, ttl);
  std::vector<FieldValue> get_fvs;
  s = hash_->GetAll(key_, &get_fvs);
  ASSERT_EQ(get_fvs.size(), fvs.size());

  s = redis_->Del(key_);
}

TEST_F(RedisTypeTest, PersistTTL) {
  uint64_t ret = 0;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < fields_.size(); i++) {
    fvs.emplace_back(fields_[i].ToString(), values_[i].ToString());
  }
  rocksdb::Status s = hash_->MSet(key_, fvs, false, &ret);
  EXPECT_TRUE(s.ok() && fvs.size() == ret);
  int64_t now = 0;
  rocksdb::Env::Default()->GetCurrentTime(&now);
  s = redis_->Expire(key_, now * 1000 + 2000);
  int64_t ttl = 0;
  s = redis_->TTL(key_, &ttl);
  ASSERT_GT(ttl, 0);
  ASSERT_LE(ttl, 2000);

  // persist key
  s = redis_->Expire(key_, 0);
  s = redis_->TTL(key_, &ttl);
  ASSERT_EQ(ttl, -1);
  int n = 0;
  s = redis_->Exists({key_}, &n);
  ASSERT_EQ(n, 1);
  uint64_t size = 0;
  s = hash_->Size(key_, &size);
  ASSERT_EQ(size, fields_.size());

  s = redis_->Del(key_);
}

TEST_F(RedisTypeTest, PersistNoTTL) {
  uint64_t ret = 0;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < fields_.size(); i++) {
    fvs.emplace_back(fields_[i].ToString(), values_[i].ToString());
  }
  rocksdb::Status s = hash_->MSet(key_, fvs, false, &ret);
  EXPECT_TRUE(s.ok() && fvs.size() == ret);

  s = redis_->Expire(key_, 0);
  int64_t ttl = 0;
  s = redis_->TTL(key_, &ttl);
  ASSERT_EQ(ttl, -1);
  uint64_t size = 0;
  s = hash_->Size(key_, &size);
  ASSERT_EQ(size, fields_.size());

  s = redis_->Del(key_);
}

TEST(Metadata, MetadataDecoding64BitSimpleKey) {
  auto expire_at = (util::GetTimeStamp() + 10) * 1000;
  Metadata md_old(kRedisString, true);
  md_old.expire = expire_at;
  std::string encoded_bytes;
  md_old.Encode(&encoded_bytes);
  EXPECT_EQ(encoded_bytes.size(), len_simple_metadata_withttl);

  Metadata md_new(kRedisNone, false);
  ASSERT_TRUE(md_new.Decode(encoded_bytes).ok());
  EXPECT_EQ(md_new.Type(), kRedisString);
  EXPECT_EQ(md_new.expire, expire_at);
}

TEST(Metadata, Metadata64bitNoExpiration) {
  Metadata md_src(kRedisString, true);
  std::string encoded_bytes;
  md_src.Encode(&encoded_bytes);
  EXPECT_EQ(encoded_bytes.size(), len_simple_metadata_nottl);

  Metadata md_decoded(kRedisNone, false);
  ASSERT_TRUE(md_decoded.Decode(encoded_bytes).ok());
  EXPECT_EQ(md_decoded.Type(), kRedisString);
  EXPECT_EQ(md_decoded.expire, 0);
}

TEST(Metadata, Metadata64bitExpiration) {
  auto expire_at = util::GetTimeStampMS() + 1000;
  Metadata md_src(kRedisString, true);
  md_src.expire = expire_at;
  std::string encoded_bytes;
  md_src.Encode(&encoded_bytes);
  EXPECT_EQ(encoded_bytes.size(), len_simple_metadata_withttl);

  Metadata md_decoded(kRedisNone, false);
  ASSERT_TRUE(md_decoded.Decode(encoded_bytes).ok());
  EXPECT_EQ(md_decoded.Type(), kRedisString);
  EXPECT_EQ(md_decoded.expire, expire_at);
}

TEST(Metadata, Metadata64bitSizeNottl) {
  uint64_t big_size = 100000000000;
  Metadata md_src(kRedisHash, true);
  md_src.size = big_size;
  std::string encoded_bytes;
  md_src.Encode(&encoded_bytes);
  EXPECT_EQ(encoded_bytes.size(), len_complex_metadata_nottl);

  Metadata md_decoded(kRedisNone, false);
  ASSERT_TRUE(md_decoded.Decode(encoded_bytes).ok());
  EXPECT_EQ(md_decoded.Type(), kRedisHash);
  EXPECT_EQ(md_decoded.expire, 0);
  EXPECT_EQ(md_decoded.size, big_size);
}

TEST(Metadata, Metadata64bitSizeWithttl) {
  auto expire_at = util::GetTimeStampMS() + 1000;
  uint64_t big_size = 100000000000;
  Metadata md_src(kRedisHash, true);
  md_src.expire = expire_at;
  md_src.size = big_size;
  std::string encoded_bytes;
  md_src.Encode(&encoded_bytes);
  EXPECT_EQ(encoded_bytes.size(), len_complex_metadata_withttl);

  Metadata md_decoded(kRedisNone, false);
  ASSERT_TRUE(md_decoded.Decode(encoded_bytes).ok());
  EXPECT_EQ(md_decoded.Type(), kRedisHash);
  EXPECT_EQ(md_decoded.expire, expire_at);
  EXPECT_EQ(md_decoded.size, big_size);
}

TEST(Metadata, GetTypeByName) {
  for (int i = kRedisNone; i <= kRedisJson; i++) {
    auto type = static_cast<RedisType>(i);
    EXPECT_EQ(GetRedisTypeByName(RedisTypeNames[i]), type);
  }

  EXPECT_EQ(GetRedisTypeByName("STRing"), RedisType::kRedisString);
  EXPECT_EQ(GetRedisTypeByName("abcd"), RedisType::kRedisNone);
}
