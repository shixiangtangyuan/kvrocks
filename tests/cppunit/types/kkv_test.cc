#include <gtest/gtest.h>

#include "test_base.h"
#include "types/redis_kkv.h"

namespace redis {

class RedisKKVTest : public TestBase {
 public:
  rocksdb::Status GetMetaData(Slice user_key, HashMetadata *meta_data) {
    auto meta_key = kkv_->AppendNamespacePrefix(user_key);
    return kkv_->GetMetadata(meta_key, meta_data);
  }

  void CheckMetaData(Slice user_key, HashMetadata *expect, int line_num) {
    HashMetadata actual;
    auto s = GetMetaData(user_key, &actual);
    if (!expect) {
      EXPECT_TRUE(s.IsNotFound()) << "line " << line_num;
      return;
    }
    EXPECT_TRUE(s.ok()) << s.ToString() << "line " << line_num;
    EXPECT_EQ(actual.size, expect->size) << "line " << line_num;
    EXPECT_EQ(actual.HasSubFlag(), expect->HasSubFlag()) << "line " << line_num;
    EXPECT_EQ(actual.IsSubTTLSet(), expect->IsSubTTLSet()) << "line " << line_num;
    EXPECT_EQ(actual.persist_field_size, expect->persist_field_size) << "line " << line_num;
    EXPECT_EQ(actual.fields_max_expire_at, expect->fields_max_expire_at) << "line " << line_num;
  }

  void CheckFields(Slice user_key, std::vector<KKVFieldValue> &&expect, int line_num) {
    CheckFields(user_key, expect, line_num);
  }

  void CheckFields(Slice user_key, std::vector<KKVFieldValue> &expect, int line_num) {
    for (auto &fv : expect) {
      std::string val;
      auto s = hash_->Get(user_key, fv.field, &val);
      if (fv.value.Expired()) {
        EXPECT_TRUE(s.IsNotFound()) << "line " << line_num;
      } else {
        EXPECT_TRUE(s.ok()) << "line " << line_num;
        EXPECT_EQ(val, fv.value.value) << "line " << line_num;
      }
      std::vector<std::pair<GetTTLStatus, uint64_t>> ttl_ret;
      s = hash_->HExpireTime(user_key, {fv.field}, ttl_ret);
      EXPECT_TRUE(s.ok()) << "line " << line_num;
      EXPECT_EQ(ttl_ret.size(), 1) << "line " << line_num;
      if (fv.value.Expired()) {
        EXPECT_EQ(ttl_ret[0].first, GetTTLStatus::HGETEX_GET_NO_FIELD) << "line " << line_num;
        EXPECT_EQ(ttl_ret[0].second, 0) << "line " << line_num;
      } else if (!fv.value.HasTTL()) {
        EXPECT_EQ(ttl_ret[0].first, GetTTLStatus::HGETEX_GET_NO_TTL) << "line " << line_num;
        EXPECT_EQ(ttl_ret[0].second, 0) << "line " << line_num;
      } else {
        EXPECT_EQ(ttl_ret[0].first, GetTTLStatus::HGETEX_OK) << "line " << line_num;
        EXPECT_EQ(ttl_ret[0].second, fv.value.expire) << "line " << line_num;
      }
    }
  }

 protected:
  explicit RedisKKVTest() {
    kkv_ = std::make_shared<redis::KKV>(storage_, "kkv_ns");
    hash_ = std::shared_ptr<redis::Hash>(kkv_, kkv_.get());
  }
  ~RedisKKVTest() override = default;

  void SetUp() override { prev_encode_hash_sub_flag_ = encode_hash_sub_flag.exchange(true); }
  void TearDown() override { encode_hash_sub_flag.store(prev_encode_hash_sub_flag_); }

  std::shared_ptr<redis::KKV> kkv_;
  std::shared_ptr<redis::Hash> hash_;
  bool prev_encode_hash_sub_flag_;
};

TEST_F(RedisKKVTest, GetMetaData) {
  std::string str1 = "str1", str1_bk = "str1", str2 = "str2";
  // slice cmp
  Slice s1(str1), s2(str1_bk);
  EXPECT_TRUE(s1 == s2 && s1.data() != s2.data());
  // sub data cmp
  HashSubData h1, h2;
  EXPECT_TRUE(h1 == h2 && !(h1 != h2));
  EXPECT_TRUE(!h1.HasTTL() && !h1.Expired());
  EXPECT_TRUE(!h2.HasTTL() && !h2.Expired());
  h1.SetValue(str1);
  h2.SetValue(str2);
  EXPECT_TRUE(h1 != h2 && !(h1 == h2));
  EXPECT_TRUE(!h1.HasTTL() && !h1.Expired());
  EXPECT_TRUE(!h2.HasTTL() && !h2.Expired());
  h2.SetValue(str1_bk);
  EXPECT_TRUE(h1 == h2 && !(h1 != h2));
  EXPECT_TRUE(!h1.HasTTL() && !h1.Expired());
  EXPECT_TRUE(!h2.HasTTL() && !h2.Expired());
  // sub data cmp and expire
  h1.SetExpire(0);
  EXPECT_TRUE(h1 == h2 && !(h1 != h2));
  EXPECT_TRUE(!h1.HasTTL() && !h1.Expired());
  h1.SetExpire(1);
  EXPECT_TRUE(h1 != h2 && !(h1 == h2));
  EXPECT_TRUE(h1.HasTTL() && h1.Expired(0));
  h2.SetExpire(1);
  EXPECT_TRUE(h1 == h2 && !(h1 != h2));
  EXPECT_TRUE(h2.HasTTL() && h2.Expired(2));
  h1.SetExpire(0);
  h2.SetExpire(0);
  EXPECT_TRUE(h1 == h2 && !(h1 != h2));
  EXPECT_TRUE(!h1.HasTTL() && !h1.Expired(0));
  EXPECT_TRUE(!h2.HasTTL() && !h2.Expired(2));
  auto user_key = "getmetadata";
  auto field1 = "field-0";
  auto value1 = "value-0";
  uint64_t added_cnt = 0;
  // get meta data without sub flag
  encode_hash_sub_flag.store(false);
  auto s = hash_->Set(user_key, field1, value1, &added_cnt);
  EXPECT_TRUE(s.ok() && added_cnt == 1);
  HashMetadata meta_data;
  s = GetMetaData(user_key, &meta_data);
  EXPECT_TRUE(!s.ok() && s.IsNotSupported());
  s = hash_->Del(user_key);
  EXPECT_TRUE(s.ok());
  encode_hash_sub_flag.store(true);
  // get meta data with ttl
  s = hash_->Set(user_key, field1, value1, &added_cnt);
  EXPECT_TRUE(s.ok() && added_cnt == 1);
  s = hash_->Expire(user_key, util::GetTimeStampMS() + 50);
  EXPECT_TRUE(s.ok());
  s = GetMetaData(user_key, &meta_data);
  EXPECT_TRUE(!s.ok() && s.IsNotSupported());
}

TEST_F(RedisKKVTest, Set) {
  auto user_key = "set";
  HashMetadata exp_meta;
  auto curr_ts = util::GetTimeStampMS();
  std::vector<HashSubData> vals = {HashSubData("value-0"), HashSubData("value-1"), HashSubData("value-2")};
  std::vector<KKVFieldValue> fvs = {{"field-0", vals[0]}, {"field-1", vals[1]}, {"field-2", vals[2]}};
  // set nothing
  auto s = kkv_->Set(user_key, {});
  EXPECT_TRUE(s.ok());
  CheckMetaData(user_key, nullptr, __LINE__);
  // set field_0 without ttl and field_0 with expired ts
  auto fv0 = fvs[0];
  fv0.value.SetExpire(curr_ts - 50);
  s = kkv_->Set(user_key, {fvs[0], fv0});
  EXPECT_TRUE(s.ok());
  CheckMetaData(user_key, nullptr, __LINE__);
  // set field_0 without ttl
  s = kkv_->Set(user_key, {fvs[0]});
  EXPECT_TRUE(s.ok());
  exp_meta.size += 1;
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, {fvs[0]}, __LINE__);
  // set field_0 without ttl, field_1 with unexpired_ts, field_2 with expired_ts
  fvs[0].value.SetExpire(0);
  fvs[1].value.SetExpire(curr_ts + 30);
  fvs[2].value.SetExpire(curr_ts - 30);
  s = kkv_->Set(user_key, fvs);
  EXPECT_TRUE(s.ok());
  exp_meta.size += 1;
  exp_meta.SetSubTTL(1, curr_ts + 30);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, fvs, __LINE__);
  // set field_0 with unexpired_ts, field_1 with expired_ts, field_2 without ttl
  fvs[0].value.SetExpire(curr_ts + 40);
  fvs[1].value.SetExpire(curr_ts - 40);
  fvs[2].value.SetExpire(0);
  s = kkv_->Set(user_key, fvs);
  EXPECT_TRUE(s.ok());
  exp_meta.SetSubTTL(1, curr_ts + 40);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, fvs, __LINE__);
  // set field_0 without ttl, field_1 with unexpired_ts, field_2 with expired_ts
  fvs[0].value.SetExpire(0);
  fvs[1].value.SetExpire(curr_ts + 50);
  fvs[2].value.SetExpire(curr_ts - 50);
  s = kkv_->Set(user_key, fvs);
  EXPECT_TRUE(s.ok());
  exp_meta.SetSubTTL(1, curr_ts + 50);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, fvs, __LINE__);
  // get field_1 and meta_data after expired
  usleep(60000);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, fvs, __LINE__);
  // set field_1 with unexpired_ts after expired
  curr_ts = util::GetTimeStampMS();
  fvs[1].value.SetExpire(curr_ts + 50);
  s = kkv_->Set(user_key, {fvs[1]});
  EXPECT_TRUE(s.ok());
  exp_meta.SetSubTTL(1, curr_ts + 50);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, fvs, __LINE__);
}

TEST_F(RedisKKVTest, SetNX) {
  auto user_key = "setnx";
  HashMetadata exp_meta;
  auto curr_ts = util::GetTimeStampMS();
  std::vector<HashSubData> vals = {HashSubData("value-0"), HashSubData("value-1"), HashSubData("value-2")};
  std::vector<KKVFieldValue> fvs = {{"field-0", vals[0]}, {"field-1", vals[1]}, {"field-2", vals[2]}};
  bool flag = false;
  std::optional<std::string> actual_value;
  auto reset_resp = [&actual_value, &flag]() {
    actual_value = std::nullopt;
    flag = false;
  };
  // setnx field_0 with expired_ts
  reset_resp();
  fvs[0].value.SetExpire(curr_ts - 50);
  auto s = kkv_->SetNX(user_key, fvs[0].field, fvs[0].value, actual_value, &flag);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(flag);
  EXPECT_TRUE(!actual_value.has_value());
  CheckMetaData(user_key, nullptr, __LINE__);
  CheckFields(user_key, {fvs[0]}, __LINE__);
  // setnx field_0 without ttl
  reset_resp();
  fvs[0].value.SetExpire(0);
  s = kkv_->SetNX(user_key, fvs[0].field, fvs[0].value, actual_value, &flag);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(flag);
  EXPECT_TRUE(!actual_value.has_value());
  exp_meta.size += 1;
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, {fvs[0]}, __LINE__);
  // setnx field_0 with unexpired_ts
  reset_resp();
  fvs[0].value.SetExpire(curr_ts + 50);
  s = kkv_->SetNX(user_key, fvs[0].field, fvs[0].value, actual_value, &flag);
  fvs[0].value.SetExpire(0);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(!flag);
  EXPECT_TRUE(actual_value.has_value() && actual_value.value() == fvs[0].value.value);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, {fvs[0]}, __LINE__);
  // setnx field_1 with unexpired_ts
  reset_resp();
  fvs[1].value.SetExpire(curr_ts + 50);
  s = kkv_->SetNX(user_key, fvs[1].field, fvs[1].value, actual_value, &flag);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(flag);
  EXPECT_TRUE(!actual_value.has_value());
  exp_meta.size += 1;
  exp_meta.SetSubTTL(1, curr_ts + 50);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, {fvs[0], fvs[1]}, __LINE__);
  // setnx field_1 after expired
  usleep(60000);
  reset_resp();
  curr_ts = util::GetTimeStampMS();
  fvs[1].value.SetExpire(curr_ts + 50);
  s = kkv_->SetNX(user_key, fvs[1].field, fvs[1].value, actual_value, &flag);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(flag);
  EXPECT_TRUE(!actual_value.has_value());
  exp_meta.SetSubTTL(1, curr_ts + 50);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, {fvs[0], fvs[1]}, __LINE__);
  // setnx field_2 with expired_ts
  reset_resp();
  fvs[2].value.SetExpire(curr_ts - 50);
  s = kkv_->SetNX(user_key, fvs[2].field, fvs[2].value, actual_value, &flag);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(flag);
  EXPECT_TRUE(!actual_value.has_value());
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, {fvs[0], fvs[1], fvs[2]}, __LINE__);
}

TEST_F(RedisKKVTest, CAS) {
  auto user_key = "cas";
  HashMetadata exp_meta;
  auto curr_ts = util::GetTimeStampMS();
  std::vector<HashSubData> vals = {HashSubData("value-0"), HashSubData("value-1")};
  std::vector<KKVFieldValue> fvs = {{"field-0", vals[0]}, {"field-1", vals[1]}};
  bool flag = false;
  std::optional<std::string> actual_value;
  auto reset_resp = [&actual_value, &flag]() {
    actual_value = std::nullopt;
    flag = false;
  };
  // cas field_0 without ttl
  reset_resp();
  auto s = kkv_->CAS(user_key, fvs[0].field, fvs[0].value.value, fvs[0].value.value, nullptr, actual_value, &flag);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(!flag);
  EXPECT_TRUE(!actual_value.has_value());
  CheckMetaData(user_key, nullptr, __LINE__);
  // set field_0 without ttl, field_1 with unexpired_ts
  fvs[0].value.SetExpire(0);
  fvs[1].value.SetExpire(curr_ts + 30);
  s = kkv_->Set(user_key, {fvs[0], fvs[1]});
  EXPECT_TRUE(s.ok());
  exp_meta.size += 2;
  exp_meta.SetSubTTL(1, curr_ts + 30);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, fvs, __LINE__);
  // cas field_0 with mismatched value
  reset_resp();
  s = kkv_->CAS(user_key, fvs[0].field, fvs[1].value.value, fvs[1].value.value, nullptr, actual_value, &flag);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(!flag);
  EXPECT_TRUE(actual_value.has_value() && actual_value.value() == fvs[0].value.value);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, fvs, __LINE__);
  // cas field_0 with same value
  reset_resp();
  s = kkv_->CAS(user_key, fvs[0].field, fvs[0].value.value, fvs[0].value.value, nullptr, actual_value, &flag);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(flag);
  EXPECT_TRUE(actual_value.has_value() && actual_value.value() == fvs[0].value.value);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, fvs, __LINE__);
  // cas field_0 with unexpired_ts
  reset_resp();
  fvs[0].value.SetExpire(curr_ts + 40);
  s = kkv_->CAS(user_key, fvs[0].field, fvs[0].value.value, fvs[0].value.value, &fvs[0].value.expire, actual_value,
                &flag);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(flag);
  EXPECT_TRUE(actual_value.has_value() && actual_value.value() == fvs[0].value.value);
  exp_meta.SetSubTTL(0, curr_ts + 40);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, fvs, __LINE__);
  // cas field_0 without ttl
  reset_resp();
  fvs[0].value.SetExpire(0);
  s = kkv_->CAS(user_key, fvs[0].field, fvs[0].value.value, fvs[0].value.value, &fvs[0].value.expire, actual_value,
                &flag);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(flag);
  EXPECT_TRUE(actual_value.has_value() && actual_value.value() == fvs[0].value.value);
  exp_meta.SetSubTTL(1, curr_ts + 40);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, fvs, __LINE__);
  // cas field_1 with same value and same unexpired_ts
  reset_resp();
  s = kkv_->CAS(user_key, fvs[1].field, fvs[1].value.value, fvs[1].value.value, &fvs[1].value.expire, actual_value,
                &flag);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(flag);
  EXPECT_TRUE(actual_value.has_value() && actual_value.value() == fvs[1].value.value);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, fvs, __LINE__);
  // cas field_1 with expired_ts after expired
  usleep(40000);
  reset_resp();
  s = kkv_->CAS(user_key, fvs[1].field, fvs[1].value.value, fvs[1].value.value, &fvs[1].value.expire, actual_value,
                &flag);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(!flag);
  EXPECT_TRUE(!actual_value.has_value());
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, fvs, __LINE__);
}

TEST_F(RedisKKVTest, CAD) {
  auto user_key = "cad";
  HashMetadata exp_meta;
  auto curr_ts = util::GetTimeStampMS();
  std::vector<HashSubData> vals = {HashSubData("value-0"), HashSubData("value-1"), HashSubData("value-2")};
  std::vector<KKVFieldValue> fvs = {{"field-0", vals[0]}, {"field-1", vals[1]}, {"field-2", vals[2]}};
  bool flag = false;
  std::optional<std::string> actual_value;
  auto reset_resp = [&actual_value, &flag]() {
    actual_value = std::nullopt;
    flag = false;
  };
  // cad field_0 without ttl
  reset_resp();
  auto s = kkv_->CAD(user_key, fvs[0].field, fvs[0].value.value, actual_value, &flag);
  EXPECT_TRUE(!flag);
  EXPECT_TRUE(!actual_value.has_value());
  CheckMetaData(user_key, nullptr, __LINE__);
  // set field_0 without ttl, field_1 with unexpired_ts
  fvs[0].value.SetExpire(0);
  fvs[1].value.SetExpire(curr_ts + 30);
  fvs[2].value.SetExpire(curr_ts + 40);
  s = kkv_->Set(user_key, fvs);
  EXPECT_TRUE(s.ok());
  exp_meta.size += 3;
  exp_meta.SetSubTTL(1, curr_ts + 40);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, fvs, __LINE__);
  // cad field_0 with mismatched value
  reset_resp();
  s = kkv_->CAD(user_key, fvs[0].field, fvs[1].value.value, actual_value, &flag);
  EXPECT_TRUE(!flag);
  EXPECT_TRUE(actual_value.has_value() && actual_value.value() == fvs[0].value.value);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, fvs, __LINE__);
  // cad field_0 with matched value
  reset_resp();
  s = kkv_->CAD(user_key, fvs[0].field, fvs[0].value.value, actual_value, &flag);
  EXPECT_TRUE(flag);
  EXPECT_TRUE(actual_value.has_value() && actual_value.value() == fvs[0].value.value);
  exp_meta.size -= 1;
  exp_meta.SetSubTTL(0, curr_ts + 40);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  fvs[0].value.SetExpire(curr_ts - 30);
  CheckFields(user_key, fvs, __LINE__);
  // cad field_1 before expire
  reset_resp();
  s = kkv_->CAD(user_key, fvs[1].field, fvs[1].value.value, actual_value, &flag);
  EXPECT_TRUE(flag);
  EXPECT_TRUE(actual_value.has_value() && actual_value.value() == fvs[1].value.value);
  exp_meta.size -= 1;
  CheckMetaData(user_key, &exp_meta, __LINE__);
  fvs[1].value.SetExpire(curr_ts - 30);
  CheckFields(user_key, fvs, __LINE__);
  // cad field_2 after expired
  usleep(50000);
  reset_resp();
  s = kkv_->CAD(user_key, fvs[2].field, fvs[2].value.value, actual_value, &flag);
  EXPECT_TRUE(!flag);
  EXPECT_TRUE(!actual_value.has_value());
  CheckMetaData(user_key, nullptr, __LINE__);
  CheckFields(user_key, fvs, __LINE__);
}

TEST_F(RedisKKVTest, RemRange) {
  auto user_key = "rem_range";
  HashMetadata exp_meta;
  auto curr_ts = util::GetTimeStampMS();
  std::vector<HashSubData> vals = {HashSubData("value-0"), HashSubData("value-1"), HashSubData("value-2")};
  std::vector<KKVFieldValue> fvs = {{"field-0", vals[0]}, {"field-1", vals[1]}, {"field-2", vals[2]}};
  // remrange [field_1, field_2)
  std::string min = "[field_1";
  std::string max = "(field_2";
  RangeLexSpec spec;
  auto st = ParseRangeLexSpec(min, max, &spec);
  ASSERT_TRUE(st.IsOK());
  auto s = kkv_->RemRange(user_key, spec);
  EXPECT_TRUE(s.ok());
  std::vector<FieldValue> ret;
  s = kkv_->GetAll(user_key, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(ret.empty());
  // set field_0 without ttl, field_1 with unexpired_ts, field_2 with unexpired_ts
  fvs[0].value.SetExpire(0);
  fvs[1].value.SetExpire(curr_ts + 30);
  fvs[2].value.SetExpire(curr_ts + 50);
  s = kkv_->Set(user_key, fvs);
  EXPECT_TRUE(s.ok());
  exp_meta.size += 3;
  exp_meta.SetSubTTL(1, curr_ts + 50);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, fvs, __LINE__);
  // remrange [field_3, field_5)
  min = "[field-3";
  max = "(field-5";
  st = ParseRangeLexSpec(min, max, &spec);
  ASSERT_TRUE(st.IsOK());
  s = kkv_->RemRange(user_key, spec);
  EXPECT_TRUE(s.ok());
  CheckMetaData(user_key, &exp_meta, __LINE__);
  CheckFields(user_key, fvs, __LINE__);
  // remrange [field_0, field_2) after field_1 expired
  usleep(40000);
  min = "[field-0";
  max = "(field-2";
  st = ParseRangeLexSpec(min, max, &spec);
  s = kkv_->RemRange(user_key, spec);
  EXPECT_TRUE(s.ok());
  exp_meta.size -= 2;
  exp_meta.SetSubTTL(0, curr_ts + 50);
  CheckMetaData(user_key, &exp_meta, __LINE__);
  fvs[0].value.SetExpire(curr_ts + 30);
  CheckFields(user_key, fvs, __LINE__);
}

}  // namespace redis
