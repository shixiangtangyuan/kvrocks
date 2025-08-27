#include "types/redis_kkv.h"

#include <unordered_set>

#include "common/db_util.h"

namespace redis {

rocksdb::Status KKV::Set(const Slice &user_key, const std::vector<KKVFieldValue> &field_values) {
  if (field_values.empty()) return rocksdb::Status::OK();
  HashMetadata meta_data;
  std::string ns_key = AppendNamespacePrefix(user_key);
  auto s = GetMetadata(ns_key, &meta_data);
  if (!s.ok() && !s.IsNotFound()) return s;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash, {EnumToString(RedisHashCommand::kCmdKKVHSet)});
  batch->PutLogData(log_data.Encode());

  bool update_sub = false;
  auto curr_ts = util::GetTimeStampMS();
  std::unordered_set<std::string_view> fields_set;
  int64_t delta_cnt = 0, delta_persist_field_cnt = 0;
  uint64_t max_expire_ts = meta_data.fields_max_expire_at;
  for (auto it = field_values.rbegin(); it != field_values.rend(); ++it) {
    if (!fields_set.insert(it->field.ToStringView()).second) continue;
    bool data_valid = !it->value.Expired(curr_ts);
    // skip when key not exist && field expired
    if (meta_data.size == 0 && !data_valid) continue;

    bool do_put = false, do_del = false;
    auto sub_key = InternalKey(ns_key, it->field, meta_data.version, storage_->IsSlotIdEncoded()).Encode();
    if (meta_data.size != 0) {
      StringOrPinSlice read_val;
      s = storage_->Get(rocksdb::ReadOptions(), sub_key, &read_val);
      if (s.ok()) {
        HashSubData prev_data;
        s = prev_data.Decode(&meta_data, &read_val, curr_ts);
        if (s.ok()) {
          if (data_valid) {
            // update when sub key alive
            delta_persist_field_cnt += prev_data.HasTTL() - it->value.HasTTL();
            do_put = it->value != prev_data;
          } else {
            // delete when sub key alive
            if (!prev_data.HasTTL()) --delta_persist_field_cnt;
            do_del = true;
            --delta_cnt;
          }
        } else if (s.IsNotFound()) {
          if (data_valid) {
            // update when sub key expired
            if (!it->value.HasTTL()) ++delta_persist_field_cnt;
            do_put = true;
          }
        } else {
          return s;
        }
      } else if (s.IsNotFound()) {
        if (data_valid) {
          // create when sub key not exist
          if (!it->value.HasTTL()) ++delta_persist_field_cnt;
          do_put = true;
          ++delta_cnt;
        }
      } else {
        return s;
      }
    } else {
      // create when sub key not exist
      if (!it->value.HasTTL()) ++delta_persist_field_cnt;
      do_put = true;
      ++delta_cnt;
    }

    if (do_put) {
      if (it->value.HasTTL()) {
        max_expire_ts = std::max(max_expire_ts, it->value.expire);
      }
      std::string write_val;
      it->value.Encode(&meta_data, &write_val);
      s = batch->Put(sub_key, write_val);
      if (!s.ok()) return s;
      update_sub = true;
    } else if (do_del) {
      s = batch->Delete(sub_key);
      if (!s.ok()) return s;
      update_sub = true;
    }
  }

  bool update_meta = false;
  s = updateMetaData(ns_key, meta_data, delta_cnt, delta_persist_field_cnt, max_expire_ts, batch, update_meta);
  if (s.ok() && (update_meta || update_sub)) {
    return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }
  return s;
}

rocksdb::Status KKV::SetNX(const Slice &user_key, const Slice &field, const HashSubData &sub_data,
                           std::optional<std::string> &actual_val, bool *flag) {
  *flag = false;
  HashMetadata meta_data;
  std::string ns_key = AppendNamespacePrefix(user_key);
  auto s = GetMetadata(ns_key, &meta_data);
  if (!s.ok() && !s.IsNotFound()) return s;
  auto curr_ts = util::GetTimeStampMS();
  bool data_valid = !sub_data.Expired(curr_ts);
  // create an expired sub key
  if (meta_data.size == 0 && !data_valid) {
    *flag = true;
    return rocksdb::Status::OK();
  };

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash,
                             {EnumToString(RedisHashCommand::kCmdKKVHSetNX), std::to_string(sub_data.expire)});
  batch->PutLogData(log_data.Encode());

  int64_t delta_cnt = 0, delta_persist_field_cnt = 0;
  auto sub_key = InternalKey(ns_key, field, meta_data.version, storage_->IsSlotIdEncoded()).Encode();
  if (meta_data.size != 0) {
    StringOrPinSlice read_val;
    s = storage_->Get(rocksdb::ReadOptions(), sub_key, &read_val);
    if (s.ok()) {
      HashSubData prev_data;
      s = prev_data.Decode(&meta_data, &read_val, curr_ts);
      if (s.ok()) {
        // sub key alive
        actual_val = prev_data.value.ToString();
        return rocksdb::Status::OK();
      } else if (s.IsNotFound()) {
        // sub key expired
        if (!sub_data.HasTTL()) ++delta_persist_field_cnt;
      } else {
        return s;
      }
    } else if (s.IsNotFound()) {
      // sub key not exist
      if (!sub_data.HasTTL()) ++delta_persist_field_cnt;
      ++delta_cnt;
    } else {
      return s;
    }
  } else {
    if (!sub_data.HasTTL()) ++delta_persist_field_cnt;
    ++delta_cnt;
  }

  *flag = true;
  // create an expired sub key
  if (!data_valid) return rocksdb::Status::OK();
  // create sub key
  std::string write_val;
  sub_data.Encode(&meta_data, &write_val);
  s = batch->Put(sub_key, write_val);
  if (!s.ok()) return s;
  auto max_expire_ts = std::max(sub_data.expire, meta_data.fields_max_expire_at);
  s = updateMetaData(ns_key, meta_data, delta_cnt, delta_persist_field_cnt, max_expire_ts, batch);
  if (!s.ok()) return s;
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status KKV::CAS(const Slice &user_key, const Slice &field, const Slice &cmp_val, const Slice &swap_val,
                         uint64_t *swap_ts, std::optional<std::string> &actual_val, bool *flag) {
  *flag = false;
  HashMetadata meta_data;
  std::string ns_key = AppendNamespacePrefix(user_key);
  auto s = GetMetadata(ns_key, &meta_data);
  if (!s.ok() && !s.IsNotFound()) return s;
  // sub key not exist
  if (meta_data.size == 0) return rocksdb::Status::OK();

  uint64_t expire_at = 0;
  HashSubData sub_data{swap_val};
  int64_t delta_persist_field_cnt = 0;
  auto curr_ts = util::GetTimeStampMS();
  auto sub_key = InternalKey(ns_key, field, meta_data.version, storage_->IsSlotIdEncoded()).Encode();
  StringOrPinSlice read_val;
  s = storage_->Get(rocksdb::ReadOptions(), sub_key, &read_val);
  if (s.ok()) {
    HashSubData prev_data;
    s = prev_data.Decode(&meta_data, &read_val, curr_ts);
    if (s.ok()) {
      actual_val = prev_data.value.ToString();
      // sub value compare mismatch
      if (prev_data.value != cmp_val) {
        return rocksdb::Status::OK();
      }
      *flag = true;
      if (swap_ts) {
        sub_data.SetExpire(*swap_ts);
        expire_at = *swap_ts;
      } else {
        sub_data.SetExpire(prev_data.expire);
        expire_at = prev_data.expire;
      }
      // sub value not changed
      if (sub_data == prev_data) {
        return rocksdb::Status::OK();
      }
      delta_persist_field_cnt += prev_data.HasTTL() - sub_data.HasTTL();
    }
  }
  // sub key not exist or expired
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash, {EnumToString(RedisHashCommand::kCmdKKVHCAS), std::to_string(expire_at)});
  batch->PutLogData(log_data.Encode());

  // sub value compare matched
  int64_t delta_cnt = 0;
  bool data_valid = !sub_data.Expired(curr_ts);
  uint64_t max_expire_ts = meta_data.fields_max_expire_at;
  if (data_valid) {
    if (sub_data.HasTTL()) {
      max_expire_ts = std::max(max_expire_ts, sub_data.expire);
    }
    std::string write_val;
    sub_data.Encode(&meta_data, &write_val);
    s = batch->Put(sub_key, write_val);
    if (!s.ok()) return s;
  } else {
    --delta_cnt;
    s = batch->Delete(sub_key);
    if (!s.ok()) return s;
  }
  s = updateMetaData(ns_key, meta_data, delta_cnt, delta_persist_field_cnt, max_expire_ts, batch);
  if (!s.ok()) return s;
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status KKV::CAD(const Slice &user_key, const Slice &field, const Slice &cmp_val,
                         std::optional<std::string> &actual_val, bool *flag) {
  *flag = false;
  HashMetadata meta_data;
  std::string ns_key = AppendNamespacePrefix(user_key);
  auto s = GetMetadata(ns_key, &meta_data);
  if (!s.ok() && !s.IsNotFound()) return s;
  // sub key not exist
  if (meta_data.size == 0) {
    return rocksdb::Status::OK();
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash, {EnumToString(RedisHashCommand::kCmdKKVHCAD)});
  batch->PutLogData(log_data.Encode());

  auto curr_ts = util::GetTimeStampMS();
  auto sub_key = InternalKey(ns_key, field, meta_data.version, storage_->IsSlotIdEncoded()).Encode();
  StringOrPinSlice read_val;
  s = storage_->Get(rocksdb::ReadOptions(), sub_key, &read_val);
  if (s.ok()) {
    HashSubData prev_data;
    s = prev_data.Decode(&meta_data, &read_val, curr_ts);
    if (s.ok()) {
      actual_val = prev_data.value.ToString();
      // sub value compare mismatch
      if (prev_data.value != cmp_val) {
        return rocksdb::Status::OK();
      }
      if (!prev_data.HasTTL() && meta_data.IsSubTTLSet()) {
        --meta_data.persist_field_size;
      }
      --meta_data.size;
    }
  }
  // sub key not exist or expired
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  // sub value compare matched
  *flag = true;
  s = batch->Delete(sub_key);
  if (!s.ok()) return s;
  std::string write_val;
  meta_data.Encode(&write_val);
  s = batch->Put(metadata_cf_handle_, ns_key, write_val);
  if (!s.ok()) return s;
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status KKV::RemRange(const Slice &user_key, const RangeLexSpec &spec) {
  if (spec.min.compare(spec.max) >= 0) {
    return rocksdb::Status::InvalidArgument("lower field should small than upper field");
  }
  HashMetadata meta_data;
  std::string ns_key = AppendNamespacePrefix(user_key);
  auto s = GetMetadata(ns_key, &meta_data);
  if (!s.ok() && !s.IsNotFound()) return s;
  // no sub key in range [lower, upper)
  if (meta_data.size == 0) return rocksdb::Status::OK();

  std::string lower_key = InternalKey(ns_key, spec.min, meta_data.version, storage_->IsSlotIdEncoded()).Encode();
  std::string upper_key = InternalKey(ns_key, spec.max, meta_data.version, storage_->IsSlotIdEncoded()).Encode();
  auto lower_bound = rocksdb::Slice(lower_key);
  auto upper_bound = rocksdb::Slice(upper_key);

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  read_options.iterate_lower_bound = &lower_bound;
  read_options.iterate_upper_bound = &upper_bound;
  auto iter = util::UniqueIterator(storage_, read_options);
  iter->Seek(lower_bound);
  // no sub key in range [lower, upper) or iterate failed
  if (!iter->Valid()) return iter->status();
  auto curr_ts = util::GetTimeStampMS();
  for (; iter->Valid(); iter->Next()) {
    HashSubData sub_data;
    s = sub_data.Decode(&meta_data, iter->value(), curr_ts);
    if (s.ok()) {
      if (!sub_data.HasTTL() && meta_data.IsSubTTLSet()) {
        --meta_data.persist_field_size;
      }
      --meta_data.size;
    } else if (s.IsNotFound()) {
      --meta_data.size;
    } else {
      return s;
    }
  }
  // iterate failed
  s = iter->status();
  if (!s.ok()) return s;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash,
                             {EnumToString(RedisHashCommand::kCmdKKVHRemRangeByLex), spec.para_min, spec.para_max});
  batch->PutLogData(log_data.Encode());
  s = batch->DeleteRange(lower_bound, upper_bound);
  if (!s.ok()) return s;
  std::string write_val;
  meta_data.Encode(&write_val);
  s = batch->Put(metadata_cf_handle_, ns_key, write_val);
  if (!s.ok()) return s;
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

}  // namespace redis
