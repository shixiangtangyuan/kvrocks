#pragma once

#include "common/range_spec.h"
#include "types/redis_hash.h"

namespace redis {

struct KKVFieldValue {
  KKVFieldValue(Slice field, HashSubData &value) : field(field), value(value) {}

  Slice field;
  HashSubData value;
};

class KKV : public Hash {
 public:
  KKV(engine::Storage *storage, const std::string &ns) : Hash(storage, ns) {}

  // set kkv data
  rocksdb::Status Set(const Slice &user_key, const std::vector<KKVFieldValue> &field_values);
  // set kkv data when not exist
  rocksdb::Status SetNX(const Slice &user_key, const Slice &field, const HashSubData &sub_data,
                        std::optional<std::string> &actual_val, bool *flag);
  // update kkv data when compare matched
  rocksdb::Status CAS(const Slice &user_key, const Slice &field, const Slice &cmp_val, const Slice &swap_val,
                      uint64_t *swap_ts, std::optional<std::string> &actual_val, bool *flag);
  // delete kkv data when compare matched
  rocksdb::Status CAD(const Slice &user_key, const Slice &field, const Slice &cmp_val,
                      std::optional<std::string> &actual_val, bool *flag);
  // remove kkv data with field in range [lower, upper) under user_key
  rocksdb::Status RemRange(const Slice &user_key, const RangeLexSpec &spec);

 private:
  friend class RedisKKVTest;

  rocksdb::Status GetMetadata(const Slice &ns_key, HashMetadata *meta_data) {
    auto s = Hash::GetMetadata(ns_key, meta_data);
    if (s.ok()) {
      if (!meta_data->HasSubFlag()) {
        return rocksdb::Status::NotSupported("sub value encode not enabled for sub keys under this meta key");
      }
      if (meta_data->HasTTL()) {
        return rocksdb::Status::NotSupported("sub value ttl not enabled for sub keys under this meta key");
      }
    }
    return s;
  }

  [[nodiscard]] rocksdb::Status updateMetaData(const Slice &ns_key, HashMetadata &meta_data, int64_t delta_cnt,
                                               int64_t delta_persist_field_cnt, uint64_t max_expire_ts,
                                               ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch) {
    bool updated = false;
    return updateMetaData(ns_key, meta_data, delta_cnt, delta_persist_field_cnt, max_expire_ts, batch, updated);
  }

  [[nodiscard]] rocksdb::Status updateMetaData(const Slice &ns_key, HashMetadata &meta_data, int64_t delta_cnt,
                                               int64_t delta_persist_field_cnt, uint64_t max_expire_ts,
                                               ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch, bool &updated) {
    max_expire_ts = std::max(max_expire_ts, meta_data.fields_max_expire_at);
    if (max_expire_ts != 0) {
      if (delta_persist_field_cnt != 0 || max_expire_ts > meta_data.fields_max_expire_at) {
        uint64_t persist_field_size = meta_data.persist_field_size;
        if (!meta_data.IsSubTTLSet()) persist_field_size = meta_data.size;
        meta_data.SetSubTTL(persist_field_size + delta_persist_field_cnt, max_expire_ts);
        updated = true;
      }
    }
    if (delta_cnt != 0) {
      meta_data.size += delta_cnt;
      updated = true;
    }
    if (updated) {
      std::string write_val;
      meta_data.Encode(&write_val);
      return batch->Put(metadata_cf_handle_, ns_key, write_val);
    }
    return rocksdb::Status::OK();
  }
};

}  // namespace redis
