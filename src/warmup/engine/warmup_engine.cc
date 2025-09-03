#include "warmup/engine/warmup_engine.h"

#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/utilities/checkpoint.h>

#include <thread>

#include "warmup/engine/rate_limiter.h"
#include "warmup/metrics/warmup_metrics.h"

namespace warmup {

rocksdb::Status WarmupEngine::WarmupByIterator(const std::vector<IterRange>& ranges, const EngineOptions& opts,
                                               uint64_t* out_keys, uint64_t* out_bytes) {
  if (!db_) return rocksdb::Status::InvalidArgument("db null");
  uint64_t keys = 0, bytes = 0;
  SimpleRateLimiter limiter(opts.rate_limit_mb);

  // For simplicity, we'll use the default column family handle
  // In a real implementation, you might want to get handles from the storage layer
  std::unordered_map<std::string, rocksdb::ColumnFamilyHandle*> cfmap;
  cfmap["default"] = nullptr;  // nullptr means default column family

  for (auto& r : ranges) {
    auto it_cf = cfmap.find(r.cf.empty() ? "default" : r.cf);
    rocksdb::ColumnFamilyHandle* h = (it_cf == cfmap.end() ? nullptr : it_cf->second);
    rocksdb::ReadOptions ro;
    ro.read_tier = opts.read_tier == 0 ? rocksdb::kReadAllTier : rocksdb::kBlockCacheTier;
    ro.fill_cache = opts.fill_cache;
    if (opts.readahead_size > 0) {
      ro.readahead_size = opts.readahead_size;
    }
    ro.verify_checksums = opts.verify_checksums;

    // PATCH: 设置范围约束，避免误拉 Data Block
    if (opts.constrain_range && !r.end_key.empty()) {
      rocksdb::Slice upper_bound_slice(r.end_key);
      ro.iterate_upper_bound = &upper_bound_slice;
    }
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro, h));
    if (!r.start_key.empty())
      it->Seek(r.start_key);
    else
      it->SeekToFirst();
    uint64_t scanned_bytes = 0;
    uint64_t tombstone_hits = 0;
    uint64_t no_hit_steps = 0;

    for (; it->Valid(); it->Next()) {
      const auto k = it->key();
      const auto v = it->value();
      keys++;
      bytes += k.size() + v.size();
      scanned_bytes += k.size() + v.size();

      // 墓碑密集早停检测
      if (v.size() == 0) {  // 简单墓碑检测：空值
        tombstone_hits++;
        no_hit_steps = 0;  // 重置无命中计数
      } else {
        no_hit_steps++;
      }

      // 早停条件：墓碑密度过高或无命中步数过多
      if (tombstone_hits >= opts.tombstone_stop_after || no_hit_steps >= opts.tombstone_stop_after * 2) {
        break;
      }

      if (!r.end_key.empty() && k.compare(r.end_key) > 0) break;
      if (opts.max_scan_mb > 0 && scanned_bytes > (uint64_t)opts.max_scan_mb * 1024ULL * 1024ULL) break;
      if (opts.rate_limit_mb > 0) limiter.Consume(k.size() + v.size());
    }
    if (!it->status().ok()) return it->status();
  }

  if (out_keys) *out_keys = keys;
  if (out_bytes) *out_bytes = bytes;
  return rocksdb::Status::OK();
}

rocksdb::Status WarmupEngine::WarmupByFI(const std::vector<FIFile>& files, const EngineOptions& opts,
                                         uint64_t* out_files, uint64_t* out_meta_bytes) {
  if (!db_) return rocksdb::Status::InvalidArgument("db null");
  uint64_t cnt = 0, meta = 0;
  SimpleRateLimiter limiter(opts.rate_limit_mb);
  // 通过 Seek 到每个文件的 smallest key，触发 table 打开并把 index/filter 放入 block cache
  for (auto& f : files) {
    rocksdb::ReadOptions ro;
    ro.read_tier = rocksdb::kReadAllTier;
    ro.fill_cache = true;
    ro.readahead_size = 0;  // PATCH: 避免预读 Data Block

    // PATCH: 设置范围约束，只预热 Index/Filter
    if (!f.smallest_key.empty()) {
      // 设置一个小的范围，避免读取太多数据
      std::string upper_bound = f.smallest_key;
      upper_bound.push_back('\0');  // 简单的范围约束
      rocksdb::Slice upper_bound_slice(upper_bound);
      ro.iterate_upper_bound = &upper_bound_slice;
    }

    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro));
    if (!f.smallest_key.empty())
      it->Seek(f.smallest_key);
    else
      it->SeekToFirst();

    // PATCH: 只做 Seek，不调用 Next()，避免读取 Data Block
    if (it->Valid()) {
      auto s = it->status();
      if (!s.ok()) return s;
    }
    cnt++;
    meta += f.meta_bytes_est ? f.meta_bytes_est : 64 * 1024;  // 粗估，避免除零
    if (opts.rate_limit_mb > 0) limiter.Consume(64 * 1024);
  }
  if (out_files) *out_files = cnt;
  if (out_meta_bytes) *out_meta_bytes = meta;
  return rocksdb::Status::OK();
}
}  // namespace warmup
