#pragma once
#include <rocksdb/db.h>

#include <string>
#include <vector>

namespace warmup {

class WarmupEngine {
 public:
  struct EngineOptions {
    int concurrency{4};
    int64_t rate_limit_mb{0};
    int read_tier{0};  // 0=ReadAllTier,1=BlockCacheTier
    bool fill_cache{true};
    int64_t timeout_sec{300};
    // Iterator options
    int64_t max_scan_mb{1024};
    int seek_step{4096};
    uint64_t readahead_size{0};           // Iterator readahead 大小
    bool verify_checksums{false};         // 是否校验 checksum
    bool prefix_same_as_start{false};     // 是否使用 prefix_same_as_start
    uint64_t tombstone_stop_after{1000};  // 墓碑命中阈值
    bool constrain_range{true};           // 是否使用 iterate_upper_bound 约束范围
  };
  struct IterRange {
    std::string cf;
    std::string start_key;
    std::string end_key;
  };
  struct FIFile {
    std::string cf;
    std::string smallest_key;
    uint64_t meta_bytes_est{0};
  };

  explicit WarmupEngine(rocksdb::DB* db) : db_(db) {}

  rocksdb::Status WarmupByIterator(const std::vector<IterRange>& ranges, const EngineOptions& opts, uint64_t* out_keys,
                                   uint64_t* out_bytes);

  rocksdb::Status WarmupByFI(const std::vector<FIFile>& files, const EngineOptions& opts, uint64_t* out_files,
                             uint64_t* out_meta_bytes);

 private:
  rocksdb::DB* db_{nullptr};
};
}  // namespace warmup
