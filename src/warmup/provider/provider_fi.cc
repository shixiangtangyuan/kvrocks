#include <unordered_set>

#include "config/config.h"
#include "warmup/engine/warmup_engine.h"
#include "warmup/metrics/warmup_metrics.h"
#include "warmup/provider/warmup_provider.h"
#include "warmup/scoring/fi_selector.h"

namespace warmup {
class FIProvider : public WarmupProvider {
 public:
  explicit FIProvider(const Config* cfg) : cfg_(cfg) {}
  rocksdb::Status Plan(rocksdb::DB* db, PlanContext* plan) override {
    if (!db || !plan) return rocksdb::Status::InvalidArgument("null");
    std::vector<rocksdb::LiveFileMetaData> metas;
    db->GetLiveFilesMetaData(&metas);
    for (auto& m : metas) {
      plan->fi_files.push_back({m.column_family_name, m.smallestkey, 0});
    }
    return rocksdb::Status::OK();
  }
  rocksdb::Status Execute(rocksdb::DB* db, const PlanContext& plan, Progress* prog) override {
    if (!db || !prog) return rocksdb::Status::InvalidArgument("null");
    std::vector<rocksdb::LiveFileMetaData> metas;
    db->GetLiveFilesMetaData(&metas);
    std::vector<FileInfo> files;
    files.reserve(metas.size());
    for (auto& m : metas) {
      FileInfo f;
      f.cf_name = m.column_family_name;
      f.file_path = m.name;
      f.smallest_key = m.smallestkey;
      f.largest_key = m.largestkey;
      f.file_size_bytes = m.size;
      f.level = m.level;
      files.push_back(std::move(f));
    }
    uint64_t cache_bytes = cfg_->warmup_block_cache_bytes ? cfg_->warmup_block_cache_bytes : (16ULL << 30);
    uint64_t budget = (uint64_t)(cache_bytes * cfg_->warmup_fi_budget_ratio);
    FISelectorConfig sc;
    sc.coverage_ratio = cfg_->warmup_fi_coverage_ratio;
    sc.max_single_file_ratio = cfg_->warmup_fi_max_single_file_ratio;
    sc.min_files_per_level = cfg_->warmup_fi_min_files_per_level;
    sc.max_files_per_level = cfg_->warmup_fi_max_files_per_level;
    sc.kappa = cfg_->warmup_fi_kappa;
    sc.dedupe_with_tablecache = cfg_->warmup_fi_dedupe_with_table_cache;
    sc.level_weights.clear();
    // parse level weights: "0:0.5,1:0.3,2:0.15,3:0.05"
    {
      std::string s = cfg_->warmup_fi_level_weights;
      size_t i = 0;
      while (i < s.size()) {
        auto j = s.find(',', i);
        auto item = s.substr(i, j == std::string::npos ? s.size() - i : j - i);
        auto p = item.find(':');
        if (p != std::string::npos) {
          uint32_t lv = (uint32_t)std::stoul(item.substr(0, p));
          double w = std::stod(item.substr(p + 1));
          sc.level_weights[lv] = w;
        }
        if (j == std::string::npos)
          break;
        else
          i = j + 1;
      }
    }
    FISelector selector(sc);

    // 如果启用了去重，获取已打开的文件列表
    std::vector<FileInfo> files_to_process = files;
    if (cfg_->fi_dedup_enabled) {
      std::unordered_set<std::string> opened_files;

      // 获取 RocksDB 已打开的文件列表
      // 通过 table cache 信息来获取已打开的文件
      std::string table_cache_usage;
      if (db->GetProperty("rocksdb.estimate-table-readers-mem", &table_cache_usage)) {
        // 如果 table cache 有使用，说明有文件被打开
        // 这里我们可以通过遍历所有文件，检查哪些文件已经在 table cache 中
        // 由于 RocksDB 没有直接的 API 获取已打开文件列表，我们暂时跳过这个功能
        // 在实际实现中，可能需要通过 RocksDB 的内部 API 或者监控 table cache 状态
        LOG(INFO) << "FI dedup enabled but table cache info not available, skipping deduplication";
      }

      // 暂时跳过去重，因为需要更复杂的 RocksDB 内部 API
      // files_to_process = selector.DeduplicateFiles(files, opened_files);
    }

    auto chosen = selector.MakePlan(files_to_process, budget);
    std::vector<WarmupEngine::FIFile> targets;
    for (auto& c : chosen.coverage) targets.push_back({c.cf_name, c.smallest_key, c.meta_bytes_est});
    for (auto& c : chosen.efficiency) targets.push_back({c.cf_name, c.smallest_key, c.meta_bytes_est});

    WarmupEngine engine(db);
    WarmupEngine::EngineOptions opts;
    opts.concurrency = cfg_->warmup_concurrency;
    opts.rate_limit_mb = cfg_->warmup_rate_limit_mb;
    opts.read_tier = 0;
    opts.fill_cache = true;
    opts.timeout_sec = cfg_->warmup_timeout_sec;
    uint64_t nfiles = 0, nmeta = 0;
    auto s = engine.WarmupByFI(targets, opts, &nfiles, &nmeta);
    if (!s.ok()) return s;
    prog->processed_files = nfiles;
    prog->processed_bytes = nmeta;
    prog->ratio = chosen.total_meta_bytes > 0 ? (double)nmeta / (double)chosen.total_meta_bytes : 1.0;
    return rocksdb::Status::OK();
  }

 private:
  const Config* cfg_;
};

std::unique_ptr<WarmupProvider> MakeFIProvider(const Config* cfg) { return std::make_unique<FIProvider>(cfg); }
}  // namespace warmup
