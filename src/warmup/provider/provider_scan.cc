#include <sstream>

#include "config/config.h"
#include "warmup/engine/warmup_engine.h"
#include "warmup/provider/warmup_provider.h"

namespace warmup {
static std::vector<std::string> SplitComma(const std::string& s) {
  std::vector<std::string> out;
  std::stringstream ss(s);
  std::string t;
  while (std::getline(ss, t, ','))
    if (!t.empty()) out.push_back(t);
  return out;
}
class ScanProvider : public WarmupProvider {
 public:
  explicit ScanProvider(const Config* cfg) : cfg_(cfg) {}
  rocksdb::Status Plan(rocksdb::DB* db, PlanContext* plan) override {
    if (!db || !plan) return rocksdb::Status::InvalidArgument("null");
    auto cfs = cfg_->warmup_iter_cf.empty() ? std::vector<std::string>{"default"} : SplitComma(cfg_->warmup_iter_cf);
    if (cfg_->warmup_iter_ranges.empty()) {
      for (auto& cf : cfs) {
        IterPlanRange range;
        range.cf = cf;
        range.start_key = "";
        range.end_key = "";
        plan->iter_ranges.push_back(range);
      }
    } else {
      // 支持 "a:z,1000:1FFF" 或 "meta:a:z"
      std::stringstream ss(cfg_->warmup_iter_ranges);
      std::string tok;
      while (std::getline(ss, tok, ',')) {
        auto p1 = tok.find(':');
        if (p1 == std::string::npos) continue;
        auto p2 = tok.find(':', p1 + 1);
        if (p2 == std::string::npos) {
          for (auto& cf : cfs) {
            IterPlanRange range;
            range.cf = cf;
            range.start_key = tok.substr(0, p1);
            range.end_key = tok.substr(p1 + 1);
            plan->iter_ranges.push_back(range);
          }
        } else {
          IterPlanRange range;
          range.cf = tok.substr(0, p1);
          range.start_key = tok.substr(p1 + 1, p2 - (p1 + 1));
          range.end_key = tok.substr(p2 + 1);
          plan->iter_ranges.push_back(range);
        }
      }
    }
    return rocksdb::Status::OK();
  }
  rocksdb::Status Execute(rocksdb::DB* db, const PlanContext& plan, Progress* prog) override {
    WarmupEngine engine(db);
    WarmupEngine::EngineOptions opts;
    opts.concurrency = cfg_->warmup_concurrency;
    opts.rate_limit_mb = cfg_->warmup_rate_limit_mb;
    opts.read_tier = cfg_->warmup_iter_read_tier;
    opts.fill_cache = cfg_->warmup_iter_fill_cache;
    opts.max_scan_mb = cfg_->warmup_iter_max_mb;
    opts.seek_step = cfg_->warmup_iter_seek_step;
    opts.timeout_sec = cfg_->warmup_timeout_sec;
    opts.readahead_size = cfg_->warmup_iter_readahead_kb * 1024;  // KB to bytes
    opts.verify_checksums = cfg_->warmup_iter_verify_checksums;
    opts.prefix_same_as_start = cfg_->warmup_iter_prefix_same_as_start;
    opts.tombstone_stop_after = cfg_->warmup_iter_tombstone_stop_after;
    opts.read_tier = cfg_->warmup_iter_read_tier;  // 使用新的 read_tier 配置
    opts.constrain_range = true;                   // 启用范围约束
    uint64_t keys = 0, bytes = 0;
    // Convert IterPlanRange to WarmupEngine::IterRange
    std::vector<WarmupEngine::IterRange> engine_ranges;
    for (const auto& range : plan.iter_ranges) {
      engine_ranges.push_back({range.cf, range.start_key, range.end_key});
    }
    auto s = engine.WarmupByIterator(engine_ranges, opts, &keys, &bytes);
    if (!s.ok()) return s;
    prog->processed_files = 0;
    prog->processed_bytes = bytes;
    prog->ratio = 1.0;
    return rocksdb::Status::OK();
  }

 private:
  const Config* cfg_;
};
std::unique_ptr<WarmupProvider> MakeScanProvider(const Config* cfg) { return std::make_unique<ScanProvider>(cfg); }
}  // namespace warmup
