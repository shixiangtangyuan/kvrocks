#pragma once
#include <rocksdb/db.h>

#include <memory>
#include <string>
#include <vector>

struct Config;  // forward

namespace warmup {
struct IterPlanRange {
  std::string cf, start_key, end_key;
};
struct FIPlanEntry {
  std::string cf_name, smallest_key;
  uint64_t meta_bytes_est{0};
};

struct PlanContext {
  // iter
  std::vector<IterPlanRange> iter_ranges;
  // fi
  std::vector<FIPlanEntry> fi_files;
  uint64_t fi_budget_bytes{0};
};

struct Progress {
  uint64_t processed_files{0};
  uint64_t processed_bytes{0};
  double ratio{0.0};
};

class WarmupProvider {
 public:
  virtual ~WarmupProvider() = default;
  virtual rocksdb::Status Plan(rocksdb::DB* db, PlanContext* plan) = 0;
  virtual rocksdb::Status Execute(rocksdb::DB* db, const PlanContext& plan, Progress* prog) = 0;
};

std::unique_ptr<WarmupProvider> MakeFIProvider(const Config* cfg);
std::unique_ptr<WarmupProvider> MakeScanProvider(const Config* cfg);
}  // namespace warmup
