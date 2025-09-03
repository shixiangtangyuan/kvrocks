#include "warmup/scoring/fi_selector.h"

#include <algorithm>
#include <cmath>
#include <unordered_map>
#include <unordered_set>

namespace warmup {
static uint64_t EstimateMetaBytes(uint64_t file_size_bytes) {
  uint64_t est = static_cast<uint64_t>(file_size_bytes * 0.015);
  if (est < 1024ULL) est = 1024ULL;
  return est;
}

static double LevelScore(uint32_t level) {
  static const double lw[] = {1.0, 0.8, 0.6, 0.4, 0.3, 0.2, 0.1};
  return lw[level < 7 ? level : 6];
}

FIPlan FISelector::MakePlan(const std::vector<FileInfo>& files, uint64_t budget_bytes) const {
  // 1) 分层收集，并按 meta_bytes 升序排序，做覆盖阶段
  std::unordered_map<uint32_t, std::vector<FIChosen>> by_level;
  for (auto& f : files) {
    by_level[f.level].push_back({f.cf_name, f.smallest_key, EstimateMetaBytes(f.file_size_bytes), f.level});
  }
  for (auto& [lv, vec] : by_level) {
    std::sort(vec.begin(), vec.end(),
              [](const FIChosen& a, const FIChosen& b) { return a.meta_bytes_est < b.meta_bytes_est; });
  }

  // 预算分配
  uint64_t coverage_budget = (uint64_t)(budget_bytes * cfg_.coverage_ratio);
  uint64_t efficiency_budget = budget_bytes - coverage_budget;
  uint64_t single_cap = (uint64_t)(budget_bytes * cfg_.max_single_file_ratio);

  FIPlan plan;
  // 覆盖阶段：各层按照 level_weights 分配 coverage_budget
  double total_w = 0.0;
  for (auto& kv : cfg_.level_weights) total_w += kv.second;
  for (auto& kv : cfg_.level_weights) {
    uint32_t lv = kv.first;
    double w = (total_w > 0 ? kv.second / total_w : 0.0);
    uint64_t lv_budget = (uint64_t)(coverage_budget * w);
    uint64_t used = 0;
    int selected = 0;
    auto it = by_level.find(lv);
    if (it == by_level.end()) continue;
    for (auto& ch : it->second) {
      if (ch.meta_bytes_est > single_cap) continue;
      if (used + ch.meta_bytes_est > lv_budget) break;
      plan.coverage.push_back(ch);
      used += ch.meta_bytes_est;
      selected++;
      if (selected >= cfg_.max_files_per_level) break;
    }
    plan.total_meta_bytes += used;
  }

  // 效率阶段：剩余文件按 cost_efficiency 降序贪心
  struct Node {
    FIChosen ch;
    double score;
    double ce;
  };
  std::vector<Node> pool;
  pool.reserve(files.size());
  // 建表已选
  std::unordered_map<std::string, bool> picked;
  for (auto& ch : plan.coverage) picked[ch.cf_name + "|" + ch.smallest_key] = true;

  for (auto& f : files) {
    FIChosen ch{f.cf_name, f.smallest_key, EstimateMetaBytes(f.file_size_bytes), f.level};
    if (picked[ch.cf_name + "|" + ch.smallest_key]) continue;
    double level_w = LevelScore(f.level);
    double warmup_eff = 1.0 / (1.0 + std::sqrt(ch.meta_bytes_est / 1024.0));      // KB 基准
    double total = level_w * 0.30 + warmup_eff * 0.25 + 0.0 * 0.30 + 0.0 * 0.15;  // 简化版（无历史）
    double ce = total / (1.0 + cfg_.kappa * std::sqrt(ch.meta_bytes_est / 1024.0));
    pool.push_back({ch, total, ce});
  }
  std::sort(pool.begin(), pool.end(), [](const Node& a, const Node& b) {
    if (a.ce != b.ce) return a.ce > b.ce;
    return a.ch.level < b.ch.level;
  });

  uint64_t used = 0;

  for (auto& n : pool) {
    if (n.ch.meta_bytes_est > single_cap) continue;
    if (used + n.ch.meta_bytes_est > efficiency_budget) break;

    plan.efficiency.push_back(n.ch);
    used += n.ch.meta_bytes_est;
  }
  plan.total_meta_bytes += used;

  return plan;
}

std::vector<FileInfo> FISelector::DeduplicateFiles(const std::vector<FileInfo>& files,
                                                   const std::unordered_set<std::string>& opened_files) const {
  std::vector<FileInfo> deduplicated_files;
  deduplicated_files.reserve(files.size());

  for (const auto& f : files) {
    if (opened_files.find(f.file_path) == opened_files.end()) {
      deduplicated_files.push_back(f);
    }
  }

  return deduplicated_files;
}
}  // namespace warmup
