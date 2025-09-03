#pragma once
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace warmup {
struct FileInfo {
  std::string cf_name;
  std::string file_path;
  std::string smallest_key;
  std::string largest_key;
  uint64_t file_size_bytes{0};
  uint32_t level{0};
};

struct FISelectorConfig {
  double coverage_ratio{0.2};
  double max_single_file_ratio{0.10};
  int min_files_per_level{3};
  int max_files_per_level{100};
  double kappa{1.0};
  bool dedupe_with_tablecache{false};  // 是否与 tablecache 去重
  std::unordered_map<uint32_t, double> level_weights{{0, 0.5}, {1, 0.3}, {2, 0.15}, {3, 0.05}};
};

struct FIChosen {
  std::string cf_name;
  std::string smallest_key;
  uint64_t meta_bytes_est{0};
  uint32_t level{0};
};

struct FIPlan {
  std::vector<FIChosen> coverage;
  std::vector<FIChosen> efficiency;
  uint64_t total_meta_bytes{0};
};

class FISelector {
 public:
  explicit FISelector(const FISelectorConfig& c) : cfg_(c) {}
  FIPlan MakePlan(const std::vector<FileInfo>& files, uint64_t budget_bytes) const;
  // 去重方法：排除已经打开的文件
  std::vector<FileInfo> DeduplicateFiles(const std::vector<FileInfo>& files,
                                         const std::unordered_set<std::string>& opened_files) const;

 private:
  FISelectorConfig cfg_;
};
}  // namespace warmup
