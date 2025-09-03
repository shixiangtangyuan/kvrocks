#pragma once
#include <atomic>
#include <cstdint>

namespace warmup {
struct WarmupMetrics {
  std::atomic<uint64_t> trigger_requests{0};
  std::atomic<uint64_t> success_total{0};
  std::atomic<uint64_t> failure_total{0};
  std::atomic<uint64_t> processed_files{0};
  std::atomic<uint64_t> processed_bytes{0};
  std::atomic<uint64_t> iter_processed_keys{0};
  std::atomic<uint64_t> total_duration_ms{0};
  std::atomic<int> in_progress{0};
  std::atomic<double> progress_ratio{0.0};

  static WarmupMetrics& Instance();
};
}  // namespace warmup
