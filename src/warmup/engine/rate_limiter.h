#pragma once
#include <atomic>
#include <chrono>
#include <thread>

namespace warmup {
class SimpleRateLimiter {
 public:
  explicit SimpleRateLimiter(uint64_t mb_per_sec) { Reset(mb_per_sec); }
  void Reset(uint64_t mb_per_sec) {
    bytes_per_sec_ = mb_per_sec > 0 ? (mb_per_sec * 1024ULL * 1024ULL) : 0;
    window_start_ = std::chrono::steady_clock::now();
    window_bytes_ = 0;
  }
  void Consume(uint64_t bytes) {
    if (bytes_per_sec_ == 0) return;
    window_bytes_ += bytes;
    auto now = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - window_start_).count();
    if (ms <= 0) return;
    auto expected_ms = (window_bytes_ * 1000ULL) / bytes_per_sec_;
    if (expected_ms > (uint64_t)ms) {
      std::this_thread::sleep_for(std::chrono::milliseconds(expected_ms - ms));
    }
    if (ms > 1000) {
      window_start_ = std::chrono::steady_clock::now();
      window_bytes_ = 0;
    }
  }

 private:
  uint64_t bytes_per_sec_{0};
  uint64_t window_bytes_{0};
  std::chrono::steady_clock::time_point window_start_;
};
}  // namespace warmup
