#pragma once
#include <atomic>
#include <mutex>
#include <string>

#include "warmup/provider/warmup_provider.h"

namespace rocksdb {
class DB;
}
struct Config;

namespace warmup {
class WarmupOrchestrator {
 public:
  enum class State { kIdle, kRunning, kFinished };

  WarmupOrchestrator(rocksdb::DB* db, const Config* cfg);

  // === 新增：标准化互斥/状态查询 ===
  bool IsRunning() const { return state_.load(std::memory_order_acquire) == State::kRunning; }
  bool IsIdle() const { return state_.load(std::memory_order_acquire) == State::kIdle; }
  State GetState() const { return state_.load(std::memory_order_acquire); }

  // 仅当 Idle 才会启动；否则返回 false
  bool StartIfIdle(const std::string& mode, double threshold);

  // 保留原有 StartOnce 接口（如已有）
  bool StartOnce(const std::string& source, double progress_threshold);

  // 供上层感知收尾（可选）
  void MarkFinished() { state_.store(State::kFinished, std::memory_order_release); }

  // 兼容性接口
  bool InProgress() const { return IsRunning(); }
  bool Busy() const { return IsRunning(); }

 private:
  std::unique_ptr<WarmupProvider> MakeProvider() const;
  rocksdb::DB* db_;
  const Config* cfg_;
  mutable std::mutex mu_;
  std::atomic<State> state_{State::kIdle};
  std::mutex start_mu_;  // 防止同进程多入口同时触发
};
}  // namespace warmup
