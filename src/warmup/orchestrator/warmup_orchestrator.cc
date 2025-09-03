#include "warmup/orchestrator/warmup_orchestrator.h"

#include <sstream>

#include "config/config.h"
#include "warmup/metrics/warmup_metrics.h"
#include "warmup/provider/warmup_provider.h"

namespace warmup {

WarmupOrchestrator::WarmupOrchestrator(rocksdb::DB* db, const Config* cfg) : db_(db), cfg_(cfg) {}

std::unique_ptr<WarmupProvider> WarmupOrchestrator::MakeProvider() const {
  if (cfg_->warmup_strategy == "iter") return MakeScanProvider(cfg_);
  // hybrid 目前等价 fi，可在此扩展
  return MakeFIProvider(cfg_);
}

// 仅当 Idle 才会启动；否则返回 false
bool WarmupOrchestrator::StartIfIdle(const std::string& mode, double threshold) {
  std::lock_guard<std::mutex> lk(start_mu_);
  if (!IsIdle()) return false;
  return StartOnce(mode, threshold);
}

// 若项目里没有 StartOnce 的互斥，这里补一层 CAS，保证即便上层没加锁也安全
bool WarmupOrchestrator::StartOnce(const std::string& source, double progress_threshold) {
  auto expected = State::kIdle;
  if (!state_.compare_exchange_strong(expected, State::kRunning, std::memory_order_acq_rel)) {
    return false;  // 已在跑或已结束
  }

  // 真实启动逻辑应在线程池/后台线程执行；这里只做触发与状态管理
  // 具体执行结束后需要调用 MarkFinished() 收尾（见 StorageManager 的启动线程）

  WarmupMetrics::Instance().trigger_requests++;
  WarmupMetrics::Instance().in_progress = 1;

  uint64_t start_ms = (uint64_t)(std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::steady_clock::now().time_since_epoch())
                                     .count());

  auto provider = MakeProvider();
  PlanContext plan;
  auto ps = provider->Plan(db_, &plan);
  if (!ps.ok()) {
    state_.store(State::kIdle, std::memory_order_release);
    WarmupMetrics::Instance().in_progress = 0;
    WarmupMetrics::Instance().failure_total++;
    return false;
  }

  Progress prog;
  auto es = provider->Execute(db_, plan, &prog);
  uint64_t end_ms = (uint64_t)(std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now().time_since_epoch())
                                   .count());

  WarmupMetrics::Instance().total_duration_ms = end_ms - start_ms;
  WarmupMetrics::Instance().in_progress = 0;

  if (!es.ok()) {
    WarmupMetrics::Instance().failure_total++;
    state_.store(State::kIdle, std::memory_order_release);
    return false;
  }

  WarmupMetrics::Instance().success_total++;
  WarmupMetrics::Instance().processed_files += prog.processed_files;
  WarmupMetrics::Instance().processed_bytes += prog.processed_bytes;
  WarmupMetrics::Instance().progress_ratio = prog.ratio;

  // 执行完成，标记为完成状态
  state_.store(State::kFinished, std::memory_order_release);

  return true;
}

}  // namespace warmup
