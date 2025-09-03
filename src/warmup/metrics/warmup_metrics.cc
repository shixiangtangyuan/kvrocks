#include "warmup/metrics/warmup_metrics.h"
namespace warmup {
WarmupMetrics& WarmupMetrics::Instance() {
  static WarmupMetrics g;
  return g;
}
}  // namespace warmup
