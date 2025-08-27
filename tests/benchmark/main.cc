#include <benchmark/benchmark.h>

#include <absl/base/internal/endian.h>
#include <glog/logging.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <kv/datanode/v1/common.pb.h>
#include <rocksdb/convenience.h>
#include <rocksdb/iostats_context.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/statistics.h>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <sys/utsname.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <iterator>
#include <jsoncons/json.hpp>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <type_traits>
#include <utility>

#include "commands/commander.h"
#include "common/pb_util.h"
#include "common/sync_status.h"
#include "config.h"
#include "fmt/format.h"
#include "stats/stats.h"
#include "storage/compaction_checker.h"
#include "storage/redis_db.h"
#include "storage/storage.h"
#include "string_util.h"
#include "sync/dts_sender.h"
#include "sync/repl_sender.h"
#include "sync/sync_receiver.h"
#include "thread_util.h"
#include "time_util.h"
#include "version.h"


int main(int argc, char** argv) {
  // init commands stats here to prevent concurrent insert, and cause core
  auto commands = redis::CommandTable::GetOriginal();
  for (const auto &iter : *commands) {
    GlobalStatsInstance().commands_stats[iter.first].success_calls = 0;
    GlobalStatsInstance().commands_stats[iter.first].fail_calls = 0;
  }

  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
