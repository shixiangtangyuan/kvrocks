
#include <benchmark/benchmark.h>
#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <kv/controller/v1/api.grpc.pb.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <chrono>
#include <cstddef>
#include <ostream>
#include <random>
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <sstream>
#include <string>

#include "server/redis_connection.h"
#include "stats/stats.h"

static void RecordCommandLatency(benchmark::State& state) {
    auto& cmd_map = GlobalStatsInstance().commands_stats;
    std::vector<std::string> cmd_names;
    cmd_names.reserve(cmd_map.size());
    for (const auto& cmd : cmd_map) {
        cmd_names.push_back(cmd.first);
    }
    thread_local std::mt19937 rng{std::random_device{}()};
    std::uniform_int_distribution<std::size_t> dist(0, cmd_names.size() - 1);
    std::uniform_int_distribution<uint64_t> latency(0, 60000000);
    std::string cmd_name = cmd_names[dist(rng)];

    uint64_t latency_value = latency(rng);
    const uint64_t fetch_every = 60000;
    uint64_t counter = 0;

    for (auto _ : state) {
        thread_local_metric_array.RecordCommadLatency(
            cmd_name, "[0-16383]", static_cast<uint64_t>(latency_value)
        );
        if (++counter == fetch_every) {
            counter = 0;
            std::string reply;
            GlobalStatsInstance().MetricInfo(
                MetricType::COMMAND_LATENCY,
                &reply
            );
            benchmark::DoNotOptimize(reply);
        }
    }
}


BENCHMARK(RecordCommandLatency)
    ->MeasureProcessCPUTime()
    ->UseRealTime()
    ->Iterations(1000000)
    ->ThreadRange(1, 48)
    ->Unit(benchmark::kMicrosecond);


