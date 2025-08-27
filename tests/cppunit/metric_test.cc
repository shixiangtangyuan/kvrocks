#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <kv/controller/v1/api.grpc.pb.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <mutex>
#include <random>
#include <string>

#include "server/redis_connection.h"
#include "stats/stats.h"

void ClearGlobalStatsInstance() {
  for (auto& metric : GlobalStatsInstance().global_metrics) {
    metric.clear();
  }
  for (auto& metric : GlobalStatsInstance().sum_metrics) {
    metric.reset();
  }
}

TEST(MetricTest, SingleThread) {
  ClearGlobalStatsInstance();
  Attributes k1 = {{"cmd", "k1v1"}, {"slot_range", "k1v2"}};
  uint64_t total_k1 = 0l;
  Attributes k2 = {{"cmd", "k2v1"}, {"slot_range", "k2v2"}};
  uint64_t total_k2 = 0l;
  // defalut bucket
  std::thread t2([&]() {
    for (int i = 0; i < 10; ++i) {
      auto latency = default_latency_spans_[i];
      thread_local_metric_array.RecordCommadLatency("k1v1", "k1v2", latency);
      total_k1 += latency;
      thread_local_metric_array.RecordCommadLatency("k2v1", "k2v2", 5000000 + 1);
      total_k2 += 5000000 + 1;
    }
    sleep(1);
    auto hist_map =
        GlobalStatsInstance()
            .global_metrics[static_cast<int>(MetricType::COMMAND_LATENCY)][thread_local_metric_array.thread_id]
            ->GetHistograms();
    ASSERT_EQ(hist_map.size(), 2);

    for (size_t i = 0; i < default_latency_spans_.size(); ++i) {
      if (i < 10) {
        ASSERT_EQ(hist_map.at(k1).GetTimeSpans()[i].count, 1);
        ASSERT_EQ(hist_map.at(k1).GetTimeSpans()[i].total, default_latency_spans_[i]);
      } else {
        ASSERT_EQ(hist_map.at(k1).GetTimeSpans()[i].count, 0);
        ASSERT_EQ(hist_map.at(k1).GetTimeSpans()[i].total, 0);
      }
      ASSERT_EQ(hist_map.at(k2).GetTimeSpans()[i].count, 0);
      ASSERT_EQ(hist_map.at(k2).GetTimeSpans()[i].total, 0);
    }
    ASSERT_EQ(hist_map.at(k1).GetLast().count, 0);
    ASSERT_EQ(hist_map.at(k1).GetLast().total, 0);
    ASSERT_EQ(hist_map.at(k2).GetLast().count, 10);
    ASSERT_EQ(hist_map.at(k2).GetLast().total, total_k2);
  });
  t2.join();
  sleep(1);
  auto global_size = GlobalStatsInstance().global_metrics[static_cast<int>(MetricType::COMMAND_LATENCY)].size();
  ASSERT_EQ(global_size, 0);
  auto sum_metric = GlobalStatsInstance().sum_metrics[static_cast<int>(MetricType::COMMAND_LATENCY)]->GetHistograms();
  for (size_t i = 0; i < default_latency_spans_.size(); ++i) {
    if (i < 10) {
      ASSERT_EQ(sum_metric.at(k1).GetTimeSpans()[i].count, 1);
      ASSERT_EQ(sum_metric.at(k1).GetTimeSpans()[i].total, default_latency_spans_[i]);
    } else {
      ASSERT_EQ(sum_metric.at(k1).GetTimeSpans()[i].count, 0);
      ASSERT_EQ(sum_metric.at(k1).GetTimeSpans()[i].total, 0);
    }
    ASSERT_EQ(sum_metric.at(k2).GetTimeSpans()[i].count, 0);
    ASSERT_EQ(sum_metric.at(k2).GetTimeSpans()[i].total, 0);
  }
  ASSERT_EQ(sum_metric.at(k1).GetLast().count, 0);
  ASSERT_EQ(sum_metric.at(k1).GetLast().total, 0);
  ASSERT_EQ(sum_metric.at(k2).GetLast().count, 10);
  ASSERT_EQ(sum_metric.at(k2).GetLast().total, total_k2);
}

TEST(MetricTest, MultiThread) {
  ClearGlobalStatsInstance();
  GlobalStatsInstance().sum_metrics[static_cast<int>(MetricType::COMMAND_LATENCY)].reset();
  GlobalStatsInstance().sum_metrics[static_cast<int>(MetricType::COMMAND_LATENCY)] = nullptr;
  std::atomic<bool> stop_collect{false};
  // thread1 collect metric
  std::thread t1([&]() {
    std::string reply;
    while (stop_collect.load()) {
      for (int i = 0; i <= static_cast<int>(MetricType::METRIC_TYPE_MAX); ++i) {
        auto type = static_cast<MetricType>(i);
        GlobalStatsInstance().MetricInfo(type, &reply);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  });

  // start 100 thread to record metric
  std::atomic<bool> pause_record{true};
  std::vector<std::thread> threads;
  uint64_t record_thread_num = 100;
  uint64_t record_count_per_thread = 2;
  threads.reserve(record_thread_num);
  uint64_t total_count = record_thread_num * record_count_per_thread;
  uint64_t total_sum = record_thread_num * record_count_per_thread * 10;
  for (size_t i = 0; i < record_thread_num; ++i) {
    threads.emplace_back([&]() {
      for (size_t j = 0; j < record_count_per_thread; ++j) {
        thread_local_metric_array.RecordCommadLatency("k1v1", "k1v2", 10);
        thread_local_metric_array.RecordCommadLatency("k2v1", "k2v2", 10);
        thread_local_metric_array.RecordMglLockLatency(redis::mgl::LockMode::LOCK_IS, "k1v2", true, 10);
        thread_local_metric_array.RecordMglLockLatency(redis::mgl::LockMode::LOCK_IS, "k2v2", true, 10);
      }
      while (pause_record) {
        sleep(1);
      }
      for (size_t j = 0; j < record_count_per_thread; ++j) {
        thread_local_metric_array.RecordCommadLatency("k1v1", "k1v2", 10);
        thread_local_metric_array.RecordCommadLatency("k2v1", "k2v2", 10);
        thread_local_metric_array.RecordMglLockLatency(redis::mgl::LockMode::LOCK_IS, "k1v2", true, 10);
        thread_local_metric_array.RecordMglLockLatency(redis::mgl::LockMode::LOCK_IS, "k2v2", true, 10);
      }
    });
  }
  sleep(5);
  for (int i = 0; i < static_cast<int>(MetricType::METRIC_TYPE_MAX); ++i) {
    auto type = static_cast<MetricType>(i);
    auto hist_map = GlobalStatsInstance().collectHistogramFromMetric(type);
    if (type == MetricType::COMMAND_LATENCY) {
      for (auto& [k, v] : hist_map) {
        ASSERT_EQ(v.GetTimeSpans().size(), default_latency_spans_.size());
        ASSERT_EQ(v.GetTimeSpans()[0].count, total_count);
        ASSERT_EQ(v.GetTimeSpans()[0].total, total_sum);
      }
    }
    if (type == MetricType::MGL_LOCK_LATENCY) {
      for (auto& [k, v] : hist_map) {
        ASSERT_EQ(v.GetTimeSpans().size(), default_latency_spans_.size());
        ASSERT_EQ(v.GetTimeSpans()[0].count, total_count);
        ASSERT_EQ(v.GetTimeSpans()[0].total, total_sum);
      }
    }
  }
  pause_record.store(false);
  for (auto& t : threads) {
    t.join();
  }
  stop_collect.store(true);
  t1.join();
  total_count *= 2;
  total_sum *= 2;
  for (int i = 0; i <= static_cast<int>(MetricType::METRIC_TYPE_MAX); ++i) {
    auto type = static_cast<MetricType>(i);
    auto hist_map = GlobalStatsInstance().collectHistogramFromMetric(type);
    if (type == MetricType::COMMAND_LATENCY) {
      for (auto& [k, v] : hist_map) {
        ASSERT_EQ(v.GetTimeSpans().size(), default_latency_spans_.size());
        ASSERT_EQ(v.GetTimeSpans()[0].count, total_count);
        ASSERT_EQ(v.GetTimeSpans()[0].total, total_sum);
      }
    }
    if (type == MetricType::MGL_LOCK_LATENCY) {
      for (auto& [k, v] : hist_map) {
        ASSERT_EQ(v.GetTimeSpans().size(), default_latency_spans_.size());
        ASSERT_EQ(v.GetTimeSpans()[0].count, total_count);
        ASSERT_EQ(v.GetTimeSpans()[0].total, total_sum);
      }
    }
  }
}

TEST(MetricTest, MultiThreadRandomLatency) {
  ClearGlobalStatsInstance();
  GlobalStatsInstance().sum_metrics[static_cast<int>(MetricType::COMMAND_LATENCY)].reset();
  GlobalStatsInstance().sum_metrics[static_cast<int>(MetricType::COMMAND_LATENCY)] = nullptr;

  // start 100 thread to record metric
  std::vector<std::thread> threads;
  uint64_t record_thread_num = 100;
  uint64_t record_count_per_thread = 10000;

  std::mutex mtx;
  std::map<std::string, std::pair<std::uint64_t, std::uint64_t>> collect_map;

  threads.reserve(record_thread_num);
  uint64_t total_count = record_thread_num * record_count_per_thread;
  for (size_t i = 0; i < record_thread_num; ++i) {
    threads.emplace_back([&]() {
      std::map<std::string, std::pair<std::uint64_t, std::uint64_t>> t_map;
      std::random_device rd;
      std::mt19937 gen(rd());
      std::uniform_int_distribution<uint32_t> dist(1, 6000000);
      for (size_t j = 0; j < record_count_per_thread; ++j) {
        auto random_latency = dist(gen);
        thread_local_metric_array.RecordCommadLatency("k1v1", "k1v2", random_latency);
        bool stored = false;
        for (auto span : default_latency_spans_) {
          if (span >= random_latency) {
            t_map[std::to_string(span)].first++;
            t_map[std::to_string(span)].second += random_latency;
            stored = true;
            break;
          }
        }
        if (!stored) {
          t_map["Inf"].first++;
          t_map["Inf"].second += random_latency;
        }
      }
      // lock
      std::lock_guard<std::mutex> lock(mtx);
      for (auto& [k, v] : t_map) {
        collect_map[k].first += v.first;
        collect_map[k].second += v.second;
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  sleep(2);
  uint64_t collect_total_sum = 0;
  for (int i = 0; i < static_cast<int>(MetricType::METRIC_TYPE_MAX); ++i) {
    auto type = static_cast<MetricType>(i);
    auto hist_map = GlobalStatsInstance().collectHistogramFromMetric(type);
    if (type == MetricType::COMMAND_LATENCY) {
      for (auto& [k, v] : hist_map) {
        ASSERT_EQ(v.GetTimeSpans().size(), default_latency_spans_.size());
        auto spans = v.GetTimeSpans();
        for (const auto& span : spans) {
          auto it = collect_map.find(std::to_string(span.span));
          if (it != collect_map.end()) {
            ASSERT_EQ(it->second.first, span.count);
            ASSERT_EQ(it->second.second, span.total);
            collect_total_sum += span.count;
          } else {
            ASSERT_EQ(span.count, 0);
            ASSERT_EQ(span.total, 0);
          }
        }
        ASSERT_EQ(collect_map["Inf"].first, v.GetLast().count);
        ASSERT_EQ(collect_map["Inf"].second, v.GetLast().total);
        collect_total_sum += v.GetLast().count;
      }
    }
  }
  ASSERT_EQ(collect_total_sum, total_count);
}

TEST(MetricTest, CounterTest) {
  ClearGlobalStatsInstance();
  GlobalStatsInstance().sum_metrics[static_cast<int>(MetricType::MGL_LOCK_FAILED_COUNT)].reset();
  GlobalStatsInstance().sum_metrics[static_cast<int>(MetricType::MGL_LOCK_FAILED_COUNT)] = nullptr;

  // start 100 thread to record metric
  std::vector<std::thread> threads;
  uint64_t record_thread_num = 100;
  uint64_t record_count_per_thread = 10000;

  std::mutex mtx;
  std::map<Attributes, uint64_t, AttrCmp> collect_map;

  threads.reserve(record_thread_num);
  uint64_t total_count = record_thread_num * record_count_per_thread;
  for (size_t i = 0; i < record_thread_num; ++i) {
    threads.emplace_back([&]() {
      std::map<Attributes, uint64_t, AttrCmp> t_map;
      for (size_t j = 0; j < record_count_per_thread; ++j) {
        thread_local_metric_array.CountMglLockFailed(redis::mgl::LockMode::LOCK_IS, "[1,16383]", true,
                                                     redis::mgl::LockRes::LOCKRES_DEADLOCK, 1);
        std::string mgl = redis::mgl::LockModeToString[static_cast<std::size_t>(redis::mgl::LockMode::LOCK_IS)];
        std::string status =
            redis::mgl::LockResToString[static_cast<std::size_t>(redis::mgl::LockRes::LOCKRES_DEADLOCK)];
        const Attributes attrs = {
            {"mgl", mgl}, {"slot_range", "[1,16383]"}, {"lock_object", "slot_range"}, {"status", status}};
        t_map.try_emplace(attrs, 0).first->second++;
      }
      // lock
      std::lock_guard<std::mutex> lock(mtx);
      for (auto& [k, v] : t_map) {
        collect_map.try_emplace(k, 0).first->second += v;
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  sleep(2);
  uint64_t collect_total_sum = 0;
  for (int i = 0; i < static_cast<int>(MetricType::METRIC_TYPE_MAX); ++i) {
    auto type = static_cast<MetricType>(i);
    auto counter_map = GlobalStatsInstance().collectCounterFromMetric(type);
    if (type == MetricType::MGL_LOCK_FAILED_COUNT) {
      for (auto& [k, v] : counter_map) {
        auto it = collect_map.find(k);
        if (it != collect_map.end()) {
          ASSERT_EQ(v, it->second);
          collect_total_sum += v;
        } else {
          ASSERT_TRUE(false);
        }
      }
    }
  }
  ASSERT_EQ(collect_total_sum, total_count);
}

TEST(MetricTest, AttributeTest) {
  ClearGlobalStatsInstance();
  GlobalStatsInstance().sum_metrics[static_cast<int>(MetricType::COMMAND_LATENCY)].reset();
  GlobalStatsInstance().sum_metrics[static_cast<int>(MetricType::COMMAND_LATENCY)] = nullptr;
  // test empty label
  Metric metric(MetricType::COMMAND_LATENCY);
  metric.Record({{"cmd", "k1v1"}, {"slot_range", "k1v2"}}, 10);
  ASSERT_EQ(metric.GetHistograms().size(), 1);
  ASSERT_EQ(metric.GetHistograms().begin()->second.GetTimeSpans()[0].count, 1);
  ASSERT_EQ(metric.GetHistograms().begin()->second.GetTimeSpans()[0].total, 10);
  // test label
  metric.Record({}, 10);
  ASSERT_EQ(metric.GetHistograms().size(), 2);
}