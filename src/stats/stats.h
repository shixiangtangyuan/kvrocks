/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#pragma once

#include <gtest/gtest.h>
#include <gtest/gtest_prod.h>
#include <kv/datanode/v1/common.pb.h>
#include <rocksdb/status.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/sync_status.h"
#include "common/time_util.h"
#include "ingest/ingest_monitor.h"
#include "lock/lock_defines.h"
#include "status.h"

const int STATS_METRIC_SAMPLES = 16;                             // Number of samples per metric
const uint64_t SLOW_QUERY_SLOWER_THAN = 10000;                   // Threshold of slow query
constexpr uint64_t SET_TOPO_STAGE_INTERVAL = 120 * 1000 * 1000;  // us of 2min

struct OpsInstMetric {
  OpsInstMetric() : last_sample_time(util::GetTimeStampMS()), last_sample_count(0), idx(0) {
    for (uint64_t& sample : samples) {
      sample = 0;
    }
  }

  uint64_t last_sample_time;   // Timestamp of the last sample in ms
  uint64_t last_sample_count;  // Count in the last sample
  uint64_t samples[STATS_METRIC_SAMPLES];
  int idx;
};

struct CommandStat {
  std::atomic<uint64_t> success_calls;
  std::atomic<uint64_t> fail_calls;
  mutable std::shared_mutex inst_metrics_mutex;
};

class InstantaneousStats {
 public:
  mutable std::shared_mutex inst_metrics_mutex;
  std::vector<OpsInstMetric> inst_metrics;

  explicit InstantaneousStats(int metric_count);
  void TrackInstantaneousMetric(int metric, uint64_t current_reading);
  uint64_t GetInstantaneousMetric(int metric) const;
};

class StorageStats : public InstantaneousStats {
 public:
  enum StatsMetricFlag {
    STATS_METRIC_ROCKSDB_PUT = 0,   // Number of calls of Put and Write in rocksdb
    STATS_METRIC_ROCKSDB_GET,       // Number of calls of get in rocksdb
    STATS_METRIC_ROCKSDB_MULTIGET,  // Number of calls of mulget in rocksdb
    STATS_METRIC_ROCKSDB_SEEK,      // Number of calls of seek in rocksdb
    STATS_METRIC_ROCKSDB_NEXT,      // Number of calls of next in rocksdb
    STATS_METRIC_ROCKSDB_PREV,      // Number of calls of prev in rocksdb
    STATS_METRIC_COUNT
  };

  StorageStats();

  void IncrWriteResult(const rocksdb::Status& status);
  void GetFailedWriteInfo(std::ostringstream& oss, const std::string& prefix) const;

 private:
  // now we only record failed write.
  std::vector<std::vector<std::atomic<uint64_t>>> write_result_counters_;
};

struct RPCStats {
  void IncrCount(const OptionalSyncError& err) {
    ++count;
    if (err.has_value()) {
      ++error_count[err->code()];
    }
  }

  std::ostringstream& Append(std::ostringstream& oss, const std::string& prefix) {
    oss << prefix << "count:" << count << "\r\n";
    if (error_count.size() > 0) {
      std::string error_prefix = prefix + "error_count.";
      for (auto& [code, count] : error_count) {
        oss << error_prefix << kv::datanode::v1::ErrorCode_Name(code) << ":" << count << "\r\n";
      }
    }
    return oss;
  }

  uint64_t count = 0;
  std::unordered_map<kv::datanode::v1::ErrorCode, uint64_t> error_count;
};

struct ReportSyncErrorStats : RPCStats {
  void IncrReportSyncErrorCount(const OptionalSyncError& error, const kv::datanode::v1::Error& report_error) {
    RPCStats::IncrCount(error);
    if (error.has_value()) {
      ++report_fail_count[report_error.code()];
    } else {
      ++report_succ_count[report_error.code()];
    }
  }

  std::ostringstream& Append(std::ostringstream& oss, const std::string& slot_range) {
    std::string prefix = "report_sync_error." + slot_range + ".";
    RPCStats::Append(oss, prefix);
    if (report_succ_count.size() > 0) {
      std::string succ_prefix = prefix + "report_succ_count.";
      for (auto& [code, count] : report_succ_count) {
        oss << succ_prefix << kv::datanode::v1::ErrorCode_Name(code) << ":" << count << "\r\n";
      }
    }
    if (report_fail_count.size() > 0) {
      std::string fail_prefix = prefix + "report_fail_count.";
      for (auto& [code, count] : report_fail_count) {
        oss << fail_prefix << kv::datanode::v1::ErrorCode_Name(code) << ":" << count << "\r\n";
      }
    }
    return oss;
  }

  std::unordered_map<kv::datanode::v1::ErrorCode, uint64_t> report_succ_count;
  std::unordered_map<kv::datanode::v1::ErrorCode, uint64_t> report_fail_count;
};

struct StreamRPCStats : RPCStats {
  void IncrCount() { ++count; }

  std::ostringstream& Append(std::ostringstream& oss, const std::string& prefix) {
    RPCStats::Append(oss, prefix);
    if (auto active_count = count - finish_count; active_count > 0) {
      oss << prefix << "active_count:" << active_count << "\r\n";
    }
    return oss;
  }

  void IncrErrorCount(const OptionalSyncError& err) {
    ++finish_count;
    if (err.has_value()) {
      ++error_count[err->code()];
    }
  }

  uint64_t finish_count = 0;
};

struct SyncStreamStats : StreamRPCStats {
  void IncrSyncStats(uint64_t delta_updates, uint64_t delta_bytes) {
    sync_updates += delta_updates;
    sync_bytes += delta_bytes;
    ++stream_op_count;
  }

  std::ostringstream& Append(std::ostringstream& oss, const std::string& prefix) {
    StreamRPCStats::Append(oss, prefix);
    oss << prefix << "stream_op_count:" << stream_op_count << "\r\n";
    oss << prefix << "sync_updates:" << sync_updates << "\r\n";
    oss << prefix << "sync_bytes:" << sync_bytes << "\r\n";
    return oss;
  }

  uint64_t stream_op_count = 0;
  uint64_t sync_updates = 0;
  uint64_t sync_bytes = 0;
};

struct ReplSenderStats : SyncStreamStats {
  void IncrStopWriteSuccCount() { ++stop_write_succ_count; }

  void IncrStopWriteFailCount() { ++stop_write_fail_count; }

  void IncrStopWriteTime(uint64_t delta_stop_write_time_ms) { stop_write_time_ms += delta_stop_write_time_ms; }

  void IncrCaughtUpCount() { ++caught_up_count; }

  void IncrWaitTopoTimeoutCount() { ++wait_topo_timeout_count; }

  std::ostringstream& Append(std::ostringstream& oss, const std::string& slot_range) {
    std::string prefix = "repl_sender." + slot_range + ".";
    SyncStreamStats::Append(oss, prefix);
    oss << prefix << "stop_write_succ_count:" << stop_write_succ_count << "\r\n";
    oss << prefix << "stop_write_fail_count:" << stop_write_fail_count << "\r\n";
    oss << prefix << "stop_write_time_ms:" << stop_write_time_ms << "\r\n";
    oss << prefix << "caught_up_count:" << caught_up_count << "\r\n";
    oss << prefix << "wait_topo_timeout_count:" << wait_topo_timeout_count << "\r\n";
    return oss;
  }

  uint64_t stop_write_succ_count = 0;
  uint64_t stop_write_fail_count = 0;
  uint64_t stop_write_time_ms = 0;
  uint64_t caught_up_count = 0;
  uint64_t wait_topo_timeout_count = 0;
};

struct ReplPullerStats : SyncStreamStats {
  void IncrStopWriteCount() { ++stop_write_count; }

  void IncrCaughtUpCount() { ++caught_up_count; }

  std::ostringstream& Append(std::ostringstream& oss, const std::string& slot_range) {
    std::string prefix = "repl_puller." + slot_range + ".";
    SyncStreamStats::Append(oss, prefix);
    oss << prefix << "stop_write_count:" << stop_write_count << "\r\n";
    oss << prefix << "caught_up_count:" << caught_up_count << "\r\n";
    return oss;
  }

  uint64_t stop_write_count = 0;
  uint64_t caught_up_count = 0;
};

enum class MetricType {
  COMMAND_LATENCY = 0,
  COMMAND_ESTIMATED_SUBKEY_NUM = 1,
  COMMAND_ESTIMATED_SUBKEY_BYTE_SIZE = 2,
  COMMAND_REPLY_SIZE = 3,
  COMMAND_QUEUE_LATENCY_ON_CONNECTION = 4,
  MGL_LOCK_LATENCY = 5,
  MGL_LOCK_FAILED_COUNT = 6,
  REQUESTS_BATCH_SIZE = 7,
  EVENT_QUEUE_SIZE = 8,
  APPLY_TOPO_LATENCY = 9,
  CDC_PARSE_BATCH_LATENCY = 10,
  CDC_PARSE_BATCH_SIZE_HISTOGRAM = 11,
  CDC_SEND_BATCH_SIZE_HISTOGRAM = 12,
  CLIENT_OUT_BUFFER_SIZE = 13,
  GRPC_CLIENT_REQUEST_NUM = 14,
  GRPC_CLIENT_REQUEST_LATENCY = 15,
  GRPC_CLIENT_SEND_MESSAGE_LATENCY = 16,
  GRPC_CLIENT_RECV_MESSAGE_INTERVAL = 17,
  GRPC_SERVER_REQUEST_NUM = 18,
  GRPC_SERVER_REQUEST_LATENCY = 19,
  GRPC_SERVER_SEND_MESSAGE_LATENCY = 20,
  GRPC_SERVER_RECV_MESSAGE_INTERVAL = 21,
  METRIC_TYPE_MAX = 22,
};

enum class MetricKind { HISTOGRAM = 0, COUNTER = 1 };

// MetricType -> (name, is_counter)
static const std::unordered_map<MetricType, std::pair<std::string, MetricKind>> kMetricInfos = {
    {MetricType::COMMAND_LATENCY, {"CommandLatency", MetricKind::HISTOGRAM}},
    {MetricType::COMMAND_ESTIMATED_SUBKEY_NUM, {"CommandEstimatedSubkeyNum", MetricKind::HISTOGRAM}},
    {MetricType::COMMAND_ESTIMATED_SUBKEY_BYTE_SIZE, {"CommandEstimatedSubkeyByteSize", MetricKind::HISTOGRAM}},
    {MetricType::COMMAND_REPLY_SIZE, {"CommandReplySize", MetricKind::HISTOGRAM}},
    {MetricType::COMMAND_QUEUE_LATENCY_ON_CONNECTION, {"CommandQueueLatencyOnConnection", MetricKind::HISTOGRAM}},
    {MetricType::MGL_LOCK_LATENCY, {"MglLockLatency", MetricKind::HISTOGRAM}},
    {MetricType::MGL_LOCK_FAILED_COUNT, {"MglLockFailedCount", MetricKind::COUNTER}},
    {MetricType::REQUESTS_BATCH_SIZE, {"RequestsBatchSize", MetricKind::HISTOGRAM}},
    {MetricType::EVENT_QUEUE_SIZE, {"EventQueueSize", MetricKind::HISTOGRAM}},
    {MetricType::APPLY_TOPO_LATENCY, {"ApplyTopoLatency", MetricKind::HISTOGRAM}},
    {MetricType::CDC_PARSE_BATCH_LATENCY, {"CdcParseBatchLatency", MetricKind::HISTOGRAM}},
    {MetricType::CDC_PARSE_BATCH_SIZE_HISTOGRAM, {"CdcParseBatchSizeHistogram", MetricKind::HISTOGRAM}},
    {MetricType::CDC_SEND_BATCH_SIZE_HISTOGRAM, {"CdcSendBatchSizeHistogram", MetricKind::HISTOGRAM}},
    {MetricType::CLIENT_OUT_BUFFER_SIZE, {"ClientOutBufferSize", MetricKind::HISTOGRAM}},
    {MetricType::GRPC_CLIENT_REQUEST_NUM, {"GrpcClientReqNum", MetricKind::COUNTER}},
    {MetricType::GRPC_CLIENT_REQUEST_LATENCY, {"GrpcClientReqLatency", MetricKind::HISTOGRAM}},
    {MetricType::GRPC_CLIENT_SEND_MESSAGE_LATENCY, {"GrpcClientSendMsgLatency", MetricKind::HISTOGRAM}},
    {MetricType::GRPC_CLIENT_RECV_MESSAGE_INTERVAL, {"GrpcClientRecvMsgInterval", MetricKind::HISTOGRAM}},
    {MetricType::GRPC_SERVER_REQUEST_NUM, {"GrpcServerReqNum", MetricKind::COUNTER}},
    {MetricType::GRPC_SERVER_REQUEST_LATENCY, {"GrpcServerReqLatency", MetricKind::HISTOGRAM}},
    {MetricType::GRPC_SERVER_SEND_MESSAGE_LATENCY, {"GrpcServerSendMsgLatency", MetricKind::HISTOGRAM}},
    {MetricType::GRPC_SERVER_RECV_MESSAGE_INTERVAL, {"GrpcServerRecvMsgInterval", MetricKind::HISTOGRAM}},
    {MetricType::METRIC_TYPE_MAX, {"MetricUnknown", MetricKind::HISTOGRAM}},
};

inline std::string MetricTypeToString(MetricType m) {
  if (auto it = kMetricInfos.find(m); it != kMetricInfos.end()) {
    return it->second.first;
  }
  return "Unknown";
}

inline bool MetricIsCounter(MetricType m) {
  if (auto it = kMetricInfos.find(m); it != kMetricInfos.end()) {
    return it->second.second == MetricKind::COUNTER;
  }
  return false;
}

inline bool MetricIsHistogram(MetricType m) {
  if (auto it = kMetricInfos.find(m); it != kMetricInfos.end()) {
    return it->second.second == MetricKind::HISTOGRAM;
  }
  return false;
}

class Metric;

class GlobalStats;

extern std::atomic<uint64_t> global_thread_id;

extern const std::vector<uint64_t> default_latency_spans_;

extern const std::vector<uint64_t> default_count_spans_;

extern const std::unordered_map<MetricType, std::vector<uint64_t>> metric_span_scope_maps_;

const std::vector<uint64_t>& GetMetricSpanByType(MetricType type) noexcept;

template <class T>
inline void GetHash(size_t& seed, const T& arg) {
  std::hash<T> hasher;
  // reference -
  // https://www.boost.org/doc/libs/1_37_0/doc/html/hash/reference.html#boost.hash_combine
  seed ^= hasher(arg) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

struct AttributeView {
  friend struct AttrCmp;
  friend struct Attributes;

 private:
  std::initializer_list<std::pair<std::string_view, std::string_view>> attributes_;
  size_t hash_ = 0;

 public:
  AttributeView(std::initializer_list<std::pair<std::string_view, std::string_view>> init) : attributes_(init) {
    for (const auto& [k, v] : init) {
      GetHash(hash_, k);
      GetHash(hash_, v);
    }
  }

  size_t Hash() const { return hash_; }
};

struct Attributes {
  friend struct AttrCmp;

 private:
  std::vector<std::pair<std::string, std::string>> attributes_;
  size_t hash_ = 0;

 public:
  Attributes(std::initializer_list<std::pair<std::string_view, std::string_view>> init) {
    attributes_.reserve(init.size());
    for (const auto& [k, v] : init) {
      attributes_.emplace_back(k, v);
      GetHash(hash_, k);
      GetHash(hash_, v);
    }
  }

  explicit Attributes(const AttributeView& view) : hash_(view.hash_) {
    attributes_.reserve(view.attributes_.size());
    for (const auto& [k, v] : view.attributes_) {
      attributes_.emplace_back(k, v);
    }
  }

  bool ForEachKeyValue(std::function<bool(std::string_view, std::string_view)>& callback) const;

  friend std::ostream& operator<<(std::ostream& os, const Attributes& attrs);

  bool operator==(const Attributes& other) const;

  size_t Hash() const { return hash_; }
};

struct AttrCmp {
  using is_transparent = std::true_type;  // NOLINT(readability-identifier-naming)
  bool operator()(const Attributes& a, const Attributes& b) const { return less(a, b); }
  bool operator()(const Attributes& a, const AttributeView& b) const { return less(a, b); }
  bool operator()(const AttributeView& a, const Attributes& b) const { return less(a, b); }
  bool operator()(const AttributeView& a, const AttributeView& b) const { return less(a, b); }

 private:
  template <typename T, typename U>
  inline static bool less(const T& a, const U& b) {
    if (a.Hash() == b.Hash()) {
      if (a.attributes_.size() == b.attributes_.size()) {
        for (auto [it1, it2] = std::make_pair(a.attributes_.begin(), b.attributes_.begin()); it1 != a.attributes_.end();
             ++it1, ++it2) {
          if (it1->second != it2->second) {
            return it1->second < it2->second;
          }
        }
        return false;
      } else {
        return a.attributes_.size() < b.attributes_.size();
      }
    } else {
      return a.Hash() < b.Hash();
    }
  }
};
namespace benchmark {
class State;
}
void RecordCommandLatency(benchmark::State&);
class Histogram {
 public:
  struct TimeSpan {
    uint64_t span = 0;
    uint64_t total = 0;
    uint64_t count = 0;
    TimeSpan& operator+=(const TimeSpan& s) {
      count += s.count;
      total += s.total;
      return *this;
    }
    bool operator<(const TimeSpan& oth) const { return span < oth.span; }
  };
  explicit Histogram(const std::vector<uint64_t>& spans);

  Histogram& operator+=(const Histogram& m);

  friend std::ostringstream& operator<<(std::ostringstream& oss, const Histogram& lm);

  void Record(uint64_t v);

  const std::vector<TimeSpan>& GetTimeSpans() const noexcept { return time_spans_; }

  const TimeSpan& GetLast() const noexcept { return last_; }

 private:
  std::vector<TimeSpan> time_spans_;
  TimeSpan last_;
};

class Metric {
 public:
  explicit Metric(MetricType type) : type_(type) {}

  void Record(const std::initializer_list<std::pair<std::string_view, std::string_view>>& labels, uint64_t val);

  void Count(const std::initializer_list<std::pair<std::string_view, std::string_view>>& labels, uint64_t val);

  std::map<Attributes, Histogram, AttrCmp> GetHistograms() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return histograms_;
  }

  std::map<Attributes, uint64_t, AttrCmp> GetCounters() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return counters_;
  }

  Metric& operator+=(const Metric& rhs);

  MetricType GetType() const { return type_; }

 private:
  MetricType type_;
  std::map<Attributes, Histogram, AttrCmp> histograms_;
  std::map<Attributes, uint64_t, AttrCmp> counters_;
  // lock
  mutable std::mutex mutex_;
};

/**
 * To add a new percentile metric, add a corresponding entry to MetricType and
 * call Record(type, label, value).
 *
 * To use custom buckets, add a new entry to metric_span_scope_maps_ in
 */
struct ThreadLocalMetricArray {
  ThreadLocalMetricArray() = default;
  void Record(MetricType type, const std::initializer_list<std::pair<std::string_view, std::string_view>>& label,
              uint64_t val);
  void Count(MetricType type, const std::initializer_list<std::pair<std::string_view, std::string_view>>& labels,
             uint64_t val);
  void InitMetric(MetricType type);
  void RecordCommadLatency(const std::string& cmd, const std::string& slotrange, uint64_t val);
  void RecordCommadSize(const std::string& cmd, const std::string& slotrange, int64_t tokens_num, uint64_t tokens_size);
  void RecordReplySize(const std::string& cmd, const std::string& slotrange, uint64_t val);
  void RecordMglLockLatency(redis::mgl::LockMode mode, const std::string& slotrange, bool is_slotrange, uint64_t val);
  void CountMglLockFailed(redis::mgl::LockMode mode, const std::string& slotrange, bool is_slotrange,
                          const redis::mgl::LockRes& status, uint64_t val);
  void RecordCDCParseBatchLatency(const std::string& slotrange, uint64_t val);
  void RecordCDCParseBatchSizeHistogram(const std::string& slotrange, uint64_t val);
  void RecordCDCSendBatchSizeHistogram(const std::string& slotrange, uint64_t val);
  ~ThreadLocalMetricArray();
  uint64_t thread_id = global_thread_id.fetch_add(1, std::memory_order_seq_cst);
  std::shared_ptr<Metric> metrics[static_cast<int>(MetricType::METRIC_TYPE_MAX) + 1];
};

extern thread_local ThreadLocalMetricArray thread_local_metric_array;

using GlobalMetricsArray =
    std::unordered_map<uint64_t, std::shared_ptr<Metric>>[static_cast<int>(MetricType::METRIC_TYPE_MAX) + 1];
using SumMetricArray = std::shared_ptr<Metric>[static_cast<int>(MetricType::METRIC_TYPE_MAX) + 1];

struct CDCSenderStats : StreamRPCStats {
  void IncrSenderStats(uint64_t delta_events, uint64_t delta_bytes, uint64_t seq) {
    sent_events += delta_events;
    sent_bytes += delta_bytes;
    ++sent_count;
    next_seq = seq;
  }

  void RecordDataLag(uint64_t duration_ms, uint64_t count) {
    data_lag_ms = duration_ms;
    data_lag_seqs = count;
  }

  std::ostringstream& Append(std::ostringstream& oss, const std::string& prefix) {
    StreamRPCStats::Append(oss, prefix);
    oss << prefix << "sent_events:" << sent_events << "\r\n";
    oss << prefix << "sent_bytes:" << sent_bytes << "\r\n";
    oss << prefix << "sent_count:" << sent_count << "\r\n";
    oss << prefix << "data_lag_ms:" << data_lag_ms << "\r\n";
    oss << prefix << "data_lag_seqs:" << data_lag_seqs << "\r\n";
    oss << prefix << "next_seq:" << next_seq << "\r\n";
    return oss;
  }

  uint64_t sent_events = 0;
  uint64_t sent_bytes = 0;
  uint64_t sent_count = 0;
  uint64_t data_lag_ms = 0;
  uint64_t data_lag_seqs = 0;
  uint64_t next_seq = 0;
};

class GlobalStats : public InstantaneousStats {
 public:
  enum StatsMetricFlag {
    STATS_METRIC_COMMAND = 0,  // Number of commands executed
    STATS_METRIC_NET_INPUT,    // Bytes read to network
    STATS_METRIC_NET_OUTPUT,   // Bytes written to network
    STATS_METRIC_SLOW_QUERY,
    STATS_METRIC_COUNT
  };

  enum RequestResult {
    FAIL_USAGE_ERROR = 0,
    FAIL_SLOTRANGE_LOCK_TIMEOUT,
    FAIL_SLOTRANGE_STOP_WRITE,
    FAIL_UNCONCERNED_ERROR,
    FAIL_KKV_EXPIRE_EXCEED_REDIS,
    FAIL_DISABLED_CMD,
    FAIL_TYPE_COUNT,
    SUCCEED
  };

  inline static std::string RequestResultToString(RequestResult result) {
    switch (result) {
      case FAIL_USAGE_ERROR:
        return "FAIL_USAGE_ERROR";
      case FAIL_SLOTRANGE_LOCK_TIMEOUT:
        return "FAIL_SLOTRANGE_LOCK_TIMEOUT";
      case FAIL_SLOTRANGE_STOP_WRITE:
        return "FAIL_SLOTRANGE_STOP_WRITE";
      case FAIL_UNCONCERNED_ERROR:
        return "FAIL_UNCONCERNED_ERROR";
      case FAIL_KKV_EXPIRE_EXCEED_REDIS:
        return "FAIL_KKV_EXPIRE_EXCEED_REDIS";
      case FAIL_DISABLED_CMD:
        return "FAIL_DISABLED_CMD";
      case FAIL_TYPE_COUNT:
        return "FAIL_TYPE_COUNT";
      case SUCCEED:
        return "SUCCEED";
      default:
        return "UNKNOWN";
    }
  }
  std::atomic<uint64_t> total_calls = {0};
  std::atomic<uint64_t> slow_query_calls = {0};
  std::atomic<uint64_t> total_requests = {0};
  std::atomic<uint64_t> total_succ_requests = {0};
  std::atomic<uint64_t> total_fail_requests = {0};
  std::atomic<uint64_t> in_bytes = {0};
  std::atomic<uint64_t> out_bytes = {0};
  std::atomic<uint64_t> client_buffer_congested = {0};
  std::atomic<uint64_t> client_buffer_exceed_limit = {0};
  std::map<std::string, CommandStat> commands_stats;
  std::vector<std::atomic<uint64_t>> failed_requests;

  std::atomic<bool> is_blocked = {false};
  std::mutex global_and_sum_metric_mutex;
  GlobalMetricsArray global_metrics;
  SumMetricArray sum_metrics;

  mutable std::shared_mutex get_sync_point_mtx;
  std::unordered_map<std::string, RPCStats> get_sync_point_stats;
  mutable std::shared_mutex report_sync_error_mtx;
  std::unordered_map<std::string, ReportSyncErrorStats> report_sync_error_stats;
  mutable std::shared_mutex dts_sender_mtx;
  std::unordered_map<std::string, SyncStreamStats> dts_sender_stats;
  mutable std::shared_mutex dts_recver_mtx;
  std::unordered_map<std::string, SyncStreamStats> dts_recver_stats;
  mutable std::shared_mutex repl_sender_mtx;
  std::unordered_map<std::string, ReplSenderStats> repl_sender_stats;
  mutable std::shared_mutex repl_puller_mtx;
  std::unordered_map<std::string, ReplPullerStats> repl_puller_stats;
  mutable std::shared_mutex get_cdc_point_mtx;
  std::unordered_map<std::string, RPCStats> get_cdc_point_stats;
  mutable std::shared_mutex get_cdc_restart_point_mtx;
  std::unordered_map<std::string, RPCStats> get_cdc_restart_point_stats;
  mutable std::shared_mutex get_cdc_oldest_point_mtx;
  std::unordered_map<std::string, RPCStats> get_cdc_oldest_point_stats;
  mutable std::shared_mutex cdc_sender_mtx;
  std::unordered_map<std::string, CDCSenderStats> cdc_sender_stats;

  // ingest
  mutable std::shared_mutex ingest_mtx;
  std::unordered_map<std::string, ingest::IngestMonitor> ingest_stats;
  std::atomic<uint64_t> datanode_only_read_start_time = 0;
  std::atomic<uint64_t> datanode_only_read_ms = 0;

  GlobalStats() : InstantaneousStats(GlobalStats::STATS_METRIC_COUNT), failed_requests(GlobalStats::FAIL_TYPE_COUNT){};
  void RegisterMetric(MetricType type, uint64_t thread_id, std::shared_ptr<Metric> metric);
  void UnregisterMetric(MetricType type, uint64_t thread_id);
  void MetricInfo(MetricType type, std::string* reply);
  void IncrCalls(const std::string& command_name, bool is_success);
  void IncrRequests(RequestResult type);
  void IncrSlowQueryCallsIfNeeded(uint64_t duration);
  void IncrInbondBytes(uint64_t bytes) { in_bytes.fetch_add(bytes, std::memory_order_relaxed); }
  void IncrOutbondBytes(uint64_t bytes) { out_bytes.fetch_add(bytes, std::memory_order_relaxed); }
  static int64_t GetMemoryRSS();
  void GetRequestStatsInfo(std::ostringstream& oss) const;
  void GetFailedRequestInfo(std::ostringstream& oss, GlobalStats::RequestResult) const;
  // sync stats
  std::ostringstream& GetSyncStatsInfo(std::ostringstream& oss);
  void IncrGetSyncPointCount(const std::string& slot_range, const OptionalSyncError& err);
  void IncrReportSyncErrorCount(const std::string& slot_range, const OptionalSyncError& error,
                                const kv::datanode::v1::Error& report_error);
  void IncrSyncCount(const std::string& slot_range, SyncStreamType type);
  void IncrSyncErrorCount(const std::string& slot_range, SyncStreamType type, const OptionalSyncError& err);
  void IncrSyncWriteStreamStats(const std::string& slot_range, SyncStreamType type, uint64_t delta_updates,
                                uint64_t delta_bytes);
  void IncrSyncReadStreamStats(const std::string& slot_range, SyncStreamType type, uint64_t delta_updates,
                               uint64_t delta_bytes);
  void IncrReplSenderStopWriteSuccCount(const std::string& slot_range);
  void IncrReplSenderStopWriteFailCount(const std::string& slot_range);
  void IncrReplSenderStopWriteTime(const std::string& slot_range, uint64_t stop_write_time_ms);
  void IncrReplSenderCaughtUpCount(const std::string& slot_range);
  void IncrReplSenderWaitTopoTimeoutCount(const std::string& slot_range_name);
  void IncrReplPullerStopWriteCount(const std::string& slot_range);
  void IncrReplPullerCaughtUpCount(const std::string& slot_range);
  // sync stats for cdc
  std::ostringstream& GetCDCStatsInfo(std::ostringstream& oss);
  void IncrGetCDCPointCount(const std::string& slot_range, const OptionalSyncError& err);
  void IncrGetCDCRestartPointCount(const std::string& slot_range, const OptionalSyncError& err);
  void IncrGetCDCOldestPointCount(const std::string& slot_range, const OptionalSyncError& err);
  void IncrCDCSenderCount(const std::string& slot_range);
  void IncrCDCSenderErrorCount(const std::string& slot_range, const OptionalSyncError& err);
  void IncrCDCSenderWriteStats(const std::string& slot_range, uint64_t delta_events, uint64_t delta_bytes,
                               uint64_t next_seq);
  void RecordCDCDataLag(const std::string& slot_range, uint64_t duration_ms, uint64_t count);

  void IncrKKVExpireExceedRedisCount(uint64_t inc);

  // ingest Stats
  void RecordIngestStats(const std::string& slot_range, const ingest::IngestMonitor::IngestStatsRecord& record);
  void GetIngestStatsInfo(std::ostringstream& oss);
  void StartDatanodeDtsWrProthibited(const std::string& slot_range);
  void FinishDatanodeDtsWrProthibited(const std::string& slot_range);
  void StartDatanodeOnlyRead();
  void FinishDatanodeOnlyRead();

  void IncrClientBufferCongestedCount() { client_buffer_congested.fetch_add(1); }
  void IncrClientBufferExceedLimitCount() { client_buffer_exceed_limit.fetch_add(1); }
  void GetClientsInfo(std::ostringstream& oss);

 private:
  friend class GlobalStatsHelper;

  std::map<Attributes, Histogram, AttrCmp> collectHistogramFromMetric(MetricType type);
  std::map<Attributes, uint64_t, AttrCmp> collectCounterFromMetric(MetricType type);
  friend class MetricTest;
  FRIEND_TEST(MetricTest, SingleThread);
  FRIEND_TEST(MetricTest, MultiThread);
  FRIEND_TEST(MetricTest, MultiThreadRandomLatency);
  FRIEND_TEST(MetricTest, AttributeTest);
  FRIEND_TEST(MetricTest, CounterTest);
};

inline GlobalStats& GlobalStatsInstance() {
  static GlobalStats inst;
  return inst;
}
