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

#include "stats.h"

#include <glog/logging.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <mutex>

#include "fmt/format.h"
#include "lock/lock_defines.h"
#include "time_util.h"

std::atomic<uint64_t> global_thread_id{0};
thread_local ThreadLocalMetricArray thread_local_metric_array;

// 1 ~ 10000
const std::vector<uint64_t> default_count_spans_ = {1,   2,   3,   5,    10,   20,   30,   50,   100,
                                                    200, 300, 500, 1000, 2000, 3000, 5000, 10000};

// 10us ~ 5s
const std::vector<uint64_t> default_latency_spans_ = {
    10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 20000, 50000, 70000, 200000, 500000, 1000000, 2000000, 5000000};

// 10B ~ 200MB
const std::vector<uint64_t> default_length_spans_ = {10,      50,       100,      500,       1024,     2048,
                                                     5120,    10240,    51200,    102400,    512000,   1048576,
                                                     5242880, 10485760, 52428800, 104857600, 209715200};

const std::unordered_map<MetricType, std::vector<uint64_t>> metric_span_scope_maps_ = {
    // add custom spans here
    {MetricType::COMMAND_LATENCY, default_latency_spans_},
    {MetricType::MGL_LOCK_LATENCY, default_latency_spans_},
    {MetricType::REQUESTS_BATCH_SIZE, default_count_spans_},
    {MetricType::COMMAND_QUEUE_LATENCY_ON_CONNECTION, default_latency_spans_},
    {MetricType::COMMAND_ESTIMATED_SUBKEY_NUM, default_count_spans_},
    {MetricType::COMMAND_ESTIMATED_SUBKEY_BYTE_SIZE, default_length_spans_},
    {MetricType::COMMAND_REPLY_SIZE, default_length_spans_},
    {MetricType::EVENT_QUEUE_SIZE, default_count_spans_},
    {MetricType::APPLY_TOPO_LATENCY, default_latency_spans_},
    {MetricType::CLIENT_OUT_BUFFER_SIZE, default_length_spans_},
    {MetricType::GRPC_CLIENT_REQUEST_LATENCY, default_latency_spans_},
    {MetricType::GRPC_CLIENT_SEND_MESSAGE_LATENCY, default_latency_spans_},
    {MetricType::GRPC_CLIENT_RECV_MESSAGE_INTERVAL, default_latency_spans_},
    {MetricType::GRPC_SERVER_REQUEST_LATENCY, default_latency_spans_},
    {MetricType::GRPC_SERVER_SEND_MESSAGE_LATENCY, default_latency_spans_},
    {MetricType::GRPC_SERVER_RECV_MESSAGE_INTERVAL, default_latency_spans_},
};

const char* FAILED_REQUEST_PREFIX[GlobalStats::FAIL_TYPE_COUNT] = {
    "user_usage_error", "slot_range_get_lock_timeout",         "slot_range_write_stopped",
    "others",           "kkv_expire_exceed_redis_limit_count", "disabled_cmd"};

const char* WRITE_RESULT_CODE_INFO[rocksdb::Status::Code::kMaxCode] = {"ok",
                                                                       "not_found",
                                                                       "corruption",
                                                                       "not_supported",
                                                                       "invalid_argument",
                                                                       "io_error",
                                                                       "merge_in_progress",
                                                                       "result_incomplete",
                                                                       "shutdown_in_progress",
                                                                       "operation_time_out",
                                                                       "operation_aborted",
                                                                       "resource_busy",
                                                                       "operation_expired",
                                                                       "try_again",
                                                                       "compaction_too_large",
                                                                       "column_family_dropped"};

const char* WRITE_RESULT_SUBCODE_INFO[rocksdb::Status::SubCode::kMaxSubCode] = {"none",
                                                                                "timeout_acquiring_mutex",
                                                                                "timeout_locking_key",
                                                                                "lock_number_limit",
                                                                                "no_space",
                                                                                "dead_lock",
                                                                                "stale_file",
                                                                                "memory_limit",
                                                                                "space_limit",
                                                                                "path_not_found",
                                                                                "merge_operands_insufficient_capacity",
                                                                                "manual_compaction_paused",
                                                                                "overwritten",
                                                                                "txn_not_prepared",
                                                                                "io_fenced",
                                                                                "merge_operator_failed",
                                                                                "merge_operand_threshold_exceeded"};

InstantaneousStats::InstantaneousStats(int metric_count) {
  this->inst_metrics = std::vector<OpsInstMetric>(metric_count);
}

StorageStats::StorageStats() : InstantaneousStats(STATS_METRIC_COUNT) {
  for (size_t i = 0; i < rocksdb::Status::Code::kMaxCode; ++i) {
    this->write_result_counters_.emplace_back(rocksdb::Status::SubCode::kMaxSubCode);
  }
}

#if defined(__APPLE__)
#include <mach/mach_init.h>
#include <mach/task.h>

int64_t GlobalStats::GetMemoryRSS() {
  task_t task = MACH_PORT_NULL;
  task_basic_info t_info;
  mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;
  if (task_for_pid(current_task(), getpid(), &task) != KERN_SUCCESS) return 0;
  task_info(task, TASK_BASIC_INFO, (task_info_t)&t_info, &t_info_count);
  return static_cast<int64_t>(t_info.resident_size);
}
#else
#include <fcntl.h>

#include <cstring>
#include <string>

#include "unique_fd.h"

int64_t GlobalStats::GetMemoryRSS() {
  char buf[4096];
  auto fd = UniqueFD(open(fmt::format("/proc/{}/stat", getpid()).c_str(), O_RDONLY));
  if (!fd) return 0;
  if (read(*fd, buf, sizeof(buf)) <= 0) {
    return 0;
  }
  fd.Close();

  char* start = buf;
  int count = 23;  // RSS is the 24th field in /proc/<pid>/stat
  while (start && count--) {
    start = strchr(start, ' ');
    if (start) start++;
  }
  if (!start) return 0;
  char* stop = strchr(start, ' ');
  if (!stop) return 0;
  *stop = '\0';
  int rss = std::atoi(start);
  return static_cast<int64_t>(rss * sysconf(_SC_PAGESIZE));
}
#endif

const std::vector<uint64_t>& GetMetricSpanByType(MetricType type) noexcept {
  auto it = metric_span_scope_maps_.find(type);
  if (it != metric_span_scope_maps_.end()) {
    return it->second;
  }
  return default_latency_spans_;
}

void GlobalStats::RegisterMetric(MetricType type, uint64_t thread_id, std::shared_ptr<Metric> metric) {
  if (type > MetricType::METRIC_TYPE_MAX) {
    type = MetricType::METRIC_TYPE_MAX;
  }
  std::lock_guard<std::mutex> lock(global_and_sum_metric_mutex);
  global_metrics[static_cast<int>(type)][thread_id] = std::move(metric);
  if (!sum_metrics[static_cast<int>(type)]) {
    sum_metrics[static_cast<int>(type)] = std::make_shared<Metric>(type);
  }
}

void GlobalStats::UnregisterMetric(MetricType type, uint64_t thread_id) {
  if (type > MetricType::METRIC_TYPE_MAX) {
    type = MetricType::METRIC_TYPE_MAX;
  }
  std::lock_guard<std::mutex> lock(global_and_sum_metric_mutex);
  auto& metrics_map = global_metrics[static_cast<int>(type)];
  auto it = metrics_map.find(thread_id);
  if (it == metrics_map.end() || !it->second) {
    return;
  }
  auto& metric = it->second;
  if (metric) {
    if (sum_metrics[static_cast<int>(type)]) {
      *sum_metrics[static_cast<int>(type)] += *metric;
    }

    global_metrics[static_cast<int>(type)].erase(thread_id);
  }
}

std::map<Attributes, Histogram, AttrCmp> GlobalStats::collectHistogramFromMetric(MetricType type) {
  if (type > MetricType::METRIC_TYPE_MAX) {
    type = MetricType::METRIC_TYPE_MAX;
  }
  std::lock_guard<std::mutex> lock(global_and_sum_metric_mutex);
  auto& metrics = global_metrics[static_cast<int>(type)];
  auto ret_metric = Metric(type);
  if (sum_metrics[static_cast<int>(type)]) {
    ret_metric += *sum_metrics[static_cast<int>(type)];
  }
  for (const auto& [id, m] : metrics) {
    ret_metric += *m;
  }
  return ret_metric.GetHistograms();
}

std::map<Attributes, uint64_t, AttrCmp> GlobalStats::collectCounterFromMetric(MetricType type) {
  if (type > MetricType::METRIC_TYPE_MAX) {
    type = MetricType::METRIC_TYPE_MAX;
  }
  std::lock_guard<std::mutex> lock(global_and_sum_metric_mutex);
  auto& metrics = global_metrics[static_cast<int>(type)];
  auto ret_metric = Metric(type);
  if (sum_metrics[static_cast<int>(type)]) {
    ret_metric += *sum_metrics[static_cast<int>(type)];
  }
  for (const auto& [id, m] : metrics) {
    ret_metric += *m;
  }
  return ret_metric.GetCounters();
}

void GlobalStats::MetricInfo(MetricType type, std::string* reply) {
  std::ostringstream oss;
  if (MetricIsHistogram(type)) {
    auto hist_map = collectHistogramFromMetric(type);
    if (hist_map.empty()) {
      return;
    }
    oss << "# [HISTOGRAM] " << MetricTypeToString(type) << "\r\n";
    for (const auto& [label, histogram] : hist_map) {
      oss << label << ":";
      oss << histogram;
      oss << "\r\n";
    }
    oss << "\r\n";
    *reply += oss.str();
  } else if (MetricIsCounter(type)) {
    auto counter_map = collectCounterFromMetric(type);
    if (counter_map.empty()) {
      return;
    }
    oss << "# [COUNTER] " << MetricTypeToString(type) << "\r\n";
    for (const auto& [label, counter] : counter_map) {
      oss << label << ":" << std::to_string(counter) << "\r\n";
    }
    oss << "\r\n";
    *reply += oss.str();
  } else {
    LOG(WARNING) << "[stats] Invalid Metric Type:" << MetricTypeToString(type);
  }
}

void GlobalStats::IncrCalls(const std::string& command_name, bool is_success) {
  total_calls.fetch_add(1, std::memory_order_relaxed);
  if (is_success) {
    commands_stats[command_name].success_calls.fetch_add(1, std::memory_order_relaxed);
  } else {
    commands_stats[command_name].fail_calls.fetch_add(1, std::memory_order_relaxed);
  }
}

void GlobalStats::IncrRequests(RequestResult type) {
  total_requests.fetch_add(1, std::memory_order_relaxed);
  if (type == SUCCEED) {
    total_succ_requests.fetch_add(1, std::memory_order_relaxed);
  } else if (type < FAIL_TYPE_COUNT) {
    failed_requests[type].fetch_add(1, std::memory_order_relaxed);
    total_fail_requests.fetch_add(1, std::memory_order_relaxed);
  } else if (type == FAIL_TYPE_COUNT) {
    LOG(WARNING) << "[stats] Invalid Request Result:"
                 << "FAIL_TYPE_COUNT";
  } else {
    LOG(WARNING) << "[stats] Invalid Request Result:"
                 << "unknown_result";
  }
}

void GlobalStats::IncrSlowQueryCallsIfNeeded(uint64_t duration) {
  if (duration >= SLOW_QUERY_SLOWER_THAN) {
    slow_query_calls.fetch_add(1, std::memory_order_relaxed);
  }
}

void InstantaneousStats::TrackInstantaneousMetric(int metric, uint64_t current_reading) {
  uint64_t curr_time = util::GetTimeStampMS();
  std::unique_lock<std::shared_mutex> lock(inst_metrics_mutex);
  uint64_t t = curr_time - inst_metrics[metric].last_sample_time;
  uint64_t operations = current_reading - inst_metrics[metric].last_sample_count;
  uint64_t ops = t > 0 ? (operations * 1000 / t) : 0;
  inst_metrics[metric].samples[inst_metrics[metric].idx] = ops;
  inst_metrics[metric].idx++;
  inst_metrics[metric].idx %= STATS_METRIC_SAMPLES;
  inst_metrics[metric].last_sample_time = curr_time;
  inst_metrics[metric].last_sample_count = current_reading;
}

uint64_t InstantaneousStats::GetInstantaneousMetric(int metric) const {
  std::shared_lock<std::shared_mutex> lock(inst_metrics_mutex);
  uint64_t sum = 0;
  for (uint64_t sample : inst_metrics[metric].samples) sum += sample;
  return sum / STATS_METRIC_SAMPLES;
}

void GlobalStats::GetRequestStatsInfo(std::ostringstream& oss) const {
  oss << "total_received_requests:" << total_requests.load() << "\r\n";
  oss << "total_succ_requests:" << total_succ_requests.load() << "\r\n";
  oss << "total_fail_requests:" << total_fail_requests.load() << "\r\n";

  for (int i = 0; i < FAIL_TYPE_COUNT; ++i) {
    GetFailedRequestInfo(oss, static_cast<RequestResult>(i));
  }
}

void GlobalStats::GetFailedRequestInfo(std::ostringstream& oss, GlobalStats::RequestResult type) const {
  if (type < FAIL_TYPE_COUNT) {
    oss << "fail_request." << FAILED_REQUEST_PREFIX[type] << ":" << failed_requests[type].load() << "\r\n";
  }
}

std::ostringstream& GlobalStats::GetSyncStatsInfo(std::ostringstream& oss) {
  {
    std::shared_lock<std::shared_mutex> lk(get_sync_point_mtx);
    for (auto& [slot_range, stats] : get_sync_point_stats) {
      auto prefix = "get_sync_point." + slot_range + ".";
      stats.Append(oss, prefix);
    }
  }
  {
    std::shared_lock<std::shared_mutex> lk(report_sync_error_mtx);
    for (auto& [slot_range, stats] : report_sync_error_stats) {
      stats.Append(oss, slot_range);
    }
  }
  {
    std::shared_lock<std::shared_mutex> lk(dts_sender_mtx);
    for (auto& [slot_range, stats] : dts_sender_stats) {
      auto prefix = "dts_sender." + slot_range + ".";
      stats.Append(oss, prefix);
    }
  }
  {
    std::shared_lock<std::shared_mutex> lk(dts_recver_mtx);
    for (auto& [slot_range, stats] : dts_recver_stats) {
      auto prefix = "dts_recver." + slot_range + ".";
      stats.Append(oss, prefix);
    }
  }
  {
    std::shared_lock<std::shared_mutex> lk(repl_sender_mtx);
    for (auto& [slot_range, stats] : repl_sender_stats) {
      stats.Append(oss, slot_range);
    }
  }
  {
    std::shared_lock<std::shared_mutex> lk(repl_puller_mtx);
    for (auto& [slot_range, stats] : repl_puller_stats) {
      stats.Append(oss, slot_range);
    }
  }

  return oss;
}

void GlobalStats::IncrGetSyncPointCount(const std::string& slot_range, const OptionalSyncError& err) {
  std::unique_lock<std::shared_mutex> lk(get_sync_point_mtx);

  get_sync_point_stats[slot_range].IncrCount(err);
}

void GlobalStats::IncrReportSyncErrorCount(const std::string& slot_range, const OptionalSyncError& error,
                                           const kv::datanode::v1::Error& report_error) {
  std::unique_lock<std::shared_mutex> lk(report_sync_error_mtx);

  report_sync_error_stats[slot_range].IncrReportSyncErrorCount(error, report_error);
}

void GlobalStats::IncrSyncCount(const std::string& slot_range, SyncStreamType type) {
  switch (type) {
    case DtsSender: {
      std::unique_lock<std::shared_mutex> lk(dts_sender_mtx);
      dts_sender_stats[slot_range].IncrCount();
      break;
    }
    case DtsRecver: {
      std::unique_lock<std::shared_mutex> lk(dts_recver_mtx);
      dts_recver_stats[slot_range].IncrCount();
      break;
    }
    case ReplSender: {
      std::unique_lock<std::shared_mutex> lk(repl_sender_mtx);
      repl_sender_stats[slot_range].IncrCount();
      break;
    }
    case ReplPuller: {
      std::unique_lock<std::shared_mutex> lk(repl_puller_mtx);
      repl_puller_stats[slot_range].IncrCount();
      break;
    }
    default:
      break;
  }
}

void GlobalStats::IncrSyncErrorCount(const std::string& slot_range, SyncStreamType type, const OptionalSyncError& err) {
  switch (type) {
    case DtsSender: {
      std::unique_lock<std::shared_mutex> lk(dts_sender_mtx);
      dts_sender_stats[slot_range].IncrErrorCount(err);
      break;
    }
    case DtsRecver: {
      std::unique_lock<std::shared_mutex> lk(dts_recver_mtx);
      dts_recver_stats[slot_range].IncrErrorCount(err);
      break;
    }
    case ReplSender: {
      std::unique_lock<std::shared_mutex> lk(repl_sender_mtx);
      repl_sender_stats[slot_range].IncrErrorCount(err);
      break;
    }
    case ReplPuller: {
      std::unique_lock<std::shared_mutex> lk(repl_puller_mtx);
      repl_puller_stats[slot_range].IncrErrorCount(err);
      break;
    }
    default:
      break;
  }
}

void GlobalStats::IncrSyncWriteStreamStats(const std::string& slot_range, SyncStreamType type, uint64_t delta_updates,
                                           uint64_t delta_bytes) {
  switch (type) {
    case DtsSender: {
      std::unique_lock<std::shared_mutex> lk(dts_sender_mtx);
      dts_sender_stats[slot_range].IncrSyncStats(delta_updates, delta_bytes);
      break;
    }
    case ReplSender: {
      std::unique_lock<std::shared_mutex> lk(repl_sender_mtx);
      repl_sender_stats[slot_range].IncrSyncStats(delta_updates, delta_bytes);
      break;
    }
    default:
      break;
  }
}

void GlobalStats::IncrSyncReadStreamStats(const std::string& slot_range, SyncStreamType type, uint64_t delta_updates,
                                          uint64_t delta_bytes) {
  switch (type) {
    case DtsRecver: {
      std::unique_lock<std::shared_mutex> lk(dts_recver_mtx);
      dts_recver_stats[slot_range].IncrSyncStats(delta_updates, delta_bytes);
      break;
    }
    case ReplPuller: {
      std::unique_lock<std::shared_mutex> lk(repl_puller_mtx);
      repl_puller_stats[slot_range].IncrSyncStats(delta_updates, delta_bytes);
      break;
    }
    default:
      break;
  }
}

void GlobalStats::IncrReplSenderStopWriteSuccCount(const std::string& slot_range) {
  std::unique_lock<std::shared_mutex> lk(repl_sender_mtx);

  repl_sender_stats[slot_range].IncrStopWriteSuccCount();
}

void GlobalStats::IncrReplSenderStopWriteFailCount(const std::string& slot_range) {
  std::unique_lock<std::shared_mutex> lk(repl_sender_mtx);

  repl_sender_stats[slot_range].IncrStopWriteFailCount();
}

void GlobalStats::IncrReplSenderStopWriteTime(const std::string& slot_range, uint64_t stop_write_time_ms) {
  std::unique_lock<std::shared_mutex> lk(repl_sender_mtx);

  repl_sender_stats[slot_range].IncrStopWriteTime(stop_write_time_ms);
}

void GlobalStats::IncrReplSenderCaughtUpCount(const std::string& slot_range) {
  std::unique_lock<std::shared_mutex> lk(repl_sender_mtx);

  repl_sender_stats[slot_range].IncrCaughtUpCount();
}

void GlobalStats::IncrReplSenderWaitTopoTimeoutCount(const std::string& slot_range_name) {
  std::unique_lock<std::shared_mutex> lk(repl_sender_mtx);

  repl_sender_stats[slot_range_name].IncrWaitTopoTimeoutCount();
}

void GlobalStats::IncrReplPullerStopWriteCount(const std::string& slot_range) {
  std::unique_lock<std::shared_mutex> lk(repl_puller_mtx);

  repl_puller_stats[slot_range].IncrStopWriteCount();
}

void GlobalStats::IncrReplPullerCaughtUpCount(const std::string& slot_range) {
  std::unique_lock<std::shared_mutex> lk(repl_puller_mtx);

  repl_puller_stats[slot_range].IncrCaughtUpCount();
}

std::ostringstream& GlobalStats::GetCDCStatsInfo(std::ostringstream& oss) {
  {
    std::shared_lock<std::shared_mutex> lk(get_cdc_point_mtx);
    for (auto& [slot_range, stats] : get_cdc_point_stats) {
      auto prefix = "get_cdc_point." + slot_range + ".";
      stats.Append(oss, prefix);
    }
  }
  {
    std::shared_lock<std::shared_mutex> lk(get_cdc_restart_point_mtx);
    for (auto& [slot_range, stats] : get_cdc_restart_point_stats) {
      auto prefix = "get_cdc_restart_point." + slot_range + ".";
      stats.Append(oss, prefix);
    }
  }
  {
    std::shared_lock<std::shared_mutex> lk(get_cdc_oldest_point_mtx);
    for (auto& [slot_range, stats] : get_cdc_oldest_point_stats) {
      auto prefix = "get_cdc_oldest_point." + slot_range + ".";
      stats.Append(oss, prefix);
    }
  }
  {
    std::shared_lock<std::shared_mutex> lk(cdc_sender_mtx);
    for (auto& [slot_range, stats] : cdc_sender_stats) {
      auto prefix = "cdc_sender." + slot_range + ".";
      stats.Append(oss, prefix);
    }
  }
  return oss;
}

void GlobalStats::IncrGetCDCPointCount(const std::string& slot_range, const OptionalSyncError& err) {
  std::unique_lock<std::shared_mutex> lk(get_cdc_point_mtx);

  get_cdc_point_stats[slot_range].IncrCount(err);
}

void GlobalStats::IncrGetCDCRestartPointCount(const std::string& slot_range, const OptionalSyncError& err) {
  std::unique_lock<std::shared_mutex> lk(get_cdc_restart_point_mtx);

  get_cdc_restart_point_stats[slot_range].IncrCount(err);
}

void GlobalStats::IncrGetCDCOldestPointCount(const std::string& slot_range, const OptionalSyncError& err) {
  std::unique_lock<std::shared_mutex> lk(get_cdc_oldest_point_mtx);

  get_cdc_oldest_point_stats[slot_range].IncrCount(err);
}

void GlobalStats::IncrCDCSenderCount(const std::string& slot_range) {
  std::unique_lock<std::shared_mutex> lk(cdc_sender_mtx);

  cdc_sender_stats[slot_range].IncrCount();
}

void GlobalStats::IncrCDCSenderErrorCount(const std::string& slot_range, const OptionalSyncError& err) {
  std::unique_lock<std::shared_mutex> lk(cdc_sender_mtx);

  cdc_sender_stats[slot_range].IncrErrorCount(err);
}

void GlobalStats::IncrCDCSenderWriteStats(const std::string& slot_range, uint64_t delta_events, uint64_t delta_bytes,
                                          uint64_t next_seq) {
  std::unique_lock<std::shared_mutex> lk(cdc_sender_mtx);

  cdc_sender_stats[slot_range].IncrSenderStats(delta_events, delta_bytes, next_seq);
}

void GlobalStats::RecordCDCDataLag(const std::string& slot_range, uint64_t duration_ms, uint64_t count) {
  std::unique_lock<std::shared_mutex> lk(cdc_sender_mtx);

  cdc_sender_stats[slot_range].RecordDataLag(duration_ms, count);
}

void StorageStats::IncrWriteResult(const rocksdb::Status& status) {
  rocksdb::Status::Code code = status.code();
  rocksdb::Status::SubCode subcode = status.subcode();

  if (code >= rocksdb::Status::Code::kMaxCode || code < rocksdb::Status::Code::kOk ||
      subcode >= rocksdb::Status::SubCode::kMaxSubCode || subcode < rocksdb::Status::SubCode::kNone) {
    LOG(WARNING) << "[stats] Invalid Rocksdb Write Result: Code:" << code << ", SubCode:" << subcode;
    return;
  }

  write_result_counters_[code][subcode].fetch_add(1, std::memory_order_relaxed);
}

void StorageStats::GetFailedWriteInfo(std::ostringstream& oss, const std::string& prefix) const {
  for (uint32_t i = 0; i < rocksdb::Status::Code::kMaxCode; ++i) {
    for (uint32_t j = 0; j < rocksdb::Status::SubCode::kMaxSubCode; ++j) {
      if (write_result_counters_[i][j].load() > 0) {
        oss << prefix << "failed_write." << WRITE_RESULT_CODE_INFO[i] << "." << WRITE_RESULT_SUBCODE_INFO[j] << ":"
            << write_result_counters_[i][j].load() << "\r\n";
      }
    }
  }
}

bool Attributes::ForEachKeyValue(std::function<bool(std::string_view, std::string_view)>& callback) const {
  for (const auto& [key, val] : attributes_) {
    if (!callback(key, val)) {
      return false;
    }
  }
  return true;
}

bool Attributes::operator==(const Attributes& other) const {
  if (attributes_.size() != other.attributes_.size() || hash_ != other.hash_) {
    return false;
  }
  for (size_t i = 0; i < attributes_.size(); ++i) {
    if (attributes_[i].first != other.attributes_[i].first || attributes_[i].second != other.attributes_[i].second) {
      return false;
    }
  }
  return true;
}

std::ostream& operator<<(std::ostream& os, const Attributes& attrs) {
  bool first = true;
  // If label is empty, set default label to ensure exporter can parse
  if (attrs.attributes_.empty()) {
    os << "label=default";
  }
  for (const auto& [key, val] : attrs.attributes_) {
    if (!first) {
      // Use ';' as delimiter, because some fields (e.g., slot_range=[1,16383]) include ','.
      os << ";";
    }
    os << key << "=" << val;
    first = false;
  }
  return os;
}

Histogram::Histogram(const std::vector<uint64_t>& spans) {
  for (const auto& rspan : spans) {
    TimeSpan time_span{rspan, 0, 0};
    time_spans_.push_back(time_span);
  }
  last_ = {0, 0, 0};
}

Histogram& Histogram::operator+=(const Histogram& m) {
  if (time_spans_.size() != m.time_spans_.size()) {
    DCHECK(false);
    return *this;
  }
  for (size_t i = 0; i < time_spans_.size(); ++i) {
    if (time_spans_[i].span != m.time_spans_[i].span) {
      DCHECK(false);
      return *this;
    }
    time_spans_[i] += m.time_spans_[i];
  }
  last_ += m.last_;
  return *this;
}

void Histogram::Record(uint64_t v) {
  TimeSpan span;
  span.span = v;
  auto it = std::lower_bound(time_spans_.begin(), time_spans_.end(), span);
  if (it == time_spans_.end()) {
    last_.total += v;
    ++last_.count;
  } else {
    it->total += v;
    ++it->count;
  }
}

Metric& Metric::operator+=(const Metric& rhs) {
  if (this == &rhs) return *this;
  auto rhs_copy_histograms = rhs.GetHistograms();
  auto rhs_copy_counters = rhs.GetCounters();
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& [label, h] : rhs_copy_histograms) {
    histograms_.try_emplace(label, GetMetricSpanByType(type_)).first->second += h;
  }
  for (const auto& [label, c] : rhs_copy_counters) {
    counters_.try_emplace(label).first->second += static_cast<uint64_t>(c);
  }
  return *this;
}

void Metric::Record(const std::initializer_list<std::pair<std::string_view, std::string_view>>& labels, uint64_t val) {
  if (!MetricIsHistogram(type_)) {
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  auto attr_view = AttributeView(labels);
  auto it = histograms_.find(attr_view);
  if (it == histograms_.end()) {
    auto attrs = Attributes(attr_view);
    auto histogram = Histogram(GetMetricSpanByType(type_));
    histogram.Record(val);
    histograms_.emplace(std::move(attrs), std::move(histogram));

  } else {
    it->second.Record(val);
  }
}

void Metric::Count(const std::initializer_list<std::pair<std::string_view, std::string_view>>& labels, uint64_t val) {
  if (!MetricIsCounter(type_)) {
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  auto attr_view = AttributeView(labels);
  auto it = counters_.find(attr_view);
  if (it == counters_.end()) {
    auto attrs = Attributes(labels);
    counters_.emplace(std::move(attrs), val);
  } else {
    it->second += val;
  }
}

std::ostringstream& operator<<(std::ostringstream& oss, const Histogram& lm) {
  uint64_t t_count = lm.last_.count;
  uint64_t t_sum = lm.last_.total;
  for (auto& s : lm.time_spans_) {
    char buf[128];
    t_sum += s.total;
    t_count += s.count;
    snprintf(buf, 128, "span_%ld=%ld;", s.span, s.count);
    oss << buf;
  }
  {
    char buf[128];
    snprintf(buf, 128, "span_inf_count=%ld;span_inf_sum=%ld;", lm.last_.count, lm.last_.total);
    oss << buf;
  }
  char buf[128];
  snprintf(buf, 128, "sum=%ld;count=%ld", t_sum, t_count);
  oss << buf;
  return oss;
}

void ThreadLocalMetricArray::RecordCommadLatency(const std::string& cmd, const std::string& slotrange, uint64_t val) {
  Record(MetricType::COMMAND_LATENCY, {{"cmd", cmd}, {"slot_range", slotrange}}, val);
}

void ThreadLocalMetricArray::RecordCommadSize(const std::string& cmd, const std::string& slotrange,
                                              int64_t subkey_count, uint64_t subkey_byte_size) {
  if (subkey_count >= 0) {
    Record(MetricType::COMMAND_ESTIMATED_SUBKEY_NUM, {{"cmd", cmd}, {"slot_range", slotrange}}, subkey_count);
  }
  Record(MetricType::COMMAND_ESTIMATED_SUBKEY_BYTE_SIZE, {{"cmd", cmd}, {"slot_range", slotrange}}, subkey_byte_size);
}
void ThreadLocalMetricArray::RecordReplySize(const std::string& cmd, const std::string& slotrange, uint64_t val) {
  Record(MetricType::COMMAND_REPLY_SIZE, {{"cmd", cmd}, {"slot_range", slotrange}}, val);
}

void ThreadLocalMetricArray::RecordMglLockLatency(redis::mgl::LockMode mode, const std::string& slotrange,
                                                  bool is_slotrange, uint64_t val) {
  if (is_slotrange) {
    Record(MetricType::MGL_LOCK_LATENCY,
           {{"mgl", redis::mgl::LockModeToString[static_cast<std::size_t>(mode)]},
            {"slot_range", slotrange},
            {"lock_object", "slot_range"}},
           val);
  } else {
    Record(MetricType::MGL_LOCK_LATENCY,
           {{"mgl", redis::mgl::LockModeToString[static_cast<std::size_t>(mode)]},
            {"slot_range", slotrange},
            {"lock_object", "key"}},
           val);
  }
}

void ThreadLocalMetricArray::CountMglLockFailed(redis::mgl::LockMode mode, const std::string& slotrange,
                                                bool is_slotrange, const redis::mgl::LockRes& status, uint64_t val) {
  if (__builtin_expect(status >= redis::mgl::LockRes::LOCKRES_NUM, 0)) {
    DCHECK(false) << "[stats] Invalid MGL Lock Result: " << static_cast<std::size_t>(status);
    LOG(WARNING) << "[stats] Invalid MGL Lock Result: " << static_cast<std::size_t>(status);
    return;
  }
  if (is_slotrange) {
    Count(MetricType::MGL_LOCK_FAILED_COUNT,
          {{"mgl", redis::mgl::LockModeToString[static_cast<std::size_t>(mode)]},
           {"slot_range", slotrange},
           {"lock_object", "slot_range"},
           {"status", redis::mgl::LockResToString[static_cast<std::size_t>(status)]}},
          val);
  } else {
    Count(MetricType::MGL_LOCK_FAILED_COUNT,
          {{"mgl", redis::mgl::LockModeToString[static_cast<std::size_t>(mode)]},
           {"slot_range", slotrange},
           {"lock_object", "key"},
           {"status", redis::mgl::LockResToString[static_cast<std::size_t>(status)]}},
          val);
  }
}

void ThreadLocalMetricArray::RecordCDCParseBatchLatency(const std::string& slotrange, uint64_t val) {
  Record(MetricType::CDC_PARSE_BATCH_LATENCY, {{"slot_range", slotrange}}, val);
}

void ThreadLocalMetricArray::RecordCDCParseBatchSizeHistogram(const std::string& slotrange, uint64_t val) {
  Record(MetricType::CDC_PARSE_BATCH_SIZE_HISTOGRAM, {{"slot_range", slotrange}}, val);
}

void ThreadLocalMetricArray::RecordCDCSendBatchSizeHistogram(const std::string& slotrange, uint64_t val) {
  Record(MetricType::CDC_SEND_BATCH_SIZE_HISTOGRAM, {{"slot_range", slotrange}}, val);
}

void ThreadLocalMetricArray::Record(MetricType type,
                                    const std::initializer_list<std::pair<std::string_view, std::string_view>>& labels,
                                    uint64_t val) {
  if (type > MetricType::METRIC_TYPE_MAX) {
    type = MetricType::METRIC_TYPE_MAX;
  }
  auto& metric = metrics[static_cast<int>(type)];
  if (!metric) {
    InitMetric(type);
  }
  metric->Record(labels, val);
}

void ThreadLocalMetricArray::Count(MetricType type,
                                   const std::initializer_list<std::pair<std::string_view, std::string_view>>& labels,
                                   uint64_t val) {
  if (type > MetricType::METRIC_TYPE_MAX) {
    type = MetricType::METRIC_TYPE_MAX;
  }
  auto& metric = metrics[static_cast<int>(type)];
  if (!metric) {
    InitMetric(type);
  }
  metric->Count(labels, val);
}

void ThreadLocalMetricArray::InitMetric(MetricType type) {
  if (type > MetricType::METRIC_TYPE_MAX) {
    type = MetricType::METRIC_TYPE_MAX;
  }
  auto& metric = metrics[static_cast<int>(type)];
  if (!metric) {
    metric = std::make_shared<Metric>(type);
    GlobalStatsInstance().RegisterMetric(type, thread_id, metric);
  }
}

ThreadLocalMetricArray::~ThreadLocalMetricArray() {
  for (size_t i = 0; i <= static_cast<size_t>(MetricType::METRIC_TYPE_MAX); ++i) {
    if (metrics[i]) {
      GlobalStatsInstance().UnregisterMetric(static_cast<MetricType>(i), thread_id);
      metrics[i].reset();
    }
  }
};

void GlobalStats::RecordIngestStats(const std::string& slot_range,
                                    const ingest::IngestMonitor::IngestStatsRecord& record) {
  std::unique_lock<std::shared_mutex> lk(ingest_mtx);
  ingest_stats[slot_range].RecordIngestMonitor(record);
}

void GlobalStats::GetIngestStatsInfo(std::ostringstream& oss) {
  std::shared_lock<std::shared_mutex> lk(ingest_mtx);
  for (auto& [slot_range, stats] : ingest_stats) {
    auto prefix = "Ingest." + slot_range + ".";
    stats.OutputToString(oss, prefix);
  }

  uint64_t now_datanode_only_read_ms = 0;
  if (datanode_only_read_start_time > 0) {
    now_datanode_only_read_ms = util::GetTimeStampMS() - datanode_only_read_start_time;
  }

  oss << "Ingest.Datanode.datanode_only_read_start_time:" << datanode_only_read_start_time << "\r\n";
  oss << "Ingest.Datanode.datanode_only_read_ms:" << now_datanode_only_read_ms << "\r\n";
}

void GlobalStats::StartDatanodeDtsWrProthibited(const std::string& slot_range) {
  std::unique_lock<std::shared_mutex> lk(ingest_mtx);
  ingest_stats[slot_range].StartDatanodeDtsWrProthibited();
}

void GlobalStats::FinishDatanodeDtsWrProthibited(const std::string& slot_range) {
  std::unique_lock<std::shared_mutex> lk(ingest_mtx);
  ingest_stats[slot_range].FinishDatanodeDtsWrProthibited();
}

void GlobalStats::StartDatanodeOnlyRead() { datanode_only_read_start_time = util::GetTimeStampMS(); }

void GlobalStats::FinishDatanodeOnlyRead() {
  if (datanode_only_read_start_time > 0) {
    datanode_only_read_ms = util::GetTimeStampMS() - datanode_only_read_start_time;
    datanode_only_read_start_time = 0;
  }
}

void GlobalStats::GetClientsInfo(std::ostringstream& oss) {
  oss << "client_buffer_congested:" << client_buffer_congested.load() << "\r\n";
  oss << "client_buffer_exceed_limit:" << client_buffer_exceed_limit.load() << "\r\n";
}
