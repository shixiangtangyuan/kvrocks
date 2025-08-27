#pragma once

#include <ingest/ingest_stats.h>
#include <memory.h>
#include <storage/storage.h>

#include <mutex>
#include <string>
#include <tuple>
#include <vector>

#include "ingest_monitor.h"
#include "status.h"
#include "time_util.h"

namespace ingest {

inline constexpr const char* errInvalidIngestInfo = "Invalid ingest info";
inline constexpr const char* errIllegalColumnFamily = "Illegal column family";
using TaskID = std::string;
using TaskStats = std::string;
using CheckSum = bool;
using SstPath = std::string;
using IngestCfName = std::string;
using IngestArg = std::tuple<ingest::IngestCfName, ingest::SstPath, ingest::CheckSum>;

struct IngestTaskInfo {
  IngestTaskInfo(std::string name, std::vector<IngestArg> args, std::string id)
      : slot_range_name(std::move(name)), req_args(std::move(args)), task_id(std::move(id)) {}
  std::string slot_range_name;
  std::vector<IngestArg> req_args;
  std::string task_id;
  std::vector<rocksdb::IngestExternalFileArg> args;
  bool org_enable_compact_range;
  bool org_enable_compact_full;
  std::string org_disable_auto_compactions_str;
  bool ingest_sucessed = true;
  uint64_t ingest_start_time = util::GetTimeStampMS();
  uint64_t ingest_finish_time = 0;
  uint64_t sst_file_number = 0;
  uint64_t sst_file_size = 0;
  uint64_t ingest_duration_time_ms = 0;
  IngestMonitor::Reason failed_reason = IngestMonitor::Reason::IngestOtherError;

  std::string Detail() {
    std::ostringstream oss;
    ingest_finish_time = util::GetTimeStampMS();
    oss << ", detail{";
    oss << "task_id:" << task_id;
    oss << ", ingest_sucessed:" << (ingest_sucessed ? "true" : "false");
    oss << ", start_time:" << ingest_start_time;
    oss << ", finish_time:" << ingest_finish_time;
    oss << ", sst_number:" << sst_file_number;
    oss << ", sst_size:" << sst_file_size;
    oss << ", rocksdb_ingest_time:" << ingest_duration_time_ms;
    if (!ingest_sucessed) {
      oss << ", failed_reason:" << IngestMonitor::ReasonToString(failed_reason);
    }

    oss << "}";
    return oss.str();
  }
};

class Ingester {
 public:
  explicit Ingester(std::shared_ptr<engine::Storage> storage) : storage_(storage) {}

  Status Run(IngestTaskInfo& task_info);
  kv::datanode::v1::Error GetResult(const std::string& task_id);
  void ResetFlag() {
    std::lock_guard<std::mutex> lg(mutex_lock_);
    ingesting_ = false;
  };

  void IngestDone(IngestTaskInfo& task_info);

  bool IsRunning() {
    std::lock_guard<std::mutex> lg(mutex_lock_);
    return ingesting_;
  }

 private:
  Status storeDBConfig(IngestTaskInfo& task_info);

  void recordIngestStats(const TaskID& task_key, TaskStats task_stats) {
    std::lock_guard<std::mutex> lg(mutex_lock_);
    task_stats_[task_key] = std::move(task_stats);
  }

  Status parseIngestArgs(IngestTaskInfo& task_info);
  Status check(IngestTaskInfo& task_info);
  void asyncCompaction();

  std::atomic_bool ingesting_{false};
  std::mutex mutex_lock_;
  std::map<TaskID, TaskStats> task_stats_;

  std::weak_ptr<engine::Storage> storage_;
};
}  // namespace ingest
