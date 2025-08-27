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

#include <absl/time/clock.h>
#include <inttypes.h>
#include <kv/controller/v1/model.pb.h>
#include <kv/datanode/v1/service.grpc.pb.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cluster/cdc_manager.h"
#include "cluster/cluster.h"
#include "cluster/sync_manager.h"
#include "commands/commander.h"
#include "commands/scan_util.h"
#include "kv/datanode/v1/ingest.pb.h"
#include "kv/datanode/v1/monitorcmd.grpc.pb.h"
#include "lock/mgl_mgr.h"
#include "migration/migration.h"
#include "rpc/rpc_cluster_controller.h"
#include "script/script_manager.h"
#include "server/redis_connection.h"
#include "stats/log_collector.h"
#include "stats/stats.h"
#include "status.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"
#include "task_runner.h"
#include "tls_util.h"
#include "worker.h"

struct DBScanInfo {
  time_t last_scan_time = 0;
  KeyNumStats key_num_stats;
  bool is_scanning = false;
};

struct ConnContext {
  Worker *owner;
  int fd;

  ConnContext(Worker *w, int fd) : owner(w), fd(fd) {}

  bool operator<(const ConnContext &c) const {
    if (owner == c.owner) {
      return fd < c.fd;
    }

    return owner < c.owner;
  }

  bool operator==(const ConnContext &c) const { return owner == c.owner && fd == c.fd; }
};

struct StreamConsumer {
  Worker *owner;
  int fd;
  std::string ns;
  redis::StreamEntryID last_consumed_id;
  StreamConsumer(Worker *w, int fd, std::string ns, redis::StreamEntryID id)
      : owner(w), fd(fd), ns(std::move(ns)), last_consumed_id(id) {}
};

struct ChannelSubscribeNum {
  std::string channel;
  size_t subscribe_num;
};

/*
// CURSOR_DICT_SIZE must be 2^n where n <= 16
constexpr const size_t CURSOR_DICT_SIZE = 1024 * 16;
static_assert((CURSOR_DICT_SIZE & (CURSOR_DICT_SIZE - 1)) == 0, "CURSOR_DICT_SIZE must be 2^n");
static_assert(CURSOR_DICT_SIZE <= (1 << 16), "CURSOR_DICT_SIZE must be less than or equal to 2^16");

struct CursorDictElement;

class NumberCursor {
 public:
  NumberCursor() = default;
  explicit NumberCursor(CursorType cursor_type, uint16_t counter, const std::string &key_name);
  explicit NumberCursor(uint64_t number_cursor) : cursor_(number_cursor) {}
  size_t GetIndex() const { return cursor_ % CURSOR_DICT_SIZE; }
  bool IsMatch(const CursorDictElement &element, CursorType cursor_type) const;
  std::string ToString() const { return std::to_string(cursor_); }

 private:
  CursorType getCursorType() const { return static_cast<CursorType>(cursor_ >> 61); }
  uint64_t cursor_;
};

struct CursorDictElement {
  NumberCursor cursor;
  std::string key_name;
};
*/

enum SlowLog {
  kSlowLogMaxArgc = 32,
  kSlowLogMaxString = 128,
};

enum ClientType {
  kTypeNormal = (1ULL << 0),  // normal client
  kTypePubsub = (1ULL << 1),  // pubsub client
  kTypeMaster = (1ULL << 2),  // master client
  kTypeSlave = (1ULL << 3),   // slave client
};

enum ServerLogType { kServerLogNone, kReplIdLog };

class ServerLogData {
 public:
  // Redis::WriteBatchLogData always starts with digit ascii, we use alphabetic to
  // distinguish ServerLogData with Redis::WriteBatchLogData.
  static const char kReplIdTag = 'r';
  static bool IsServerLogData(const char *header) {
    if (header) return *header == kReplIdTag;
    return false;
  }

  ServerLogData() = default;
  explicit ServerLogData(ServerLogType type, std::string content)
      : type_(type), time_nanos_(absl::GetCurrentTimeNanos()), content_(std::move(content)) {}
  explicit ServerLogData(ServerLogType type, uint64_t time_nanos, std::string content)
      : type_(type), time_nanos_(time_nanos), content_(std::move(content)) {}

  bool operator==(const ServerLogData &rhs) const {
    return type_ == rhs.type_ && time_nanos_ == rhs.time_nanos_ && content_ == rhs.content_;
  }

  ServerLogType GetType() const { return type_; }
  uint64_t GetTimeNanos() const { return time_nanos_; }
  std::string GetContent() const { return content_; }
  std::string Encode() const;
  Status Decode(const rocksdb::Slice &blob);

 private:
  ServerLogType type_ = kServerLogNone;
  uint64_t time_nanos_{0};
  static const size_t kPrefixSize = sizeof(kReplIdTag) + sizeof(uint64_t);
  std::string content_;
};

// An extractor to extract update from raw writebatch
class ReplIdExtractor : public rocksdb::WriteBatch::Handler {
 public:
  rocksdb::Status PutCF(uint32_t column_family_id, const Slice &key, const Slice &value) override {
    return rocksdb::Status::OK();
  }
  rocksdb::Status DeleteCF(uint32_t column_family_id, const rocksdb::Slice &key) override {
    return rocksdb::Status::OK();
  }
  rocksdb::Status DeleteRangeCF(uint32_t column_family_id, const rocksdb::Slice &begin_key,
                                const rocksdb::Slice &end_key) override {
    return rocksdb::Status::OK();
  }

  void LogData(const rocksdb::Slice &blob) override {
    // Currently, we always put replid log data at the end.
    if (ServerLogData::IsServerLogData(blob.data())) {
      ServerLogData server_log;
      if (server_log.Decode(blob).IsOK()) {
        if (server_log.GetType() == kReplIdLog) {
          time_nanos_in_wal_ = server_log.GetTimeNanos();
          replid_in_wal_ = server_log.GetContent();
        }
      }
    }
  };

  uint64_t GetTimeNanos() const { return time_nanos_in_wal_; }

  std::string GetReplId() { return replid_in_wal_; }

 private:
  uint64_t time_nanos_in_wal_{0};
  std::string replid_in_wal_;
};

// legacyslots compaction info
extern std::atomic<int16_t> global_legacyslots_compacting_count;
extern std::atomic<uint64_t> global_legacyslots_last_compact_time;
extern std::atomic<uint64_t> global_lagacyslots_last_compact_duration;

class Server final : public kv::datanode::v1::DataNodeService::CallbackService,
                     public std::enable_shared_from_this<Server> {
 public:
  explicit Server(Config *config);
  ~Server() override;

  Server(const Server &) = delete;
  Server &operator=(const Server &) = delete;
  Server(Server &&) = delete;
  Server &operator=(Server &&) = delete;

  Status Start();
  void Stop();
  void Join();
  bool IsStopped() const { return stop_; }
  bool IsLoading() const { return is_loading_; }
  Config *GetConfig() { return config_; }
  static Status LookupAndCreateCommand(const std::string &cmd_name, std::unique_ptr<redis::Commander> *cmd);
  void AdjustOpenFilesLimit();
  void AdjustWorkerThreads();

  // Worker load balancing methods
  Worker *SelectWorkerWithLeastConnections();
  size_t GetWorkerCount() const { return worker_threads_.size(); }

  void FeedMonitorConns(redis::Connection *conn, const std::vector<std::string> &tokens);
  void IncrFetchFileThread() { fetch_file_threads_num_++; }
  void DecrFetchFileThread() { fetch_file_threads_num_--; }
  int GetFetchFileThreadNum() const { return fetch_file_threads_num_; }

  void SubscribeChannel(const std::string &channel, redis::Connection *conn);
  void UnsubscribeChannel(const std::string &channel, redis::Connection *conn);
  void GetChannelsByPattern(const std::string &pattern, std::vector<std::string> *channels);
  void ListChannelSubscribeNum(const std::vector<std::string> &channels,
                               std::vector<ChannelSubscribeNum> *channel_subscribe_nums);
  void PSubscribeChannel(const std::string &pattern, redis::Connection *conn);
  void PUnsubscribeChannel(const std::string &pattern, redis::Connection *conn);
  size_t GetPubSubPatternSize() const { return pubsub_patterns_.size(); }

  void BlockOnKey(const std::string &key, redis::Connection *conn);
  void UnblockOnKey(const std::string &key, redis::Connection *conn);
  void BlockOnStreams(const std::vector<std::string> &keys, const std::vector<redis::StreamEntryID> &entry_ids,
                      redis::Connection *conn);
  void UnblockOnStreams(const std::vector<std::string> &keys, redis::Connection *conn);
  void WakeupBlockingConns(const std::string &key, size_t n_conns);
  void OnEntryAddedToStream(const std::string &ns, const std::string &key, const redis::StreamEntryID &entry_id);

  std::string GetLastRandomKeyCursor();
  void SetLastRandomKeyCursor(const std::string &cursor);

  static int64_t GetCachedUnixTime();
  int64_t GetLastBgsaveTime();
  void GetStatsInfo(std::string *info);
  void GetNetInfo(std::string *info);
  void GetServerInfo(std::string *info);
  void GetMemoryInfo(std::string *info);
  static void GetCounterMetric(std::string *info);
  static void GetHistogramMetric(std::string *info);
  void GetClientsInfo(std::string *info);
  void GetCommandsStatsInfo(std::string *info);
  void GetClusterInfo(std::string *info);
  void GetInfo(const std::string &ns, const std::string &section, std::string *info);
  void GetRocksDBInfo(std::string *info);
  void GetLegacyslotsCompactInfo(std::string *info);
  void GetDBAndCFList(std::string *info);
  void GetDBPropertyInfo(const std::string &query_params, std::string *info);
  void HandleCFOptionCommand(const std::string &query_params, std::string *result);
  static void GetMetricInfo(const std::string &ns, const std::string &section, std::string *reply);
  std::string GetRocksDBStatsJson() const;
  std::string GetRocksDBStats() const;
  void GetCtrlClientInfo(std::string *info) const;
  std::string IngestJobIsRunning();

  void WaitNoMigrateProcessing();
  void GetLatestKeyNumStats(const std::string &ns, KeyNumStats *stats);
  time_t GetLastScanTime(const std::string &ns);

  std::string GenerateCursorFromKeyName(const std::string &key_name, CursorType cursor_type, const char *prefix = "");
  std::string GetKeyNameFromCursor(const std::string &cursor, CursorType cursor_type);

  int DecrClientNum(const std::thread::id &tid);
  int IncrClientNum(const std::thread::id &tid);
  int IncrMonitorClientNum();
  int DecrMonitorClientNum();
  int IncrBlockedClientNum();
  int DecrBlockedClientNum();
  std::string GetClientsStr();
  uint64_t GetClientID();
  void KillClient(int64_t *killed, const std::string &addr, uint64_t id, uint64_t type, bool skipme,
                  redis::Connection *conn);

  LogCollector<PerfEntry> *GetPerfLog() { return &perf_log_; }
  LogCollector<SlowEntry> *GetSlowLog() { return &slow_log_; }
  void SlowlogPushEntryIfNeeded(const std::vector<std::string> *args, uint64_t duration, const redis::Connection *conn,
                                bool is_profiling, std::optional<std::pair<std::string, std::string>> &perf_io_context,
                                int64_t prepare_duration = -1, int64_t command_queue_latency_on_connection = -1,
                                int64_t estimated_subkey_count = -1);

  // concurrent_unordered_map guarantee concurrent update, insert, iterate while erasing is not concurrent safe.
  // so never use erase in thread safe scenario.
  // see https://learn.microsoft.com/en-us/cpp/parallel/concrt/reference/concurrent-unordered-map-class?view=msvc-170
  tbb::concurrent_unordered_map<std::thread::id, uint32_t> number_of_worker_connections_;
  std::shared_ptr<engine::StorageManager> storage_mgr;
  std::unique_ptr<lua::ScriptManager> script_mgr;
  std::unique_ptr<redis::CursorLRUcache> scan_lru_cache;
  std::unique_ptr<redis::CursorLRUcache> xscan_lru_cache;
  std::unique_ptr<redis::Cluster> cluster;
  static inline std::atomic<int64_t> unix_time = 0;

  std::unique_ptr<ControllerApiClient> ctrl_rpc_client;
  std::unique_ptr<redis::SyncManager> sync_manager;
  std::unique_ptr<redis::Migration> migration;
  std::unique_ptr<util::TimeoutManager> timeout_mgr;

  std::unique_ptr<redis::CDCManager> cdc_manager;

  void UpdateWatchedKeysFromArgs(const std::vector<std::string> &args, const redis::CommandAttributes &attr);
  void UpdateWatchedKeysManually(const std::vector<std::string> &keys);
  void WatchKey(redis::Connection *conn, const std::vector<std::string> &keys);
  static bool IsWatchedKeysModified(redis::Connection *conn);
  void ResetWatchedKeys(redis::Connection *conn);

  redis::mgl::MGLockMgr *GetMGLockMgr() { return mgl_mgr_.get(); }

  std::string GetClusterId() const { return cluster->ClusterId(); };

  std::shared_ptr<redis::SlotRange> GetSlotRangeByIndex(const kv::controller::v1::SlotRangeIndex &index) const {
    return cluster->GetSlotRangeByIndex(index);
  };

  bool CheckClusterId(const std::string &expect) const { return GetClusterId() == expect; };

  bool CheckDataNodeId(const std::string &expect) const { return cluster->DatanodeId() == expect; };

  // grpc service interface
  grpc::ServerUnaryReactor *GetSyncPoint(grpc::CallbackServerContext *, const kv::datanode::v1::GetSyncPointRequest *,
                                         kv::datanode::v1::GetSyncPointResponse *) override;
  grpc::ServerWriteReactor<kv::datanode::v1::PullSyncDataResponse> *PullSyncData(
      grpc::CallbackServerContext *, const kv::datanode::v1::PullSyncDataRequest *) override;
  grpc::ServerReadReactor<kv::datanode::v1::PushSyncDataRequest> *PushSyncData(
      grpc::CallbackServerContext *, kv::datanode::v1::PushSyncDataResponse *) override;
  grpc::ServerUnaryReactor *ReportSyncError(grpc::CallbackServerContext *,
                                            const kv::datanode::v1::ReportSyncErrorRequest *,
                                            kv::datanode::v1::ReportSyncErrorResponse *) override;
  grpc::ServerUnaryReactor *GetSyncConfig(grpc::CallbackServerContext *,
                                          const ::kv::datanode::v1::GetSyncConfigRequest *,
                                          kv::datanode::v1::GetSyncConfigResponse *) override;
  grpc::ServerUnaryReactor *UpdateSyncConfig(grpc::CallbackServerContext *,
                                             const kv::datanode::v1::UpdateSyncConfigRequest *,
                                             kv::datanode::v1::UpdateSyncConfigResponse *) override;
  grpc::ServerBidiReactor<kv::datanode::v1::SyncDataRequest, kv::datanode::v1::SyncDataResponse> *SyncData(
      grpc::CallbackServerContext *) override;
  grpc::ServerUnaryReactor *Failover(grpc::CallbackServerContext *, const kv::datanode::v1::FailoverRequest *,
                                     kv::datanode::v1::FailoverResponse *) override;
  grpc::ServerUnaryReactor *Migrate(grpc::CallbackServerContext *, const kv::datanode::v1::MigrateRequest *,
                                    kv::datanode::v1::MigrateResponse *) override;

  // standalone mode, data recovery
  grpc::ServerUnaryReactor *GetLatestPoint(grpc::CallbackServerContext *,
                                           const kv::datanode::v1::GetLatestPointRequest *,
                                           kv::datanode::v1::GetLatestPointResponse *) override;
  grpc::ServerUnaryReactor *GetDataWithCmd(grpc::CallbackServerContext *,
                                           const kv::datanode::v1::GetDataWithCmdRequest *,
                                           kv::datanode::v1::GetDataWithCmdResponse *) override;
  grpc::ServerUnaryReactor *Ingest(grpc::CallbackServerContext *, const kv::datanode::v1::IngestRequest *,
                                   kv::datanode::v1::IngestResponse *) override;
  grpc::ServerUnaryReactor *GetIngestInfo(grpc::CallbackServerContext *, const kv::datanode::v1::GetIngestInfoRequest *,
                                          kv::datanode::v1::GetIngestInfoResponse *) override;

  grpc::ServerUnaryReactor *StopDatanodeDts(grpc::CallbackServerContext *,
                                            const kv::datanode::v1::StopDatanodeDtsRequest *,
                                            kv::datanode::v1::StopDatanodeDtsResponse *) override;

  grpc::ServerUnaryReactor *StartDatanodeDts(grpc::CallbackServerContext *,
                                             const kv::datanode::v1::StartDatanodeDtsRequest *,
                                             kv::datanode::v1::StartDatanodeDtsResponse *) override;
  // cdc service
  grpc::ServerUnaryReactor *CDCGetLatestPoint(grpc::CallbackServerContext *,
                                              const kv::datanode::v1::CDCGetLatestPointRequest *,
                                              kv::datanode::v1::CDCGetLatestPointResponse *) override;
  grpc::ServerWriteReactor<kv::datanode::v1::CDCGetEventsResponse> *CDCGetEvents(
      grpc::CallbackServerContext *, const kv::datanode::v1::CDCGetEventsRequest *) override;
  grpc::ServerUnaryReactor *CDCGetRestartPoint(grpc::CallbackServerContext *,
                                               const kv::datanode::v1::CDCGetRestartPointRequest *,
                                               kv::datanode::v1::CDCGetRestartPointResponse *) override;
  grpc::ServerUnaryReactor *CDCGetOldestPoint(grpc::CallbackServerContext *,
                                              const kv::datanode::v1::CDCGetOldestPointRequest *,
                                              kv::datanode::v1::CDCGetOldestPointResponse *) override;

  grpc::ServerUnaryReactor *MonitorCmd(grpc::CallbackServerContext *, const kv::datanode::v1::MonitorCmdRequest *,
                                       kv::datanode::v1::MonitorCmdResponse *) override;
#ifdef ENABLE_OPENSSL
  UniqueSSLContext ssl_ctx;
#endif

  Status SetDBOption(const std::string &key, const std::string &value);
  Status SetDBOptionForAllColumnFamilies(const std::string &key, const std::string &value);
  Status SetDBOptionForColumnFamily(const std::string &db_name, const std::string &cf_name, const std::string &key,
                                    const std::string &value);
  Status SubCompactJob(const std::string &begin_key, std::string &end_key, const std::string &name, bool is_compact,
                       bool with_filter = true);
  Status SubCompactLegacyJob() const;
  void GetAllSlotRangeName(std::string *output) const;
  std::chrono::nanoseconds GetWorkersBlockedDuration() const;

 private:
  friend class MockServer;

  void cron();
  void recordInstantaneousMetrics();
  static void updateCachedTime();
  Status autoResizeBlockAndSST();
  void updateWatchedKeysFromRange(const std::vector<std::string> &args, const redis::CommandKeyRange &range);
  void updateAllWatchedKeys();
  void increaseWorkerThreads(size_t delta);
  void decreaseWorkerThreads(size_t delta);
  void cleanupExitedWorkerThreads(bool force);

  std::atomic<bool> stop_ = false;
  std::atomic<bool> is_loading_ = false;
  int64_t start_time_;
  Config *config_ = nullptr;
  std::string last_random_key_cursor_;
  std::mutex last_random_key_cursor_mu_;

  redis::Connection *curr_connection_ = nullptr;

  // client counters
  std::atomic<uint64_t> client_id_{1};
  std::atomic<int> connected_clients_{0};
  std::atomic<int> monitor_clients_{0};
  std::atomic<uint64_t> total_clients_{0};

  // slave
  std::atomic<int> fetch_file_threads_num_ = 0;

  std::map<std::string, DBScanInfo> db_scan_infos_;

  LogCollector<SlowEntry> slow_log_;
  LogCollector<PerfEntry> perf_log_;

  std::map<std::string, std::list<ConnContext>> pubsub_channels_;
  std::map<std::string, std::list<ConnContext>> pubsub_patterns_;
  std::mutex pubsub_channels_mu_;
  std::map<std::string, std::list<ConnContext>> blocking_keys_;
  std::mutex blocking_keys_mu_;

  std::atomic<int> blocked_clients_{0};

  std::mutex blocked_stream_consumers_mu_;
  std::map<std::string, std::set<std::shared_ptr<StreamConsumer>>> blocked_stream_consumers_;

  // threads
  std::thread cron_thread_;
  TaskRunner task_runner_;
  std::vector<std::unique_ptr<WorkerThread>> worker_threads_;
  tbb::concurrent_queue<std::unique_ptr<WorkerThread>> recycle_worker_threads_;

  // memory
  std::atomic<int64_t> memory_startup_use_ = 0;

  // transaction
  std::atomic<size_t> watched_key_size_ = 0;
  std::map<std::string, std::set<redis::Connection *>> watched_key_map_;
  std::shared_mutex watched_key_mutex_;

  // SCAN ring buffer
  // std::atomic<uint16_t> cursor_counter_ = {0};
  // using CursorDictType = std::array<CursorDictElement, CURSOR_DICT_SIZE>;
  // std::unique_ptr<CursorDictType> cursor_dict_;

  std::unique_ptr<redis::mgl::MGLockMgr> mgl_mgr_;

  std::unique_ptr<grpc::Server> grpc_server_;
};
