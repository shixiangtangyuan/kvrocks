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

#include <event2/bufferevent.h>
#include <gtest/gtest.h>
#include <kv/datanode/v1/cdc.pb.h>
#include <kv/datanode/v1/sync.pb.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/backup_engine.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/string_util.h"
#include "common/task_runner.h"
#include "common/thread_util.h"
#include "common/timeout_manager.h"
#include "config/config.h"
#include "ingest/ingest.h"
#include "ingest/ingest_stats.h"
#include "observer_or_unique.h"
#include "rocksdb/convenience.h"
#include "stats/stats.h"
#include "status.h"
#include "warmup/orchestrator/warmup_orchestrator.h"

const int kReplIdLength = 16;

enum ColumnFamilyID {
  kColumnFamilyIDDefault,
  kColumnFamilyIDMetadata,
  kColumnFamilyIDZSetScore,
  kColumnFamilyIDPubSub,
  kColumnFamilyIDPropagate,
  kColumnFamilyIDStream,
};

namespace redis {
class Cluster;
class StorageTestHelper;
}  // namespace redis

namespace ingest {
class Ingester;
}
namespace engine {

extern const int64_t kIORateLimitMaxMb;
constexpr const char *kPubSubColumnFamilyName = "pubsub";
constexpr const char *kZSetScoreColumnFamilyName = "zset_score";
constexpr const char *kMetadataColumnFamilyName = "metadata";
constexpr const char *kSubkeyColumnFamilyName = "default";
constexpr const char *kPropagateColumnFamilyName = "propagate";
constexpr const char *kStreamColumnFamilyName = "stream";

constexpr const char *kPropagateScriptCommand = "script";

constexpr const char *kLuaFuncSHAPrefix = "lua_f_";
constexpr const char *kLuaFuncLibPrefix = "lua_func_lib_";
constexpr const char *kLuaLibCodePrefix = "lua_lib_code_";

struct CompressionOption {
  rocksdb::CompressionType type;
  const std::string name;
  const std::string val;
};

inline const std::vector<CompressionOption> CompressionOptions = {
    {rocksdb::kNoCompression, "no", "kNoCompression"},
    {rocksdb::kSnappyCompression, "snappy", "kSnappyCompression"},
    {rocksdb::kZlibCompression, "zlib", "kZlibCompression"},
    {rocksdb::kLZ4Compression, "lz4", "kLZ4Compression"},
    {rocksdb::kZSTD, "zstd", "kZSTD"},
};

class StorageManager;

class Storage : public std::enable_shared_from_this<Storage> {
 public:
  explicit Storage(Config *config);
  ~Storage();

  void SetWriteOptions(const Config::RocksDB::WriteOptions &config);
  Status Open(const std::string &dir, bool read_only = false, std::shared_ptr<rocksdb::Cache> block_cache = nullptr,
              std::shared_ptr<rocksdb::RateLimiter> rate_limiter = nullptr);
  void CloseDB();
  rocksdb::BlockBasedTableOptions InitTableOptions();
  void SetBlobDB(rocksdb::ColumnFamilyOptions *cf_options);
  rocksdb::Options InitRocksDBOptions();
  Status SetOptionForAllColumnFamilies(const std::string &key, const std::string &value);
  Status SetOptionForColumnFamily(const std::string &cf_name, const std::string &key, const std::string &value);
  Status SetOption(const std::string &key, const std::string &value);
  Status SetDBOption(const std::string &key, const std::string &value);
  Status CreateColumnFamilies(const rocksdb::Options &options, const std::string &db_dir);
  Status GetWALIter(rocksdb::SequenceNumber seq, std::unique_ptr<rocksdb::TransactionLogIterator> *iter);
  // Return update count and data size when apply wb success
  StatusOr<std::pair<size_t, size_t>> ReplicaApplyWriteBatch(const rocksdb::WriteOptions &,
                                                             const std::string &raw_batch);
  rocksdb::SequenceNumber LatestSeqNumber();

  // The sequence_number will be pointed to the value of the sequence number in range of DB,
  // but can't promise it's the latest sequence number. So you must check it by yourself before_begin
  // using it
  Status CreateBackup(uint64_t *sequence_number = nullptr);
  [[nodiscard]] rocksdb::Status Get(const rocksdb::ReadOptions &options, const rocksdb::Slice &key, std::string *value);
  [[nodiscard]] rocksdb::Status Get(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family,
                                    const rocksdb::Slice &key, std::string *value);
  [[nodiscard]] rocksdb::Status Get(const rocksdb::ReadOptions &options, const rocksdb::Slice &key,
                                    rocksdb::PinnableSlice *value);
  [[nodiscard]] rocksdb::Status Get(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family,
                                    const rocksdb::Slice &key, rocksdb::PinnableSlice *value);
  void MultiGet(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family, size_t num_keys,
                const rocksdb::Slice *keys, rocksdb::PinnableSlice *values, rocksdb::Status *statuses);
  rocksdb::Iterator *NewIterator(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family);
  rocksdb::Iterator *NewIterator(const rocksdb::ReadOptions &options);

  [[nodiscard]] rocksdb::Status Write(const rocksdb::WriteOptions &options, rocksdb::WriteBatch *updates);
  const rocksdb::WriteOptions &DefaultWriteOptions() { return write_opts_; }
  const rocksdb::WriteOptions &DefaultWriteOptionsForPuller() { return write_opts_for_puller_; }
  const rocksdb::WriteOptions &DefaultWriteOptionsForReceiver() { return write_opts_for_receiver_; }
  rocksdb::ReadOptions DefaultScanOptions() const;
  rocksdb::ReadOptions DefaultMultiGetOptions() const;
  [[nodiscard]] rocksdb::Status Delete(const rocksdb::WriteOptions &options, rocksdb::ColumnFamilyHandle *cf_handle,
                                       const rocksdb::Slice &key);
  [[nodiscard]] rocksdb::Status DeleteRange(const std::string &first_key, const std::string &last_key,
                                            const std::string &cf_name = kMetadataColumnFamilyName);
  [[nodiscard]] rocksdb::Status FlushScripts(const rocksdb::WriteOptions &options,
                                             rocksdb::ColumnFamilyHandle *cf_handle);
  bool WALHasNewData(rocksdb::SequenceNumber seq) { return seq <= LatestSeqNumber(); }
  Status InWALBoundary(rocksdb::SequenceNumber seq);
  Status WriteToPropagateCF(const std::string &key, const std::string &value);

  [[nodiscard]] rocksdb::Status Compact(rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice *begin,
                                        const rocksdb::Slice *end);
  rocksdb::DB *GetDB();
  bool IsClosing() const { return db_closing_; }
  std::string GetName() const { return config_->db_name; }
  rocksdb::ColumnFamilyHandle *GetCFHandle(const std::string &name);
  std::vector<rocksdb::ColumnFamilyHandle *> *GetCFHandles() { return &cf_handles_; }
  uint64_t GetTotalSize(const std::string &ns = kDefaultNamespace);
  void CheckDBSizeLimit();
  void SetIORateLimit(int64_t max_io_mb);

  std::shared_lock<std::shared_mutex> ReadLockGuard();
  std::unique_lock<std::shared_mutex> WriteLockGuard();

  uint64_t GetFlushCount() const { return flush_count_; }
  void IncrFlushCount(uint64_t n) { flush_count_.fetch_add(n); }
  uint64_t GetCompactionCount() const { return compaction_count_; }
  void IncrCompactionCount(uint64_t n) { compaction_count_.fetch_add(n); }
  bool IsSlotIdEncoded() const { return config_->slot_id_encoded; }
  Config *GetConfig() const { return config_; }

  Status BeginTxn();
  Status CommitTxn();
  ObserverOrUniquePtr<rocksdb::WriteBatchBase> GetWriteBatchBase();

  void Cron();
  Status AsyncBgSaveDB();
  std::string GetJobInfo(std::string &prefix);
  std::string GetCronInfo(std::string &prefix);

  Status AsyncCompactDB(const std::string &begin_key, const std::string &end_key, bool with_filter,
                        bool is_legacy = false);
  Status CancleCompactDB();

  Storage(const Storage &) = delete;
  Storage &operator=(const Storage &) = delete;

  // Full replication data files manager
  class ReplDataManager {
   public:
    // Master side
    static Status CleanInvalidFiles(Storage *storage, const std::string &dir, std::vector<std::string> valid_files);
    struct CheckpointInfo {
      std::atomic<time_t> create_time = 0;
      std::atomic<time_t> access_time = 0;
      uint64_t latest_seq = 0;
    };

    // Slave side
    struct MetaInfo {
      int64_t timestamp;
      rocksdb::SequenceNumber seq;
      std::string meta_data;
      // [[filename, checksum]...]
      std::vector<std::pair<std::string, uint32_t>> files;
    };
    static std::unique_ptr<rocksdb::WritableFile> NewTmpFile(Storage *storage, const std::string &dir,
                                                             const std::string &repl_file);
    static Status SwapTmpFile(Storage *storage, const std::string &dir, const std::string &repl_file);
    static bool FileExists(Storage *storage, const std::string &dir, const std::string &repl_file, uint32_t crc);
  };

  bool ExistCheckpoint();
  bool ExistSyncCheckpoint();
  time_t GetCheckpointCreateTime() const { return checkpoint_info_.create_time; }
  void SetCheckpointAccessTime(time_t t) { checkpoint_info_.access_time = t; }
  time_t GetCheckpointAccessTime() const { return checkpoint_info_.access_time; }
  void SetDBInRetryableIOError(bool yes_or_no) { db_in_retryable_io_error_ = yes_or_no; }
  bool IsDBInRetryableIOError() const { return db_in_retryable_io_error_; }

  Status ShiftReplId();
  StatusOr<kv::datanode::v1::CDCPoint> GetCDCPoint();
  StatusOr<kv::datanode::v1::CDCPoint> GetCDCPoint(rocksdb::SequenceNumber);
  const kv::datanode::v1::CDCPoint &GetCDCRestartPoint() { return cdc_restart_point_; }
  StatusOr<kv::datanode::v1::CDCPoint> GetCDCOldestPoint();
  StatusOr<kv::datanode::v1::SyncPoint> GetSyncPoint();
  StatusOr<kv::datanode::v1::SyncPoint> GetSyncPoint(rocksdb::SequenceNumber);
  StatusOr<std::string> GetReplIdFromWalBySeq(rocksdb::SequenceNumber seq);
  StatusOr<kv::datanode::v1::SyncPoint> GetSyncPointFromWalBySeq(rocksdb::SequenceNumber seq);
  StatusOr<std::string> GetReplIdFromDbEngine();
  void RecordInstantaneousDBMetrics();

  void SetDBId(uint64_t db_id) { db_id_ = db_id; }
  auto GetDBId() { return db_id_; }
  auto GetDBDir() { return db_dir_; };
  auto GetIngester() {
    std::lock_guard<std::mutex> lg(ingest_lock_);
    if (!ingester_) {
      ingester_ = std::make_shared<ingest::Ingester>(shared_from_this());
    }
    return ingester_;
  }

  StorageStats stats;
  Status StartTaskThreads(uint64_t id);
  void ForceStop() {
    {
      auto guard = WriteLockGuard();
      if (!db_) return;
      force_stop_ = true;
      task_stop_ = true;
      rocksdb::CancelAllBackgroundWork(db_.get(), true);
      task_runner_.Cancel();
      timeout_mgr_->Stop();
    }
    if (auto s = util::ThreadJoin(cron_thread_); !s) {
      LOG(WARNING) << "Cron thread operation failed: " << s.Msg();
    }
    if (auto s = task_runner_.Join(); !s) {
      LOG(WARNING) << "[storage] " << s.Msg();
    }
    CloseDB();
  }

  Status AsyncPurgeOldBackups(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours);
  void PurgeOldBackups(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours);

  void SetWriteStall(const std::string &cf_name, bool write_stall) { cfs_is_write_stall_[cf_name] = write_stall; }

  std::string GetWriteStall(const std::string &db_prefix);

  std::string GetOpsLatency(const std::string &db_prefix);

  std::string GetCompressionInfo(const std::string &prefix);

  void SetRocksdbStatsLevel(rocksdb::StatsLevel stats_level) { rocksdb_stats_->set_stats_level(stats_level); }

  void ResetRocksdbStats() { rocksdb_stats_->Reset(); }

  Status SyncWal() {
    std::shared_lock<std::shared_mutex> lk(db_rw_lock_);
    if (db_closing_ || !db_) {
      return {Status::NotOK, "db is closing"};
    }
    auto s = db_->SyncWAL();
    if (!s.ok()) {
      return {Status::NotOK, s.ToString()};
    }
    return Status::OK();
  }

 private:
  friend class StorageManager;
  friend class redis::Cluster;
  friend class redis::StorageTestHelper;
  FRIEND_TEST(StorageManager, simpletest);

  void setStorageManager(const std::shared_ptr<StorageManager> &storage_mgr) { storage_mgr_ = storage_mgr; }

  void setDBSizeLimitReached(bool limit_reached);

  void resetStorageManager() { storage_mgr_.reset(); }

  kv::datanode::v1::SyncPoint getSyncPointLocked();

  kv::datanode::v1::CDCPoint getCDCPointLocked();

  Status setDisableAutoCompactionsOption(bool disable_auto_compactions) {
    auto val = disable_auto_compactions ? "true" : "false";
    return SetOptionForAllColumnFamilies("disable_auto_compactions", val);
  }

  // reset disable_auto_compactions after datanode is serving
  Status resetDisableAutoCompactionsOption() {
    auto val = config_->rocks_db.disable_auto_compactions ? "true" : "false";
    return SetOptionForAllColumnFamilies("disable_auto_compactions", val);
  }

  std::weak_ptr<StorageManager> storage_mgr_;
  std::unique_ptr<rocksdb::DB> db_ = nullptr;
  std::string replid_;
  time_t backup_creating_time_;
  rocksdb::Env *env_;
  std::shared_ptr<rocksdb::SstFileManager> sst_file_manager_;
  std::shared_ptr<rocksdb::RateLimiter> rate_limiter_;
  ReplDataManager::CheckpointInfo checkpoint_info_;
  std::mutex checkpoint_mu_;
  Config *config_ = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
  bool db_size_limit_reached_ = false;
  std::atomic<uint64_t> flush_count_{0};
  std::atomic<uint64_t> compaction_count_{0};

  std::shared_mutex db_rw_lock_;
  bool db_closing_ = true;
  bool force_stop_ = false;

  std::string db_dir_;
  std::string checkpoint_dir_;
  std::string backup_dir_;

  // The system clock time when the backup was created.
  int64_t backup_creating_time_secs_;
  std::mutex backup_mu_;

  bool task_stop_{false};
  // NOTE(mingfo): taks_runner has only one thread. Compaction legacy depends on this.
  TaskRunner task_runner_;
  std::thread cron_thread_;
  std::string cron_thread_name_;

  std::unique_ptr<util::TimeoutManager> timeout_mgr_;

  std::mutex cron_compact_mu_;
  // compact range
  bool is_cron_compact_range_in_progress_ = false;
  uint64_t cron_compact_range_spend_ms_ = 0;
  uint64_t cron_compact_range_times_ = 0;
  uint64_t cron_compact_range_delete_tombs_ = 0;

  // compact full
  bool is_cron_compact_full_in_progress_ = false;
  int64_t last_cron_compact_full_timestamp_secs_ = -1;
  int64_t last_cron_compact_full_duration_secs_ = -1;
  uint64_t cron_compact_full_times_ = 0;
  // not all cf success
  uint64_t cron_compact_full_partial_success_ = 0;

  // Some jobs to operate DB should be unique
  std::mutex db_job_mu_;
  bool is_bgsave_in_progress_ = false;
  bool job_compacting_ = false;
  uint64_t job_compact_timstamp_secs_ = 0;
  uint64_t job_compact_duration_secs_ = 0;
  uint64_t last_bgsave_timestamp_secs_ = 0;
  std::string last_bgsave_status_ = "ok";
  int64_t last_bgsave_duration_secs_ = -1;
  uint64_t job_compact_times_ = 0;
  uint64_t bgsave_times_ = 0;

  std::atomic<bool> db_in_retryable_io_error_{false};

  std::atomic<bool> is_txn_mode_ = false;
  // txn_write_batch_ is used as the global write batch for the transaction mode,
  // all writes will be grouped in this write batch when entering the transaction mode,
  // then write it at once when committing.
  //
  // Notice: the reason why we can use the global transaction? because the EXEC is an exclusive
  // command, so it won't have multi transactions to be executed at the same time.
  std::unique_ptr<rocksdb::WriteBatchWithIndex> txn_write_batch_;

  rocksdb::WriteOptions write_opts_ = rocksdb::WriteOptions();
  rocksdb::WriteOptions write_opts_for_puller_ = rocksdb::WriteOptions();
  rocksdb::WriteOptions write_opts_for_receiver_ = rocksdb::WriteOptions();

  rocksdb::Status writeToDB(const rocksdb::WriteOptions &options, rocksdb::WriteBatch *updates);
  void checkAndRecordFailedWrite(const rocksdb::Status status);

  uint64_t db_id_{0};

  std::map<std::string, std::atomic_bool> cfs_is_write_stall_;
  std::shared_ptr<rocksdb::Statistics> rocksdb_stats_;
  std::mutex ingest_lock_;
  std::shared_ptr<ingest::Ingester> ingester_;
  kv::datanode::v1::CDCPoint cdc_restart_point_;
};

class StorageManager : public std::enable_shared_from_this<StorageManager> {
 public:
  StorageManager() = default;
  ~StorageManager() = default;
  StorageManager(const StorageManager &) = delete;
  StorageManager(StorageManager &&) = delete;
  StorageManager operator=(const StorageManager &) = delete;
  StorageManager operator=(StorageManager &&) = delete;

  Status CreateStorages(Config *config) {
    if (config->datadir_list.empty()) {
      return {Status::NotOK, "at least one volume should be given to create storages"};
    }

    initRocksDBComm(config);
    for (const auto &db_dir : config->datadir_list) {
      bool is_dir = false;
      auto res = rocksdb::Env::Default()->IsDirectory(db_dir, &is_dir);
      if (!is_dir) return {Status::NotOK, db_dir + " is not directory, please check it"};

      auto store = std::make_shared<Storage>(config);
      auto tokens = util::Split(db_dir, "/");
      uint64_t db_id = std::stoull(tokens.back());
      store->SetDBId(db_id);
      bool read_only = false;
      if (!config->cluster_enabled) {
        // NOTE(mingfo): Current wal ttl is 3h. if the data recovery operation exceeds 3h after Failover force,
        // wals will be cleared when standalone datanode startup. Use read_only mode to keep wals.
        read_only = true;
      }
      auto s = store->Open(db_dir, read_only, block_cache_, rate_limiter_);
      if (!s.IsOK()) {
        LOG(ERROR) << "Failed to open storage at: " << db_dir << " err: " << s.Msg();
        return {Status::NotOK, "failed to open storage"};
      }
      AddStorage(db_id, store);

      // Start warmup for this storage if enabled
      if (config->warmup_enabled) {
        StartWarmupForStorage(db_id);
      }
    }

    return Status::OK();
  }

  std::shared_ptr<Storage> GetStorageByDBID(const uint64_t &db_id) {
    std::shared_lock<std::shared_mutex> lk(mutex_);
    auto it = store_map_.find(db_id);
    return it == store_map_.end() ? nullptr : it->second;
  }

  void AddStorage(const uint64_t &db_id, std::shared_ptr<Storage> store) {
    std::unique_lock<std::shared_mutex> lk(mutex_);
    store_map_.emplace(db_id, store);
    store->setStorageManager(shared_from_this());
    // TODO(ying.qiu): check db size reach limit or not when start the datanode.
    // now we skip it to escape from the case that store reach limit and switch
    // to stop-write model but we need write to recovery from it during start,
    // e.g. apply topo, compact after delete range.
  }

  void RemoveStorage(const uint64_t &db_id) {
    std::unique_lock<std::shared_mutex> lk(mutex_);
    if (auto it = store_map_.find(db_id); it != store_map_.end()) {
      it->second->resetStorageManager();
      store_map_.erase(it);
      checkDBSizeLimitLocked();
    }
  }

  auto GetAllStorage() {
    std::unique_lock<std::shared_mutex> lk(mutex_);
    return store_map_;
  }

  void CheckDBSizeLimit() {
    std::shared_lock<std::shared_mutex> lk(mutex_);
    checkDBSizeLimitLocked();
  }

  Status SetBlockCacheSize(int block_cache_size_mb);

  void SetIORateLimit(int max_io_mb);

  void SetStatsLevel(rocksdb::StatsLevel stats_level) {
    std::shared_lock<std::shared_mutex> lk(mutex_);
    for (auto &[_, storage] : store_map_) {
      storage->SetRocksdbStatsLevel(stats_level);
    }
  }

  void ResetStats() {
    std::shared_lock<std::shared_mutex> lk(mutex_);
    for (auto &[_, storage] : store_map_) {
      storage->ResetRocksdbStats();
    }
  }

  Status GetLatestPoints(const std::vector<uint64_t> &db_ids, std::vector<kv::datanode::v1::LatestPoint> *result);
  Status GetWalDataWithCmd(uint64_t db_id, uint64_t *next_seq, std::vector<std::vector<std::string>> *result,
                           bool *is_finished);

  // Warmup management
  void StartWarmupForStorage(uint64_t db_id);
  void StopWarmupForStorage(uint64_t db_id);

  // === 新增：统一的 orchestrator 管理与互斥入口 ===
  // 启动（仅当 Idle），返回是否成功提交
  bool StartWarmupIfIdle(uint64_t db_id, const std::string &mode, double threshold);
  // 查询
  bool IsWarmupRunning(uint64_t db_id);
  std::shared_ptr<warmup::WarmupOrchestrator> GetWarmupOrchestrator(uint64_t db_id);

 private:
  FRIEND_TEST(StorageManager, simpletest);

  Config *config_ = nullptr;
  std::shared_ptr<rocksdb::Cache> block_cache_ = nullptr;
  std::shared_ptr<rocksdb::RateLimiter> rate_limiter_ = nullptr;
  mutable std::shared_mutex mutex_;
  std::unordered_map<uint64_t, std::shared_ptr<Storage>> store_map_;

  // Warmup orchestrators for each storage
  mutable std::shared_mutex warmup_mutex_;
  std::unordered_map<uint64_t, std::shared_ptr<warmup::WarmupOrchestrator>> warmup_orchestrators_;

  void initRocksDBComm(Config *config);

  void checkDBSizeLimitLocked() {
    bool limit_reached = false;
    if (auto max_db_size = config_->max_db_size; max_db_size > 0) {
      uint64_t used_db_size = 0;
      for (auto &[_, storage] : store_map_) {
        used_db_size += storage->GetTotalSize();
      }
      limit_reached = used_db_size >= max_db_size * GiB;
    }
    for (auto &[_, storage] : store_map_) {
      storage->setDBSizeLimitReached(limit_reached);
    }
  }
};

}  // namespace engine
