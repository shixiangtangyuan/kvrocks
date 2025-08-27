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

#include "storage.h"

#include <event2/buffer.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <rocksdb/advanced_cache.h>
#include <rocksdb/convenience.h>
#include <rocksdb/env.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/sst_file_manager.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/utilities/table_properties_collectors.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "batch_extractor.h"
#include "common/status.h"
#include "compact_filter.h"
#include "compaction_checker.h"
#include "db_util.h"
#include "event_listener.h"
#include "event_util.h"
#include "redis_db.h"
#include "redis_metadata.h"
#include "rocksdb/statistics.h"
#include "rocksdb_crc32c.h"
#include "server/server.h"
#include "stats/stats.h"
#include "table_properties_collector.h"
#include "time_util.h"
#include "unique_fd.h"

namespace engine {

constexpr const char *kReplicationIdKey = "replication_id_";

const int64_t kIORateLimitMaxMb = 1024;

using rocksdb::Slice;

static std::shared_ptr<rocksdb::Cache> BuildBlockCache(int block_cache_size_mb) {
  return rocksdb::NewLRUCache(block_cache_size_mb * MiB, -1, false, 0.75);
}

static std::shared_ptr<rocksdb::RateLimiter> BuildRateLimiter(int max_io_mb, bool auto_tuned) {
  if (max_io_mb <= 0) max_io_mb = kIORateLimitMaxMb;
  return std::shared_ptr<rocksdb::RateLimiter>(
      rocksdb::NewGenericRateLimiter(max_io_mb * static_cast<int64_t>(MiB), 100 * 1000, /* default */
                                     10,                                                /* default */
                                     rocksdb::RateLimiter::Mode::kWritesOnly, auto_tuned));
}

Storage::Storage(Config *config)
    : backup_creating_time_(util::GetTimeStamp()), env_(rocksdb::Env::Default()), config_(config) {
  Metadata::InitVersionCounter();
  SetWriteOptions(config->rocks_db.write_options);
  std::vector<std::string> cf_names = {kSubkeyColumnFamilyName, kMetadataColumnFamilyName,  kZSetScoreColumnFamilyName,
                                       kPubSubColumnFamilyName, kPropagateColumnFamilyName, kStreamColumnFamilyName};

  for (auto &cf_name : cf_names) {
    cfs_is_write_stall_.emplace(cf_name, false);
  }

  timeout_mgr_ = std::make_unique<util::TimeoutManager>();
  timeout_mgr_->Run();
}

Storage::~Storage() {
  if (!force_stop_) {
    {
      auto guard = WriteLockGuard();
      if (!db_) return;
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
}

void Storage::CloseDB() {
  auto guard = WriteLockGuard();
  db_closing_ = true;
  db_->SyncWAL();
  // rocksdb::CancelAllBackgroundWork(db_.get(), true);
  for (auto handle : cf_handles_) db_->DestroyColumnFamilyHandle(handle);
  db_ = nullptr;
}

Status Storage::StartTaskThreads(uint64_t id) {
  if (auto s = task_runner_.Start(); !s) {
    LOG(ERROR) << "Failed to start task runner: " << s.Msg();
    return {Status::NotOK, s.Msg()};
  }

  cron_thread_name_ = "store-cron-" + std::to_string(id);
  cron_thread_ = GET_OR_RET(util::CreateThread(cron_thread_name_.c_str(), [this] { this->Cron(); }));

  return Status::OK();
}

void Storage::SetWriteOptions(const Config::RocksDB::WriteOptions &config) {
  write_opts_.sync = config.sync;
  write_opts_.disableWAL = config.disable_wal;
  write_opts_.no_slowdown = config.no_slowdown;
  write_opts_.low_pri = config.low_pri;
  write_opts_.memtable_insert_hint_per_batch = config.memtable_insert_hint_per_batch;
  // write options for sync puller
  write_opts_for_puller_ = write_opts_;
  write_opts_for_puller_.sync = false;
  // write options for sync receiver
  write_opts_for_receiver_ = write_opts_;
  write_opts_for_receiver_.sync = config.sync_for_receiver;
}

rocksdb::ReadOptions Storage::DefaultScanOptions() const {
  rocksdb::ReadOptions read_options;
  read_options.fill_cache = config_->rocks_db.read_options.fill_cache;
  read_options.async_io = config_->rocks_db.read_options.async_io;

  return read_options;
}

rocksdb::ReadOptions Storage::DefaultMultiGetOptions() const {
  rocksdb::ReadOptions read_options;
  read_options.async_io = config_->rocks_db.read_options.async_io;

  return read_options;
}

rocksdb::BlockBasedTableOptions Storage::InitTableOptions() {
  rocksdb::BlockBasedTableOptions table_options;
  table_options.format_version = 5;
  table_options.index_type = rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  table_options.partition_filters = true;
  table_options.optimize_filters_for_memory = true;
  table_options.metadata_block_size = 4096;
  table_options.data_block_index_type = rocksdb::BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinaryAndHash;
  table_options.data_block_hash_table_util_ratio = 0.75;
  table_options.block_size = static_cast<size_t>(config_->rocks_db.block_size);
  return table_options;
}

void Storage::SetBlobDB(rocksdb::ColumnFamilyOptions *cf_options) {
  cf_options->enable_blob_files = config_->rocks_db.enable_blob_files;
  cf_options->min_blob_size = config_->rocks_db.min_blob_size;
  cf_options->blob_file_size = config_->rocks_db.blob_file_size;
  cf_options->blob_compression_type = config_->rocks_db.blob_compression_type;
  cf_options->enable_blob_garbage_collection = config_->rocks_db.enable_blob_garbage_collection;
  // Use 100.0 to force converting blob_garbage_collection_age_cutoff to double
  cf_options->blob_garbage_collection_age_cutoff = config_->rocks_db.blob_garbage_collection_age_cutoff / 100.0;
  cf_options->blob_garbage_collection_force_threshold =
      config_->rocks_db.blob_garbage_collection_force_threshold / 100.0;
}

rocksdb::Options Storage::InitRocksDBOptions() {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  // options.IncreaseParallelism(2);
  // NOTE: the overhead of statistics is 5%-10%, so it should be configurable in prod env
  // See: https://github.com/facebook/rocksdb/wiki/Statistics
  rocksdb_stats_ = rocksdb::CreateDBStatistics();
  rocksdb_stats_->set_stats_level(config_->rocks_db.stats_level);
  options.statistics = rocksdb_stats_;
  options.stats_dump_period_sec = config_->rocks_db.stats_dump_period_sec;
  options.max_open_files = config_->rocks_db.max_open_files;
  options.compaction_style = rocksdb::CompactionStyle::kCompactionStyleLevel;
  options.max_subcompactions = static_cast<uint32_t>(config_->rocks_db.max_sub_compactions);
  options.max_write_buffer_number = config_->rocks_db.max_write_buffer_number;
  options.min_write_buffer_number_to_merge = kMinWriteBufferNumberToMerge;
  options.write_buffer_size = config_->rocks_db.write_buffer_size * MiB;
  options.num_levels = 7;
  options.compression_per_level.resize(options.num_levels);
  // only compress levels >= 2
  for (int i = 0; i < options.num_levels; ++i) {
    if (i < 2) {
      options.compression_per_level[i] = rocksdb::CompressionType::kNoCompression;
    } else {
      options.compression_per_level[i] = config_->rocks_db.compression;
    }
  }
  options.enable_pipelined_write = config_->rocks_db.enable_pipelined_write;
  options.target_file_size_base = config_->rocks_db.target_file_size_base * MiB;
  options.max_manifest_file_size = 64 * MiB;
  options.max_log_file_size = 256 * MiB;
  options.keep_log_file_num = 12;
  // Version 1.18.0 introduces stricter synchronization checkpoint validation
  // Failure scenario:
  // - When no writes occur (exceeding WAL_ttl_seconds) before upgrade:
  //   1. Importing datanode startup triggers WAL deletion (sypoint ts is 0)
  //   2. Serving datanod  WAL(delay delete, sypoint ts > 0)  causes checkpoint mismatch
  // Resolution:
  // Set WAL_ttl_seconds = 0 to keep at least one WAL to make ts match
  // ensuring successful checkpoint validation
  options.WAL_ttl_seconds = 0;
  options.WAL_size_limit_MB = static_cast<uint64_t>(config_->rocks_db.wal_size_limit_mb);
  options.wal_compression =
      (config_->rocks_db.wal_compression ? rocksdb::CompressionType::kZSTD : rocksdb::CompressionType::kNoCompression);
  options.max_total_wal_size = static_cast<uint64_t>(config_->rocks_db.max_total_wal_size * MiB);
  options.listeners.emplace_back(new EventListener(this));
  options.dump_malloc_stats = true;
  sst_file_manager_ = std::shared_ptr<rocksdb::SstFileManager>(rocksdb::NewSstFileManager(rocksdb::Env::Default()));
  options.sst_file_manager = sst_file_manager_;
  options.delayed_write_rate = static_cast<uint64_t>(config_->rocks_db.delayed_write_rate);
  options.compaction_readahead_size = static_cast<size_t>(config_->rocks_db.compaction_readahead_size);
  options.level0_slowdown_writes_trigger = config_->rocks_db.level0_slowdown_writes_trigger;
  options.level0_stop_writes_trigger = config_->rocks_db.level0_stop_writes_trigger;
  options.level0_file_num_compaction_trigger = config_->rocks_db.level0_file_num_compaction_trigger;
  options.max_bytes_for_level_base = config_->rocks_db.max_bytes_for_level_base;
  options.max_bytes_for_level_multiplier = config_->rocks_db.max_bytes_for_level_multiplier;
  options.level_compaction_dynamic_level_bytes = config_->rocks_db.level_compaction_dynamic_level_bytes;
  options.max_background_jobs = config_->rocks_db.max_background_jobs;
  options.target_file_size_multiplier = config_->rocks_db.target_file_size_multiplier;
  options.arena_block_size = config_->rocks_db.arena_block_size;
  options.soft_pending_compaction_bytes_limit = config_->rocks_db.soft_pending_compaction_bytes_limit;
  options.hard_pending_compaction_bytes_limit = config_->rocks_db.hard_pending_compaction_bytes_limit;
  options.max_compaction_bytes = config_->rocks_db.max_compaction_bytes;
  options.max_sequential_skip_in_iterations = config_->rocks_db.max_sequential_skip_in_iterations;
  options.paranoid_file_checks = config_->rocks_db.paranoid_file_checks;
  options.report_bg_io_stats = config_->rocks_db.report_bg_io_stats;
  options.sample_for_compression = config_->rocks_db.sample_for_compression;
  options.periodic_compaction_seconds = config_->rocks_db.periodic_compaction_seconds;
  options.ttl = config_->rocks_db.ttl;
  options.bottommost_file_compaction_delay = config_->rocks_db.bottommost_file_compaction_delay;
  options.delete_obsolete_files_period_micros = config_->rocks_db.delete_obsolete_files_period_micros;
  options.writable_file_max_buffer_size = config_->rocks_db.writable_file_max_buffer_size;
  options.bytes_per_sync = config_->rocks_db.bytes_per_sync;
  options.wal_bytes_per_sync = config_->rocks_db.wal_bytes_per_sync;
  options.avoid_flush_during_shutdown = config_->rocks_db.avoid_flush_during_shutdown;
  options.stats_persist_period_sec = config_->rocks_db.stats_persist_period_sec;
  options.stats_history_buffer_size = config_->rocks_db.stats_history_buffer_size;

  // avoid blocking io on iteration
  // see https://github.com/facebook/rocksdb/wiki/IO#avoid-blocking-io
  options.avoid_unnecessary_blocking_io = config_->rocks_db.avoid_unnecessary_blocking_io;
  return options;
}

Status Storage::SetOptionForAllColumnFamilies(const std::string &key, const std::string &value) {
  for (auto &cf_handle : cf_handles_) {
    auto s = db_->SetOptions(cf_handle, {{key, value}});
    if (!s.ok()) return {Status::NotOK, s.ToString()};
  }
  return Status::OK();
}

Status Storage::SetOptionForColumnFamily(const std::string &cf_name, const std::string &key, const std::string &value) {
  auto cf_handle = GetCFHandle(cf_name);
  if (!cf_handle) {
    return {Status::NotOK, "Column family not found: " + cf_name};
  }

  auto s = db_->SetOptions(cf_handle, {{key, value}});
  if (!s.ok()) return {Status::NotOK, s.ToString()};
  return Status::OK();
}

Status Storage::SetOption(const std::string &key, const std::string &value) {
  auto s = db_->SetOptions({{key, value}});
  if (!s.ok()) return {Status::NotOK, s.ToString()};
  return Status::OK();
}

Status Storage::SetDBOption(const std::string &key, const std::string &value) {
  auto s = db_->SetDBOptions({{key, value}});
  if (!s.ok()) return {Status::NotOK, s.ToString()};
  return Status::OK();
  void CancelTask();
}

Status Storage::CreateColumnFamilies(const rocksdb::Options &options, const std::string &db_dir) {
  rocksdb::ColumnFamilyOptions cf_options(options);
  auto res = util::DBOpen(options, db_dir);
  if (res) {
    std::vector<std::string> cf_names = {kMetadataColumnFamilyName, kZSetScoreColumnFamilyName, kPubSubColumnFamilyName,
                                         kPropagateColumnFamilyName, kStreamColumnFamilyName};
    std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
    auto s = (*res)->CreateColumnFamilies(cf_options, cf_names, &cf_handles);
    if (!s.ok()) {
      return {Status::DBOpenErr, s.ToString()};
    }

    for (auto handle : cf_handles) (*res)->DestroyColumnFamilyHandle(handle);
    (*res)->Close();
  } else {
    // We try to create column families by opening the database without column families.
    // If it's ok means we didn't create column families (cannot open without column families if created).
    // When goes wrong, we need to check whether it's caused by column families NOT being opened or not.
    // If the status message contains `Column families not opened` means that we have created the column
    // families, let's ignore the error.
    const char *not_opened_prefix = "Column families not opened";
    if (res.Msg().find(not_opened_prefix) != std::string::npos) {
      return Status::OK();
    }

    return res;
  }

  return Status::OK();
}

Status Storage::Open(const std::string &db_dir, bool read_only, std::shared_ptr<rocksdb::Cache> block_cache,
                     std::shared_ptr<rocksdb::RateLimiter> rate_limiter) {
  auto guard = WriteLockGuard();
  db_closing_ = false;
  db_dir_ = db_dir;
  backup_dir_ = db_dir + "/backup";
  checkpoint_dir_ = db_dir + "/checkpoint";

  rocksdb::Options options = InitRocksDBOptions();
  std::filesystem::path db_dir_path(db_dir);
  std::filesystem::path log_dir(config_->log_dir);

  if (std::filesystem::exists(log_dir) && std::filesystem::is_directory(log_dir)) {
    options.db_log_dir = log_dir / "dbs" / db_dir_path.filename();
    std::filesystem::create_directories(options.db_log_dir);
  }

  if (!read_only) {
    if (auto s = CreateColumnFamilies(options, db_dir); !s.IsOK()) {
      return s.Prefixed("failed to create column families");
    }
  }

  if (!block_cache) {
    LOG(WARNING) << "Use unshared block cache for db_id " << db_id_;
    block_cache = BuildBlockCache(config_->rocks_db.block_cache_size);
  }
  if (!rate_limiter) {
    LOG(WARNING) << "Use unshared io rate limiter for db_id " << db_id_;
    rate_limiter = BuildRateLimiter(config_->max_io_mb, config_->rocks_db.rate_limiter_auto_tuned);
  }
  if (config_->rocks_db.enable_row_cache) {
    options.row_cache = block_cache;
  }
  options.rate_limiter = rate_limiter;
  rate_limiter_ = rate_limiter;

  bool cache_index_and_filter_blocks = config_->rocks_db.cache_index_and_filter_blocks;
  bool disable_auto_compactions =
      config_->disable_auto_compactions_before_serving ? true : config_->rocks_db.disable_auto_compactions;
  if (disable_auto_compactions && !config_->rocks_db.disable_auto_compactions) {
    LOG(INFO) << "Disable auto compactions before servinig, db id:" << db_id_;
  }
  rocksdb::BlockBasedTableOptions metadata_table_opts = InitTableOptions();
  metadata_table_opts.block_cache = block_cache;
  metadata_table_opts.pin_l0_filter_and_index_blocks_in_cache = true;
  metadata_table_opts.cache_index_and_filter_blocks = cache_index_and_filter_blocks;
  metadata_table_opts.cache_index_and_filter_blocks_with_high_priority = true;

  rocksdb::ColumnFamilyOptions metadata_opts(options);
  if (config_->rocks_db.enable_blob_cache) {
    metadata_opts.blob_cache = block_cache;
  }
  metadata_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(metadata_table_opts));
  metadata_opts.compaction_filter_factory = std::make_shared<MetadataFilterFactory>(this);
  metadata_opts.disable_auto_compactions = disable_auto_compactions;
  // Enable whole key bloom filter in memtable
  metadata_opts.memtable_whole_key_filtering = true;
  metadata_opts.memtable_prefix_bloom_size_ratio = 0.1;
  metadata_opts.table_properties_collector_factories.emplace_back(
      NewCompactOnExpiredTableCollectorFactory(kMetadataColumnFamilyName, 0.3));
  SetBlobDB(&metadata_opts);

  rocksdb::BlockBasedTableOptions subkey_table_opts = InitTableOptions();
  subkey_table_opts.block_cache = block_cache;
  subkey_table_opts.pin_l0_filter_and_index_blocks_in_cache = true;
  subkey_table_opts.cache_index_and_filter_blocks = cache_index_and_filter_blocks;
  subkey_table_opts.cache_index_and_filter_blocks_with_high_priority = true;
  rocksdb::ColumnFamilyOptions subkey_opts(options);
  if (config_->rocks_db.enable_blob_cache) {
    subkey_opts.blob_cache = block_cache;
  }
  subkey_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(subkey_table_opts));
  subkey_opts.compaction_filter_factory = std::make_shared<SubKeyFilterFactory>(this);
  subkey_opts.disable_auto_compactions = disable_auto_compactions;
  subkey_opts.table_properties_collector_factories.emplace_back(
      NewCompactOnExpiredTableCollectorFactory(kSubkeyColumnFamilyName, 0.3));
  SetBlobDB(&subkey_opts);

  rocksdb::BlockBasedTableOptions pubsub_table_opts = InitTableOptions();
  rocksdb::ColumnFamilyOptions pubsub_opts(options);
  pubsub_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(pubsub_table_opts));
  pubsub_opts.compaction_filter_factory = std::make_shared<PubSubFilterFactory>();
  pubsub_opts.disable_auto_compactions = disable_auto_compactions;
  SetBlobDB(&pubsub_opts);

  rocksdb::BlockBasedTableOptions propagate_table_opts = InitTableOptions();
  rocksdb::ColumnFamilyOptions propagate_opts(options);
  propagate_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(propagate_table_opts));
  propagate_opts.compaction_filter_factory = std::make_shared<PropagateFilterFactory>();
  propagate_opts.disable_auto_compactions = disable_auto_compactions;
  SetBlobDB(&propagate_opts);

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  // Caution: don't change the order of column family, or the handle will be mismatched
  column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, subkey_opts);
  column_families.emplace_back(kMetadataColumnFamilyName, metadata_opts);
  column_families.emplace_back(kZSetScoreColumnFamilyName, subkey_opts);
  column_families.emplace_back(kPubSubColumnFamilyName, pubsub_opts);
  column_families.emplace_back(kPropagateColumnFamilyName, propagate_opts);
  column_families.emplace_back(kStreamColumnFamilyName, subkey_opts);

  std::vector<std::string> old_column_families;
  auto s = rocksdb::DB::ListColumnFamilies(options, db_dir, &old_column_families);
  if (!s.ok()) return {Status::NotOK, s.ToString()};

  auto start = std::chrono::high_resolution_clock::now();
  auto dbs = read_only ? util::DBOpenForReadOnly(options, db_dir, column_families, &cf_handles_)
                       : util::DBOpen(options, db_dir, column_families, &cf_handles_);
  auto end = std::chrono::high_resolution_clock::now();
  int64_t duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  if (!dbs.IsOK()) {
    LOG(WARNING) << "[storage] Failed to load the data from disk: " << duration << " ms";
    return {Status::DBOpenErr, dbs.Msg()};
  }

  db_ = std::move(*dbs);
  cdc_restart_point_.CopyFrom(getCDCPointLocked());
  LOG(INFO) << "[storage] Success to load the data from disk: " << duration << " ms";
  return Status::OK();
}

Status Storage::GetWALIter(rocksdb::SequenceNumber seq, std::unique_ptr<rocksdb::TransactionLogIterator> *iter) {
  auto s = db_->GetUpdatesSince(seq, iter);
  if (!s.ok()) return {Status::DBGetWALErr, s.ToString()};

  if (!(*iter)->Valid()) return {Status::DBGetWALErr, "iterator is not valid"};

  return Status::OK();
}

rocksdb::SequenceNumber Storage::LatestSeqNumber() { return db_->GetLatestSequenceNumber(); }

rocksdb::Status Storage::Get(const rocksdb::ReadOptions &options, const rocksdb::Slice &key, std::string *value) {
  return Get(options, db_->DefaultColumnFamily(), key, value);
}

rocksdb::Status Storage::Get(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family,
                             const rocksdb::Slice &key, std::string *value) {
  if (is_txn_mode_ && txn_write_batch_->GetWriteBatch()->Count() > 0) {
    return txn_write_batch_->GetFromBatchAndDB(db_.get(), options, column_family, key, value);
  }
  return db_->Get(options, column_family, key, value);
}

rocksdb::Status Storage::Get(const rocksdb::ReadOptions &options, const rocksdb::Slice &key,
                             rocksdb::PinnableSlice *value) {
  return Get(options, db_->DefaultColumnFamily(), key, value);
}

rocksdb::Status Storage::Get(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family,
                             const rocksdb::Slice &key, rocksdb::PinnableSlice *value) {
  if (is_txn_mode_ && txn_write_batch_->GetWriteBatch()->Count() > 0) {
    return txn_write_batch_->GetFromBatchAndDB(db_.get(), options, column_family, key, value);
  }
  return db_->Get(options, column_family, key, value);
}

rocksdb::Iterator *Storage::NewIterator(const rocksdb::ReadOptions &options) {
  return NewIterator(options, db_->DefaultColumnFamily());
}

rocksdb::Iterator *Storage::NewIterator(const rocksdb::ReadOptions &options,
                                        rocksdb::ColumnFamilyHandle *column_family) {
  auto iter = db_->NewIterator(options, column_family);
  if (is_txn_mode_ && txn_write_batch_->GetWriteBatch()->Count() > 0) {
    return txn_write_batch_->NewIteratorWithBase(column_family, iter, &options);
  }
  return iter;
}

void Storage::MultiGet(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family,
                       const size_t num_keys, const rocksdb::Slice *keys, rocksdb::PinnableSlice *values,
                       rocksdb::Status *statuses) {
  if (is_txn_mode_ && txn_write_batch_->GetWriteBatch()->Count() > 0) {
    txn_write_batch_->MultiGetFromBatchAndDB(db_.get(), options, column_family, num_keys, keys, values, statuses,
                                             false);
  } else {
    db_->MultiGet(options, column_family, num_keys, keys, values, statuses, false);
  }
}

rocksdb::Status Storage::Write(const rocksdb::WriteOptions &options, rocksdb::WriteBatch *updates) {
  if (is_txn_mode_) {
    // The batch won't be flushed until the transaction was committed or rollback
    return rocksdb::Status::OK();
  }
  return writeToDB(options, updates);
}

void Storage::checkAndRecordFailedWrite(const rocksdb::Status status) {
  if (!status.ok()) {
    LOG(ERROR) << "[storage] write to DB error, err : " << status.ToString();
    stats.IncrWriteResult(status);
  }
}

rocksdb::Status Storage::writeToDB(const rocksdb::WriteOptions &options, rocksdb::WriteBatch *updates) {
  if (db_size_limit_reached_) {
    return rocksdb::Status::SpaceLimit();
  }

  // Put replication id logdata at the end of write batch
  if (replid_.length() == kReplIdLength) {
    updates->PutLogData(ServerLogData(kReplIdLog, replid_).Encode());
  }

  rocksdb::Status write_result = db_->Write(options, updates);
  checkAndRecordFailedWrite(write_result);

  return write_result;
}

rocksdb::Status Storage::Delete(const rocksdb::WriteOptions &options, rocksdb::ColumnFamilyHandle *cf_handle,
                                const rocksdb::Slice &key) {
  auto batch = GetWriteBatchBase();
  batch->Delete(cf_handle, key);
  return Write(options, batch->GetWriteBatch());
}

rocksdb::Status Storage::DeleteRange(const std::string &first_key, const std::string &last_key,
                                     const std::string &cf_name) {
  auto batch = GetWriteBatchBase();
  rocksdb::ColumnFamilyHandle *cf_handle = GetCFHandle(cf_name);
  auto s = batch->DeleteRange(cf_handle, first_key, last_key);
  if (!s.ok()) {
    return s;
  }

  return Write(write_opts_, batch->GetWriteBatch());
}

rocksdb::Status Storage::FlushScripts(const rocksdb::WriteOptions &options, rocksdb::ColumnFamilyHandle *cf_handle) {
  std::string begin_key = kLuaFuncSHAPrefix, end_key = begin_key;
  // we need to increase one here since the DeleteRange api
  // didn't contain the end key.
  end_key[end_key.size() - 1] += 1;

  auto batch = GetWriteBatchBase();
  auto s = batch->DeleteRange(cf_handle, begin_key, end_key);
  if (!s.ok()) {
    return s;
  }

  return Write(options, batch->GetWriteBatch());
}

StatusOr<std::pair<size_t, size_t>> Storage::ReplicaApplyWriteBatch(const rocksdb::WriteOptions &write_opts,
                                                                    const std::string &raw_batch) {
  if (db_size_limit_reached_) {
    return {Status::NotOK, "reach space limit"};
  }

  auto batch = rocksdb::WriteBatch(raw_batch);
  auto s = db_->Write(write_opts, &batch);
  checkAndRecordFailedWrite(s);

  if (!s.ok()) {
    return {Status::NotOK, s.ToString()};
  }

  return {batch.Count(), batch.GetDataSize()};
}

rocksdb::ColumnFamilyHandle *Storage::GetCFHandle(const std::string &name) {
  if (name == kMetadataColumnFamilyName) {
    return cf_handles_[1];
  } else if (name == kZSetScoreColumnFamilyName) {
    return cf_handles_[2];
  } else if (name == kPubSubColumnFamilyName) {
    return cf_handles_[3];
  } else if (name == kPropagateColumnFamilyName) {
    return cf_handles_[4];
  } else if (name == kStreamColumnFamilyName) {
    return cf_handles_[5];
  }
  return cf_handles_[0];
}

rocksdb::Status Storage::Compact(rocksdb::ColumnFamilyHandle *cf, const Slice *begin, const Slice *end) {
  rocksdb::CompactRangeOptions compact_opts;
  compact_opts.change_level = true;
  // For the manual compaction, we would like to force the bottommost level to be compacted.
  // Or it may use the trivial mode and some expired key-values were still exist in the bottommost level.
  compact_opts.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForceOptimized;
  const auto &cf_handles = cf ? std::vector<rocksdb::ColumnFamilyHandle *>{cf} : cf_handles_;
  for (const auto &cf_handle : cf_handles) {
    rocksdb::Status s = db_->CompactRange(compact_opts, cf_handle, begin, end);
    if (!s.ok()) return s;
  }
  return rocksdb::Status::OK();
}

uint64_t Storage::GetTotalSize(const std::string &ns) {
  if (ns == kDefaultNamespace) {
    return sst_file_manager_->GetTotalSize();
  }

  std::string begin_key, end_key;
  std::string prefix = ComposeNamespaceKey(ns, "", false);

  redis::Database db(this, ns);
  uint64_t size = 0, total_size = 0;
  rocksdb::DB::SizeApproximationFlags include_both =
      rocksdb::DB::SizeApproximationFlags::INCLUDE_FILES | rocksdb::DB::SizeApproximationFlags::INCLUDE_MEMTABLES;

  for (auto cf_handle : cf_handles_) {
    if (cf_handle == GetCFHandle(kPubSubColumnFamilyName) || cf_handle == GetCFHandle(kPropagateColumnFamilyName)) {
      continue;
    }

    auto s = db.FindKeyRangeWithPrefix(prefix, std::string(), &begin_key, &end_key, cf_handle);
    if (!s.ok()) continue;

    rocksdb::Range r(begin_key, end_key);
    db_->GetApproximateSizes(cf_handle, &r, 1, &size, include_both);
    total_size += size;
  }

  return total_size;
}

void Storage::CheckDBSizeLimit() {
  if (auto storage_mgr = storage_mgr_.lock(); storage_mgr) {
    storage_mgr->CheckDBSizeLimit();
  }
}

void Storage::setDBSizeLimitReached(bool limit_reached) {
  if (db_size_limit_reached_ == limit_reached) {
    return;
  }

  db_size_limit_reached_ = limit_reached;
  if (db_size_limit_reached_) {
    LOG(WARNING) << "[storage] ENABLE db_size limit " << config_->max_db_size << " GB."
                 << "Switch datanode to read-only mode.";
  } else {
    LOG(WARNING) << "[storage] DISABLE db_size limit. Switch datanode to read-write mode.";
  }
}

void Storage::SetIORateLimit(int64_t max_io_mb) {
  if (max_io_mb <= 0) max_io_mb = kIORateLimitMaxMb;
  rate_limiter_->SetBytesPerSecond(max_io_mb * static_cast<int64_t>(MiB));
}

rocksdb::DB *Storage::GetDB() { return db_.get(); }

Status Storage::BeginTxn() {
  if (is_txn_mode_) {
    return Status{Status::NotOK, "cannot begin a new transaction while already in transaction mode"};
  }
  // The EXEC command is exclusive and shouldn't have multi transaction at the same time,
  // so it's fine to reset the global write batch without any lock.
  is_txn_mode_ = true;
  txn_write_batch_ = std::make_unique<rocksdb::WriteBatchWithIndex>();
  return Status::OK();
}

Status Storage::CommitTxn() {
  if (!is_txn_mode_) {
    return Status{Status::NotOK, "cannot commit while not in transaction mode"};
  }

  auto s = writeToDB(write_opts_, txn_write_batch_->GetWriteBatch());

  is_txn_mode_ = false;
  txn_write_batch_ = nullptr;
  if (s.ok()) {
    return Status::OK();
  }
  return {Status::NotOK, s.ToString()};
}

ObserverOrUniquePtr<rocksdb::WriteBatchBase> Storage::GetWriteBatchBase() {
  if (is_txn_mode_) {
    return ObserverOrUniquePtr<rocksdb::WriteBatchBase>(txn_write_batch_.get(), ObserverOrUnique::Observer);
  }
  return ObserverOrUniquePtr<rocksdb::WriteBatchBase>(new rocksdb::WriteBatch(), ObserverOrUnique::Unique);
}

Status Storage::WriteToPropagateCF(const std::string &key, const std::string &value) {
  auto batch = GetWriteBatchBase();
  auto cf = GetCFHandle(kPropagateColumnFamilyName);
  batch->Put(cf, key, value);
  auto s = Write(write_opts_, batch->GetWriteBatch());
  if (!s.ok()) {
    return {Status::NotOK, s.ToString()};
  }
  return Status::OK();
}

Status Storage::ShiftReplId() {
  static constexpr std::string_view charset = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  // Do nothing if rsid psync is not enabled
  if (!config_->use_rsid_psync) return Status::OK();

  std::random_device rd;
  std::mt19937 gen(rd() + getpid());
  std::uniform_int_distribution<size_t> distrib(0, charset.size() - 1);

  std::string rand_str(kReplIdLength, 0);
  for (int i = 0; i < kReplIdLength; i++) {
    rand_str[i] = charset[distrib(gen)];
  }
  replid_ = std::move(rand_str);
  LOG(INFO) << "[replication] New replication id: " << replid_;

  // Write new replication id into db engine
  return WriteToPropagateCF(kReplicationIdKey, replid_);
}

kv::datanode::v1::CDCPoint Storage::getCDCPointLocked() {
  return redis::CovertSyncPointToCDCPoint(getSyncPointLocked());
}

StatusOr<kv::datanode::v1::CDCPoint> Storage::GetCDCPoint() {
  auto ret = GetSyncPoint();
  if (!ret.IsOK()) {
    return ret.ToStatus();
  }
  return redis::CovertSyncPointToCDCPoint(ret.GetValue());
}

StatusOr<kv::datanode::v1::CDCPoint> Storage::GetCDCPoint(rocksdb::SequenceNumber seq) {
  auto ret = GetSyncPoint(seq);
  if (!ret.IsOK()) {
    return ret.ToStatus();
  }
  return redis::CovertSyncPointToCDCPoint(ret.GetValue());
}

StatusOr<kv::datanode::v1::CDCPoint> Storage::GetCDCOldestPoint() {
  auto ret = GetSyncPoint(0);
  if (!ret.IsOK()) {
    return ret.ToStatus();
  }
  return redis::CovertSyncPointToCDCPoint(ret.GetValue());
}

kv::datanode::v1::SyncPoint Storage::getSyncPointLocked() {
  auto seq_id = LatestSeqNumber();
  // empty db engine
  if (seq_id == 0) {
    kv::datanode::v1::SyncPoint sync_point;
    sync_point.set_next_seq_id(1);
    return sync_point;
  }
  // get sync point from wal
  auto sync_point_ret = GetSyncPointFromWalBySeq(seq_id);
  if (sync_point_ret.IsOK()) {
    return sync_point_ret.GetValue();
  }
  LOG(WARNING) << "[sync point] Get local sync point failed, slot range:" << GetName()
               << ", err:" << sync_point_ret.Msg();
  // get replica id from engine
  kv::datanode::v1::SyncPoint sync_point;
  sync_point.set_next_seq_id(seq_id + 1);
  auto repl_id_ret = GetReplIdFromDbEngine();
  if (repl_id_ret.IsOK()) {
    sync_point.set_prev_rep_id(repl_id_ret.GetValue());
    return sync_point;
  }
  // empty log ts and replica id
  LOG(WARNING) << "[sync point] Get local replica id failed, slot range:" << GetName() << ", err:" << repl_id_ret.Msg();
  return sync_point;
}

StatusOr<kv::datanode::v1::SyncPoint> Storage::GetSyncPoint() {
  std::shared_lock<std::shared_mutex> lk(db_rw_lock_);
  if (db_closing_ || !db_) {
    return {Status::NotOK, "storage is closing"};
  }
  return getSyncPointLocked();
}

StatusOr<kv::datanode::v1::SyncPoint> Storage::GetSyncPoint(rocksdb::SequenceNumber seq) {
  std::shared_lock<std::shared_mutex> lk(db_rw_lock_);
  if (db_closing_ || !db_) {
    return {Status::NotOK, "storage is closing"};
  }
  return GetSyncPointFromWalBySeq(seq);
}

StatusOr<std::string> Storage::GetReplIdFromWalBySeq(rocksdb::SequenceNumber seq) {
  auto ret = GetSyncPointFromWalBySeq(seq);
  if (!ret.IsOK()) {
    return ret.ToStatus();
  }
  return ret->prev_rep_id();
}

StatusOr<kv::datanode::v1::SyncPoint> Storage::GetSyncPointFromWalBySeq(rocksdb::SequenceNumber seq) {
  if (seq != 0 && !WALHasNewData(seq)) {
    return {Status::NotOK, "log id too big, local latest log id=" + std::to_string(LatestSeqNumber())};
  }
  std::unique_ptr<rocksdb::TransactionLogIterator> iter = nullptr;
  if (auto s = GetWALIter(seq, &iter); !s.IsOK()) {
    return s;
  }
  auto batch = iter->GetBatch();
  if (seq != 0 && batch.sequence > seq) {
    return {Status::NotOK, "log id maybe outdated, iter log id=" + std::to_string(batch.sequence)};
  }
  ReplIdExtractor write_batch_handler;
  rocksdb::Status s = batch.writeBatchPtr->Iterate(&write_batch_handler);
  if (!s.ok()) {
    return {Status::NotOK,
            "handle log data failed, iter log id=" + std::to_string(batch.sequence) + ", err=" + s.ToString()};
  }

  kv::datanode::v1::SyncPoint sync_point;
  sync_point.set_next_seq_id(batch.sequence + batch.writeBatchPtr->Count());
  sync_point.set_prev_log_ts(write_batch_handler.GetTimeNanos());
  sync_point.set_prev_rep_id(write_batch_handler.GetReplId());
  return sync_point;
}

StatusOr<std::string> Storage::GetReplIdFromDbEngine() {
  std::string replid_in_db;
  auto cf = GetCFHandle(kPropagateColumnFamilyName);
  auto s = db_->Get(rocksdb::ReadOptions(), cf, kReplicationIdKey, &replid_in_db);
  if (!s.ok() && !s.IsNotFound()) {
    return {Status::NotOK, s.ToString()};
  }
  return replid_in_db;
}

std::shared_lock<std::shared_mutex> Storage::ReadLockGuard() { return std::shared_lock(db_rw_lock_); }

std::unique_lock<std::shared_mutex> Storage::WriteLockGuard() { return std::unique_lock(db_rw_lock_); }

Status Storage::InWALBoundary(rocksdb::SequenceNumber seq) {
  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  auto s = GetWALIter(seq, &iter);
  if (!s.IsOK()) return s;
  auto wal_seq = iter->GetBatch().sequence;
  if (seq < wal_seq) {
    return {Status::NotOK, fmt::format("checkpoint seq: {} is smaller than the WAL seq: {}", seq, wal_seq)};
  }
  return Status::OK();
}

Status Storage::ReplDataManager::CleanInvalidFiles(Storage *storage, const std::string &dir,
                                                   std::vector<std::string> valid_files) {
  if (!storage->env_->FileExists(dir).ok()) {
    return Status::OK();
  }

  std::vector<std::string> tmp_files, files;
  storage->env_->GetChildren(dir, &tmp_files);
  for (const auto &file : tmp_files) {
    if (file == "." || file == "..") continue;
    files.push_back(file);
  }

  // Find invalid files
  std::sort(files.begin(), files.end());
  std::sort(valid_files.begin(), valid_files.end());
  std::vector<std::string> invalid_files(files.size() + valid_files.size());
  auto it =
      std::set_difference(files.begin(), files.end(), valid_files.begin(), valid_files.end(), invalid_files.begin());

  // Delete invalid files
  Status ret;
  invalid_files.resize(it - invalid_files.begin());
  for (it = invalid_files.begin(); it != invalid_files.end(); ++it) {
    auto s = storage->env_->DeleteFile(dir + "/" + *it);
    if (!s.ok()) {
      ret = Status(Status::NotOK, s.ToString());
      LOG(INFO) << "[storage] Failed to delete invalid file " << *it << " of master checkpoint";
    } else {
      LOG(INFO) << "[storage] Succeed deleting invalid file " << *it << " of master checkpoint";
    }
  }
  return ret;
}

Status MkdirRecursively(rocksdb::Env *env, const std::string &dir) {
  if (env->CreateDirIfMissing(dir).ok()) return Status::OK();

  std::string parent;
  for (auto pos = dir.find('/', 1); pos != std::string::npos; pos = dir.find('/', pos + 1)) {
    parent = dir.substr(0, pos);
    if (auto s = env->CreateDirIfMissing(parent); !s.ok()) {
      LOG(ERROR) << "[storage] Failed to create directory '" << parent << "' recursively. Error: " << s.ToString();
      return {Status::NotOK};
    }
  }

  if (env->CreateDirIfMissing(dir).ok()) return Status::OK();

  return {Status::NotOK};
}

std::unique_ptr<rocksdb::WritableFile> Storage::ReplDataManager::NewTmpFile(Storage *storage, const std::string &dir,
                                                                            const std::string &repl_file) {
  std::string tmp_file = dir + "/" + repl_file + ".tmp";
  auto s = storage->env_->FileExists(tmp_file);
  if (s.ok()) {
    LOG(ERROR) << "[storage] Data file exists, override";
    storage->env_->DeleteFile(tmp_file);
  }

  // Create directory if missing
  auto abs_dir = tmp_file.substr(0, tmp_file.rfind('/'));
  if (!MkdirRecursively(storage->env_, abs_dir).IsOK()) {
    return nullptr;
  }

  std::unique_ptr<rocksdb::WritableFile> wf;
  s = storage->env_->NewWritableFile(tmp_file, &wf, rocksdb::EnvOptions());
  if (!s.ok()) {
    LOG(ERROR) << "[storage] Failed to create data file '" << tmp_file << "'. Error: " << s.ToString();
    return nullptr;
  }

  return wf;
}

Status Storage::ReplDataManager::SwapTmpFile(Storage *storage, const std::string &dir, const std::string &repl_file) {
  std::string tmp_file = dir + "/" + repl_file + ".tmp";
  std::string orig_file = dir + "/" + repl_file;

  auto s = storage->env_->RenameFile(tmp_file, orig_file);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("unable to rename '{}' to '{}'. Error: {}", tmp_file, orig_file, s.ToString())};
  }

  return Status::OK();
}

bool Storage::ReplDataManager::FileExists(Storage *storage, const std::string &dir, const std::string &repl_file,
                                          uint32_t crc) {
  if (storage->IsClosing()) return false;

  auto file_path = dir + "/" + repl_file;
  auto s = storage->env_->FileExists(file_path);
  if (!s.ok()) return false;

  // If crc is 0, we needn't verify, return true directly.
  if (crc == 0) return true;

  std::unique_ptr<rocksdb::SequentialFile> src_file;
  const rocksdb::EnvOptions env_options;
  s = storage->env_->NewSequentialFile(file_path, &src_file, env_options);
  if (!s.ok()) return false;

  uint64_t size = 0;
  s = storage->env_->GetFileSize(file_path, &size);
  if (!s.ok()) return false;

  auto src_reader = std::make_unique<rocksdb::SequentialFileWrapper>(src_file.get());

  char buffer[4096];
  Slice slice;
  uint32_t tmp_crc = 0;
  while (size > 0) {
    size_t bytes_to_read = std::min(sizeof(buffer), static_cast<size_t>(size));
    s = src_reader->Read(bytes_to_read, &slice, buffer);
    if (!s.ok()) return false;

    if (slice.size() == 0) return false;

    tmp_crc = rocksdb::crc32c::Extend(0, slice.data(), slice.size());
    size -= slice.size();
  }

  return crc == tmp_crc;
}

void Storage::RecordInstantaneousDBMetrics() {
  auto rocksdb_stats = db_->GetDBOptions().statistics;
  stats.TrackInstantaneousMetric(StorageStats::STATS_METRIC_ROCKSDB_PUT,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_KEYS_WRITTEN));
  stats.TrackInstantaneousMetric(StorageStats::STATS_METRIC_ROCKSDB_GET,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_KEYS_READ));
  stats.TrackInstantaneousMetric(StorageStats::STATS_METRIC_ROCKSDB_MULTIGET,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_MULTIGET_KEYS_READ));
  stats.TrackInstantaneousMetric(StorageStats::STATS_METRIC_ROCKSDB_SEEK,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_DB_SEEK));
  stats.TrackInstantaneousMetric(StorageStats::STATS_METRIC_ROCKSDB_NEXT,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_DB_NEXT));
  stats.TrackInstantaneousMetric(StorageStats::STATS_METRIC_ROCKSDB_PREV,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_DB_PREV));
}

void Storage::Cron() {
  uint64_t counter = 0;
  time_t last_compact_date = 0;
  CompactionChecker compaction_checker{this};
  std::vector<std::string> range_cf_names = {engine::kMetadataColumnFamilyName, engine::kSubkeyColumnFamilyName,
                                             engine::kZSetScoreColumnFamilyName, engine::kStreamColumnFamilyName};
  std::vector<std::string> full_cf_names = {engine::kMetadataColumnFamilyName,  engine::kSubkeyColumnFamilyName,
                                            engine::kZSetScoreColumnFamilyName, engine::kStreamColumnFamilyName,
                                            engine::kPubSubColumnFamilyName,    engine::kPropagateColumnFamilyName};

  while (!task_stop_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    counter++;

    auto guard = ReadLockGuard();
    if (IsClosing()) {
      continue;
    }

    // check every 20s (use 20s instead of 60s so that cron will execute in critical condition)
    /*if (counter != 0 && counter % 200 == 0) {
      auto t = static_cast<time_t>(util::GetTimeStamp());
      std::tm now{};
      localtime_r(&t, &now);

      if (config_->bgsave_cron.IsEnabled() && config_->bgsave_cron.IsTimeMatch(&now) && !task_stop_) {
        Status s = AsyncBgSaveDB();
        LOG(INFO) << "[server] Schedule to bgsave the db, result: " << s.Msg();
      }
    }

    // check every 10s
    if (counter != 0 && counter % 100 == 0 && !task_stop_) {
      Status s = AsyncPurgeOldBackups(config_->max_backup_to_keep, config_->max_backup_keep_hours);
    }*/

    // cron compact range
    {
      if (counter % 600 == 0  // check every minute
          && config_->enable_compact_range && config_->compaction_checker_range.Enabled() && !task_stop_) {
        auto now = static_cast<time_t>(util::GetTimeStamp());
        std::tm local_time{};
        localtime_r(&now, &local_time);

        if (local_time.tm_hour >= config_->compaction_checker_range.start &&
            local_time.tm_hour <= config_->compaction_checker_range.stop) {
          std::vector<std::string> cf_names = {engine::kMetadataColumnFamilyName, engine::kSubkeyColumnFamilyName,
                                               engine::kZSetScoreColumnFamilyName, engine::kStreamColumnFamilyName};
          {
            std::lock_guard lg(cron_compact_mu_);
            is_cron_compact_range_in_progress_ = true;
          }
          std::vector<CompactResult> rets;
          for (const auto &cf_name : range_cf_names) {
            rets.emplace_back(compaction_checker.PickCompactionFiles(cf_name));
          }
          {
            std::lock_guard lg(cron_compact_mu_);
            is_cron_compact_range_in_progress_ = false;
            for (const auto &ret : rets) {
              if (ret.compact_range_times > 0) {
                cron_compact_range_times_ += ret.compact_range_times;
                cron_compact_range_delete_tombs_ += ret.compact_range_delete_tombs;
                cron_compact_range_spend_ms_ += ret.compact_range_spent_ms;
              }
            }
          }
        }
      }
    }

    if (!config_->new_cron_full_compaction.Enabled()) {
      // compact full once per day
      auto full_compact_start = static_cast<time_t>(util::GetTimeStamp());
      std::tm full_compact_time{};
      localtime_r(&full_compact_start, &full_compact_time);
      auto now_hours = full_compact_start / 3600;

      // timeout endtime,  stop full compact, timeout = compaction_checker_full.stop + 1s;
      std::shared_ptr<util::TimeoutTask> timeout_task;
      int64_t timeout_s = 1;
      auto add_func = [this](void *) mutable {
        db_->DisableManualCompaction();
        db_->EnableManualCompaction();
        LOG(INFO) << "[cron full compact] timeout task execute success ";
      };

      // Note: if new_cron_full_compaction is enabled, we don't use the old logic to compact full
      if (counter % 600 == 0 && config_->enable_compact_full && config_->compaction_checker_full.Enabled() &&
          now_hours != 0 && last_compact_date != now_hours / 24 &&
          full_compact_time.tm_hour >= config_->compaction_checker_full.start &&
          full_compact_time.tm_hour <= config_->compaction_checker_full.stop && !task_stop_) {
        size_t success_cf = 0;

        auto start_full_time_secs = util::GetTimeStamp<std::chrono::seconds>();
        LOG(INFO) << "[cron full compact] start task";
        {
          std::lock_guard lg(cron_compact_mu_);
          is_cron_compact_full_in_progress_ = true;
        }

        // reset rate limiter
        if (config_->max_io_mb_full_compact > 0) {
          LOG(INFO) << "[cron full compact] reset db limite from" << rate_limiter_->GetBytesPerSecond() / MiB
                    << "M/s to" << config_->max_io_mb_full_compact << "M/s";
          SetIORateLimit(config_->max_io_mb_full_compact);
        }

        for (const auto &cf_name : full_cf_names) {
          auto now = static_cast<time_t>(util::GetTimeStamp());
          std::tm local_time{};
          localtime_r(&now, &local_time);

          if (local_time.tm_hour >= config_->compaction_checker_full.start &&
              local_time.tm_hour <= config_->compaction_checker_full.stop) {
            {
              std::lock_guard lg(cron_compact_mu_);
              if (success_cf == 0) {
                cron_compact_full_times_++;
                last_compact_date = now_hours / 24;

                // compute timeout
                {
                  std::tm timeout_time = full_compact_time;
                  timeout_time.tm_hour = config_->compaction_checker_full.stop;
                  timeout_time.tm_min = 59;
                  timeout_time.tm_sec = 59;
                  timeout_s += std::mktime(&timeout_time) - full_compact_start;
                }

                timeout_task = timeout_mgr_->AddTimeoutTask(timeout_s * 1000, add_func, nullptr);
                LOG(INFO) << "[cron full compact] add timeout task, timeout: " << timeout_s << " s ";
              }
            }

            compaction_checker.CompactFullCF(cf_name);
            success_cf++;
          }
        }

        {
          // recover rate limiter
          SetIORateLimit(config_->max_io_mb);
          std::lock_guard lg(cron_compact_mu_);
          if (success_cf != full_cf_names.size()) {
            cron_compact_full_partial_success_++;
            LOG(INFO) << "[cron full compact] finished task partial success";
          } else {
            LOG(INFO) << "[cron full compact] finished task all success";
          }

          is_cron_compact_full_in_progress_ = false;
          last_cron_compact_full_timestamp_secs_ = start_full_time_secs;
          auto stop_full_time_secs = util::GetTimeStamp<std::chrono::seconds>();
          last_cron_compact_full_duration_secs_ = stop_full_time_secs - start_full_time_secs;
        }
        timeout_task.reset();
      }
    }

    if (config_->new_cron_full_compaction.Enabled()) {
      // new cron full compaction logic
      std::shared_ptr<util::TimeoutTask> timeout_task;
      int64_t timeout_s = 1;
      auto add_func = [this](void *) mutable {
        db_->DisableManualCompaction();
        db_->EnableManualCompaction();
        LOG(INFO) << "[cron full compact] timeout task execute success ";
      };

      auto now_timestamp = static_cast<time_t>(util::GetTimeStamp());
      std::tm now_local_time{};
      localtime_r(&now_timestamp, &now_local_time);
      auto now_local_day = now_local_time.tm_mday;
      // if new_cron_full_compaction is enabled, we don't use the old logic to compact full
      if (counter % 600 == 0 && config_->enable_compact_full && last_compact_date != now_local_day && !task_stop_) {
        // get new cron full compaction config
        auto cron_start_hour = config_->new_cron_full_compaction.start_hour;
        auto cron_end_hour = config_->new_cron_full_compaction.end_hour;

        now_local_time.tm_hour = cron_start_hour;
        now_local_time.tm_min = 0;
        now_local_time.tm_sec = 0;
        auto cron_start_timestamp = std::mktime(&now_local_time);

        // if cron_end_hour is -1, set the cron_end_hour to 23 this day
        if (cron_end_hour == -1) {
          cron_end_hour = 23;
        }
        now_local_time.tm_hour = cron_end_hour;
        now_local_time.tm_min = 59;
        now_local_time.tm_sec = 59;
        auto cron_end_timestamp = std::mktime(&now_local_time);
        if ((now_timestamp >= cron_start_timestamp && now_timestamp <= cron_end_timestamp) &&
            config_->new_cron_full_compaction.IsInCompactionDays(now_local_day) && !task_stop_) {
          LOG(INFO) << "[cron full compact] new cron full compaction logic";
          size_t success_cf = 0;

          auto start_full_time_secs = util::GetTimeStamp<std::chrono::seconds>();
          LOG(INFO) << "[cron full compact] start task";
          {
            std::lock_guard lg(cron_compact_mu_);
            is_cron_compact_full_in_progress_ = true;
          }

          // reset rate limiter
          if (config_->max_io_mb_full_compact > 0) {
            LOG(INFO) << "[cron full compact] reset db limite from" << rate_limiter_->GetBytesPerSecond() / MiB
                      << "M/s to" << config_->max_io_mb_full_compact << "M/s";
            SetIORateLimit(config_->max_io_mb_full_compact);
          }

          for (const auto &cf_name : full_cf_names) {
            auto current_timestamp = static_cast<time_t>(util::GetTimeStamp());

            if (current_timestamp >= cron_start_timestamp && current_timestamp <= cron_end_timestamp) {
              {
                std::lock_guard lg(cron_compact_mu_);
                if (success_cf == 0) {
                  cron_compact_full_times_++;
                  last_compact_date = now_local_day;

                  // compute timeout
                  timeout_s += cron_end_timestamp - current_timestamp;

                  timeout_task = timeout_mgr_->AddTimeoutTask(timeout_s * 1000, add_func, nullptr);
                  LOG(INFO) << "[cron full compact] add timeout task, timeout: " << timeout_s << " s ";
                }
              }

              compaction_checker.CompactFullCF(cf_name);
              success_cf++;
            }
          }

          {
            // recover rate limiter
            SetIORateLimit(config_->max_io_mb);
            std::lock_guard lg(cron_compact_mu_);
            if (success_cf != full_cf_names.size()) {
              cron_compact_full_partial_success_++;
              LOG(INFO) << "[cron full compact] finished task partial success";
            } else {
              LOG(INFO) << "[cron full compact] finished task all success";
            }

            is_cron_compact_full_in_progress_ = false;
            last_cron_compact_full_timestamp_secs_ = start_full_time_secs;
            auto stop_full_time_secs = util::GetTimeStamp<std::chrono::seconds>();
            last_cron_compact_full_duration_secs_ = stop_full_time_secs - start_full_time_secs;
          }

          timeout_task.reset();
        }
      }
    }
  }
}

Status Storage::AsyncBgSaveDB() {
  std::lock_guard<std::mutex> lg(db_job_mu_);
  if (is_bgsave_in_progress_) {
    return {Status::NotOK, "bgsave in-progress"};
  }

  is_bgsave_in_progress_ = true;

  return task_runner_.TryPublish([this] {
    auto start_bgsave_time_secs = util::GetTimeStamp<std::chrono::seconds>();
    Status s = CreateBackup();
    auto stop_bgsave_time_secs = util::GetTimeStamp<std::chrono::seconds>();

    std::lock_guard<std::mutex> lg(db_job_mu_);
    is_bgsave_in_progress_ = false;
    last_bgsave_timestamp_secs_ = start_bgsave_time_secs;
    last_bgsave_status_ = s.IsOK() ? "ok" : "err";
    last_bgsave_duration_secs_ = stop_bgsave_time_secs - start_bgsave_time_secs;
    bgsave_times_++;
  });
}

Status Storage::CreateBackup(uint64_t *sequence_number) {
  LOG(INFO) << "[storage] Start to create new backup";
  std::lock_guard<std::mutex> lg(backup_mu_);
  std::string task_backup_dir = backup_dir_;

  std::string tmpdir = task_backup_dir + ".tmp";
  // Maybe there is a dirty tmp checkpoint, try to clean it
  rocksdb::DestroyDB(tmpdir, rocksdb::Options());

  // 1) Create checkpoint of rocksdb for backup
  rocksdb::Checkpoint *checkpoint = nullptr;
  rocksdb::Status s = rocksdb::Checkpoint::Create(db_.get(), &checkpoint);
  if (!s.ok()) {
    LOG(WARNING) << "Failed to create checkpoint object for backup. Error: " << s.ToString();
    return {Status::NotOK, s.ToString()};
  }

  std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint);
  s = checkpoint->CreateCheckpoint(tmpdir, config_->rocks_db.write_buffer_size * MiB, sequence_number);
  if (!s.ok()) {
    LOG(WARNING) << "Failed to create checkpoint (snapshot) for backup. Error: " << s.ToString();
    return {Status::DBBackupErr, s.ToString()};
  }

  // 2) Rename tmp backup to real backup dir
  if (s = rocksdb::DestroyDB(task_backup_dir, rocksdb::Options()); !s.ok()) {
    LOG(WARNING) << "[storage] Failed to clean old backup. Error: " << s.ToString();
    return {Status::NotOK, s.ToString()};
  }

  if (s = env_->RenameFile(tmpdir, task_backup_dir); !s.ok()) {
    LOG(WARNING) << "[storage] Failed to rename tmp backup. Error: " << s.ToString();
    // Just try best effort
    if (s = rocksdb::DestroyDB(tmpdir, rocksdb::Options()); !s.ok()) {
      LOG(WARNING) << "[storage] Failed to clean tmp backup. Error: " << s.ToString();
    }

    return {Status::NotOK, s.ToString()};
  }

  // 'backup_mu_' can guarantee 'backup_creating_time_secs_' is thread-safe
  backup_creating_time_secs_ = util::GetTimeStamp<std::chrono::seconds>();

  LOG(INFO) << "[storage] Success to create new backup";
  return Status::OK();
}

Status Storage::AsyncPurgeOldBackups(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours) {
  return task_runner_.TryPublish([num_backups_to_keep, backup_max_keep_hours, this] {
    this->PurgeOldBackups(num_backups_to_keep, backup_max_keep_hours);
  });
}

void Storage::PurgeOldBackups(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours) {
  auto now_secs = util::GetTimeStamp<std::chrono::seconds>();
  std::lock_guard<std::mutex> lg(backup_mu_);
  std::string task_backup_dir = backup_dir_;

  // Return if there is no backup
  auto s = env_->FileExists(task_backup_dir);
  if (!s.ok()) return;

  // No backup is needed to keep or the backup is expired, we will clean it.
  bool backup_expired =
      (backup_max_keep_hours != 0 && backup_creating_time_secs_ + backup_max_keep_hours * 3600 < now_secs);
  if (num_backups_to_keep == 0 || backup_expired) {
    s = rocksdb::DestroyDB(task_backup_dir, rocksdb::Options());
    if (s.ok()) {
      LOG(INFO) << "[storage] Succeeded cleaning old backup that was created at " << backup_creating_time_secs_;
    } else {
      LOG(INFO) << "[storage] Failed cleaning old backup that was created at " << backup_creating_time_secs_
                << ". Error: " << s.ToString();
    }
  }
}

std::string Storage::GetJobInfo(std::string &prefix) {
  std::ostringstream string_stream;
  std::lock_guard<std::mutex> lg(db_job_mu_);
  string_stream << prefix << "is_job_bgsaving:" << (is_bgsave_in_progress_ ? 1 : 0) << "\r\n";
  string_stream << prefix << "last_job_bgsave_time:" << last_bgsave_timestamp_secs_ << "\r\n";
  string_stream << prefix << "last_job_bgsave_status:" << last_bgsave_status_ << "\r\n";
  string_stream << prefix << "last_job_bgsave_spent_sec:" << last_bgsave_duration_secs_ << "\r\n";
  string_stream << prefix << "is_job_compacting:" << (job_compacting_ ? 1 : 0) << "\r\n";
  string_stream << prefix << "last_job_compact_time:" << job_compact_timstamp_secs_ << "\r\n";
  string_stream << prefix << "last_job_compact_spent_sec:" << job_compact_duration_secs_ << "\r\n";
  string_stream << prefix << "job_bgsave_times:" << bgsave_times_ << "\r\n";
  string_stream << prefix << "job_compact_times:" << job_compact_times_ << "\r\n";
  return string_stream.str();
}

std::string Storage::GetCronInfo(std::string &prefix) {
  std::ostringstream string_stream;
  std::lock_guard<std::mutex> lg(cron_compact_mu_);
  // compact range info
  string_stream << prefix << "is_cron_compact_range:" << (is_cron_compact_range_in_progress_ ? 1 : 0) << "\r\n";
  string_stream << prefix << "cron_compact_range_spent_ms:" << cron_compact_range_spend_ms_ << "\r\n";
  string_stream << prefix << "cron_compact_range_times:" << cron_compact_range_times_ << "\r\n";
  string_stream << prefix << "cron_compact_range_delete_tombs:" << cron_compact_range_delete_tombs_ << "\r\n";

  // compact full info
  string_stream << prefix << "is_cron_compact_full:" << (is_cron_compact_full_in_progress_ ? 1 : 0) << "\r\n";
  string_stream << prefix << "last_cron_compact_full_time:" << last_cron_compact_full_timestamp_secs_ << "\r\n";
  string_stream << prefix << "last_cron_compact_full_spent_sec:" << last_cron_compact_full_duration_secs_ << "\r\n";
  string_stream << prefix << "cron_compact_full_times:" << cron_compact_full_times_ << "\r\n";
  string_stream << prefix << "cron_compact_full_partial_success:" << cron_compact_full_partial_success_ << "\r\n";
  return string_stream.str();
}

static std::vector<std::pair<rocksdb::Histograms, std::string>> DBCompressHistograms = {
    {rocksdb::Histograms::COMPRESSION_TIMES_NANOS, "compress_time_nanos"},
    {rocksdb::Histograms::DECOMPRESSION_TIMES_NANOS, "decompress_time_nanos"},
    {rocksdb::Histograms::BLOB_DB_COMPRESSION_MICROS, "blobdb_compress_time_micros"},
    {rocksdb::Histograms::BLOB_DB_DECOMPRESSION_MICROS, "blobdb_decompress_time_micros"}};

std::string Storage::GetCompressionInfo(const std::string &prefix) {
  std::ostringstream oss;
  oss << prefix
      << "compress_bypass_bytes:" << rocksdb_stats_->getTickerCount(rocksdb::Tickers::BYTES_COMPRESSION_BYPASSED)
      << "\r\n";
  oss << prefix
      << "compress_reject_bytes:" << rocksdb_stats_->getTickerCount(rocksdb::Tickers::BYTES_COMPRESSION_REJECTED)
      << "\r\n";
  oss << prefix << "compress_from_bytes:" << rocksdb_stats_->getTickerCount(rocksdb::Tickers::BYTES_COMPRESSED_FROM)
      << "\r\n";
  oss << prefix << "compress_to_bytes:" << rocksdb_stats_->getTickerCount(rocksdb::Tickers::BYTES_COMPRESSED_TO)
      << "\r\n";
  oss << prefix << "decompress_from_bytes:" << rocksdb_stats_->getTickerCount(rocksdb::Tickers::BYTES_DECOMPRESSED_FROM)
      << "\r\n";
  oss << prefix << "decompress_to_bytes:" << rocksdb_stats_->getTickerCount(rocksdb::Tickers::BYTES_DECOMPRESSED_TO)
      << "\r\n";
  for (const auto &iter : DBCompressHistograms) {
    rocksdb::HistogramData hist_data;
    rocksdb_stats_->histogramData(iter.first, &hist_data);
    oss << prefix << iter.second << "_count:" << hist_data.count << "\r\n";
    oss << prefix << iter.second << "_p50:" << hist_data.median << "\r\n";
    oss << prefix << iter.second << "_p95:" << hist_data.percentile95 << "\r\n";
    oss << prefix << iter.second << "_p99:" << hist_data.percentile99 << "\r\n";
    oss << prefix << iter.second << "_avg:" << hist_data.average << "\r\n";
  }
  return oss.str();
}

std::string Storage::GetWriteStall(const std::string &db_prefix) {
  std::ostringstream string_stream;
  for (auto &[cf_name, is_write_stall] : cfs_is_write_stall_) {
    string_stream << db_prefix << "is_write_stall[" << cf_name << "]:" << is_write_stall << "\r\n";
  }

  return string_stream.str();
}

static std::vector<std::pair<rocksdb::Histograms, std::string>> DBHistogramsNameMap = {
    {rocksdb::Histograms::DB_GET, "rocksdb.db.get.micros"},
    {rocksdb::Histograms::DB_WRITE, "rocksdb.db.write.micros"},
    {rocksdb::Histograms::DB_MULTIGET, "rocksdb.db.multiget.micros"},
    {rocksdb::Histograms::DB_SEEK, "rocksdb.db.seek.micros"}};

std::string Storage::GetOpsLatency(const std::string &db_prefix) {
  std::ostringstream string_stream;
  auto rocksdb_stats = db_->GetOptions().statistics;
  for (const auto &iter : DBHistogramsNameMap) {
    rocksdb::HistogramData hist_data;
    rocksdb_stats->histogramData(iter.first, &hist_data);
    std::string name = iter.second;
    std::replace(name.begin(), name.end(), '.', '_');
    string_stream << db_prefix << name << "_p50:" << hist_data.median << "\r\n";
    string_stream << db_prefix << name << "_p95:" << hist_data.percentile95 << "\r\n";
    string_stream << db_prefix << name << "_p99:" << hist_data.percentile99 << "\r\n";
    string_stream << db_prefix << name << "_avg:" << hist_data.average << "\r\n";
  }

  return string_stream.str();
}

Status Storage::AsyncCompactDB(const std::string &begin_key, const std::string &end_key, bool with_filter,
                               bool is_legacy) {
  {
    std::lock_guard<std::mutex> cron_compact_lg(cron_compact_mu_);
    if (!is_legacy && is_cron_compact_full_in_progress_) {
      return {Status::NotOK, "compact in-progress"};
    }
  }

  std::lock_guard<std::mutex> lg(db_job_mu_);
  if (!is_legacy) {
    if (job_compacting_) {
      // kv-operator depends on this return, it can't be changed
      return {Status::NotOK, "compact in-progress"};
    }

    job_compacting_ = true;
    job_compact_timstamp_secs_ = util::GetTimeStamp<std::chrono::seconds>();
  }

  return task_runner_.TryPublish([begin_key, end_key, with_filter, is_legacy, this] {
    if (is_legacy) {
      job_compacting_ = true;
      job_compact_timstamp_secs_ = util::GetTimeStamp<std::chrono::seconds>();
    }

    std::unique_ptr<Slice> begin = nullptr, end = nullptr;
    if (!begin_key.empty()) begin = std::make_unique<Slice>(begin_key);
    if (!end_key.empty()) end = std::make_unique<Slice>(end_key);

    // reset rate limiter
    if (config_->max_io_mb_full_compact > 0) {
      LOG(INFO) << "[job full compact] reset db limite from " << rate_limiter_->GetBytesPerSecond() / MiB << "M/s to "
                << config_->max_io_mb_full_compact << "M/s";
      SetIORateLimit(config_->max_io_mb_full_compact);
    }

    if (!with_filter) GetConfig()->SetCompactWithoutFilter();
    LOG(INFO) << "[job full compact] Trigger compaction " << (is_legacy ? "lagecy " : "")
              << (with_filter ? "WITH" : "WITHOUT") << " filter";
    auto s = Compact(nullptr, begin.get(), end.get());
    if (!s.ok()) {
      LOG(ERROR) << "[job full compact] Failed to do compaction " << (is_legacy ? "lagecy " : "")
                 << (with_filter ? "WITH" : "WITHOUT") << " filter, Err: " << s.ToString();
    } else {
      LOG(INFO) << "[job full compact] success to do compaction " << (is_legacy ? "lagecy " : "")
                << (with_filter ? "WITH" : "WITHOUT") << " filter";
    }
    if (!with_filter) GetConfig()->SetCompactWithFilter();

    // recover rate limiter
    SetIORateLimit(config_->max_io_mb);
    if (is_legacy) {
      global_legacyslots_compacting_count.fetch_sub(1);
      if (global_legacyslots_compacting_count == 0) {
        global_lagacyslots_last_compact_duration.store(util::GetTimeStamp<std::chrono::seconds>() -
                                                       global_legacyslots_last_compact_time.load());
      }
    }
    std::lock_guard<std::mutex> lg(db_job_mu_);
    job_compacting_ = false;
    job_compact_duration_secs_ = util::GetTimeStamp<std::chrono::seconds>() - job_compact_timstamp_secs_;
    job_compact_times_++;
  });
}

Status Storage::CancleCompactDB() {
  std::lock_guard<std::mutex> lg(db_job_mu_);
  db_->DisableManualCompaction();
  db_->EnableManualCompaction();

  return Status::OK();
}

void StorageManager::initRocksDBComm(Config *config) {
  config_ = config;
  block_cache_ = BuildBlockCache(config->rocks_db.block_cache_size);
  rate_limiter_ = BuildRateLimiter(config_->max_io_mb, config_->rocks_db.rate_limiter_auto_tuned);
}

Status StorageManager::SetBlockCacheSize(int block_cache_size_mb) {
  if (block_cache_size_mb < 0) {
    return {Status::NotOK, "invalid block cache size"};
  }
  block_cache_->SetCapacity(block_cache_size_mb * MiB);
  return Status::OK();
}

void StorageManager::SetIORateLimit(int max_io_mb) {
  if (max_io_mb <= 0) max_io_mb = kIORateLimitMaxMb;
  rate_limiter_->SetBytesPerSecond(max_io_mb * static_cast<int64_t>(MiB));
}

Status StorageManager::GetLatestPoints(const std::vector<uint64_t> &db_ids,
                                       std::vector<kv::datanode::v1::LatestPoint> *result) {
  std::shared_lock<std::shared_mutex> lk(mutex_);
  for (auto &id : db_ids) {
    // check db_id
    auto it = store_map_.find(id);
    if (it == store_map_.end()) {
      return {Status::NotOK, fmt::format("id {} id not belongings to me", id)};
    }

    // get latest points
    auto seq_id = it->second->GetDB()->GetLatestSequenceNumber();
    auto res = it->second->GetReplIdFromDbEngine();
    if (!res.IsOK()) {
      LOG(WARNING) << "[storage manager] Failed to get repl Id, err: " << res.Msg();
      return res.ToStatus();
    }
    auto repl_id = res.GetValue();

    // set result
    kv::datanode::v1::LatestPoint point;
    point.set_db_id(id);
    point.set_seq_id(seq_id);
    point.set_repl_id(repl_id);
    result->emplace_back(std::move(point));
  }

  return Status::OK();
}

Status StorageManager::GetWalDataWithCmd(uint64_t db_id, uint64_t *next_seq,
                                         std::vector<std::vector<std::string>> *result, bool *is_finished) {
  std::shared_lock<std::shared_mutex> lk(mutex_);
  // check parameters
  auto it = store_map_.find(db_id);
  if (it == store_map_.end()) {
    return {Status::NotOK, fmt::format("db_id {} is not found", db_id)};
  }
  auto storage = it->second;
  auto latest_seq = storage->GetDB()->GetLatestSequenceNumber();
  if (*next_seq == latest_seq + 1) {
    *is_finished = true;
    return Status::OK();
  }
  if (*next_seq > latest_seq + 1) {
    return {Status::NotOK, fmt::format("wrong sequence {}, latest sequence is {}", *next_seq, latest_seq)};
  }

  // get data from wal
  std::unique_ptr<rocksdb::TransactionLogIterator> iter = nullptr;
  auto s = storage->GetWALIter(*next_seq, &iter);
  if (!s.IsOK()) {
    LOG(WARNING) << "[storage] Failed to create wal iterator, err: " << s.Msg();
    return s;
  }

  size_t max_cmds_limit = 2000;
  while (iter->Valid()) {
    auto batch = iter->GetBatch();
    if (batch.sequence != *next_seq) {
      LOG(WARNING) << "[storage] Unmachted batch seq: " << batch.sequence << ", input seq: " << *next_seq;
      return {Status::NotOK, fmt::format("Unmatched sequence, input:{}, expected:{}", *next_seq, batch.sequence)};
    }
    LOG(INFO) << "[storage] batch sequence: " << batch.sequence;
    WriteBatchExtractor write_batch_extractor;
    auto status = batch.writeBatchPtr->Iterate(&write_batch_extractor);
    if (!status.ok()) {
      LOG(WARNING) << "[storage] Failed to parse writebatch, err: " << status.ToString();
      return {Status::NotOK, status.ToString()};
    }

    for (auto &token : write_batch_extractor.GetCommands()) {
      result->emplace_back(std::move(token));
    }

    *next_seq = batch.sequence + batch.writeBatchPtr->Count();
    if (*next_seq > latest_seq) {
      break;
    }

    // read only 2000 cmds each time
    if (result->size() >= max_cmds_limit) break;
    iter->Next();
  }

  if (*next_seq == latest_seq + 1) {
    *is_finished = true;
  }

  return Status::OK();
}

}  // namespace engine
