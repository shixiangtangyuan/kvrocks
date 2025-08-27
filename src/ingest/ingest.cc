#include "ingest.h"

#include <dirent.h>
#include <fnmatch.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/statfs.h>

#include <filesystem>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "ingest/ingest_stats.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "status.h"
#include "storage/storage.h"
#include "time_util.h"
namespace ingest {
size_t GetFileSize(const std::filesystem::path &path) {
  try {
    return std::filesystem::file_size(path);
  } catch (...) {
    return 0;
  }
}

std::string JoinPath(std::string_view dir, std::string_view filename) {
  std::string buf;

  std::size_t len = dir.size();
  if (len == 0) {
    buf.resize(filename.size() + 2);
    strcpy(&(buf[0]), "./");  // NOLINT
    strncpy(&(buf[2]), filename.begin(), filename.size());
    return buf;
  }

  if (dir[len - 1] == '/') {
    buf.resize(len + filename.size());
    strncpy(&(buf[0]), dir.data(), len);
  } else {
    buf.resize(len + filename.size() + 1);
    strncpy(&(buf[0]), dir.data(), len);
    buf[len++] = '/';
  }

  strncpy(&(buf[len]), filename.data(), filename.size());

  return buf;
}

Status ListAllFilesInDir(const char *dirpath, bool return_full_path, const char *name_pattern,
                         std::vector<std::string> &entities, uint64_t *files_size) {
  struct dirent *dir_info = nullptr;
  DIR *dir = opendir(dirpath);
  if (dir == nullptr) {
    LOG(ERROR) << "Failed to read the directory \"" << dirpath << "\" (" << errno << "): " << strerror(errno);
    return {Status::IngestInvalidInfo, strerror(errno)};
  }

  while ((dir_info = readdir(dir)) != nullptr) {
    // Skip the "." and ".."
    if (!strcmp(dir_info->d_name, ".") || !strcmp(dir_info->d_name, "..")) {
      continue;
    }
    // Mismatched
    if (name_pattern && fnmatch(name_pattern, dir_info->d_name, FNM_FILE_NAME | FNM_PERIOD)) {
      continue;
    }

    // We found one entity
    entities.emplace_back(return_full_path ? JoinPath(dirpath, std::string(dir_info->d_name))
                                           : std::string(dir_info->d_name));
    *files_size += GetFileSize(entities.back());
  }

  return Status::OK();
}

Status Ingester::storeDBConfig(IngestTaskInfo &task_info) {
  auto storage = storage_.lock();
  if (!storage) {
    LOG(ERROR) << "get storage failed";
    return {Status::RedisExecErr, "get storage failed"};
  }

  task_info.org_enable_compact_range = storage->GetConfig()->enable_compact_range;
  task_info.org_enable_compact_full = storage->GetConfig()->enable_compact_full;
  if (storage->GetConfig()->duration_ingest_close_auto_compact) {
    task_info.org_disable_auto_compactions_str =
        storage->GetConfig()->rocks_db.disable_auto_compactions ? "true" : "false";
    auto s = storage->SetOptionForAllColumnFamilies("disable_auto_compactions", "true");
    if (!s.IsOK()) {
      LOG(ERROR) << "set rocksdb disable_auto_compaction true failed, reason: " << s.Msg();
      task_info.ingest_sucessed = false;
      task_info.failed_reason = IngestMonitor::Reason::IngestSetConfigFailedError;
      recordIngestStats(task_info.task_id, "Failed:" + s.Msg() + task_info.Detail());
      return s;
    } else {
      LOG(INFO) << "set rocksdb disable_auto_compaction true successed";
    }
  }

  LOG(INFO) << "orgin config enable_compact_range[" << storage->GetConfig()->enable_compact_range
            << "], enable_compact_full[" << storage->GetConfig()->enable_compact_full << "]";

  storage->GetConfig()->enable_compact_range = false;
  storage->GetConfig()->enable_compact_full = false;
  LOG(INFO) << "set config enable_compact_range and enable_compact_full to false";

  auto res = storage->CancleCompactDB();
  if (!res.IsOK()) {
    LOG(ERROR) << "cancle compact failed, reason:" << res.Msg();
    task_info.ingest_sucessed = false;
    task_info.failed_reason = IngestMonitor::Reason::IngestSetConfigFailedError;
    recordIngestStats(task_info.task_id, "Failed:" + res.Msg() + task_info.Detail());
    return res;
  }
  return Status::OK();
}

void Ingester::IngestDone(IngestTaskInfo &task_info) {
  auto storage = storage_.lock();
  if (!storage) {
    LOG(ERROR) << "get storage failed";
    return;
  }

  storage->GetConfig()->enable_compact_range = task_info.org_enable_compact_range;
  storage->GetConfig()->enable_compact_full = task_info.org_enable_compact_full;
  storage->GetIngester()->ResetFlag();
  LOG(INFO) << "recover orgin config enable_compact_range[" << storage->GetConfig()->enable_compact_range
            << "], enable_compact_full[" << storage->GetConfig()->enable_compact_full << "]";

  if (storage->GetConfig()->duration_ingest_close_auto_compact) {
    auto rs =
        storage->SetOptionForAllColumnFamilies("disable_auto_compactions", task_info.org_disable_auto_compactions_str);
    if (!rs.OK()) {
      LOG(ERROR) << "set rocksdb disable_auto_compactions to: " << task_info.org_disable_auto_compactions_str
                 << " failed, reason:" << rs.Msg();
    }
  }

  LOG(INFO) << "ingest done recover config successed";
}

// 1. check has ingest task and record task stats
// 2. storeDBConfig and set config
// 3. execute ingest
Status Ingester::Run(IngestTaskInfo &task_info) {
  auto storage = storage_.lock();
  if (!storage) {
    LOG(ERROR) << "get storage failed";
    return {Status::RedisExecErr, "get storage failed"};
  }

  GET_OR_RET(check(task_info));
  auto s = parseIngestArgs(task_info);
  if (!s.IsOK()) {
    LOG(ERROR) << "parse ingest args failed, " << s.Msg();
    task_info.ingest_sucessed = false;
    task_info.failed_reason = IngestMonitor::Reason::IngestInvalidArgsError;
    recordIngestStats(task_info.task_id, "Failed" + s.Msg() + task_info.Detail());
    return s;
  }
  recordIngestStats(task_info.task_id, "Running");

  GET_OR_RET(storeDBConfig(task_info));

  const rocksdb::Snapshot *snapshot = storage->GetDB()->GetSnapshot();
  auto ingest_start_time = util::GetTimeStampMS();
  rocksdb::Status rs = storage->GetDB()->IngestExternalFiles(task_info.args);
  task_info.ingest_duration_time_ms = util::GetTimeStampMS() - ingest_start_time;
  storage->GetDB()->ReleaseSnapshot(snapshot);
  if (!rs.ok()) {
    LOG(ERROR) << "rocksdb ingest failed, reason: " << rs.ToString();
    TaskStats value{"Failed, status:"};
    value.append(rs.ToString());
    task_info.ingest_sucessed = false;
    task_info.failed_reason = IngestMonitor::Reason::IngestRockDBError;
    recordIngestStats(task_info.task_id, value + task_info.Detail());
    return {Status::RedisExecErr, rs.ToString()};
  }

  LOG(INFO) << "Ingest task[" << task_info.task_id << "] Successed";
  recordIngestStats(task_info.task_id, "Successed" + task_info.Detail());
  return Status::OK();
}

Status Ingester::parseIngestArgs(IngestTaskInfo &task_info) {
  if (task_info.req_args.size() == 0) {
    return {Status::IngestInvalidInfo, errInvalidIngestInfo};
  }

  for (const auto &item : task_info.req_args) {
    if (!(std::get<0>(item) == engine::kMetadataColumnFamilyName ||
          std::get<0>(item) == engine::kSubkeyColumnFamilyName)) {
      LOG(ERROR) << "Ingest request args cf name[" << std::get<0>(item) << "] is invalid";
      return {Status::IngestInvalidInfo, errIllegalColumnFamily};
    }
  }

  // generate args
  rocksdb::IngestExternalFileOptions ifo;
  ifo.move_files = true;
  ifo.failed_move_fall_back_to_copy = false;
  ifo.verify_file_checksum = false;

  std::unordered_map<std::string, std::unique_ptr<rocksdb::IngestExternalFileArg>> arg_map;
  for (const auto &item : task_info.req_args) {
    std::string cf = std::get<0>(item);
    if (arg_map.find(cf) == arg_map.end()) {
      std::unique_ptr<rocksdb::IngestExternalFileArg> arg = std::make_unique<rocksdb::IngestExternalFileArg>();
      auto storage = storage_.lock();
      if (!storage) {
        LOG(ERROR) << "get storage failed";
        return {Status::RedisExecErr, "get storage failed"};
      }
      arg->column_family = storage->GetCFHandle(cf);
      arg->options = ifo;
      arg_map[cf] = std::move(arg);
    }

    std::vector<std::string> files;
    uint64_t files_size = 0;
    GET_OR_RET(ListAllFilesInDir(std::get<1>(item).c_str(), true, "*.sst", files, &files_size));
    task_info.sst_file_number += files.size();
    task_info.sst_file_size += files_size;

    arg_map[cf]->external_files = files;
  }

  for (auto &it : arg_map) {
    task_info.args.push_back(*it.second);
  }
  return Status::OK();
}

Status Ingester::check(IngestTaskInfo &task_info) {
  std::lock_guard<std::mutex> lg(mutex_lock_);
  if (ingesting_) {
    task_info.ingest_sucessed = false;
    task_info.failed_reason = IngestMonitor::Reason::IngestJobRunningError;
    return {Status::IngestJobRunning, "Ingest Job Running"};
  }

  ingesting_ = true;
  return Status::OK();
}

kv::datanode::v1::Error Ingester::GetResult(const std::string &task_id) {
  std::lock_guard<std::mutex> lg(mutex_lock_);
  auto iter = task_stats_.find(task_id);
  if (iter == task_stats_.end()) {
    return kIngestTaskNotFoundError;
  }
  auto value = iter->second;

  LOG(INFO) << "Ingest task" << value;
  if (value.find("Running") != std::string::npos) {
    return kIngestTaskRunningError;
  } else if (value.find("Successed") != std::string::npos) {
    return kIngestTaskFinishedError;
  }

  return kIngestTaskFailedError;
}

}  // namespace ingest
