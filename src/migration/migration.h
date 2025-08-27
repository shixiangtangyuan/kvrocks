#pragma once

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <kv/datanode/v1/service.grpc.pb.h>

#include <atomic>
#include <shared_mutex>
#include <string>

#include "common/status.h"
#include "common/timeout_manager.h"

class Server;

namespace redis {

class Migration {
 public:
  struct TaskInfo {
    std::atomic<uint64_t> task_id = 0;
    std::unordered_map<std::string, std::set<std::string>> src_datanodes;  // <datanode_id, slot_range_names>
    std::string dst_datanode_id;
    int64_t timeout_point_ms = 0;
    bool need_replicate_data = true;  // for failover force

    TaskInfo() = default;

    TaskInfo(const TaskInfo &info)
        : task_id(info.task_id.load()),
          src_datanodes(info.src_datanodes),
          dst_datanode_id(info.dst_datanode_id),
          timeout_point_ms(info.timeout_point_ms),
          need_replicate_data(info.need_replicate_data) {}

    TaskInfo &operator=(const TaskInfo &info) {
      task_id.store(info.task_id.load());
      src_datanodes = info.src_datanodes;
      dst_datanode_id = info.dst_datanode_id;
      timeout_point_ms = info.timeout_point_ms;
      need_replicate_data = info.need_replicate_data;
      return *this;
    }

    bool operator==(const TaskInfo &o) const {
      return (task_id == o.task_id && src_datanodes == o.src_datanodes && dst_datanode_id == o.dst_datanode_id &&
              timeout_point_ms == o.timeout_point_ms && need_replicate_data == o.need_replicate_data);
    }
  };

  explicit Migration(Server *srv) : svr_(srv) {}
  ~Migration() { LOG(INFO) << "Migration destruction"; }

  uint64_t TaskId() const { return info_.task_id.load(); }
  bool TaskHasDone() const;

  // start task
  Status StartMigrateWithReplData(const TaskInfo &task_info);
  Status StartMigrateWithoutReplData(const TaskInfo &task_info);

  // stop task and handle failures
  void Failed(kv::datanode::v1::Error error);
  void SetFailed(const kv::datanode::v1::Error &error) {
    Failed(error);
    result_ = kv::controller::v1::Datanode::MIGRATION_RESULT_FAIL;
  }

  // timeout task
  static void TimeoutCb(TaskCallbackArg arg);
  void ResetTimeoutTask() { timeout_task_.reset(); }

  // task control
  void SetIsTopoUpdating(bool is_topo_updating) {
    std::lock_guard<std::shared_mutex> guard(shared_mutex_);
    is_topo_updating_ = is_topo_updating;
  }
  void ReceivedNewTopo();

  kv::controller::v1::Datanode::MigrationResult Result() const { return result_.load(); }

  std::string To_String() {
    std::ostringstream stream;
    stream << "Current task_id: " << info_.task_id << ", src_node_id(s): ";
    for (const auto &src : info_.src_datanodes) stream << src.first << ",";
    stream << " result: " << kv::controller::v1::Datanode_MigrationResult_Name(result_.load())
           << ", need_replicate_data: " << (info_.need_replicate_data ? "YES" : "NO");
    return stream.str();
  }

  std::pair<uint64_t, kv::controller::v1::Datanode::MigrationResult> GetProgressInfo() {
    std::shared_lock<std::shared_mutex> lk(progress_mutex_);
    return std::make_pair(info_.task_id.load(), result_.load());
  }

 private:
  FRIEND_TEST(FailoverTest, RPCTest);
  FRIEND_TEST(FailoverTest, ForceBasic);
  FRIEND_TEST(FailoverTest, ForceWithTopoUpdate);
  FRIEND_TEST(FailoverTest, FailoverBasic);
  FRIEND_TEST(FailoverTest, FailoverReplicationCallback);

  FRIEND_TEST(MigrationTest, WithoutReplDataBasic);
  FRIEND_TEST(MigrationTest, WithoutReplData);
  FRIEND_TEST(MigrationTest, WithReplDataBasic);
  FRIEND_TEST(MigrationTest, WithReplData);
  FRIEND_TEST(MigrationTest, WithReplDataError);

  FRIEND_TEST(ScaleInterfaceTest, OneSource);
  FRIEND_TEST(ScaleInterfaceTest, MultiSource);

  bool canCreateNewJob() const {
    // NOTE(mingfo): Don't allow task retry in case of previous task failure
    return (result_ == kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED) && !is_topo_updating_;
  }
  void reset();
  void resetWithLocked(const TaskInfo &task_info, kv::controller::v1::Datanode::MigrationResult result);
  Status checkMigrateWithLock(const TaskInfo &task_info) const;
  void replicationCb();
  void replStopSrcWriteCb(std::string slot_range_name);

  Server *svr_{nullptr};

  mutable std::shared_mutex shared_mutex_;
  mutable std::shared_mutex progress_mutex_;
  TaskInfo info_;
  std::atomic<kv::controller::v1::Datanode::MigrationResult> result_ =
      kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED;
  std::shared_ptr<util::TimeoutTask> timeout_task_;
  bool is_topo_updating_{false};
  std::unordered_set<std::string> in_loggap_slot_ranges_;
};

}  // namespace redis
