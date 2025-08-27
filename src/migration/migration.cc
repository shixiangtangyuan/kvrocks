#include "migration.h"

#include "cluster/sync_manager.h"
#include "common/scope_exit.h"
#include "common/time_util.h"
#include "server/server.h"
#include "sync/sync_puller.h"

#define MIG_INFO LOG(INFO) << "[migration] "
#define MIG_WARNING LOG(WARNING) << "[migration] "
#define MIG_ERROR LOG(ERROR) << "[migration] "
#define MIG_FATAL LOG(FATAL) << "[migration] "

namespace redis {

void Migration::reset() {
  MIG_INFO << "reset migration. task_id: " << info_.task_id;
  if (timeout_task_) {
    svr_->timeout_mgr->ResetTimeoutTask(timeout_task_);
    timeout_task_.reset();
  }

  std::unique_lock<std::shared_mutex> lk(progress_mutex_);
  info_.task_id = 0;
  info_.src_datanodes.clear();
  info_.dst_datanode_id = "";
  info_.timeout_point_ms = 0;
  info_.need_replicate_data = true;
  result_ = kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED;
}

void Migration::resetWithLocked(const Migration::TaskInfo &info, kv::controller::v1::Datanode::MigrationResult result) {
  if (timeout_task_) {
    svr_->timeout_mgr->ResetTimeoutTask(timeout_task_);
    timeout_task_.reset();
  }

  std::unique_lock<std::shared_mutex> lk(progress_mutex_);
  info_ = info;
  result_ = result;
  // NOTE(yanling.chen) is_block_start_ do not need to reset; will change it by cluster
}

// mean task has completed. MIGRATION_RESULT_FINISH mean replication finish, not completed
bool Migration::TaskHasDone() const {
  return result_ == kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED ||
         result_ == kv::controller::v1::Datanode::MIGRATION_RESULT_FAIL ||
         result_ == kv::controller::v1::Datanode::MIGRATION_RESULT_TIMEOUT ||
         result_ == kv::controller::v1::Datanode::MIGRATION_RESULT_STOP_WRITE_TIMEOUT;
}

Status Migration::checkMigrateWithLock(const TaskInfo &task_info) const {
  if (!svr_->cluster->TopoHasInited()) {
    return {Status::ClusterInvalidInfo, "cluster topo is not inited"};
  }
  if (canCreateNewJob()) {
    if (task_info.need_replicate_data) {
      for (const auto &src : task_info.src_datanodes) {
        auto checkRet = svr_->cluster->CanSyncPullDataSamePool(src.first);
        if (checkRet.has_value()) {
          MIG_ERROR << "Check if can sync pull from src: " << src.first << ", errcode: " << checkRet.value().code()
                    << ", msg: " << checkRet.value().message();
          return {Status::ClusterInvalidInfo, "cluster CanSyncPullDataSamePool check failed"};
        }
      }
    }
    return Status::OK();
  } else if (task_info.task_id == info_.task_id) {
    if (!(info_ == task_info)) {
      return {Status::AnotherMigrationDoing, "another migration is doing with same task id"};
    }
    // same taskid, and same taskinfo, return MigrationReentrant
    return {Status::MigrationReentrant, "MigrationReentrant"};
  }
  return {Status::AnotherMigrationDoing, "another migration is doing with different task id"};
}

Status Migration::StartMigrateWithoutReplData(const Migration::TaskInfo &task_info) {
  MIG_INFO << "Start data migration for task_id: " << task_info.task_id;

  std::unique_lock<std::shared_mutex> lk(shared_mutex_);
  auto ret = checkMigrateWithLock(task_info);
  if (ret.IsOK()) {
    resetWithLocked(task_info, kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH);
    auto timeout_task = svr_->timeout_mgr->AddTimeoutTask(task_info.timeout_point_ms - int64_t(util::GetTimeStampMS()),
                                                          TimeoutCb, this);
    timeout_task_ = std::move(timeout_task);
    if (svr_->cluster->IsActivePool()) {
      auto s = svr_->cluster->SetClientWriteRunningStatusWrite();
      if (!s.IsOK()) {
        MIG_ERROR << s.Msg() << ". " << To_String();
        SetFailed(kMigrationTaskFailedError);
        return s;
      }
    }
    svr_->ctrl_rpc_client->SendHeartbeatImmediately();
    MIG_INFO << "Start migration without replicating data succ. " << To_String();
    return Status::OK();
  } else if (ret.GetCode() == Status::MigrationReentrant) {
    MIG_INFO << "Start migration without replicating data reentrant. " << To_String();
    return Status::OK();
  } else if (ret.GetCode() == Status::AnotherMigrationDoing) {
    MIG_INFO << "Migration task is doing. received new task_id: " << task_info.task_id << "; " << To_String();
    return ret;
  }
  MIG_ERROR << ret.Msg();
  return ret;
}

Status Migration::StartMigrateWithReplData(const Migration::TaskInfo &task_info) {
  MIG_INFO << "Start migration with replicating data. task_id: " << task_info.task_id;
  auto start = std::chrono::steady_clock::now();
  auto exit = MakeScopeExit([&start]() {
    auto duration_ms = util::GetDurationMSSince(start);
    if (duration_ms > 1000) {
      MIG_WARNING << "Start migration task too slow, duration=" << duration_ms << "ms";
    } else {
      MIG_INFO << "Start migration task done, duration=" << duration_ms << "ms";
    }
  });
  std::unique_lock<std::shared_mutex> lk(shared_mutex_);
  auto check_ret = checkMigrateWithLock(task_info);
  if (check_ret.GetCode() == Status::MigrationReentrant) {
    MIG_INFO << "Migration task reentrant. " << To_String();
    return Status::OK();
  }
  if (!check_ret.IsOK()) {
    MIG_INFO << "Migration is doing. received new task_id: " << task_info.task_id << "; " << To_String();
    return check_ret;
  }

  resetWithLocked(task_info, kv::controller::v1::Datanode::MIGRATION_RESULT_DOING);
  MIG_INFO << "Set migration task succeed";
  auto timeout_task =
      svr_->timeout_mgr->AddTimeoutTask(task_info.timeout_point_ms - int64_t(util::GetTimeStampMS()), TimeoutCb, this);
  MIG_INFO << "Add timeout task succeed, timeout_point_ms=" << task_info.timeout_point_ms;
  timeout_task_ = std::move(timeout_task);
  SlotRangeWriteStoppableCB write_stoppable_cb = [this](const std::string &slot_range_name) {
    this->replStopSrcWriteCb(slot_range_name);
    return;
  };
  SlotRangeReplicationDoneCB repl_cb = [this]() {
    this->replicationCb();
    return;
  };
  for (const auto &src : task_info.src_datanodes) {
    // Get src datanode's grpc addr
    auto ret = svr_->cluster->GetDatanodeGrpcAddr(src.first);
    if (!ret.IsOK()) {
      SetFailed(kMigrationTaskFailedError);
      MIG_ERROR << "Local(src) datanode id not in topo, id: " << src.first << "; " << To_String();
      return ret.ToStatus();
    }
    MIG_INFO << "Get grpc addr succeed, node_id=" << src.first << ", grpc_addr=" << ret.GetValue();
    // Get all slot_ranges from src datanode
    std::unordered_map<std::string, std::shared_ptr<SlotRange>> slot_ranges;
    auto s = svr_->cluster->CheckAndGetSlotRangesServedByMySelf(src.second, &slot_ranges);
    if (!s.IsOK()) {
      MIG_ERROR << s.Msg() << "; " << To_String();
      SetFailed(kMigrationTaskFailedError);
      return s;
    }
    MIG_INFO << "Get local slot ranges succeed";
    // Check slot_ranges' serving datanode matches src datanode
    for (const auto &sr_mem : slot_ranges) {
      if (src.first != sr_mem.second->GetServingNodeId()) {
        MIG_ERROR << fmt::format("src id: {} is not {} serving datanode: {}", src.first, sr_mem.second->GetName(),
                                 sr_mem.second->GetServingNodeId())
                  << "; " << To_String();
        SetFailed(kMigrationTaskFailedError);
        return Status{Status::NotOK, "Src datanode can't match serving datanode"};
      }
    }
    // Create pullers
    auto status = svr_->sync_manager->CreateReplPuller(svr_->cluster->ClusterId(), task_info.dst_datanode_id, src.first,
                                                       ret.GetValue(), svr_->shared_from_this(), slot_ranges,
                                                       write_stoppable_cb, repl_cb);
    if (!status.IsOK()) {
      MIG_ERROR << "Failed to create replication puller. err: " << status.Msg() << "; " << To_String();
      SetFailed(kMigrationTaskFailedError);
      return status;
    }
    MIG_INFO << "Create replica puller succeed";

    // NOTE(yanling.chen) MUST check cluster after puller is added to syncManager;
    // topo may be changed between check cluster and add to syncManager
    auto double_check_ret = svr_->cluster->CanSyncPullDataSamePool(src.first);
    if (double_check_ret.has_value()) {
      SetFailed(kClusterTopologyChangedError);
      MIG_INFO << "start migration failed: CanSyncPullDataSamePool double check failed; " << To_String();
      return {Status::ClusterInvalidInfo, "cluster CanSyncPullDataSamePool double check failed"};
    }
    MIG_INFO << "Recheck sync pullable succeed";
  }
  MIG_INFO << "Start migration suc. " << To_String();
  return Status::OK();
}

void Migration::TimeoutCb(TaskCallbackArg arg) {
  auto migration = static_cast<Migration *>(arg);
  MIG_INFO << "Migration timeout start, " << migration->To_String();
  std::lock_guard<std::shared_mutex> guard(migration->shared_mutex_);
  migration->ResetTimeoutTask();
  if (migration->TaskHasDone()) {
    MIG_INFO << "Migration timeout while task has done, " << migration->To_String();
    return;
  }
  MIG_INFO << "Migration failed for task timeout, " << migration->To_String();
  migration->Failed(kMigrationTaskFailedError);
  migration->result_ = kv::controller::v1::Datanode::MIGRATION_RESULT_TIMEOUT;
  migration->svr_->ctrl_rpc_client->SendHeartbeatImmediately();
}

void Migration::replicationCb() {
  MIG_INFO << "Migration replication callback. current result:"
           << kv::controller::v1::Datanode_MigrationResult_Name(result_.load()) << "; " << To_String();

  std::unique_lock<std::shared_mutex> lk(shared_mutex_);
  if (TaskHasDone()) {
    MIG_INFO << "Migration replication callback. task has done. result:"
             << kv::controller::v1::Datanode_MigrationResult_Name(result_.load());
    return;
  }
  auto result = svr_->cluster->SlotRangesReplicationResult();
  if (result == result_) {
    return;
  }
  MIG_INFO << "Migration replication callback from "
           << kv::controller::v1::Datanode_MigrationResult_Name(result_.load()) << " to "
           << kv::controller::v1::Datanode_MigrationResult_Name(result) << "; " << To_String();

  switch (result) {
    case kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED:
    case kv::controller::v1::Datanode::MIGRATION_RESULT_DOING:
      break;
    case kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH:
      if (svr_->cluster->IsActivePool()) {
        auto s = svr_->cluster->SetClientWriteRunningStatusWrite();
        if (!s.IsOK()) {
          MIG_ERROR << "Migration callback failed to set running status while status is swithing to FINISH, err: "
                    << s.Msg();
          result = kv::controller::v1::Datanode::MIGRATION_RESULT_FAIL;
          Failed(kMigrationTaskFailedError);
        }
      }
      break;
    case kv::controller::v1::Datanode::MIGRATION_RESULT_FAIL:
    case kv::controller::v1::Datanode::MIGRATION_RESULT_TIMEOUT:
    case kv::controller::v1::Datanode::MIGRATION_RESULT_STOP_WRITE_TIMEOUT:
      MIG_ERROR << "Migration callback swithing result to "
                << kv::controller::v1::Datanode_MigrationResult_Name(result);
      Failed(kMigrationTaskFailedError);
      break;
    default:
      MIG_ERROR << "new migration result is wrong: " << kv::controller::v1::Datanode_MigrationResult_Name(result)
                << "; " << To_String();
      break;
  }
  MIG_INFO << "Migration replication swithing result from "
           << kv::controller::v1::Datanode_MigrationResult_Name(result_.load()) << " to "
           << kv::controller::v1::Datanode_MigrationResult_Name(result);
  result_ = result;
  if (result_ != kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED &&
      result_ != kv::controller::v1::Datanode::MIGRATION_RESULT_DOING) {
    svr_->ctrl_rpc_client->SendHeartbeatImmediately();
  }
}

void Migration::replStopSrcWriteCb(std::string slot_range_name) {
  MIG_INFO << "Migration call stop src write callback for " << slot_range_name;
  std::lock_guard<std::shared_mutex> guard(shared_mutex_);
  in_loggap_slot_ranges_.emplace(std::move(slot_range_name));
  if (in_loggap_slot_ranges_.size() == svr_->cluster->LocalSlotRanges().size()) {
    svr_->sync_manager->StopSrcWriteOfAllReplPuller();
  }
}

void Migration::ReceivedNewTopo() {
  std::lock_guard<std::shared_mutex> guard(shared_mutex_);
  MIG_INFO << "received new topo." << To_String();
  Failed(kClusterTopologyChangedError);
  svr_->cluster->ClearReplicationStatus();
  reset();
}

void Migration::Failed(kv::datanode::v1::Error error) {
  MIG_INFO << "Migration start to do failed, error: " << error << "; " << To_String();
  if (info_.need_replicate_data) {
    svr_->sync_manager->ClearAllReplPullers(error);
  }
  if (timeout_task_) {
    svr_->timeout_mgr->ResetTimeoutTask(timeout_task_);
    timeout_task_.reset();
  }
  // NOTE: DO NOT need to clear running status in slave;
  // just clear running status in master when sender closed.
}

}  // namespace redis
