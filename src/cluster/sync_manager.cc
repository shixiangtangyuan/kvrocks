#include "sync_manager.h"

#include <mutex>

#include "cluster/cluster.h"
#include "grpcpp/security/credentials.h"
#include "server/grpc_interceptor.h"
#include "sync/dts_sender.h"
#include "sync/repl_sender.h"
#include "sync/sync_puller.h"
#include "sync/sync_receiver.h"

namespace redis {

using kv::datanode::v1::DataNodeService;

SyncManager::SyncManager() = default;

SyncManager::~SyncManager() {
  ClearAll(std::nullopt);
  LOG(INFO) << "~SyncManager";
}

Status SyncManager::AddReplSender(const std::string& slot_range, ReplSender* repl_sender) {
  std::lock_guard<std::mutex> g(mu_);
  if (is_topo_updating_) {
    LOG(WARNING) << "Cannot add repl sender when topo is updating";
    return Status{Status::NotOK, "topo updating"};
  }
  if (repl_sender_.find(slot_range) != repl_sender_.end()) {
    LOG(ERROR) << "There exists a replication sender for " << slot_range;
    return Status{Status::NotOK, "There exists a replication sender for " + slot_range};
  }
  repl_sender_.emplace(slot_range, repl_sender);
  LOG(INFO) << "Added replication sender for " << slot_range;
  return Status::OK();
}

Status SyncManager::addReplPuller(const std::string& slot_range, SyncPuller* repl_puller) {
  std::lock_guard<std::mutex> g(mu_);
  if (is_topo_updating_) {
    LOG(WARNING) << "Cannot add repl puller when topo is updating";
    return Status{Status::NotOK, "topo updating"};
  }
  if (repl_puller_.find(slot_range) != repl_puller_.end()) {
    LOG(ERROR) << "There exists a replication puller for " << slot_range;
    return Status{Status::NotOK, "There exists a replication puller for " + slot_range};
  }
  // puller and receiver can't exist at the same time
  if (!dts_receiver_.empty()) {
    LOG(ERROR) << "Cannot add replication puller while dts receiver exists for " << slot_range;
    return Status{Status::NotOK, "Failed to add replication puller while dts receiver exists"};
  }
  repl_puller_.emplace(slot_range, repl_puller);
  LOG(INFO) << "Added replication puller for " << slot_range;
  return Status::OK();
}

Status SyncManager::AddDtsSender(const std::string& slot_range, DtsSender* dts_sender) {
  std::lock_guard<std::mutex> g(mu_);
  if (is_topo_updating_) {
    LOG(WARNING) << "Cannot add dts sender when topo is updating";
    return Status{Status::NotOK, "topo updating"};
  }
  if (dts_sender_.find(slot_range) != dts_sender_.end()) {
    LOG(ERROR) << "There exists a dts sender for " << slot_range;
    return Status{Status::NotOK, "There exists a dts sender for " + slot_range};
  }
  dts_sender_.emplace(slot_range, dts_sender);
  LOG(INFO) << "Added dts sender for " << slot_range;
  return Status::OK();
}

Status SyncManager::AddDtsReceiver(const std::string& slot_range, SyncReceiver* dts_receiver) {
  std::lock_guard<std::mutex> g(mu_);
  if (is_topo_updating_) {
    LOG(WARNING) << "Cannot add dts receiver when topo is updating";
    return Status{Status::NotOK, "topo updating"};
  }
  if (dts_receiver_.find(slot_range) != dts_receiver_.end()) {
    LOG(ERROR) << "There exists a dts sender for " << slot_range;
    return Status{Status::NotOK, "There exists a dts sender for " + slot_range};
  }
  // puller and receiver can't exist at the same time
  if (!repl_puller_.empty()) {
    LOG(ERROR) << "Cannot add dts receiver while replication puller exists for " << slot_range;
    return Status{Status::NotOK, "Failed to add dts receiver while replication puller exists"};
  }
  dts_receiver_.emplace(slot_range, dts_receiver);
  LOG(INFO) << "Added dts receiver for " << slot_range;
  return Status::OK();
}

void SyncManager::StopAndJoinReplPuller(SyncPuller* repl_puller, const std::optional<kv::datanode::v1::Error>& error) {
  repl_puller->MarkFinished(error);
  repl_puller->StopApplyData();  // join to wait puller stopped
}

void SyncManager::StopAndJoinDtsReceiver(const std::string& slot_range,
                                         const std::optional<kv::datanode::v1::Error>& error) {
  std::lock_guard<std::mutex> g(mu_);
  auto it = dts_receiver_.find(slot_range);
  if (it == dts_receiver_.end()) {
    LOG(WARNING) << "No dts receiver for " << slot_range;
    return;
  }
  it->second->MarkFinished(error);
  it->second->StopApplyData();  // join to wait receiver stopped
}

void SyncManager::StopAndJoinDtsSender(const std::string& slot_range,
                                       const std::optional<kv::datanode::v1::Error>& error) {
  std::lock_guard<std::mutex> g(mu_);
  auto it = dts_sender_.find(slot_range);
  if (it == dts_sender_.end()) {
    LOG(WARNING) << "No dts sender for " << slot_range;
    return;
  }
  LOG(INFO) << "stop and join  dts sender for " << slot_range;
  it->second->MarkFinished(error);
}

Status SyncManager::CreateReplPuller(const std::string& cluster_id, const std::string& puller_node_id,
                                     const std::string& sender_node_id, const std::string& sender_addr,
                                     const std::shared_ptr<Server>& srv,
                                     std::unordered_map<std::string, std::shared_ptr<SlotRange>>& slot_ranges,
                                     SlotRangeWriteStoppableCB write_stoppable_cb, SlotRangeReplicationDoneCB cb) {
  auto args = srv->GetConfig()->BuildDatanodeChannelArgs();
  std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>> interceptor_creators;
  interceptor_creators.push_back(std::make_unique<ClientStatsInterceptorFactory>());
  auto channel = grpc::experimental::CreateCustomChannelWithInterceptors(
      sender_addr, grpc::InsecureChannelCredentials(), args, std::move(interceptor_creators));
  if (channel == nullptr) {
    LOG(ERROR) << "Failed to create grpc channel, addr: " << sender_addr;
    return Status{Status::NotOK, "Failed to create channel for puller"};
  }
  LOG(INFO) << "Create grpc channel succeed, addr=" << sender_addr;

  auto stub_tmp = DataNodeService::NewStub(channel);
  if (stub_tmp == nullptr) {
    LOG(ERROR) << "Failed to create grpc stub for replication puller";
    return Status{Status::NotOK, "Failed to create stub"};
  }
  std::shared_ptr<kv::datanode::v1::DataNodeService::Stub> stub = std::move(stub_tmp);
  LOG(INFO) << "Create client stub succeed";

  for (auto& slotrange : slot_ranges) {
    auto ret = slotrange.second->GetSyncPoint();
    if (!ret.IsOK()) {
      LOG(ERROR) << "Failed to get sync point of slot_range:" << slotrange.first << ", err:" << ret.Msg();
      ClearAllReplPullers(UnknownSyncError("failed to get sync point of " + slotrange.first + ", err:" + ret.Msg()));
      return ret;
    }
    auto repl_puller = new SyncPuller(cluster_id, puller_node_id, sender_node_id, srv, slotrange.second, ret.GetValue(),
                                      stub, write_stoppable_cb, cb);
    if (repl_puller == nullptr) {
      LOG(ERROR) << "Failed to create replication puller for slot-range: " << slotrange.first;
      ClearAllReplPullers(UnknownSyncError("failed to create replication puller"));
      return Status{Status::NotOK, "Failed to careate replication puller"};
    }
    LOG(INFO) << "Create replica puller succeed";
    auto s = addReplPuller(slotrange.first, repl_puller);
    if (!s.IsOK()) {
      StopAndJoinReplPuller(repl_puller, UnknownSyncError("failed to add replication puller"));
      LOG(INFO) << "Stopped and joined replication puller of " << slotrange.first;
      return Status{Status::NotOK, "Failed to add replication puller"};
    }
    LOG(INFO) << "Add Replica puller succeed";
  }
  return Status::OK();
}

void SyncManager::RemoveReplSender(const std::string& slot_range, ReplSender* repl_sender,
                                   const std::optional<kv::datanode::v1::Error>& error) {
  std::lock_guard<std::mutex> g(mu_);
  auto it = repl_sender_.find(slot_range);
  if (it != repl_sender_.end() && it->second == repl_sender) {
    it->second->MarkFinished(error);
    LOG(INFO) << "SyncManager: stopped replication sender of slotrange: " << slot_range;
    repl_sender_.erase(slot_range);
    LOG(INFO) << "SyncManager: removed replication sender of slotrange: " << slot_range;
  }
}

void SyncManager::RemoveReplPuller(const std::string& slot_range, SyncPuller* repl_puller,
                                   const std::optional<kv::datanode::v1::Error>& error) {
  std::lock_guard<std::mutex> g(mu_);
  auto it = repl_puller_.find(slot_range);
  if (it != repl_puller_.end() && it->second == repl_puller) {
    it->second->MarkFinished(error);
    LOG(INFO) << "SyncManager: stopped replication puller of slotrange: " << it->first;
    // Invoked by stream itself, join is unnecessary
    repl_puller_.erase(slot_range);
    LOG(INFO) << "SyncManager: removed replication puller of slotrange: " << slot_range;
  }
}

void SyncManager::RemoveDtsSender(const std::string& slot_range, DtsSender* dts_sender,
                                  const std::optional<kv::datanode::v1::Error>& error) {
  std::lock_guard<std::mutex> g(mu_);
  auto it = dts_sender_.find(slot_range);
  if (it != dts_sender_.end() && it->second == dts_sender) {
    it->second->MarkFinished(error);
    LOG(INFO) << "SyncManager: stopped dts sender of slotrange: " << it->first;
    dts_sender_.erase(slot_range);
    LOG(INFO) << "SyncManager: removed dts sender of slotrange: " << slot_range;
  }
}

void SyncManager::RemoveDtsReceiver(const std::string& slot_range, SyncReceiver* dts_receiver,
                                    const std::optional<kv::datanode::v1::Error>& error) {
  std::lock_guard<std::mutex> g(mu_);
  auto it = dts_receiver_.find(slot_range);
  if (it != dts_receiver_.end() && it->second == dts_receiver) {
    it->second->MarkFinished(error);
    LOG(INFO) << "SyncManager: stopped dts receiver of slotrange: " << it->first;
    // Invoked by stream itself, join is unnecessary
    dts_receiver_.erase(slot_range);
    LOG(INFO) << "SyncManager: removed dts receiver of slotrange: " << slot_range;
  }
}

void SyncManager::ClearAllReplSenders(const std::optional<kv::datanode::v1::Error>& error, bool need_lock) {
  std::unique_lock<std::mutex> lock{mu_, std::defer_lock};
  if (need_lock) {
    lock.lock();
  }
  if (!repl_sender_.empty()) {
    for (auto it = repl_sender_.begin(); it != repl_sender_.end(); it++) {
      it->second->MarkFinished(error);
      LOG(INFO) << "SyncManager: stopped replication sender of slotrange: " << it->first;
    }
    for (auto it = repl_sender_.begin(); it != repl_sender_.end(); it++) {
      it->second->StopIterData();
      repl_sender_.erase(it->first);
      LOG(INFO) << "SyncManager: removed replication sender of slotrange: " << it->first;
    }
  }
}

void SyncManager::ClearAllReplPullers(const std::optional<kv::datanode::v1::Error>& error, bool need_lock) {
  std::unique_lock<std::mutex> lock{mu_, std::defer_lock};
  if (need_lock) {
    lock.lock();
  }
  if (!repl_puller_.empty()) {
    // mark all stream stopped
    for (auto it = repl_puller_.begin(); it != repl_puller_.end(); it++) {
      it->second->MarkFinished(error);
      LOG(INFO) << "SyncManager: stopped replication puller of slotrange: " << it->first;
    }
    // clear all stream concurrently
    for (auto it = repl_puller_.begin(); it != repl_puller_.end(); it++) {
      it->second->StopApplyData();
      repl_puller_.erase(it->first);
      LOG(INFO) << "SyncManager: removed replication puller of slotrange: " << it->first;
    }
  }
}

void SyncManager::StopAndJoinAllReplPullers(const std::optional<kv::datanode::v1::Error>& error) {
  std::lock_guard<std::mutex> g(mu_);
  for (auto it = repl_puller_.begin(); it != repl_puller_.end(); it++) {
    it->second->MarkFinished(error);
    it->second->StopApplyData();
    LOG(INFO) << "SyncManager: stopped replication puller of slotrange: " << it->first;
  }
}

void SyncManager::ClearAllDtsSenders(const std::optional<kv::datanode::v1::Error>& error, bool need_lock) {
  std::unique_lock<std::mutex> lock{mu_, std::defer_lock};
  if (need_lock) {
    lock.lock();
  }
  if (!dts_sender_.empty()) {
    for (auto it = dts_sender_.begin(); it != dts_sender_.end(); it++) {
      it->second->MarkFinished(error);
      LOG(INFO) << "SyncManager: stopped dts sender of slotrange: " << it->first;
    }
    for (auto it = dts_sender_.begin(); it != dts_sender_.end(); it++) {
      dts_receiver_.erase(it->first);
      LOG(INFO) << "SyncManager: removed dts sender of slotrange: " << it->first;
    }
  }
}

void SyncManager::ClearAllDtsReceivers(const std::optional<kv::datanode::v1::Error>& error, bool need_lock) {
  std::unique_lock<std::mutex> lock{mu_, std::defer_lock};
  if (need_lock) {
    lock.lock();
  }
  if (!dts_receiver_.empty()) {
    // mark all stream stopped
    for (auto it = dts_receiver_.begin(); it != dts_receiver_.end(); it++) {
      it->second->MarkFinished(error);
      LOG(INFO) << "SyncManager: stopped dts receiver of slotrange: " << it->first;
    }
    // clear all stream concurrently
    for (auto it = dts_receiver_.begin(); it != dts_receiver_.end(); it++) {
      it->second->StopApplyData();
      dts_receiver_.erase(it->first);
      LOG(INFO) << "SyncManager: removed dts receiver of slotrange: " << it->first;
    }
  }
}

void SyncManager::ClearAll(const std::optional<kv::datanode::v1::Error>& error) {
  std::lock_guard<std::mutex> g(mu_);
  ClearAllReplSenders(error, false);
  ClearAllReplPullers(error, false);
  ClearAllDtsSenders(error, false);
  ClearAllDtsReceivers(error, false);
}

void SyncManager::ClearAllOfOnSlotRange(const std::string& slot_range,
                                        const std::optional<kv::datanode::v1::Error>& error) {
  std::lock_guard<std::mutex> g(mu_);
  {
    auto it = repl_sender_.find(slot_range);
    if (it != repl_sender_.end()) {
      it->second->MarkFinished(error);
      it->second->StopIterData();
      repl_sender_.erase(slot_range);
      LOG(INFO) << "Clear replication sender of " << slot_range;
    }
  }
  {
    auto it = repl_puller_.find(slot_range);
    if (it != repl_puller_.end()) {
      it->second->MarkFinished(error);
      it->second->StopApplyData();
      repl_puller_.erase(slot_range);
      LOG(INFO) << "Clear replication puller of " << slot_range;
    }
  }
  {
    auto it = dts_sender_.find(slot_range);
    if (it != dts_sender_.end()) {
      it->second->MarkFinished(error);
      it->second->StopIterData();
      dts_sender_.erase(slot_range);
      LOG(INFO) << "Clear dts sender of " << slot_range;
    }
  }
  {
    auto it = dts_receiver_.find(slot_range);
    if (it != dts_receiver_.end()) {
      it->second->MarkFinished(error);
      it->second->StopApplyData();
      dts_receiver_.erase(slot_range);
      LOG(INFO) << "Clear dts receiver of " << slot_range;
    }
  }
}

void SyncManager::StopSrcWriteOfAllReplPuller() {
  std::lock_guard<std::mutex> guard(mu_);
  for (auto& item : repl_puller_) {
    LOG(INFO) << "Stop writing src " << item.first;
    item.second->StopWrite();
  }
}

}  // namespace redis
