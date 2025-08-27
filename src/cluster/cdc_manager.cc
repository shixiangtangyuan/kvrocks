#include "cluster/cdc_manager.h"

#include "cdc/cdc_sender.h"
#include "common/sync_status.h"

namespace redis {

OptionalSyncError CDCManager::AddCDCSender(const std::string& slot_range, CDCSender* cdc_sender, bool is_take_over) {
  std::unique_lock<std::mutex> lk(mutex_);

  if (is_topo_updating_) {
    return UnknownSyncError("topo is updating");
  }
  auto iter = cdc_senders_.find(slot_range);
  if (iter != cdc_senders_.end()) {
    if (!is_take_over) {
      return kSyncStreamExistedError;
    }
    auto err = UnknownSyncError("stream take over by others");
    iter->second->MarkFinished(err);
    iter->second->StopIterData();
    cdc_senders_.erase(iter);
    LOG(INFO) << "[cdc manager] Clear cdc sender succeed, slot_range:" << slot_range << ", finish_err:" << err;
  }

  LOG(INFO) << "[cdc manager] Add cdc sender succeed, slot_range:" << slot_range;
  cdc_senders_.emplace(slot_range, cdc_sender);
  return std::nullopt;
}

void CDCManager::RemoveCDCSender(const std::string& slot_range, CDCSender* cdc_sender, const OptionalSyncError& err) {
  std::unique_lock<std::mutex> lk(mutex_);

  auto iter = cdc_senders_.find(slot_range);
  if (iter != cdc_senders_.end() && iter->second == cdc_sender) {
    cdc_senders_.erase(iter);
    LOG(INFO) << "[cdc manager] Remove cdc sender succeed, slot_range:" << slot_range << ", finish_err:" << err;
  }
}

void CDCManager::ClearAllForSlotRange(const std::string& slot_range, const OptionalSyncError& err) {
  std::unique_lock<std::mutex> lk(mutex_);

  auto iter = cdc_senders_.find(slot_range);
  if (iter != cdc_senders_.end()) {
    iter->second->MarkFinished(err);
    iter->second->StopIterData();
    cdc_senders_.erase(iter);
    LOG(INFO) << "[cdc manager] Clear cdc sender succeed, slot_range:" << slot_range << ", finish_err:" << err;
  }
}

void CDCManager::ClearAll(const OptionalSyncError& err) {
  std::unique_lock<std::mutex> lk(mutex_);

  for (auto& [slot_range, cdc_sender] : cdc_senders_) {
    cdc_sender->MarkFinished(err);
  }
  for (auto& [slot_range, cdc_sender] : cdc_senders_) {
    cdc_sender->StopIterData();
    LOG(INFO) << "[cdc manager] Clear cdc sender succeed, slot_range:" << slot_range << ", finish_err:" << err;
  }
  cdc_senders_.clear();
}

}  // namespace redis
