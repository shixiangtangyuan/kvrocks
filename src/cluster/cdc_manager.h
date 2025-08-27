#pragma once

#include <gtest/gtest.h>

#include <mutex>
#include <unordered_map>

#include "common/sync_status.h"

namespace redis {

class CDCSender;

class CDCManager {
 public:
  OptionalSyncError AddCDCSender(const std::string&, CDCSender*, bool is_take_over);

  void RemoveCDCSender(const std::string&, CDCSender*, const OptionalSyncError&);

  void ClearAllForSlotRange(const std::string&, const OptionalSyncError&);

  void ClearAll(const OptionalSyncError& err);

  void SetIsTopoUpdating(bool is_topo_updating) {
    std::unique_lock<std::mutex> lk(mutex_);

    is_topo_updating_ = is_topo_updating;
  }

 private:
  FRIEND_TEST(CDCManager, Base);

  std::mutex mutex_;
  bool is_topo_updating_ = false;
  std::unordered_map<std::string, CDCSender*> cdc_senders_;
};

}  // namespace redis
