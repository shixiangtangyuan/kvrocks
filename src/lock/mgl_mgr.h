#pragma once

#include <list>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "lock_defines.h"

namespace redis {
namespace mgl {

class MGLock;

class LockSchedCtx {
 public:
  LockSchedCtx();
  LockSchedCtx(LockSchedCtx&&) = default;
  void Lock(MGLock* core);
  bool Unlock(MGLock* core);
  std::string ToString();
  std::vector<std::string> GetShardLocks();

 private:
  void schedPendingLocks();
  void incrPendingRef(LockMode mode);
  void incrRunningRef(LockMode mode);
  void decPendingRef(LockMode mode);
  void decRunningRef(LockMode mode);
  uint16_t running_modes_;
  uint16_t pending_modes_;
  std::vector<uint16_t> running_ref_cnt_;
  std::vector<uint16_t> pending_ref_cnt_;
  std::list<MGLock*> running_list_;
  std::list<MGLock*> pending_list_;
};

struct alignas(128) LockShard {
  std::mutex mutex;
  std::unordered_map<std::string, LockSchedCtx> map;
};

class MGLockMgr {
 public:
  MGLockMgr() = default;
  void Lock(MGLock* core);
  void Unlock(MGLock* core);
  std::string ToString();
  std::vector<std::string> GetLockList();

 private:
  static constexpr size_t SHARD_NUM = 32;
  LockShard _shards[SHARD_NUM];
};

}  // namespace mgl
}  // namespace redis
