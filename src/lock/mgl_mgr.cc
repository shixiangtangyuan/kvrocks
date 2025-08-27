#include "mgl_mgr.h"

#include <sstream>

#include "invariant.h"
#include "lock_defines.h"
#include "mgl.h"

namespace redis {
namespace mgl {

static const int conflictTable[] = {
    // MODE_NONE
    0,
    // MODE_IS
    (1 << Enum2Int(LockMode::LOCK_X)),
    // MODE_IX
    (1 << Enum2Int(LockMode::LOCK_S)) | (1 << Enum2Int(LockMode::LOCK_X)),
    // MODE_S
    (1 << Enum2Int(LockMode::LOCK_IX)) | (1 << Enum2Int(LockMode::LOCK_X)),
    // MODE_X
    (1 << Enum2Int(LockMode::LOCK_IS)) | (1 << Enum2Int(LockMode::LOCK_IX)) | (1 << Enum2Int(LockMode::LOCK_S)) |
        (1 << Enum2Int(LockMode::LOCK_X)),
};

const char* LockModeRepr(LockMode mode) {
  switch (mode) {
    case LockMode::LOCK_X:
      return "X";
    case LockMode::LOCK_IX:
      return "IX";
    case LockMode::LOCK_S:
      return "S";
    case LockMode::LOCK_IS:
      return "IS";
    default:
      return "?";
  }
}

bool IsConflict(uint16_t modes, LockMode mode) {
  uint16_t mode_int = Enum2Int(mode);
  return (conflictTable[mode_int] & modes) != 0;
}

LockSchedCtx::LockSchedCtx()
    : running_modes_(0),
      pending_modes_(0),
      running_ref_cnt_(Enum2Int(LockMode::LOCK_MODE_NUM), 0),
      pending_ref_cnt_(Enum2Int(LockMode::LOCK_MODE_NUM), 0) {}

void LockSchedCtx::Lock(MGLock* core) {
  auto mode = core->GetMode();
  if (IsConflict(running_modes_, mode) || pending_list_.size() >= 1) {
    auto it = pending_list_.insert(pending_list_.end(), core);
    incrPendingRef(mode);
    core->setLockResult(LockRes::LOCKRES_WAIT, it);
  } else {
    auto it = running_list_.insert(running_list_.end(), core);
    incrRunningRef(mode);
    core->setLockResult(LockRes::LOCKRES_OK, it);
  }
}

void LockSchedCtx::schedPendingLocks() {
  std::list<MGLock*>::iterator it = pending_list_.begin();
  while (it != pending_list_.end()) {
    MGLock* lock = *it;
    if (IsConflict(running_modes_, lock->GetMode())) {
      it++;
      break;
    }
    incrRunningRef(lock->GetMode());
    decPendingRef(lock->GetMode());
    auto runningIt = running_list_.insert(running_list_.end(), lock);
    it = pending_list_.erase(it);
    lock->setLockResult(LockRes::LOCKRES_OK, runningIt);
    lock->notify();
  }
}

bool LockSchedCtx::Unlock(MGLock* core) {
  auto mode = core->GetMode();
  if (core->GetStatus() == LockRes::LOCKRES_OK) {
    running_list_.erase(core->getLockIter());
    decRunningRef(mode);
    core->releaseLockResult();
    if (running_modes_ != 0) {
      return false;
    }
    INVARIANT_D(running_list_.size() == 0);
    schedPendingLocks();
  } else if (core->GetStatus() == LockRes::LOCKRES_WAIT) {
    pending_list_.erase(core->getLockIter());
    decPendingRef(mode);
    core->releaseLockResult();
    INVARIANT_D((pending_modes_ == 0 && pending_list_.size() == 0) ||
                (pending_modes_ != 0 && pending_list_.size() != 0));
    schedPendingLocks();
  } else {
    INVARIANT_D(0);
  }
  return pending_list_.empty() && running_list_.empty();
}

void LockSchedCtx::incrPendingRef(LockMode mode) {
  auto mode_int = Enum2Int(mode);
  ++pending_ref_cnt_[mode_int];
  if (pending_ref_cnt_[mode_int] == 1) {
    INVARIANT_D((pending_modes_ & (1 << mode_int)) == 0);
    pending_modes_ |= static_cast<uint16_t>((1 << mode_int));
  }
}

void LockSchedCtx::decPendingRef(LockMode mode) {
  auto mode_int = Enum2Int(mode);
  INVARIANT_D(pending_ref_cnt_[mode_int] != 0);
  --pending_ref_cnt_[mode_int];
  if (pending_ref_cnt_[mode_int] == 0) {
    INVARIANT_D((pending_modes_ & (1 << mode_int)) != 0);
    pending_modes_ &= static_cast<uint16_t>(~(1 << mode_int));
  }
}

void LockSchedCtx::incrRunningRef(LockMode mode) {
  auto mode_int = Enum2Int(mode);
  ++running_ref_cnt_[mode_int];
  if (running_ref_cnt_[mode_int] == 1) {
    INVARIANT_D((running_modes_ & (1 << mode_int)) == 0);
    running_modes_ |= static_cast<uint16_t>((1 << mode_int));
  }
}

void LockSchedCtx::decRunningRef(LockMode mode) {
  auto mode_int = Enum2Int(mode);
  INVARIANT_D(running_ref_cnt_[mode_int] != 0);
  --running_ref_cnt_[mode_int];
  if (running_ref_cnt_[mode_int] == 0) {
    INVARIANT_D((running_modes_ & (1 << mode_int)) != 0);
    running_modes_ &= static_cast<uint16_t>(~(1 << mode_int));
  }
}

std::string LockSchedCtx::ToString() {
  std::ostringstream ss;

  for (auto i : running_list_) {
    ss << "running: {" << i->ToString() << "}\r\n";
  }

  for (auto i : pending_list_) {
    ss << "pending: {" << i->ToString() << "}\r\n";
  }

  return ss.str();
}

std::vector<std::string> LockSchedCtx::GetShardLocks() {
  std::vector<std::string> tempLocks;
  for (auto i : running_list_) {
    tempLocks.push_back("running: {" + i->ToString() + "}");
  }

  for (auto i : pending_list_) {
    tempLocks.push_back("pending: {" + i->ToString() + "}");
  }
  return tempLocks;
}

void MGLockMgr::Lock(MGLock* core) {
  uint64_t hash = core->GetHash();
  LockShard& shard = _shards[hash % SHARD_NUM];
  std::lock_guard<std::mutex> lk(shard.mutex);
  auto iter = shard.map.find(core->GetTarget());
  if (iter == shard.map.end()) {
    LockSchedCtx tmp;
    auto insertResult = shard.map.emplace(core->GetTarget(), std::move(tmp));
    iter = insertResult.first;
  }
  iter->second.Lock(core);
  return;
}

void MGLockMgr::Unlock(MGLock* core) {
  uint64_t hash = core->GetHash();
  LockShard& shard = _shards[hash % SHARD_NUM];
  std::lock_guard<std::mutex> lk(shard.mutex);

  INVARIANT_D(core->GetStatus() == LockRes::LOCKRES_WAIT || core->GetStatus() == LockRes::LOCKRES_OK);

  auto iter = shard.map.find(core->GetTarget());
  INVARIANT(iter != shard.map.end());
  bool empty = iter->second.Unlock(core);
  if (empty) {
    shard.map.erase(iter);
  }
  return;
}

std::string MGLockMgr::ToString() {
  std::ostringstream ss;
  auto locklist = GetLockList();
  for (auto& vs : locklist) {
    ss << vs;
  }
  return ss.str();
}

std::vector<std::string> MGLockMgr::GetLockList() {
  std::vector<std::string> list;
  for (uint32_t i = 0; i < SHARD_NUM; i++) {
    LockShard& shard = _shards[i];
    std::lock_guard<std::mutex> lk(shard.mutex);
    for (auto& iter : shard.map) {
      auto locklist = iter.second.GetShardLocks();
      for (auto& v : locklist) {
        list.push_back(v);
      }
    }
  }
  return list;
}

}  // namespace mgl
}  // namespace redis
