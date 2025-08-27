#include "lock.h"

#include "common/status.h"
#include "invariant.h"
#include "lock_defines.h"
#include "stats/stats.h"

namespace redis {

void Context::SetWaitLock(const std::string& slot_range_name, const std::string& key, mgl::LockMode mode) {
  std::lock_guard<std::mutex> lk(mutex_);
  waiting_lock_slot_range_ = slot_range_name;
  waiting_lock_key_ = key;
  waiting_lock_mode_ = mode;
}

SLSP Context::GetWaitLock() {
  std::lock_guard<std::mutex> g(mutex_);
  return std::tuple<std::string, std::string, mgl::LockMode>(waiting_lock_slot_range_, waiting_lock_key_,
                                                             waiting_lock_mode_);
}

void Context::AddLock(ILock* lock) {
  std::lock_guard<std::mutex> g(mutex_);
  locks_.push_back(lock);
}
void Context::RemoveLock(ILock* lock) {
  std::lock_guard<std::mutex> g(mutex_);
  for (auto it = locks_.begin(); it != locks_.end(); it++) {
    if (*it == lock) {
      locks_.erase(it);
      return;
    }
  }
  INVARIANT_D(0);
}

void Context::SetKeylock(const std::string& key, mgl::LockMode mode) {
  std::lock_guard<std::mutex> g(mutex_);
  key_lock_map_[key] = mode;
}

void Context::UnsetKeylock(const std::string& key) {
  std::lock_guard<std::mutex> g(mutex_);
  INVARIANT_D(key_lock_map_.count(key) > 0);
  key_lock_map_.erase(key);
}
bool Context::IsLockedByMe(const std::string& key, mgl::LockMode mode) {
  std::lock_guard<std::mutex> g(mutex_);
  auto it = key_lock_map_.find(key);
  if (it != key_lock_map_.end()) {
    INVARIANT_D(mgl::Enum2Int(mode) <= mgl::Enum2Int(it->second));
    return true;
  }
  return false;
}

ILock::ILock(ILock* parent, mgl::MGLock* lk, Context* ctx, bool isRecursive)
    : lock_result_(mgl::LockRes::LOCKRES_WAIT), parent_(parent), mgl_(lk), ctx_(ctx), is_recursive_(isRecursive) {}

ILock::~ILock() {
  if (mgl_) {
    mgl_->Unlock();
  }
  if (parent_) {
    parent_.reset();
  }
  if (ctx_ && lock_result_ == mgl::LockRes::LOCKRES_OK && !is_recursive_) {
    ctx_->RemoveLock(this);
  }
}

mgl::LockMode ILock::GetMode() const {
  if (mgl_ == nullptr) {
    return mgl::LockMode::LOCK_NONE;
  }
  return mgl_->GetMode();
}

mgl::LockRes ILock::GetLockResult() const { return lock_result_; }

std::string ILock::GetSlotRangeName() const { return ""; }

std::string ILock::GetKey() const { return ""; }

mgl::LockMode ILock::GetParentMode(mgl::LockMode mode) {
  mgl::LockMode parentMode = mgl::LockMode::LOCK_NONE;
  switch (mode) {
    case mgl::LockMode::LOCK_IS:
    case mgl::LockMode::LOCK_S:
      parentMode = mgl::LockMode::LOCK_IS;
      break;
    case mgl::LockMode::LOCK_IX:
    case mgl::LockMode::LOCK_X:
      parentMode = mgl::LockMode::LOCK_IX;
      break;
    default:
      INVARIANT_D(0);
  }
  return parentMode;
}

StatusOr<std::unique_ptr<SlotRangeLock>> SlotRangeLock::AcquireSlotRangeLock(const std::string& slot_range_name,
                                                                             mgl::LockMode mode, Context* ctx,
                                                                             mgl::MGLockMgr* mgr,
                                                                             uint64_t lockTimeoutMs) {
  auto start_time = std::chrono::steady_clock::now();
  auto lock = std::make_unique<SlotRangeLock>(slot_range_name, mode, ctx, mgr, lockTimeoutMs);
  auto end_time = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
  auto lock_res = lock->GetLockResult();
  if (lock_res == mgl::LockRes::LOCKRES_OK) {
    thread_local_metric_array.RecordMglLockLatency(mode, slot_range_name, true, duration);
    return lock;
  } else if (lock_res == mgl::LockRes::LOCKRES_TIMEOUT) {
    thread_local_metric_array.CountMglLockFailed(mode, slot_range_name, true, lock_res, 1);
    return {Status::LockTimeOut, "Lock wait timeout"};
  } else {
    INVARIANT_D(0);
    thread_local_metric_array.CountMglLockFailed(mode, slot_range_name, true, lock_res, 1);
    return {Status::ErrUnknown, "unknown error"};
  }
}

SlotRangeLock::SlotRangeLock(const std::string& slot_range_name, mgl::LockMode mode, Context* ctx, mgl::MGLockMgr* mgr,
                             uint64_t lockTimeoutMs, bool isRecursive)
    : ILock(nullptr, new mgl::MGLock(mgr), ctx, isRecursive), slot_range_name_(slot_range_name) {
  if (ctx_) {
    ctx_->SetWaitLock(slot_range_name_, "", mode);
  }
  lock_result_ = mgl_->Lock(slot_range_name_, mode, lockTimeoutMs);
  // TENDIS_LOCK_LATENCY_RECORD(
  //   (lock_result_ = mgl_->Lock(slot_range_name_, mode, lockTimeoutMs)),
  //   ctx_,
  //   slot_range_name,
  //   LockLatencyType::LLT_STORE);
  if (ctx_) {
    ctx_->SetWaitLock(slot_range_name, "", mgl::LockMode::LOCK_NONE);
    if (lock_result_ == mgl::LockRes::LOCKRES_OK && !isRecursive) {
      ctx_->AddLock(this);
    }
  }
}

std::string SlotRangeLock::GetSlotRangeName() const { return slot_range_name_; }

StatusOr<std::unique_ptr<KeyLock>> KeyLock::AcquireKeyLock(const std::string& slot_range_name, const std::string& key,
                                                           mgl::LockMode mode, Context* ctx, mgl::MGLockMgr* mgr,
                                                           uint64_t lockTimeoutMs) {
  if (ctx->IsLockedByMe(key, mode)) {
    return std::unique_ptr<KeyLock>(nullptr);
  } else {
    auto start_time = std::chrono::steady_clock::now();
    auto lock = std::make_unique<KeyLock>(slot_range_name, key, mode, ctx, mgr, lockTimeoutMs);
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
    auto lock_res = lock->GetLockResult();
    if (lock_res == mgl::LockRes::LOCKRES_OK) {
      thread_local_metric_array.RecordMglLockLatency(mode, slot_range_name, false, duration);
      return lock;
    } else if (lock_res == mgl::LockRes::LOCKRES_TIMEOUT) {
      thread_local_metric_array.CountMglLockFailed(mode, slot_range_name, false, lock_res, 1);
      return {Status::LockTimeOut, "Lock wait timeout"};
    } else {
      INVARIANT_D(0);
      thread_local_metric_array.CountMglLockFailed(mode, slot_range_name, false, lock_res, 1);
      return {Status::ErrUnknown, "unknown error"};
    }
  }
}

KeyLock::KeyLock(const std::string& slot_range_name, const std::string& key, mgl::LockMode mode, Context* ctx,
                 mgl::MGLockMgr* mgr, uint64_t lockTimeoutMs)
    : ILock(new SlotRangeLock(slot_range_name, GetParentMode(mode), ctx, mgr, lockTimeoutMs, true),
            new mgl::MGLock(mgr), ctx, false),
      key_(key) {
  if (parent_->GetLockResult() != mgl::LockRes::LOCKRES_OK) {
    lock_result_ = parent_->GetLockResult();
  } else {
    std::string target = "key_" + key;
    if (ctx_) {
      ctx_->SetWaitLock(slot_range_name, key, mode);
    }
    lock_result_ = mgl_->Lock(target, mode, lockTimeoutMs);
    // TENDIS_LOCK_LATENCY_RECORD(
    //   (lock_result_ = mgl_->Lock(target, mode, lockTimeoutMs)),
    //   ctx_,
    //   key,
    //   LockLatencyType::LLT_KEY);
    if (ctx_) {
      ctx_->SetWaitLock(slot_range_name, "", mgl::LockMode::LOCK_NONE);
      if (lock_result_ == mgl::LockRes::LOCKRES_OK) {
        ctx_->AddLock(this);
        ctx_->SetKeylock(key, mode);
      }
    }
  }
}

KeyLock::~KeyLock() {
  if (ctx_ && lock_result_ == mgl::LockRes::LOCKRES_OK) {
    ctx_->UnsetKeylock(key_);
  }
}

std::string KeyLock::GetSlotRangeName() const { return parent_->GetSlotRangeName(); }

std::string KeyLock::GetKey() const { return key_; }

}  // namespace redis
