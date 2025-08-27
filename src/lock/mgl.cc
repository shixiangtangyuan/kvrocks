#include "mgl.h"

#include "common/string_util.h"
#include "invariant.h"
#include "lock_defines.h"
#include "mgl_mgr.h"

namespace redis {
namespace mgl {

std::atomic<uint64_t> MGLock::id_gen_(0);
std::list<MGLock*> MGLock::dummy_list_{};

MGLock::MGLock(MGLockMgr* mgr)
    : id_(id_gen_.fetch_add(1, std::memory_order_relaxed)),
      target_(""),
      target_hash_(0),
      mode_(LockMode::LOCK_NONE),
      res_(LockRes::LOCKRES_UNINITED),
      res_iter_(dummy_list_.end()),
      lock_mgr_(mgr),
      thread_id_(util::GetCurThreadId()) {
  INVARIANT_D(lock_mgr_ != nullptr);
}

MGLock::~MGLock() { INVARIANT_D(res_ == LockRes::LOCKRES_UNINITED); }

void MGLock::releaseLockResult() {
  std::lock_guard<std::mutex> lk(mutex_);
  res_ = LockRes::LOCKRES_UNINITED;
  res_iter_ = dummy_list_.end();
}

void MGLock::setLockResult(LockRes res, std::list<MGLock*>::iterator iter) {
  std::lock_guard<std::mutex> lk(mutex_);
  res_ = res;
  res_iter_ = iter;
}

void MGLock::Unlock() {
  LockRes status = GetStatus();
  if (status != LockRes::LOCKRES_UNINITED) {
    INVARIANT_D(status == LockRes::LOCKRES_OK || status == LockRes::LOCKRES_WAIT);
    lock_mgr_->Unlock(this);
    status = GetStatus();
    INVARIANT_D(status == LockRes::LOCKRES_UNINITED);
  }
}

LockRes MGLock::Lock(const std::string& target, LockMode mode, uint64_t timeout_ms) {
  target_ = target;
  mode_ = mode;
  INVARIANT_D(GetStatus() == LockRes::LOCKRES_UNINITED);
  res_iter_ = dummy_list_.end();
  if (target_ != "") {
    target_hash_ = static_cast<uint64_t>(std::hash<std::string>{}(target_));
  } else {
    target_hash_ = 0;
  }
  lock_mgr_->Lock(this);
  if (GetStatus() == LockRes::LOCKRES_OK) {
    return LockRes::LOCKRES_OK;
  }
  if (waitLock(timeout_ms)) {
    return LockRes::LOCKRES_OK;
  } else {
    return LockRes::LOCKRES_TIMEOUT;
  }
}

std::list<MGLock*>::iterator MGLock::getLockIter() const { return res_iter_; }

void MGLock::notify() { cv_.notify_one(); }

bool MGLock::waitLock(uint64_t timeout_ms) {
  std::unique_lock<std::mutex> lk(mutex_);
  return cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms), [this]() { return res_ == LockRes::LOCKRES_OK; });
}

LockRes MGLock::GetStatus() const {
  std::lock_guard<std::mutex> lk(mutex_);
  return res_;
}

std::string MGLock::ToString() const {
  std::lock_guard<std::mutex> lk(mutex_);
  char buf[256];
  std::snprintf(buf, sizeof(buf), "id:%" PRIu64 " target:%s targetHash:%" PRIu64 " LockMode:%s LockRes:%d threadId:%s",
                id_, target_.c_str(), target_hash_, LockModeRepr(mode_), static_cast<int>(res_), thread_id_.c_str());
  return std::string(buf);
}

}  // namespace mgl
}  // namespace redis
