#include "timeout_manager.h"

#include <glog/logging.h>

class Server;

using TaskCallbackArg = void*;
using TaskCallback = std::function<void(TaskCallbackArg)>;

namespace util {

TimeoutTask::TimeoutTask(struct event_base* base, int64_t timeout_in_milliseconds, TaskCallback callback,
                         TaskCallbackArg cb_arg)
    : callback_(std::move(callback)),
      cb_arg_(std::move(cb_arg)),
      timeout_event_(evtimer_new(base, &TimeoutTask::timeoutCb, this)) {
  DCHECK(!timeout_event_);

  // set timeout
  struct timeval timeout;
  evutil_timerclear(&timeout);
  timeout.tv_sec = timeout_in_milliseconds / 1000;
  timeout.tv_usec = timeout_in_milliseconds % 1000 * 1000;

  // add event to event_base
  auto ret = event_add(timeout_event_, &timeout);
  DCHECK(ret >= 0);
}

TimeoutTask::TimeoutTask(TimeoutManager* timeout_manager, struct event_base* base, int64_t timeout_in_milliseconds,
                         TaskCallback callback, TaskCallbackArg arg)
    : TimeoutTask(base, timeout_in_milliseconds, std::move(callback), arg) {
  mgr_ = timeout_manager;
}

TimeoutTask::~TimeoutTask() {
  LOG(INFO) << "TimeoutTask start destruct " << this;
  if (mgr_) mgr_->registered_tasks_.erase(this);
  if (timeout_event_) {
    LOG(INFO) << "TimeoutTask destruct timeout event";
    event_del(timeout_event_);
    event_free(timeout_event_);
  }
}

// static
void TimeoutTask::timeoutCb(evutil_socket_t fd, short event, void* arg) {
  auto* task = static_cast<TimeoutTask*>(arg);
  task->callback_(task->cb_arg_);
  if (task->mgr_) delete task;
}

TimeoutManager::TimeoutManager() : base_(event_base_new()), stop_(false) { DCHECK(!base_); }

std::shared_ptr<TimeoutTask> TimeoutManager::AddTimeoutTask(int64_t timeout_in_milliseconds, TaskCallback callback,
                                                            TaskCallbackArg arg) {
  std::lock_guard<std::mutex> lock(tasks_mutex_);
  auto task = std::make_shared<TimeoutTask>(base_, timeout_in_milliseconds, callback, arg);
  return task;
}

void TimeoutManager::ResetTimeoutTask(std::shared_ptr<TimeoutTask>& task) {
  std::lock_guard<std::mutex> lock(tasks_mutex_);
  task.reset();
}

void TimeoutManager::RegisterTimeoutTask(int64_t timeout_ms, TaskCallback cb, TaskCallbackArg arg) {
  std::lock_guard<std::mutex> lock(tasks_mutex_);
  auto task = new TimeoutTask(this, base_, timeout_ms, std::move(cb), arg);
  registered_tasks_.emplace(task);
}

void TimeoutManager::Run() {
  LOG(INFO) << "timeout manager start run";

  loop_thread_ = std::thread([this]() {
    while (!stop_.load()) {
      {
        std::lock_guard<std::mutex> lock(tasks_mutex_);
        event_base_loop(base_, EVLOOP_NONBLOCK);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    LOG(INFO) << "timeout manager loop thread exit";
  });
}

void TimeoutManager::Stop() {
  if (stop_.load()) {
    LOG(INFO) << "timeout manager has stopped!";
    return;
  }
  LOG(INFO) << "timeout manager start stop";

  stop_.store(true);

  std::lock_guard<std::mutex> lock(tasks_mutex_);
  for (auto task : registered_tasks_) {
    delete task;
  }
  event_base_loopbreak(base_);

  if (loop_thread_.joinable()) {
    loop_thread_.join();
  }
  event_base_free(base_);

  LOG(INFO) << "timeout manager stop done";
}
}  // namespace util
