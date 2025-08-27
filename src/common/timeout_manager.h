#pragma once

#include <event2/event.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>

using TaskCallbackArg = void*;
using TaskCallback = std::function<void(TaskCallbackArg)>;

namespace util {

class TimeoutManager;

class TimeoutTask {
 public:
  TimeoutTask(struct event_base* base, int64_t timeout_in_milliseconds, TaskCallback callback, TaskCallbackArg cb_arg);

  TimeoutTask(TimeoutManager*, struct event_base* base, int64_t timeout_ms, TaskCallback callback, TaskCallbackArg arg);

  ~TimeoutTask();

  TimeoutTask(const TimeoutTask&) = delete;
  TimeoutTask& operator=(const TimeoutTask&) = delete;
  TimeoutTask(TimeoutTask&&) = default;
  TimeoutTask& operator=(TimeoutTask&&) = default;

 private:
  static void timeoutCb(evutil_socket_t fd, short event, void* arg);

  TaskCallback callback_;
  TaskCallbackArg cb_arg_;
  struct event* timeout_event_{nullptr};
  TimeoutManager* mgr_{nullptr};
};

class TimeoutManager {
 public:
  TimeoutManager();

  std::shared_ptr<TimeoutTask> AddTimeoutTask(int64_t timeout_in_milliseconds, TaskCallback callback,
                                              TaskCallbackArg arg);

  void ResetTimeoutTask(std::shared_ptr<TimeoutTask>&);

  void RegisterTimeoutTask(int64_t timeout_ms, TaskCallback, TaskCallbackArg);

  void Run();

  void Stop();

 private:
  friend class TimeoutTask;

  struct event_base* base_{nullptr};
  std::thread loop_thread_;
  std::atomic<bool> stop_{false};

  std::mutex tasks_mutex_;
  std::unordered_set<TimeoutTask*> registered_tasks_;
};

}  // namespace util
