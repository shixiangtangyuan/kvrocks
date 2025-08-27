/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "log_collector.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <string>

#include "server/redis_reply.h"
#include "time_util.h"

std::string SlowEntry::ToRedisString() const {
  std::string output;
  output.append(redis::MultiLen(9));
  output.append(redis::Integer(id));
  output.append(redis::Integer(time));
  output.append(redis::Integer(duration));
  output.append(redis::MultiBulkString(args));
  output.append(redis::BulkString(ip + ":" + std::to_string(port)));
  output.append(redis::BulkString(client_name));
  output.append(redis::Integer(prepare_duration));
  output.append(redis::Integer(command_queue_latency_on_connection));
  output.append(redis::Integer(estimated_subkey_count));
  return output;
}

void OutputStringInHex(std::ostream &os, const std::string &str) {
  std::ios_base::fmtflags flags(os.flags());
  os << std::hex << std::setfill('0');
  for (unsigned char c : str) {
    if (c == '\\') {
      os << "\\\\";
    } else if (std::isprint(c)) {
      os << c;
    } else {
      os << "\\x" << std::setw(2) << static_cast<unsigned int>(c);
    }
  }
  os.flags(flags);
}

std::ostream &operator<<(std::ostream &os, const SlowEntry &s) {
  os << "req:";
  if (!s.args.empty()) {
    OutputStringInHex(os, s.args[0]);
    for (size_t i = 1; i < s.args.size(); ++i) {
      OutputStringInHex(os << ",", s.args[i]);
    }
  }
  os << ", client:" << s.ip << ":" << s.port << ", duration:" << s.duration << "us";
  if (s.prepare_duration >= 0) {
    os << ", prepare_duration:" << s.prepare_duration << "us";
  }
  if (s.command_queue_latency_on_connection >= 0) {
    os << ", command_queue_latency_on_connection:" << s.command_queue_latency_on_connection << "us";
  }
  if (s.estimated_subkey_count >= 0) {
    os << ", estimated_subkey_count:" << s.estimated_subkey_count;
  }
  return os;
}

std::string PerfEntry::ToRedisString() const {
  std::string output;
  output.append(redis::MultiLen(7));
  output.append(redis::Integer(id));
  output.append(redis::Integer(time));
  output.append(redis::BulkString(cmd_name));
  output.append(redis::Integer(duration));
  output.append(redis::BulkString(perf_context));
  output.append(redis::BulkString(iostats_context));
  output.append(redis::Integer(prepare_duration));
  output.append(redis::Integer(command_queue_latency_on_connection));
  return output;
}

template <class T>
LogCollector<T>::~LogCollector() {
  Reset();
}

template <class T>
ssize_t LogCollector<T>::Size() {
  std::lock_guard<std::mutex> guard(mu_);
  ssize_t n = entries_.size();
  return n;
}

template <class T>
void LogCollector<T>::Reset() {
  std::lock_guard<std::mutex> guard(mu_);
  while (!entries_.empty()) {
    entries_.pop_front();
  }
}

template <class T>
void LogCollector<T>::SetMaxEntries(uint64_t max_entries) {
  std::lock_guard<std::mutex> guard(mu_);
  while (entries_.size() > max_entries) {
    entries_.pop_back();
  }
  max_entries_.store(max_entries);
}

template <class T>
void LogCollector<T>::PushEntry(std::unique_ptr<T> &&entry) {
  if (max_entries_.load() <= 0) return;
  std::lock_guard<std::mutex> guard(mu_);
  auto max_entries = max_entries_.load();
  if (max_entries <= 0) return;
  entry->id = ++id_;
  entry->time = util::GetTimeStamp();
  while (!entries_.empty() && entries_.size() >= max_entries) {
    entries_.pop_back();
  }
  entries_.push_front(std::move(entry));
}

template <class T>
std::string LogCollector<T>::GetLatestEntries(int64_t cnt) {
  size_t n = 0;
  std::string output;

  std::lock_guard<std::mutex> guard(mu_);
  if (cnt > 0) {
    n = std::min(entries_.size(), static_cast<size_t>(cnt));
  } else {
    n = entries_.size();
  }
  output.append(redis::MultiLen(n));
  for (const auto &entry : entries_) {
    output.append(entry->ToRedisString());
    if (--n == 0) break;
  }
  return output;
}

template class LogCollector<SlowEntry>;
template class LogCollector<PerfEntry>;
