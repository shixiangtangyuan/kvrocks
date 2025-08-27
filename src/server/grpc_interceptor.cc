#include "server/grpc_interceptor.h"

#include <google/protobuf/message.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/interceptor.h>

#include "stats/stats.h"

namespace redis {

using grpc::experimental::InterceptionHookPoints;

std::string GRPCMethodTypeToString(const ServerRpcInfo::Type& type) {
  switch (type) {
    case ServerRpcInfo::Type::UNARY:
      return "unary";
    case ServerRpcInfo::Type::CLIENT_STREAMING:
      return "client-stream";
    case ServerRpcInfo::Type::SERVER_STREAMING:
      return "server-stream";
    case ServerRpcInfo::Type::BIDI_STREAMING:
      return "bidi-stream";
    default:
      return "";
  }
}

std::string GRPCMethodTypeToString(const ClientRpcInfo::Type& type) {
  switch (type) {
    case ClientRpcInfo::Type::UNARY:
      return "unary";
    case ClientRpcInfo::Type::CLIENT_STREAMING:
      return "client-stream";
    case ClientRpcInfo::Type::SERVER_STREAMING:
      return "server-stream";
    case ClientRpcInfo::Type::BIDI_STREAMING:
      return "bidi-stream";
    default:
      return "";
  }
}

int64_t GetDurationUS(const std::chrono::steady_clock::time_point& start,
                      const std::chrono::steady_clock::time_point& end) {
  return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

ServerStatsInterceptor::ServerStatsInterceptor(ServerRpcInfo* rpc_info) : rpc_info_(rpc_info) {}

void ServerStatsInterceptor::Intercept(InterceptorBatchMethods* methods) {
  if (methods->QueryInterceptionHookPoint(InterceptionHookPoints::POST_RECV_INITIAL_METADATA)) {
    if (rpc_info_ != nullptr) {
      type_ = rpc_info_->type();
      type_str_ = GRPCMethodTypeToString(type_);
      if (auto method = rpc_info_->method(); method != nullptr) {
        method_ = rpc_info_->method();
        // remove service prefix
        auto pos = method_.find_last_of('/');
        if (pos != method_.npos) method_.erase(0, pos + 1);
      } else {
        method_ = "";
      }
      if (auto ctx = rpc_info_->server_context(); ctx != nullptr) {
        peer_ = ctx->peer();
        // remove port suffix
        auto pos = peer_.find_last_of(':');
        if (pos != peer_.npos) peer_.erase(pos);
        // replace colon with dot to parse correctly in kv-exporter
        std::replace(peer_.begin(), peer_.end(), ':', '.');
      } else {
        peer_ = "";
      }
      std::initializer_list<std::pair<std::string_view, std::string_view>> labels = {
          {"type", type_str_}, {"method", method_}, {"peer", peer_}};
      thread_local_metric_array.Count(MetricType::GRPC_SERVER_REQUEST_NUM, labels, 1);
      auto now = std::chrono::steady_clock::now();
      last_recv_msg_time_ = now;
      start_time_ = now;
    }
  }

  if (methods->QueryInterceptionHookPoint(InterceptionHookPoints::POST_RECV_MESSAGE)) {
    if (type_ == ServerRpcInfo::Type::CLIENT_STREAMING || type_ == ServerRpcInfo::Type::BIDI_STREAMING) {
      std::initializer_list<std::pair<std::string_view, std::string_view>> labels = {
          {"type", type_str_}, {"method", method_}, {"peer", peer_}};
      auto now = std::chrono::steady_clock::now();
      thread_local_metric_array.Record(MetricType::GRPC_SERVER_RECV_MESSAGE_INTERVAL, labels,
                                       GetDurationUS(last_recv_msg_time_, now));
      last_recv_msg_time_ = now;
    }
  }

  if (methods->QueryInterceptionHookPoint(InterceptionHookPoints::PRE_SEND_MESSAGE)) {
    start_send_msg_time_ = std::chrono::steady_clock::now();
  }

  if (methods->QueryInterceptionHookPoint(InterceptionHookPoints::POST_SEND_MESSAGE)) {
    std::initializer_list<std::pair<std::string_view, std::string_view>> labels = {
        {"type", type_str_}, {"method", method_}, {"peer", peer_}};
    auto now = std::chrono::steady_clock::now();
    thread_local_metric_array.Record(MetricType::GRPC_SERVER_SEND_MESSAGE_LATENCY, labels,
                                     GetDurationUS(start_send_msg_time_, now));
  }

  if (methods->QueryInterceptionHookPoint(InterceptionHookPoints::PRE_SEND_STATUS)) {
    auto code = methods->GetSendStatus().error_code();
    auto code_str = absl::StatusCodeToString(static_cast<absl::StatusCode>(code));
    std::initializer_list<std::pair<std::string_view, std::string_view>> labels = {
        {"type", type_str_}, {"method", method_}, {"peer", peer_}, {"code", code_str}};
    auto now = std::chrono::steady_clock::now();
    thread_local_metric_array.Record(MetricType::GRPC_SERVER_REQUEST_LATENCY, labels, GetDurationUS(start_time_, now));
  }

  methods->Proceed();
}

ClientStatsInterceptor::ClientStatsInterceptor(ClientRpcInfo* rpc_info) : rpc_info_(rpc_info) {}

void ClientStatsInterceptor::Intercept(InterceptorBatchMethods* methods) {
  if (methods->QueryInterceptionHookPoint(InterceptionHookPoints::PRE_SEND_INITIAL_METADATA)) {
    if (rpc_info_ != nullptr) {
      type_ = rpc_info_->type();
      type_str_ = GRPCMethodTypeToString(type_);
      if (auto method = rpc_info_->method(); method != nullptr) {
        method_ = rpc_info_->method();
        // remove service prefix
        auto pos = method_.find_last_of('/');
        if (pos != method_.npos) method_.erase(0, pos + 1);
      } else {
        method_ = "";
      }
      if (auto ctx = rpc_info_->client_context(); ctx != nullptr) {
        peer_ = ctx->peer();
        // remove port suffix
        auto pos = peer_.find_last_of(':');
        if (pos != peer_.npos) peer_.erase(pos);
        // replace colon with dot to parse correctly in kv-exporter
        std::replace(peer_.begin(), peer_.end(), ':', '.');
      } else {
        peer_ = "";
      }
      std::initializer_list<std::pair<std::string_view, std::string_view>> labels = {
          {"type", type_str_}, {"method", method_}, {"peer", peer_}};
      thread_local_metric_array.Count(MetricType::GRPC_CLIENT_REQUEST_NUM, labels, 1);
      auto now = std::chrono::steady_clock::now();
      last_recv_msg_time_ = now;
      start_time_ = now;
    }
  }

  if (methods->QueryInterceptionHookPoint(InterceptionHookPoints::PRE_SEND_MESSAGE)) {
    start_send_msg_time_ = std::chrono::steady_clock::now();
  }

  if (methods->QueryInterceptionHookPoint(InterceptionHookPoints::POST_SEND_MESSAGE)) {
    std::initializer_list<std::pair<std::string_view, std::string_view>> labels = {
        {"type", type_str_}, {"method", method_}, {"peer", peer_}};
    auto now = std::chrono::steady_clock::now();
    thread_local_metric_array.Record(MetricType::GRPC_CLIENT_SEND_MESSAGE_LATENCY, labels,
                                     GetDurationUS(start_send_msg_time_, now));
  }

  if (methods->QueryInterceptionHookPoint(InterceptionHookPoints::POST_RECV_MESSAGE)) {
    if (type_ == ClientRpcInfo::Type::SERVER_STREAMING || type_ == ClientRpcInfo::Type::BIDI_STREAMING) {
      std::initializer_list<std::pair<std::string_view, std::string_view>> labels = {
          {"type", type_str_}, {"method", method_}, {"peer", peer_}};
      auto now = std::chrono::steady_clock::now();
      thread_local_metric_array.Record(MetricType::GRPC_CLIENT_RECV_MESSAGE_INTERVAL, labels,
                                       GetDurationUS(last_recv_msg_time_, now));
      last_recv_msg_time_ = now;
    }
  }

  if (methods->QueryInterceptionHookPoint(InterceptionHookPoints::POST_RECV_STATUS)) {
    if (auto status = methods->GetRecvStatus(); status != nullptr) {
      auto code_str = absl::StatusCodeToString(static_cast<absl::StatusCode>(status->error_code()));
      std::initializer_list<std::pair<std::string_view, std::string_view>> labels = {
          {"type", type_str_}, {"method", method_}, {"peer", peer_}, {"code", code_str}};
      auto now = std::chrono::steady_clock::now();
      thread_local_metric_array.Record(MetricType::GRPC_CLIENT_REQUEST_LATENCY, labels,
                                       GetDurationUS(start_time_, now));
    }
  }

  methods->Proceed();
}

}  // namespace redis
