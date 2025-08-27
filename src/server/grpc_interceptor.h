#include <grpcpp/support/client_interceptor.h>
#include <grpcpp/support/server_interceptor.h>

#include <chrono>

namespace redis {

using grpc::experimental::ClientRpcInfo;
using grpc::experimental::Interceptor;
using grpc::experimental::InterceptorBatchMethods;
using grpc::experimental::ServerRpcInfo;

class ServerStatsInterceptor : public grpc::experimental::Interceptor {
 public:
  explicit ServerStatsInterceptor(ServerRpcInfo* rpc_info);

  ~ServerStatsInterceptor() override = default;

  void Intercept(InterceptorBatchMethods* methods) override;

 private:
  ServerRpcInfo* rpc_info_ = nullptr;

  ServerRpcInfo::Type type_ = ServerRpcInfo::Type::BIDI_STREAMING;
  std::string type_str_;
  std::string method_;
  std::string peer_;
  std::chrono::steady_clock::time_point start_time_;
  std::chrono::steady_clock::time_point last_recv_msg_time_;
  std::chrono::steady_clock::time_point start_send_msg_time_;
};

class ServerStatsInterceptorFactory : public grpc::experimental::ServerInterceptorFactoryInterface {
 public:
  Interceptor* CreateServerInterceptor(grpc::experimental::ServerRpcInfo* info) override {
    return new ServerStatsInterceptor(info);
  }
};

class ClientStatsInterceptor : public grpc::experimental::Interceptor {
 public:
  explicit ClientStatsInterceptor(ClientRpcInfo* rpc_info);

  ~ClientStatsInterceptor() override = default;

  void Intercept(InterceptorBatchMethods* methods) override;

 private:
  ClientRpcInfo* rpc_info_ = nullptr;

  ClientRpcInfo::Type type_ = ClientRpcInfo::Type::BIDI_STREAMING;
  std::string type_str_;
  std::string method_;
  std::string peer_;
  std::chrono::steady_clock::time_point start_time_;
  std::chrono::steady_clock::time_point last_recv_msg_time_;
  std::chrono::steady_clock::time_point start_send_msg_time_;
};

class ClientStatsInterceptorFactory : public grpc::experimental::ClientInterceptorFactoryInterface {
 public:
  Interceptor* CreateClientInterceptor(ClientRpcInfo* info) override { return new ClientStatsInterceptor(info); }
};

}  // namespace redis
