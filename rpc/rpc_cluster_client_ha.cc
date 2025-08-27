#include <grpcpp/grpcpp.h>
#include <kv/controller/v1/api.grpc.pb.h>

#include <atomic>
#include <iostream>
#include <memory>
#include <shared_mutex>
#include <string>
#include <thread>

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using kv::controller::v1::ControllerApiService;
using kv::controller::v1::ReportDataNodeRequest;
using kv::controller::v1::ReportDataNodeResponse;

class ControllerApiClient {
 public:
  explicit ControllerApiClient(std::shared_ptr<Channel> channel) : stub_(ControllerApiService::NewStub(channel)) {
    context_ = std::make_unique<grpc::ClientContext>();
    st_ = stub_->ReportDataNode(context_.get());
  }

  void Start() {
    // Start writer thread
    std::thread writer([this]() {
      while (true) {
        ReportDataNodeRequest request;
        request.set_cluster_id("cluster_" + std::to_string(1));
        std::shared_lock<std::shared_mutex> sh_lk(stream_mutex_);
        auto ret = st_->Write(request);
        if (!ret) {
          std::cout << "report to controller, stream write fail." << std::endl;
          sh_lk.unlock();

          std::lock_guard<std::shared_mutex> guard(stream_mutex_);
          context_ = std::make_unique<grpc::ClientContext>();
          st_ = stub_->ReportDataNode(context_.get());

        } else {
          std::cout << "report to controller, stream write succ." << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::seconds(2));
      }
      st_->WritesDone();
    });

    // Start reader thread
    std::thread reader([this]() {
      ReportDataNodeResponse response;
      while (true) {
        std::shared_lock<std::shared_mutex> lk(stream_mutex_);
        while (st_->Read(&response)) {
          std::cout << "Received response for version: " << response.ha_cluster().version() << std::endl;
        }
        std::cout << "stream read while quit, retry" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));
      }
    });

    writer.join();
    reader.join();

    Status status = st_->Finish();
    if (!status.ok()) {
      std::cerr << "RPC error. error message:" << status.error_message() << std::endl;
    }
  }

 private:
  std::unique_ptr<ControllerApiService::Stub> stub_;
  std::unique_ptr<ClientContext> context_;

  mutable std::shared_mutex stream_mutex_;
  std::unique_ptr<::grpc::ClientReaderWriter<::kv::controller::v1::ReportDataNodeRequest,
                                             ::kv::controller::v1::ReportDataNodeResponse>>
      st_;
  // std::shared_ptr<ClientReaderWriter<ReportDataNodeRequest, ReportDataNodeResponse>> stream_{nullptr};
};

int main(int argc, char** argv) {
  ControllerApiClient client(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
  client.Start();
  return 0;
}
