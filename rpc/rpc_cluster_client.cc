#include <grpcpp/grpcpp.h>
#include <kv/controller/v1/api.grpc.pb.h>

#include <atomic>
#include <iostream>
#include <memory>
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
  explicit ControllerApiClient(std::shared_ptr<Channel> channel) : stub_(ControllerApiService::NewStub(channel)) {}

  void Start() {
    ClientContext context;
    std::shared_ptr<ClientReaderWriter<ReportDataNodeRequest, ReportDataNodeResponse>> stream(
        stub_->ReportDataNode(&context));

    // Start writer thread
    std::thread writer([stream]() {
      for (int i = 0; i < 4; ++i) {
        ReportDataNodeRequest request;
        request.set_cluster_id("cluster_" + std::to_string(i));
        std::cout << "Sending cluster_id: " << request.cluster_id() << std::endl;
        stream->Write(request);
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
      stream->WritesDone();
    });

    // Start reader thread
    std::thread reader([stream]() {
      ReportDataNodeResponse response;
      while (stream->Read(&response)) {
        // std::cout << "Received response for cluster_id: " << response.cluster_id() << std::endl;
        std::cout << "Received response for cluster_id: " << response.ha_cluster().active().cluster_id() << std::endl;
        std::cout << "version:" << response.ha_cluster().version() << std::endl;
      }
    });

    writer.join();
    reader.join();

    Status status = stream->Finish();
    if (!status.ok()) {
      std::cerr << "ReportDataNode RPC failed." << std::endl;
    }
  }

 private:
  std::unique_ptr<ControllerApiService::Stub> stub_;
};

int main(int argc, char** argv) {
  ControllerApiClient client(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
  client.Start();
  return 0;
}
