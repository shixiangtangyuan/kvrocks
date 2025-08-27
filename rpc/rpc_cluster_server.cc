#include <grpcpp/grpcpp.h>
#include <kv/controller/v1/api.grpc.pb.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>

#include <iostream>
#include <memory>
#include <string>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using kv::controller::v1::ControllerApiService;
using kv::controller::v1::ReportDataNodeRequest;
using kv::controller::v1::ReportDataNodeResponse;

class ControllerApiServiceImpl final : public ControllerApiService::Service {
 public:
  grpc::Status ReportDataNode(
      grpc::ServerContext* context,
      grpc::ServerReaderWriter<ReportDataNodeResponse, ReportDataNodeRequest>* stream) override {
    ReportDataNodeRequest request;
    while (stream->Read(&request)) {
      std::cout << "Received request for cluster_id: " << request.cluster_id() << std::endl;

      ReportDataNodeResponse response;
      response.mutable_ha_cluster()->set_version(1111);
      stream->Write(response);
    }

    std::cout << "while over" << std::endl;

    return Status::OK;
  }
};

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  ControllerApiServiceImpl service;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<Server> server(builder.BuildAndStart());
  server->GetHealthCheckService()->SetServingStatus(ControllerApiService::service_full_name(), true);
  std::cout << "Server listening on " << server_address << std::endl;

  server->Wait();
}

int main(int argc, char** argv) {
  RunServer();
  return 0;
}
