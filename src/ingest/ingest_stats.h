#pragma once

#include <google/rpc/status.pb.h>  // NOLINT
#include <grpcpp/impl/status.h>
#include <kv/datanode/v1/common.pb.h>
#include <kv/datanode/v1/ingest.pb.h>

using kv::datanode::v1::Error;
using kv::datanode::v1::ErrorCode;

inline Error CreateIngestError(ErrorCode code, const std::string& msg) {
  Error error;
  error.set_code(code);
  error.set_message(msg);
  return error;
}

inline grpc::Status IngestStatus(const Error& error) {
  google::rpc::Status s;
  s.add_details()->PackFrom(error);
  s.set_code(static_cast<int>(grpc::StatusCode::UNKNOWN));
  return {grpc::StatusCode::UNKNOWN, "", s.SerializeAsString()};
}

// Macro to generate error and status creation functions for each error code
#define INGEST_ERROR_STATUS_FUNC_GENERATE(code, name)                                                               \
  inline Error name##Error(const std::string& msg) { return CreateIngestError(ErrorCode::ERROR_CODE_##code, msg); } \
                                                                                                                    \
  inline grpc::Status name##IngestStatus(const std::string& msg) {                                                  \
    auto error = name##Error(msg);                                                                                  \
    return IngestStatus(error);                                                                                     \
  }

INGEST_ERROR_STATUS_FUNC_GENERATE(INGESTION_TASK_FINISHED, IngestTaskFinished);
INGEST_ERROR_STATUS_FUNC_GENERATE(INGESTION_TASK_RUNNING, IngestTaskRunning);
INGEST_ERROR_STATUS_FUNC_GENERATE(INGESTION_TASK_NOT_FOUND, IngestTaskNotFound);
INGEST_ERROR_STATUS_FUNC_GENERATE(INGESTION_TASK_FAILED, IngestTaskFailed);

// Predefined error and status instances
#define CONST_INGEST_ERROR_STATUS_GENERATE(name, msg) \
  const Error k##name##Error = name##Error(#msg);     \
  const grpc::Status k##name##Status = name##IngestStatus(#msg)

CONST_INGEST_ERROR_STATUS_GENERATE(IngestTaskFinished, Ingest task succeeded);
CONST_INGEST_ERROR_STATUS_GENERATE(IngestTaskRunning, ingest task is running);
CONST_INGEST_ERROR_STATUS_GENERATE(IngestTaskNotFound, ingest task not find);
CONST_INGEST_ERROR_STATUS_GENERATE(IngestTaskFailed, ingest task failed);
