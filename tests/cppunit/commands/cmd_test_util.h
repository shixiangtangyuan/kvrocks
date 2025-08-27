#pragma once

#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <kv/controller/v1/api.grpc.pb.h>

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "server/redis_connection.h"
#include "server/server.h"
#include "storage/storage.h"

namespace redis {

Status GenericExecCmd(const std::vector<std::string> &cmd_tokens, std::string *output, std::unique_ptr<Commander> &cmd,
                      Server *srv, Connection *conn, engine::Storage *storage);

}
