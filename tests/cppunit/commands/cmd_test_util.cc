#include "commands/cmd_test_util.h"

namespace redis {

Status GenericExecCmd(const std::vector<std::string> &cmd_tokens, std::string *output, std::unique_ptr<Commander> &cmd,
                      Server *srv, Connection *conn, engine::Storage *storage) {
  output->clear();

  cmd->SetArgs(cmd_tokens);
  auto s = cmd->Parse();
  if (!s.IsOK()) {
    LOG(WARNING) << "[test_exec] Err: " << s.Msg();
    return s;
  }
  return cmd->Execute(srv, conn, output, storage);
}

}  // namespace redis
