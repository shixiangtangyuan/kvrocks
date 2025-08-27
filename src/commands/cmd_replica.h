#include <gtest/gtest.h>

#include "commands/commander.h"

namespace redis {

class CommandReplica : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override;

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override;

 private:
  FRIEND_TEST(CommandReplica, Base);

  std::string subcommand_;
  int slot_id_ = -1;
  int64_t log_id_ = -1;
};

}  // namespace redis
