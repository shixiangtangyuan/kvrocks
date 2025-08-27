#include <gtest/gtest.h>

#include "commands/commander.h"

namespace redis {

class CommandCluster : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override;

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override;

 private:
  FRIEND_TEST(CommandCluster, Base);

  std::string subcommand_;
  std::string key_;
  int slot_id_ = -1;
  std::string node_id_;
};

}  // namespace redis
