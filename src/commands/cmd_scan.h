#pragma once

#include <string>
#include <vector>

#include "commands/commander.h"
#include "commands/scan_base.h"

namespace redis {

class CommandNodeScan : public CommandScanBase {
 public:
  CommandNodeScan() = default;

  Status Parse(const std::vector<std::string> &args) override;
  Status ParseSlotRange(const std::string &param);
  static std::string GenerateOutput(const std::vector<std::string> &keys, uint64_t cursor);
  int16_t GetStartSlotId() const {
    if (client_cursor_ == 0) return start_;
    return slot_id_;
  }

  Status Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) override;

 private:
  uint64_t client_cursor_;
  int16_t slot_id_;
  int16_t start_;  // startId of slotrange
  int16_t end_;    // endId of slotrange
};

}  // namespace redis
