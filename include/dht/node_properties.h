#pragma once

#include <vector>
#include <string>
#include <atomic>

struct NodeConfig {
  int id;
  std::string ip;
  int port;
  
  bool operator<(const NodeConfig& other) const { return id < other.id; }
  bool operator==(const NodeConfig& other) const { return id == other.id; }
};

struct NodeStats {
  alignas(64) std::atomic<uint32_t> local_puts_inserted{0};
  alignas(64) std::atomic<uint32_t> local_puts_updated{0};
  alignas(64) std::atomic<uint32_t> local_gets_found{0};
  alignas(64) std::atomic<uint32_t> local_gets_not_found{0};

  alignas(64) std::atomic<uint32_t> remote_puts_success{0};
  alignas(64) std::atomic<uint32_t> remote_puts_failed{0};
  alignas(64) std::atomic<uint32_t> remote_gets_success{0};
  alignas(64) std::atomic<uint32_t> remote_gets_failed{0};
};

std::vector<NodeConfig> load_config(const std::string &filename);