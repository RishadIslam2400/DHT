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
  // Local Storage Metrics (Single-key)
  alignas(64) std::atomic<uint32_t> local_puts_inserted{0};
  alignas(64) std::atomic<uint32_t> local_puts_updated{0};
  alignas(64) std::atomic<uint32_t> local_puts_dropped{0};
  alignas(64) std::atomic<uint32_t> local_gets_found{0};
  alignas(64) std::atomic<uint32_t> local_gets_not_found{0};

  // Remote Network Metrics (Single-key async/sync)
  alignas(64) std::atomic<uint32_t> remote_puts_success{0};
  alignas(64) std::atomic<uint32_t> remote_puts_failed{0};
  alignas(64) std::atomic<uint32_t> remote_gets_success{0};
  alignas(64) std::atomic<uint32_t> remote_gets_failed{0};

  // 2PC Metrics  
  // Coordinator (Client-side) metrics
  alignas(64) std::atomic<uint32_t> tx_committed{0};
  alignas(64) std::atomic<uint32_t> tx_aborted{0};

  // Cohort (Server-side) metrics for PREPARE phase analysis
  alignas(64) std::atomic<uint32_t> tx_prepare_rejected_locked{0};   // Failed due to active 2PC lock contention
  alignas(64) std::atomic<uint32_t> tx_prepare_rejected_obsolete{0}; // Failed due to LWW timestamp ordering
};

std::vector<NodeConfig> load_config(const std::string &filename);