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
  // Local Storage Metrics
  alignas(64) std::atomic<uint32_t> local_puts_inserted{0};
  alignas(64) std::atomic<uint32_t> local_puts_updated{0};
  alignas(64) std::atomic<uint32_t> local_puts_dropped{0};
  alignas(64) std::atomic<uint32_t> local_gets_found{0};
  alignas(64) std::atomic<uint32_t> local_gets_not_found{0};

  // Operation latencies
  alignas(64) std::atomic<uint64_t> remote_puts_total_ns{0};
  alignas(64) std::atomic<uint64_t> local_tx_commit_total_ns{0};

  // Remote Network Metrics
  alignas(64) std::atomic<uint32_t> remote_puts_success{0};
  alignas(64) std::atomic<uint32_t> remote_puts_failed{0};
  alignas(64) std::atomic<uint32_t> remote_gets_success{0};
  alignas(64) std::atomic<uint32_t> remote_gets_failed{0};

  // Cohort 2PC Metrics
  alignas(64) std::atomic<uint32_t> tx_prepare_rejected_locked{0};
  alignas(64) std::atomic<uint32_t> tx_prepare_rejected_obsolete{0};
  alignas(64) std::atomic<uint32_t> local_tx_committed{0};
  alignas(64) std::atomic<uint32_t> local_tx_aborted{0};

  // Coordinator 2PC metrics
  alignas(64) std::atomic<uint32_t> coordinator_tx_committed{0};
  alignas(64) std::atomic<uint32_t> coordinator_tx_retries{0};
  alignas(64) std::atomic<uint32_t> coordinator_tx_failed{0};
  alignas(64) std::atomic<uint32_t> coordinator_phase2_retries{0};

  // Consensus metrics
  alignas(64) std::atomic<uint32_t> consensus_elections_started{0};
  alignas(64) std::atomic<uint32_t> consensus_term_changes{0};
  alignas(64) std::atomic<uint32_t> consensus_log_entries_appended{0};
  alignas(64) std::atomic<uint32_t> consensus_log_truncations{0}; 
  alignas(64) std::atomic<uint32_t> consensus_state_machine_applied{0}; 
};

std::vector<NodeConfig> load_config(const std::string &filename);