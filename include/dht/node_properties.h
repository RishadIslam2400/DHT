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
  alignas(64) std::atomic<uint64_t> remote_gets_total_ns{0};

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
  alignas(64) std::atomic<uint64_t> local_tx_prepare_total_ns{0};
  alignas(64) std::atomic<uint64_t> local_tx_commit_total_ns{0};
  alignas(64) std::atomic<uint64_t> local_tx_abort_total_ns{0};

  // Coordinator 2PC metrics
  alignas(64) std::atomic<uint32_t> coordinator_prepare_success{0};
  alignas(64) std::atomic<uint32_t> coordinator_tx_committed{0};
  alignas(64) std::atomic<uint32_t> coordinator_tx_failed{0};
  alignas(64) std::atomic<uint32_t> coordinator_tx_retries{0};
  alignas(64) std::atomic<uint32_t> coordinator_phase2_retries{0};
  alignas(64) std::atomic<uint64_t> coordinator_tx_total_ns{0};
  alignas(64) std::atomic<uint64_t> coordinator_prepare_total_ns{0};
  alignas(64) std::atomic<uint64_t> coordinator_phase2_commit_ns{0};

  // Consensus metrics
  alignas(64) std::atomic<uint32_t> consensus_elections_started{0};
  alignas(64) std::atomic<uint32_t> consensus_term_changes{0};
  alignas(64) std::atomic<uint32_t> consensus_log_entries_appended{0};
  alignas(64) std::atomic<uint32_t> consensus_log_truncations{0}; 
  alignas(64) std::atomic<uint32_t> consensus_state_machine_applied{0}; 
};

std::vector<NodeConfig> load_config(const std::string &filename);

struct TelemetryBatcher {
  NodeStats* stats = nullptr;

  // Local Hash Table Metrics
  uint32_t local_inserted = 0, local_updated = 0, local_dropped = 0;
  uint32_t local_found = 0, local_not_found = 0;
  
  // Remote RPC Metrics
  uint32_t remote_put_success = 0, remote_put_failed = 0;
  uint64_t remote_get_ns = 0; uint32_t remote_get_success = 0, remote_get_failed = 0;

  // 2PC Cohort Metrics
  uint64_t tx_prepare_ns = 0; uint32_t tx_prepare_locked = 0, tx_prepare_obsolete = 0;
  uint64_t tx_commit_ns = 0;  uint32_t local_tx_committed = 0;
  uint64_t tx_abort_ns = 0;   uint32_t local_tx_aborted = 0;

  // 2PC Coordinator Metrics
  uint64_t coord_prepare_ns = 0, coord_phase2_commit_ns = 0;
  uint32_t coord_prepare_success = 0;
  uint32_t coord_tx_committed = 0, coord_tx_failed = 0;
  uint32_t coord_tx_retries = 0, coord_phase2_retries = 0;

  // Constructor
  TelemetryBatcher() = default;
  TelemetryBatcher(const TelemetryBatcher&) = delete;
  TelemetryBatcher& operator=(const TelemetryBatcher&) = delete;
  TelemetryBatcher(TelemetryBatcher&&) = delete;
  TelemetryBatcher& operator=(TelemetryBatcher&&) = delete;

  ~TelemetryBatcher() { flush_all(); }

  void flush_local_puts() {
    if (!stats) return;
    if (local_inserted > 0) stats->local_puts_inserted.fetch_add(local_inserted, std::memory_order_relaxed);
    if (local_updated > 0)  stats->local_puts_updated.fetch_add(local_updated, std::memory_order_relaxed);
    if (local_dropped > 0)  stats->local_puts_dropped.fetch_add(local_dropped, std::memory_order_relaxed);
  }

  void flush_local_gets() {
    if (!stats) return;
    if (local_found > 0)     stats->local_gets_found.fetch_add(local_found, std::memory_order_relaxed);
    if (local_not_found > 0) stats->local_gets_not_found.fetch_add(local_not_found, std::memory_order_relaxed);
  }

  void flush_remote_puts() {
    if (!stats) return;
    if (remote_put_success > 0) stats->remote_puts_success.fetch_add(remote_put_success, std::memory_order_relaxed);
    if (remote_put_failed > 0)  stats->remote_puts_failed.fetch_add(remote_put_failed, std::memory_order_relaxed);
  }

  void flush_remote_gets() {
    if (!stats) return;
    if (remote_get_ns > 0)      stats->remote_gets_total_ns.fetch_add(remote_get_ns, std::memory_order_relaxed);
    if (remote_get_success > 0) stats->remote_gets_success.fetch_add(remote_get_success, std::memory_order_relaxed);
    if (remote_get_failed > 0)  stats->remote_gets_failed.fetch_add(remote_get_failed, std::memory_order_relaxed);
  }

  void flush_cohort_tx_metrics() {
    if (!stats) return;
    if (tx_prepare_ns > 0)       stats->local_tx_prepare_total_ns.fetch_add(tx_prepare_ns, std::memory_order_relaxed);
    if (tx_prepare_locked > 0)   stats->tx_prepare_rejected_locked.fetch_add(tx_prepare_locked, std::memory_order_relaxed);
    if (tx_prepare_obsolete > 0) stats->tx_prepare_rejected_obsolete.fetch_add(tx_prepare_obsolete, std::memory_order_relaxed);
    if (tx_commit_ns > 0)        stats->local_tx_commit_total_ns.fetch_add(tx_commit_ns, std::memory_order_relaxed);
    if (local_tx_committed > 0)  stats->local_tx_committed.fetch_add(local_tx_committed, std::memory_order_relaxed);
    if (tx_abort_ns > 0)         stats->local_tx_abort_total_ns.fetch_add(tx_abort_ns, std::memory_order_relaxed);
    if (local_tx_aborted > 0)    stats->local_tx_aborted.fetch_add(local_tx_aborted, std::memory_order_relaxed);
  }

  void flush_coord_tx_metrics() {
    if (!stats) return;
    if (coord_prepare_success > 0)  stats->coordinator_prepare_success.fetch_add(coord_prepare_success, std::memory_order_relaxed);
    if (coord_tx_committed > 0)     stats->coordinator_tx_committed.fetch_add(coord_tx_committed, std::memory_order_relaxed);
    if (coord_tx_retries > 0)       stats->coordinator_tx_retries.fetch_add(coord_tx_retries, std::memory_order_relaxed);
    if (coord_tx_failed > 0)        stats->coordinator_tx_failed.fetch_add(coord_tx_failed, std::memory_order_relaxed);
    if (coord_phase2_retries > 0)   stats->coordinator_phase2_retries.fetch_add(coord_phase2_retries, std::memory_order_relaxed);
    if (coord_prepare_ns > 0)       stats->coordinator_prepare_total_ns.fetch_add(coord_prepare_ns, std::memory_order_relaxed);
    if (coord_phase2_commit_ns > 0) stats->coordinator_phase2_commit_ns.fetch_add(coord_phase2_commit_ns, std::memory_order_relaxed);
  }

  void flush_all() {
    flush_local_puts();
    flush_local_gets();
    flush_remote_puts();
    flush_remote_gets();
    flush_cohort_tx_metrics();
    flush_coord_tx_metrics();
  }
};