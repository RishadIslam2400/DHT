#pragma once

#include <cstddef>
#include <vector>
#include <optional>
#include <unordered_map>
#include <mutex>

#include "common/dht_common.h"
#include "common/spin_lock.h"

// Forward declaration to prevent circular inclusion
class StaticClusterDHTNode;

// Manages synchronous Two-Phase Commit (2PC) distributed transactions,
// backed by the consensus protocols for fault-tolerance.
class DHTTransactionManager {
private:
  StaticClusterDHTNode &dht_node;

  // Tracks the final decision of recent transactions
  std::unordered_map<uint64_t, bool> transaction_registry;
  mutable AlignedSpinlock registry_mutex;

public:
  explicit DHTTransactionManager(StaticClusterDHTNode &node);
  ~DHTTransactionManager() = default;

  // Executes an atomic distributed transaction utilizing Optimistic Concurrency Control.
  // 1. Broadcasts PREPARE to all involved Cohorts.
  // 2. Evaluates votes (Aborts if any Cohort votes No or locks conflict).
  // 3. Proposes the global COMMIT/ABORT decision to the local group.
  // 4. Returns the decision to the client, while background threads handle Phase 2.
  TransactionResult execute_transaction(const std::vector<std::pair<uint32_t, uint32_t>>& kv_pairs);

  // Read-Committed Isolation (Strictly Consistent GET)
  // Reads locally if possible, or load-balances across remote replicas.
  // Bypasses logical locks.
  GetResponse get_sync(const uint32_t key);

  // Called by the Node's apply_committed_log() when Raft applies a CoordinatorCommitIntent.
  // Populates the volatile registry so the Coordinator can instantly answer Cohort checks.
  void record_transaction_decision(uint64_t tx_timestamp, bool is_commit);

  // Called by the Node's handle_client() when a stuck Cohort asks for transaction status.
  // Returns true if the transaction was globally committed, false if aborted/unknown.
  bool is_transaction_committed(uint64_t tx_timestamp) const;
};