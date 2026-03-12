#include "dht/dht_transaction_manager.h"
#include <algorithm>
#include <thread>
#include <iostream>

// Buffer Management (Async Path)
// Flushes a single node's buffer over the network.
// Relies on the internal retry of send_batch() to handle stale sockets.
// If the node is completely unreachable, it fails fast to prevent blocking the worker thread.
void DHTTransactionManager::flush_node(NodeBatch& node_batch) {
  #ifndef NDEBUG
    if (node_batch.requests.empty())
      return;
  #endif

  size_t count = node_batch.requests.size();

  // Transmit the entire vector payload in a single TCP syscall
  bool success = dht_node.send_batch(node_batch.id, node_batch.ip, node_batch.port, node_batch.requests);

  if (success) [[likely]] {
    dht_node.stats.remote_puts_success.fetch_add(count, std::memory_order_relaxed);
  } else {
    dht_node.stats.remote_puts_failed.fetch_add(count, std::memory_order_relaxed);

    #ifndef NDEBUG
    std::cerr << "[Batcher] WARNING: Dropping batch for Node " << node_batch.id 
              << " after send_batch() retries failed. Node may be offline.\n";
    #endif
  }
  
  // Clear the buffer after a successful send or a failure
  node_batch.requests.clear();
}

// Forces all pending async writes
void DHTTransactionManager::flush_all() {
  for (size_t i = 0; i < dht_node.cluster_map.size(); ++i) {
    flush_node(buffers[i]);
  }
}

DHTTransactionManager::DHTTransactionManager(StaticClusterDHTNode &node) : dht_node(node) {
  buffers.resize(node.cluster_map.size());

  // Initialize the routing info
  for (size_t i = 0; i < buffers.size(); ++i) {
    buffers[i].id = node.cluster_map[i].id;
    buffers[i].ip = node.cluster_map[i].ip;
    buffers[i].port = node.cluster_map[i].port;
  }
}

// Eventual Consistency Implementation
void DHTTransactionManager::put_async(const uint32_t& key, const uint32_t& value) {
  uint64_t async_ts = dht_node.increment_logical_clock();
  auto replicas = dht_node.get_replica_nodes(key);

  // Broadcast the async put to all replicas
  for (int i = 0; i < dht_node.replication_degree; ++i) {
    int target_id = replicas.node_ids[i];
    
    // Write directly to local storage if this node is a replica
    if (target_id == dht_node.self_config.id) {
      dht_node.put_local(key, value, async_ts);
    } else {
      // Defer TCP transmission by appending to the node buffer
      NodeBatch &node_batch = buffers[target_id];
      node_batch.requests.emplace_back(key, value, async_ts);

      // Flush the node buffer if it reaches the batch size
      if (node_batch.requests.size() >= BATCH_SIZE) {
        flush_node(node_batch);
        node_batch.last_flush_time = std::chrono::steady_clock::now(); 
      }
    }
  }

  // Periodic timeout check to ensure low-volume buffers don't stall forever
  static thread_local uint32_t time_check_counter = 0;
  if ((++time_check_counter & 15) == 0) {
    auto now = std::chrono::steady_clock::now();
    
    // Scan all buffers and flush if not empty
    for (NodeBatch& nb : buffers) {
      if (nb.id != dht_node.self_config.id && !nb.requests.empty()) {
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - nb.last_flush_time).count();

        if (elapsed >= FLUSH_TIMEOUT_MS) {
          flush_node(nb);
          nb.last_flush_time = now;
        }
      }
    }
  }
}

GetResponse DHTTransactionManager::get_async(const uint32_t& key) {
  uint64_t read_ts = dht_node.increment_logical_clock();
  auto replicas = dht_node.get_replica_nodes(key);

  // If the current node is a replica, read directly from local storage
  for (int i = 0; i < dht_node.replication_degree; ++i) {
    if (replicas.node_ids[i] == dht_node.self_config.id) {
      std::optional<uint32_t> res = dht_node.get_local(key);
      if (res.has_value()) {
        return GetResponse::success(res.value());
      }

      // Local replica might be behind
      break;
    }
  }

  // Distribute the remote fallback evenly
  static thread_local uint32_t rr_counter = 0;
  int target_index = (rr_counter++) % dht_node.replication_degree;
  int target_id = replicas.node_ids[target_index];

  // Check the un-flushed async buffers
  const std::vector<PutRequest> &requests = buffers[target_id].requests;

  // Iterate in reverse to find the most recent write
  for (auto rev_it = requests.rbegin(); rev_it != requests.rend(); ++rev_it) {
    if (rev_it->key == key) {
      return GetResponse::success(rev_it->value);
    }
  }

  // Execute remote read operation
  const NodeConfig& target = dht_node.cluster_map[target_id];
  return dht_node.get_remote(key, read_ts, target);
}

// Strict Linearizability Implementation

PutResult DHTTransactionManager::put_sync(const uint32_t &key, const uint32_t &value) {
  if (dht_node.replication_degree == 1) {
    uint64_t sync_ts = dht_node.increment_logical_clock();
    const NodeConfig& target = dht_node.get_target_node(key);

    if (target.id == dht_node.self_config.id) {
      return dht_node.put_local(key, value, sync_ts);
    } else {
      return dht_node.put_remote(key, value, sync_ts, target);
    }
  }
  
  // Execute simple put as multi put
  std::vector<std::pair<uint32_t, uint32_t>> batch;
  batch.reserve(1);
  batch.emplace_back(key, value);
  bool success = multi_put(batch);
  
  return success ? PutResult::Inserted : PutResult::Failed;
}

GetResponse DHTTransactionManager::get_sync(const uint32_t& key) {
  uint64_t read_ts = dht_node.increment_logical_clock();

  if (dht_node.replication_degree == 1) {
    const NodeConfig& target = dht_node.get_target_node(key);

    if (target.id == dht_node.self_config.id) {
      std::optional<uint32_t> res = dht_node.get_local(key);
      if (res.has_value()) {
        return GetResponse::success(res.value());
      }

      return GetResponse::not_found();
    }

    return dht_node.get_remote(key, read_ts, target);
  }

  // For replication_degree > 1
  auto replicas = dht_node.get_replica_nodes(key);

  // Check if this node holds a replica for this key
  // Because writes are atomic 2PC, any local replica is guaranteed to be consistent
  for (int i = 0; i < dht_node.replication_degree; ++i) {
    if (replicas.node_ids[i] == dht_node.self_config.id) {
      std::optional<uint32_t> res = dht_node.get_local(key);
      if (res.has_value()) {
        return GetResponse::success(res.value());
      }
      return GetResponse::not_found();
    }
  }

  /* // Because writes are atomic 2PC, it is always safe to read from the first replica.
  int target_id = replicas.node_ids[0];
  const NodeConfig& target = dht_node.cluster_map[target_id]; */
 
  // Distribute remote reads evenly across all replicas
  // Use a thread-local counter to round-robin the requests
  static thread_local uint32_t rr_counter = 0;
  
  int target_index = (rr_counter++) % dht_node.replication_degree;
  int target_id = replicas.node_ids[target_index];

  const NodeConfig& target = dht_node.cluster_map[target_id];
  return dht_node.get_remote(key, read_ts, target);
}

// Two-Phase Commit (2PC) protocol for atomic multi-Key transactions
bool DHTTransactionManager::multi_put(const std::vector<std::pair<uint32_t, uint32_t>> &kv_pairs) {
  int attempt = 0;

  // Deduplicate keys within the batch (Last-Write-Wins)
  // If the benchmark PRNG accidentally generates the same key twice in a tight range (Range=10),
  // we collapse it to save network bandwidth and prevent self-collision locks.
  /* std::vector<std::pair<uint32_t, uint32_t>> deduped_pairs;
  for (const auto& kv : kv_pairs) {
    auto it = std::find_if(deduped_pairs.begin(), deduped_pairs.end(), 
                           [&](const auto& p) { return p.first == kv.first; });
    if (it != deduped_pairs.end()) {
      it->second = kv.second; // Update to the newest value
    } else {
      deduped_pairs.push_back(kv);
    }
  } */

  // Pre-allocate tracking vectors
  size_t num_nodes = dht_node.cluster_map.size();
  std::vector<std::vector<std::pair<uint32_t, uint32_t>>> cohorts(num_nodes);
  std::vector<int> active_cohort_ids;
  active_cohort_ids.reserve(num_nodes); // max unique nodes equal to num nodes
  std::vector<int> prepared_cohorts;
  prepared_cohorts.reserve(num_nodes);

  // Partition keys by destination nodes and fix active cohorts
  for (const auto& kv : kv_pairs) {
    auto replicas = dht_node.get_replica_nodes(kv.first);

    // Add the key to every replica's batch
    for (int i = 0; i < dht_node.replication_degree; ++i) {
      int target_id = replicas.node_ids[i];

      // Extract unique target IDs to keep track of active cohorts
      if (cohorts[target_id].empty()) {
        active_cohort_ids.push_back(target_id);
      }
      cohorts[target_id].push_back(kv);
    }
  }

  // We sort cohort IDs in strict mathematical ascending order (e.g., Node 0 -> Node 1).
  // Initially we used "local_id" first. Enforcing a strict global sequence prevents symmetric 
  // livelocks where Node A holds A and requests B, while Node B holds B and requests A.
  int local_id = dht_node.self_config.id;
  std::sort(active_cohort_ids.begin(), active_cohort_ids.end());

  // Optimistic concurrency control retry loop
  while (attempt < MAX_RETRIES) {
    attempt++;

    // Generate a strictly newer Lamport timestamp to force LWW progress
    uint64_t tx_timestamp = dht_node.increment_logical_clock();
    prepared_cohorts.clear();
    bool abort_required = false;

    // Phase 1: PREPARE
    // Request exclusive logical locks and validate Last-Write-Wins timestamps
    for (int target_id : active_cohort_ids) {
      const auto &batch = cohorts[target_id];
      bool prepared = false;

      if (target_id == dht_node.self_config.id) {
        prepared = dht_node.local_tx_prepare(tx_timestamp, batch);
      } else {
        prepared = dht_node.send_tx_prepare(target_id, tx_timestamp, batch);
      }

      if (prepared) {
        prepared_cohorts.push_back(target_id);
      } else {
        abort_required = true;
        break; // Stop preparing immediately on first rejection
      }
    }

    // Lambda for bounded phase 2 delivery guarantee
    auto enforce_phase2 = [&](int target_id, bool is_commit) {
      // Perform local commit/abort
      if (target_id == dht_node.self_config.id) {
        if (is_commit) {
          dht_node.local_tx_commit(tx_timestamp);
        } else {
          dht_node.local_tx_abort(tx_timestamp);
        }
        return;
      }
      
      // Bounded retry loop guarantees the target node receives the command and 
      // safely releases its locks, preventing permanent distributed deadlocks.
      int delivery_attempts = 0;
      bool delivered = false;
      while (delivery_attempts < 5 && !delivered) {
        if (is_commit) {
          delivered = dht_node.send_tx_commit(target_id, tx_timestamp);
        } else {
          delivered = dht_node.send_tx_abort(target_id, tx_timestamp);
        }
        
        if (!delivered) {
          delivery_attempts++;
          dht_node.stats.coordinator_phase2_retries.fetch_add(1, std::memory_order_relaxed);
          std::this_thread::sleep_for(std::chrono::milliseconds(2 * delivery_attempts));
        }
      }
      if (!delivered) {
        std::cerr << "[Coordinator] CRITICAL: Phase 2 delivery failed for Node " << target_id << ". Locks orphaned.\n";
        // Graceful termination 
        // Handle by increasing delivery attempts
        throw std::runtime_error("Fatal 2PC Error: Phase 2 Delivery Failed. Cluster state compromised.");
      }
    };

    // Phase 2: COMMIT or ABORT
    if (abort_required) {
      // Rollback the locks that successfully prepared
      for (int target_id : prepared_cohorts) {
        enforce_phase2(target_id, false);
      }

      dht_node.stats.coordinator_tx_retries.fetch_add(1, std::memory_order_relaxed);
      
      int backoff_us = 10 * (1 << std::min(attempt, 10));
      std::this_thread::sleep_for(std::chrono::microseconds(backoff_us));
      continue; 
    } else {
      // All nodes voted yes, Execute the atomic write.
      for (int target_id : active_cohort_ids) {
        enforce_phase2(target_id, true);
      }

      dht_node.stats.coordinator_tx_committed.fetch_add(1, std::memory_order_relaxed);
      return true; // Transaction completely succeeded
    }
  }

  dht_node.stats.coordinator_tx_failed.fetch_add(1, std::memory_order_relaxed);
  return false; // Exhausted all retries due to extreme contention
} 