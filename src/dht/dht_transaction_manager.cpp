#include "dht/dht_transaction_manager.h"
#include <algorithm>
#include <thread>
#include <iostream>

// Flushes a single node's buffer over the network.
// Uses exponential backoff with jitter if a node is temporarily down.
void DHTTransactionManager::flush_node(NodeBatch& node_batch) {
  if (node_batch.requests.empty())
    return;

  size_t count = node_batch.requests.size();
  bool success = false;
  int attempt = 0;
  static thread_local std::mt19937 generator(std::random_device{}());

  while (attempt < MAX_RETRIES && !success) {
    success = dht_node.send_batch(node_batch.id, node_batch.ip, node_batch.port, node_batch.requests);

    if (success) {
      dht_node.stats.remote_puts_success.fetch_add(count, std::memory_order_relaxed);
      break;
    } else {
      attempt++;
      if (attempt < MAX_RETRIES) {
        int delay_ms = BASE_DELAY_MS * (1 << (attempt - 1));
        std::uniform_int_distribution<int> distribution(delay_ms * 0.8, delay_ms * 1.2);
        int jittered_delay = distribution(generator);
        
#ifndef NDEBUG
        std::cerr << "[TansactionManager] Retry " << attempt << "/" << MAX_RETRIES 
                  << " for Node " << node_batch.id 
                  << " after " << jittered_delay << "ms...\n";
#endif

        std::this_thread::sleep_for(std::chrono::milliseconds(jittered_delay));
      }
    }
  }

  if (!success) {
    dht_node.stats.remote_puts_failed.fetch_add(count, std::memory_order_relaxed);

#ifndef NDEBUG
    std::cerr << "[Batcher] CRITICAL: Dropping batch for Node " << node_batch.id 
              << " after " << MAX_RETRIES << " attempts.\n";
#endif
  }
  
  // Clear the buffer after a successful send or a failure
  node_batch.requests.clear();
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

// Eventually Consistent PUT
// Buffers requests locally and sends them in batches to minimize TCP overhead.
void DHTTransactionManager::put_async(const uint32_t& key, const uint32_t& value) {
  // Generate the Lamport timestamp
  uint64_t async_ts = dht_node.increment_logical_clock();
  const NodeConfig& target = dht_node.get_target_node(key);

  // Write directly to the local storage if the key is local
  if (target.id == dht_node.self_config.id) {
    dht_node.put_local(key, value, async_ts);
    return;
  }

  NodeBatch &node_batch = buffers[target.id];
  node_batch.requests.emplace_back(key, value, async_ts);

  // Flush if the batch is full
  if (node_batch.requests.size() >= BATCH_SIZE) {
    flush_node(node_batch);
    node_batch.last_flush_time = std::chrono::steady_clock::now(); 
    return;
  }

  // Check the timeout once every 16 operations
  static thread_local uint32_t time_check_counter = 0;
  if ((++time_check_counter & 15) == 0) {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - node_batch.last_flush_time).count();
    
    if (elapsed >= FLUSH_TIMEOUT_MS) {
      flush_node(node_batch);
      node_batch.last_flush_time = now;
    }
  }
}

// Strictly Consistent PUT
// Bypasses the buffer and forces an immediate put request.
PutResult DHTTransactionManager::put_sync(const uint32_t &key, const uint32_t &value) {
  uint64_t sync_ts = dht_node.increment_logical_clock();
  const NodeConfig& target = dht_node.get_target_node(key);

  if (target.id == dht_node.self_config.id) {
    return dht_node.put_local(key, value, sync_ts);
  }

  return dht_node.put_remote(key, value, sync_ts, target);
}

// GET operation that updates the lamport clock with read timestamp
// Maintains session consistency using buffer snooping
GetResponse DHTTransactionManager::get_async(const uint32_t& key) {
  uint64_t read_ts = dht_node.increment_logical_clock();
  const NodeConfig& target = dht_node.get_target_node(key);

  if (target.id == dht_node.self_config.id) {
    std::optional<uint32_t> res = dht_node.get_local(key);
    if (res.has_value()) {
      return GetResponse::success(res.value());
    }

    return GetResponse::not_found();
  }

  // Check the un-flushed writes so the thread can read its own modifications
  const std::vector<PutRequest> &requests = buffers[target.id].requests;

  // Iterate backwards to find the most recent write for this key
  for (auto rev_it = requests.rbegin(); rev_it != requests.rend(); ++rev_it) {
    if (rev_it->key == key) {
      return GetResponse::success(rev_it->value);
    }
  }

  // If key not in local buffer, perform regular read commit isolation get operation
  return dht_node.get_remote(key, read_ts, target);
}

// Strictly Consistent GET
// Bypasses the local asynchronous buffer to guarantee reading the most recently
// committed data directly from the physical hash table (Read-Committed Isolation).
GetResponse DHTTransactionManager::get_sync(const uint32_t& key) {
  uint64_t read_ts = dht_node.increment_logical_clock();
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

// Forces all pending async writes
void DHTTransactionManager::flush_all() {
  for (size_t i = 0; i < dht_node.cluster_map.size(); ++i) {
    flush_node(buffers[i]);
  }
}

// Two-Phase Commit (2PC) protocol for atomic multi-Key transactions
bool DHTTransactionManager::multi_put(const std::vector<std::pair<uint32_t, uint32_t>> &kv_pairs) {
  constexpr int MAX_RETRIES = 50;
  int attempt = 0;

  // Allocate buffers for 2PC
  size_t num_nodes = dht_node.cluster_map.size();
  std::vector<std::vector<std::pair<uint32_t, uint32_t>>> cohorts(num_nodes);
  std::vector<int> active_cohort_ids;
  active_cohort_ids.reserve(num_nodes);
  std::vector<int> prepared_cohorts;
  prepared_cohorts.reserve(active_cohort_ids.size());

  // Partition keys to respective cohorts and fix active cohorts
  for (const auto& kv : kv_pairs) {
    int target_id = dht_node.get_target_node(kv.first).id;
    
    // Extract unique target IDs
    if (cohorts[target_id].empty()) {
      active_cohort_ids.push_back(target_id);
    }
    cohorts[target_id].push_back(kv);
  }

  // Local-First Sort guarantees that if the local node is involved, it is placed at index 0.
  int local_id = dht_node.self_config.id;
  std::sort(active_cohort_ids.begin(), active_cohort_ids.end(), [local_id](int a, int b) {
    if (a == local_id) return true;
    if (b == local_id) return false;
    return a < b; 
  });

  // 2PC using optimistic concurrency control - retry if failed
  while (attempt < MAX_RETRIES) {
    attempt++;

    // Generate a strictly newer timestamp for every attempt to force LWW progress
    uint64_t tx_timestamp = dht_node.increment_logical_clock();
    prepared_cohorts.clear();
    bool abort_required = false;

    // Phase 1: PREPARE
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

    // Lambda for bounded Phase 2 Delivery Guarantee
    auto enforce_phase2 = [&](int target_id, bool is_commit) {
      if (target_id == dht_node.self_config.id) {
        if (is_commit) {
          dht_node.local_tx_commit(tx_timestamp);
        } else {
          dht_node.local_tx_abort(tx_timestamp);
        }
        return;
      }
      
      // Bounded retry to guarantee target node releases its locks
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
          std::this_thread::sleep_for(std::chrono::milliseconds(2 * delivery_attempts));
        }
      }
      if (!delivered) {
        std::cerr << "[Coordinator] CRITICAL: Phase 2 delivery failed for Node " << target_id << ". Locks orphaned.\n";
      }
    };

    // Phase 2: COMMIT or ABORT
    if (abort_required) {
      // Rollback the locks that successfully prepared
      for (int target_id : prepared_cohorts) {
        enforce_phase2(target_id, false);
      }
      
      // Exponential backoff before starting the next Phase 1 attempt
      int backoff_us = 10 * (1 << std::min(attempt, 10));
      std::this_thread::sleep_for(std::chrono::microseconds(backoff_us));
      continue; 
    } else {
      // All nodes voted yes, Execute the atomic write.
      for (int target_id : active_cohort_ids) {
        enforce_phase2(target_id, true);
      }
      return true; // Transaction completely succeeded
    }
  }

  return false; // Exhausted all retries due to extreme contention
} 