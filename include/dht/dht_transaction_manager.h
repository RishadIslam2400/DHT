#pragma once

#include <cstddef>
#include <string>
#include <vector>
#include <optional>
#include <arpa/inet.h>
#include <random>

#include "common/dht_common.h"
#include "node_properties.h"
#include "dht_static_partitioning.h"

// Network retry parameters for flushing asynchronous batches
constexpr int MAX_RETRIES = 2;
constexpr int BASE_DELAY_MS = 20;
constexpr int FLUSH_TIMEOUT_MS = 20;

// Dual-purpose manager: handles both high-throughput asynchronous batching
// and acts as the Coordinator for synchronous Two-Phase Commit (2PC) transactions.
class DHTTransactionManager {
private:
  StaticClusterDHTNode &dht_node;
  
  // Thread-local buffer for caching asynchronous PUT requests destined for a specific node
  struct NodeBatch {
    std::string ip;
    int port;
    int id;
    std::vector<PutRequest> requests;
    std::chrono::steady_clock::time_point last_flush_time;

    NodeBatch() : port(-1), id(-1) {
      requests.reserve(BATCH_SIZE);
      last_flush_time = std::chrono::steady_clock::now();
    }
  };
  std::vector<NodeBatch> buffers;

  // Transmits the buffered requests to the target node via TCP and clears the buffer
  void flush_node(NodeBatch &node_batch);

public:
  DHTTransactionManager(StaticClusterDHTNode &node);

  // Ensures no asynchronous writes are lost when the worker thread terminates
  ~DHTTransactionManager() {
    flush_all();
  }

  // Force-drains all node buffers over the network immediately
  void flush_all();
    
  // Eventually Consistent: Assigns a Lamport timestamp and caches in the local buffer.
  // Maximum throughput, but data is invisible to other threads until flushed and commited.
  void put_async(const uint32_t &key, const uint32_t &value);

  // Strictly Consistent: Bypasses the buffer and blocks until the server ACKs.
  // Uses Last-Write-Wins (LWW) conflict resolution. Returns the exact storage outcome.
  PutResult put_sync(const uint32_t &key, const uint32_t &value);

  // Strictly Consistent & Atomic using 2PC 
  // Executes a distributed transaction across multiple nodes using a single master timestamp.
  // Returns true if all nodes committed, false if the transaction aborted.
  bool multi_put(const std::vector<std::pair<uint32_t, uint32_t>>& kv_pairs);

  // Read-Your-Writes Consistent: Snoops the local async buffer first (reverse iteration).
  // If not found, falls back to the physical hash table or remote network request.
  // Updates the lamport clock accross all the nodes
  // This ensures causal consistency but will hurt read throughput
  GetResponse get_async(const uint32_t& key);

  // Strictly Consistent GET
  // Bypasses the local asynchronous buffer to guarantee reading the most recently
  // committed data directly from the physical hash table (Read-Committed Isolation).
  GetResponse get_sync(const uint32_t& key);
};