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

constexpr int FLUSH_TIMEOUT_MS = 20; // Max time async writes sit in memory
constexpr int MAX_RETRIES = 50;      // Max 2PC contention retries

// Dual-purpose manager: 
// Handles high-throughput asynchronous write batching.
// Acts as the Coordinator for synchronous Two-Phase Commit (2PC) distributed transactions.
class DHTTransactionManager {
private:
  StaticClusterDHTNode &dht_node;
  
  // Thread-local memory buffer used exclusively for put_async()
  // Requests are grouped by physical destination Node ID
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

  // Serializes the local requests vector and transmits it via TCP to the target node.
  void flush_node(NodeBatch &node_batch);

public:
  DHTTransactionManager(StaticClusterDHTNode &node);

  // Ensures no asynchronous writes are lost when the worker thread terminates
  ~DHTTransactionManager() {
    flush_all();
  }

  // Flushes all the remaining buffers of all the nodes immediately
  void flush_all();

  // Asynchronous Operations (Eventual Consistency)
    
  // Eventually consistent write.
  // Local replicas: bypasses the buffer and writes instantly to local hash table.
  // Remote replicas: Defers TCP transmission by placing the request in the NodeBatch 
  // until the buffer hits BATCH_SIZE or FLUSH_TIMEOUT_MS expires.
  void put_async(const uint32_t &key, const uint32_t &value);

  // Read-Your-Own-Writes (Session Consistency)
  // Checks the local hash table first (for local replicas). 
  // If remote, it snoops the un-flushed NodeBatch buffer.
  // If not found in either, it executes a remote network read.
  GetResponse get_async(const uint32_t& key);

  // Synchronous Operations (Strict Linearizability)

  // Executes an immediate, blocking write.
  // RF=1: Bypasses local buffers, transmits directly to the target node.
  // RF>1: Automatically upgrades the single key into a Two-Phase Commit (2PC)
  // batch to guarantee all replicas commit the value atomically.
  PutResult put_sync(const uint32_t &key, const uint32_t &value);

  // Atomic distributed transaction utilizing Optimistic Concurrency Control.
  // Executes a distributed transaction across multiple nodes and all replicas using 
  // a single master timestamp. Returns true if all nodes committed.
  bool multi_put(const std::vector<std::pair<uint32_t, uint32_t>>& kv_pairs);

  // Read-Committed Isolation (Strictly Consistent GET)
  // Read the last commited data from the storage.
  // Read locally if possible, or load-balances across remote replicas.
  GetResponse get_sync(const uint32_t& key);
};