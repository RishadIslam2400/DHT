#pragma once

#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <condition_variable>
#include <set>
#include <memory>
#include <atomic>
#include <array>

#include "node_properties.h"
#include "common/dht_common.h"
#include "hash_table/timestamped_striped_lock_concurrent_hash_table.h"
#include "network/connection_pool.h"
#include "common/spin_lock.h"

// Forward declaration of the template
// template <typename DHTType>
class DHTTransactionManager;

/// This class represents a single physical server in the distributed cluster.
/// It is responsible for physical storage, network routing, connection pooling, 
/// and executing the server-side state machine for Two-Phase Commit (2PC).
class StaticClusterDHTNode {
private:
  friend class DHTTransactionManager;

  /// Topology & Storage
  std::vector<NodeConfig> cluster_map;
  NodeConfig self_config;
  TimestampedStripedLockConcurrentHashTable<uint32_t, uint32_t> storage;
  int replication_degree;

  /// Networking & Concurrency
  int server_fd;
  std::atomic<bool> running;
  std::thread listener_thread;

  // Maintains persistent, pre-warmed TCP sockets to avoid 3-way handshake overhead
  ConnectionPool connection_pool;

  // Mechanism for listener thread to gracefully join client threads during shutdown
  std::mutex thread_mutex;
  std::vector<std::thread> client_threads;
  std::vector<int> active_sockets;

  // Mechanism for distributed barrier (Synchronized start and shutdown)
  std::mutex barrier_mtx;
  std::condition_variable barrier_cv;
  std::set<int> barrier_checkins;
  std::atomic<bool> benchmark_ready;

  /// 2PC Concurrency Control Mechanism

  // Global Lamport Clock
  alignas(64) std::atomic<uint64_t> logical_clock{0};

  size_t num_logical_stripes;
  size_t logical_stripe_mask;

  // Dynamically allocated arrays for the logical state
  std::unique_ptr<Spinlock[]> stripe_locks;
  std::vector<std::unordered_set<uint32_t>> logically_locked_stripes;
  
  // Holds data payloads that have passed Phase 1 validation but await Phase 2 COMMIT
  struct StagedTx {
    uint64_t tx_timestamp;
    std::vector<std::pair<uint32_t, uint32_t>> batch;
  };

  std::unique_ptr<Spinlock[]> staging_locks;
  std::vector<std::vector<StagedTx>> staging_stripes;

  /// Routing
  inline uint64_t hash_key(const std::string &key) const {
    return XXHash64::hash(key.data(), key.length(), 0);
  }
  inline uint64_t hash_key(const uint32_t &key) const {
    return XXHash64::hash(&key, sizeof(key), 0);
  }

  // Zero-allocation stack structure to return replica target IDs
  struct ReplicaSet {
    uint8_t node_ids[8]; // Hard limit of 8 replicas
  };

  // Used when replication_degree > 1. Maps a key to multiple physical nodes.
  inline ReplicaSet get_replica_nodes(const uint32_t &key) const {
    ReplicaSet replicas;

    uint64_t hash_val = hash_key(key);
    size_t base_index = hash_val % cluster_map.size();

    // Walk the cluster ring to assign replicas sequentially
    for (int i = 0; i < replication_degree; ++i) {
      size_t target_index = (base_index + i) % cluster_map.size();
      replicas.node_ids[i] = cluster_map[target_index].id;
    }

    return replicas;
  }
  
  // Used when replication_degree == 1. Fast path for single-node partitioning.
  inline const NodeConfig &get_target_node(const uint32_t &key) const {
    uint64_t hash_val = hash_key(key);

    size_t target_index = hash_val % cluster_map.size();
    return cluster_map[target_index];
  }

  /// Network I/O Primitives
  static bool recv_n_bytes(const int sock, void *buffer, const size_t n);
  // Uses Scatter-Gather (writev) to send a command byte + payload in 1 syscall
  bool perform_rpc_single_request(const int sock, CommandType cmd,
                                  const uint8_t *request, size_t request_size,
                                  uint8_t *response, size_t response_size);
  // Wraps RPC in a retry loop using the Connection Pool
  bool send_single_request(const int target_id, const std::string &target_ip,
                           const int target_port, CommandType cmd,
                           const uint8_t *request, size_t request_size,
                           uint8_t *response, size_t response_size);
  // Sends an array of PutRequests for asynchronous batching
  bool send_batch(const int target_id, const std::string &target_ip,
                  const int target_port, const std::vector<PutRequest> &batch_requests);
  
  /// Server-Side Execution Logic
  // Enforces Causal Consistency by advancing the local clock if a packet arrives from the future
  void synchronize_clock(const uint64_t incoming_ts);

  // Direct local storage operations
  PutResult put_local(const uint32_t &key, const uint32_t &value,
                      const uint64_t &timestamp);
  std::optional<uint32_t> get_local(const uint32_t &key) const;
  
  // Wrappers for RF=1 direct network routing
  PutResult put_remote(const uint32_t &key, const uint32_t &value,
                       const uint64_t &timestamp, const NodeConfig &target);
  GetResponse get_remote(const uint32_t &key, const uint64_t &read_ts,
                         const NodeConfig &target);

  /// Two-Phase Commit (2PC) Server State Machine
  bool local_tx_prepare(const uint64_t &tx_timestamp,
                        const std::vector<std::pair<uint32_t, uint32_t>> &batch);
  void local_tx_commit(const uint64_t &tx_timestamp);
  void local_tx_abort(const uint64_t &tx_timestamp);

  // Network RPCs called by the DHTTransactionManager Coordinator
  bool send_tx_prepare(const int target_id, const uint64_t tx_timestamp,
                       const std::vector<std::pair<uint32_t, uint32_t>> &batch);
  bool send_tx_commit(const int target_id, const uint64_t tx_timestamp);
  bool send_tx_abort(const int target_id, const uint64_t tx_timestamp);

  // Connection acceptance and routing loop
  void handle_client(int client_socket);
  void listen_loop();

public:
  explicit StaticClusterDHTNode(std::vector<NodeConfig> map, NodeConfig self,
                                int hash_table_size, int num_locks, int rep_degree);
  ~StaticClusterDHTNode();

  // Lock-free telemetry counters
  mutable NodeStats stats;

  void start();
  void warmup_network(int connections_per_peer);
  void stop();
  void print_status();

  // Synchronized benchmark start
  void wait_for_barrier();

  // All-to-all broadcast barrier to prevent early shutdown connection drops
  std::mutex exit_mtx;
  std::set<int> exited_peers;
  void wait_for_exit_barrier();

  inline uint64_t increment_logical_clock() {
      return logical_clock.fetch_add(1, std::memory_order_relaxed);
  }
};