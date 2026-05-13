#pragma once

#include <thread>
#include <unordered_set>
#include <condition_variable>
#include <set>
#include <memory>
#include <atomic>
#include <vector>
#include <chrono>
#include <optional>
#include <string>

#include "node_properties.h"
#include "common/dht_common.h"
#include "hash_table/timestamped_striped_lock_concurrent_hash_table.h"
#include "network/connection_pool.h"
#include "common/spin_lock.h"
#include "consensus_interface.h"

// Forward declaration of the transaction manager
class DHTTransactionManager;

/// This class represents a single physical server in the distributed cluster.
class StaticClusterDHTNode : public IStateMachine, public INetworkTransport {
private:
  friend class DHTTransactionManager;

  // =========================================================================
  // INTERNAL DATA STRUCTURES
  // =========================================================================
  
  // Holds data payloads that have passed Phase 1 validation but await Phase 2 commit
  struct StagedTx {
    uint64_t tx_timestamp;
    int coordinator_id;
    std::chrono::steady_clock::time_point staged_at; 
    std::vector<std::pair<uint32_t, uint32_t>> batch;
  };

  // Zero-allocation stack structure to return replica target IDs
  struct ReplicaSet {
    uint8_t node_ids[8]; // Hard limit of 8 replicas
  };

  // Payload for asynchronous Phase 2 recovery routing
  struct RecoveryTask {
    int target_id;
    TwoPhaseCommitCommand cmd;
    uint64_t tx_timestamp;
  };

  // =========================================================================
  // Core State and System Components
  // =========================================================================
  
  DHTTransactionManager* tx_manager = nullptr;
  std::unique_ptr<IConsensusEngine> consensus_engine;
  
  std::vector<NodeConfig> cluster_map;
  NodeConfig self_config;
  int replication_degree;
  
  TimestampedStripedLockConcurrentHashTable<uint32_t, uint32_t> storage;

  // =========================================================================
  // Concurrency & Networking State
  // =========================================================================
  
  std::atomic<bool> running{false};
  int server_fd{-1};
  ConnectionPool connection_pool;

  // Threads
  std::thread listener_thread;
  std::thread dispatcher_thread;
  std::thread sweeper_thread;
  std::vector<std::thread> client_threads;

  // Socket Tracking
  std::mutex thread_mutex;
  std::unordered_set<int> active_sockets;

  // Distributed Barrier Mechanics
  std::mutex barrier_mtx;
  std::condition_variable barrier_cv;
  std::set<int> barrier_checkins;
  std::atomic<bool> benchmark_ready{false};
  
  AlignedSpinlock exit_mtx;
  std::set<int> exited_peers;

  // =========================================================================
  // 2PC Concurrency Control State
  // =========================================================================
  
  alignas(64) std::atomic<uint64_t> logical_clock{0};
  
  size_t num_logical_stripes;
  size_t logical_stripe_mask;
  
  std::unique_ptr<AlignedSpinlock[]> stripe_locks;
  std::vector<std::vector<uint32_t>> logically_locked_stripes;
  
  std::unique_ptr<AlignedSpinlock[]> staging_locks;
  std::vector<std::vector<StagedTx>> staging_stripes;

  AlignedSpinlock recovery_mtx;
  std::vector<RecoveryTask> recovery_queue;

  // =========================================================================
  // Inline Routing & Utility Functions
  // =========================================================================
  
  inline uint64_t hash_key(const std::string &key) const {
    return XXHash64::hash(key.data(), key.length(), 0);
  }
  
  inline uint64_t hash_key(const uint32_t &key) const {
    return XXHash64::hash(&key, sizeof(key), 0);
  }

  inline ReplicaSet get_replica_nodes(const uint32_t &key) const {
    ReplicaSet replicas;
    uint64_t hash_val = hash_key(key);
    size_t base_index = hash_val % cluster_map.size();

    for (int i = 0; i < replication_degree; ++i) {
      size_t target_index = (base_index + i) % cluster_map.size();
      replicas.node_ids[i] = cluster_map[target_index].id;
    }
    return replicas;
  }
  
  inline const NodeConfig &get_target_node(const uint32_t &key) const {
    uint64_t hash_val = hash_key(key);
    size_t target_index = hash_val % cluster_map.size();
    return cluster_map[target_index];
  }

  inline void enqueue_for_async_recovery(int target, TwoPhaseCommitCommand command, uint64_t ts) {
    std::lock_guard<Spinlock> lock(recovery_mtx.mutex);
    recovery_queue.push_back({target, command, ts});
  }

  // =========================================================================
  // 6. Network Subsystem (Mapped to dht_network.cpp)
  // =========================================================================
  
  void listen_loop();
  void handle_client(int client_socket);
  void recovery_dispatcher_loop();

  static bool recv_n_bytes(const int sock, void *buffer, const size_t n);
  
  RpcResult perform_rpc_single_request(const int sock, ProtocolType proto, uint8_t cmd,
                                       const uint8_t *request, const size_t request_size,
                                       uint8_t *response, const size_t response_size);
                                       
  bool send_single_request(const int target_id, const std::string &target_ip,
                           const int target_port, ProtocolType proto, uint8_t cmd,
                           const uint8_t *request, size_t request_size,
                           uint8_t *response, size_t response_size);
                           
  bool send_batch(const int target_id, const std::string &target_ip, const int target_port,
                  const std::vector<PutRequest> &batch_requests, TelemetryBatcher& batcher);

  // =========================================================================
  // Storage & 2PC Execution (Mapped to dht_execution.cpp)
  // =========================================================================
  
  void synchronize_clock(const uint64_t incoming_ts);
  void stale_lock_sweeper_loop();

  // Storage Engine operations
  PutResult put_local(const uint32_t key, const uint32_t value, const uint64_t timestamp, TelemetryBatcher& batcher);
  std::optional<uint32_t> get_local(const uint32_t key, TelemetryBatcher& batcher) const;
  
  PutResult put_remote(const uint32_t key, const uint32_t value, const uint64_t timestamp,
                       const NodeConfig &target, TelemetryBatcher& batcher);
  GetResponse get_remote(const uint32_t key, const uint64_t read_ts,
                         const NodeConfig &target, TelemetryBatcher& batcher);

  // 2PC Cohort Execution
  bool local_tx_prepare(const uint64_t tx_timestamp, const int coordinator_id, 
                        const std::vector<std::pair<uint32_t, uint32_t>> &batch, 
                        TelemetryBatcher& batcher);
  void local_tx_commit(const uint64_t tx_timestamp, TelemetryBatcher& batcher);
  void local_tx_abort(const uint64_t tx_timestamp, TelemetryBatcher& batcher);

  // 2PC Coordinator Execution
  bool send_tx_prepare(const int target_id, const uint64_t tx_timestamp,
                       const std::vector<std::pair<uint32_t, uint32_t>> &batch,
                       TelemetryBatcher& batcher);
  bool send_tx_commit(const int target_id, const uint64_t tx_timestamp, TelemetryBatcher& batcher);
  bool send_tx_abort(const int target_id, const uint64_t tx_timestamp, TelemetryBatcher& batcher);


  // =========================================================================
  // Consensus Engine Interfaces
  // =========================================================================
  
  void apply_committed_log(uint64_t commit_index, const uint8_t* data, size_t data_len) override;
  
  void send_message(int target_node_id, ProtocolType proto, uint8_t command_type,
                    const uint8_t* payload, size_t payload_size) override;

  std::vector<int> get_peer_ids() const override;
  int get_self_id() const override { return self_config.id; }

public:
  // =========================================================================
  // Public API (Mapped to dht_public_interface.cpp)
  // =========================================================================
  
  explicit StaticClusterDHTNode(std::vector<NodeConfig> map, NodeConfig self,
                                int hash_table_size, int num_locks, int rep_degree,
                                std::unique_ptr<IConsensusEngine> engine);
  ~StaticClusterDHTNode();

  // Connect the transaction manager post-construction
  void register_transaction_manager(DHTTransactionManager* manager) {
    tx_manager = manager;
  }

  // Lifecycle
  void start();
  void stop();
  void warmup_network(int connections_per_peer);
  
  // Synchronization
  void wait_for_barrier();
  void wait_for_exit_barrier();

  // Telemetry
  mutable NodeStats stats;
  void print_status();

  inline uint64_t increment_logical_clock() {
    return logical_clock.fetch_add(1, std::memory_order_relaxed);
  }
};