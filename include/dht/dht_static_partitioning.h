#pragma once

#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <condition_variable>
#include <set>
#include <memory>
#include <atomic>

#include "node_properties.h"
#include "common/dht_common.h"
#include "hash_table/timestamped_striped_lock_concurrent_hash_table.h"
#include "network/connection_pool.h"
#include "common/spin_lock.h"

// Forward declaration of the template
// template <typename DHTType>
class DHTTransactionManager;

class StaticClusterDHTNode {
private:
  friend class DHTTransactionManager;

  std::vector<NodeConfig> cluster_map;
  NodeConfig self_config;
  TimestampedStripedLockConcurrentHashTable<uint32_t, uint32_t> storage;

  int server_fd;
  std::atomic<bool> running;
  std::thread listener_thread;
  ConnectionPool connection_pool; // Connection pool to store active connections with other nodes in the cluster

  // Mechanism for listener thread to keep track of client threads in handle_client()
  std::mutex thread_mutex;                 // Main thread and listener thread access the thread mutex
  std::vector<std::thread> client_threads; // Active client threads
  std::vector<int> active_sockets;         // Active sockets handling clients

  // Mechanism for distributed barrier
  std::mutex barrier_mtx;
  std::condition_variable barrier_cv; // Wakes up coordinator node
  std::set<int> barrier_checkins;     // Tracks which nodes have pinged
  std::atomic<bool> benchmark_ready;

  // 2PC concurrency control mechanism
  alignas(64) std::atomic<uint64_t> logical_clock{0}; // The Lamport Logical Clock
  alignas(64) Spinlock tx_spinlock;
  std::vector<uint32_t> logically_locked_keys;
  struct StagedTx {
    uint64_t tx_timestamp;
    std::vector<std::pair<uint32_t, uint32_t>> batch;
  };
  std::vector<StagedTx> staging_area;

  // Internal routing mechanism
  inline uint64_t hash_key(const std::string &key) const {
    return XXHash64::hash(key.data(), key.length(), 0);
  }
  inline uint64_t hash_key(const uint32_t &key) const {
    return XXHash64::hash(&key, sizeof(key), 0);
  }
  inline const NodeConfig &get_target_node(const uint32_t &key) const {
    uint64_t hash_val = hash_key(key);

    size_t target_index = hash_val % cluster_map.size();
    return cluster_map[target_index];
  }

  // Network I/O methods
  static bool recv_n_bytes(const int sock, void *buffer, const size_t n);
  // Sends the request, recieves and processes the response according to the protocol
  bool perform_rpc_single_request(const int sock, CommandType cmd,
                                  const uint8_t *request, size_t request_size,
                                  uint8_t *response, size_t response_size);
  // Perform the rpc using connection pool with retries
  bool send_single_request(const int target_id, const std::string &target_ip,
                           const int target_port, CommandType cmd,
                           const uint8_t *request, size_t request_size,
                           uint8_t *response, size_t response_size);
  // Sends batch request and recieves batched response
  bool send_batch(const int target_id, const std::string &target_ip,
                  const int target_port, const std::vector<PutRequest> &batch_requests);
  
  // Server-Side Execution Logic
  void synchronize_clock(const uint64_t incoming_ts); // Updates the clock based on incoming network timestamps

  // Single-key operations on local storage
  PutResult put_local(const uint32_t &key, const uint32_t &value,
                      const uint64_t &timestamp);
  std::optional<uint32_t> get_local(const uint32_t &key) const;
  
  PutResult put_remote(const uint32_t &key, const uint32_t &value,
                       const uint64_t &timestamp, const NodeConfig &target);
  GetResponse get_remote(const uint32_t &key, const uint64_t &read_ts,
                         const NodeConfig &target);

  // Two-Phase Commit (2PC) Execution Logic
  bool local_tx_prepare(const uint64_t &tx_timestamp,
                        const std::vector<std::pair<uint32_t, uint32_t>> &batch);
  void local_tx_commit(const uint64_t &tx_timestamp);
  void local_tx_abort(const uint64_t &tx_timestamp);

  // Two-Phase Commit (2PC) Network RPCs
  bool send_tx_prepare(const int target_id, const uint64_t tx_timestamp,
                       const std::vector<std::pair<uint32_t, uint32_t>> &batch);
  bool send_tx_commit(const int target_id, const uint64_t tx_timestamp);
  bool send_tx_abort(const int target_id, const uint64_t tx_timestamp);

  // Server listener
  void handle_client(int client_socket);
  void listen_loop();

public:
  explicit StaticClusterDHTNode(std::vector<NodeConfig> map, NodeConfig self,
                                int hash_table_size, int num_locks);
  ~StaticClusterDHTNode();
  mutable NodeStats stats;

  void start();
  void warmup_network(int connections_per_peer); // Warm up the connection_pool
  void stop();
  void print_status(); // print the stats

  void wait_for_barrier();

  inline uint64_t increment_logical_clock() {
      return logical_clock.fetch_add(1, std::memory_order_relaxed);
  }
};