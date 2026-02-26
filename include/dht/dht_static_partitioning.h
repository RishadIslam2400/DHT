#pragma once

#include <thread>
#include <unordered_map>
#include <queue>
#include <condition_variable>
#include <set>
#include <memory>

#include "node_properties.h"
#include "dht_common.h"
#include "hash_table/striped_lock_concurrent_hash_table.h"
#include "network/connection_pool.h"

// Forward declaration of the template
// template <typename DHTType>
class DHTMessageBatcher;

// Make this for integer type for now
// TODO: think about creating a templated version in future
// have to move everything from src/dht_static_partitioning.cpp to here
class StaticClusterDHTNode {
private:
  friend class DHTMessageBatcher;                             // Friend class for batching
  std::vector<NodeConfig> cluster_map;                        // Cluster configuration
  NodeConfig self_config;                                     // Node information
  StripedLockConcurrentHashTable<uint32_t, uint32_t> storage; // Internal hash table

  int server_fd;                  // Server socket ID
  std::atomic<bool> running;      // Flag to turn server on/off
  std::thread listener_thread;    // Background thread listening to incoming request
  ConnectionPool connection_pool; // Connection pool to store active connections with other nodes in the cluster

  std::mutex thread_mutex;                 // Global lock for thread
  std::vector<std::thread> client_threads; // Active client threads
  std::vector<int> active_sockets;         // Active sockets handling clients

  std::mutex barrier_mtx;
  std::condition_variable barrier_cv;
  std::set<int> barrier_checkins;    // Tracks which nodes have pinged
  std::atomic<bool> benchmark_ready; // Flag to control benchmarking4

  inline uint64_t hash_key(const std::string &key) const;              // returns raw hsah value of the key
  inline uint64_t hash_key(const uint32_t &key) const;                 // int overload
  inline const NodeConfig &get_target_node(const uint32_t &key) const; // get the node id for the key

  static bool recv_n_bytes(const int sock, void *buffer, const size_t n);                                                                                                                                    // receive exatcly n bytes from the sokcket descriptor
  bool perform_rpc_single_request(const int sock, CommandType cmd, const uint8_t *request, size_t request_size, uint8_t *response, size_t response_size);                                                    // Send a request and recieve a response
  bool send_single_request(const int target_id, const std::string &target_ip, const int target_port, CommandType cmd, const uint8_t *request, size_t request_size, uint8_t *response, size_t response_size); // send a request using connection pool
  bool send_batch(const int target_id, const std::string &target_ip, const int target_port, const std::vector<PutRequest> &batch_requests);                                                                  // send a batch of requests

  bool put_local(const uint32_t &key, const uint32_t &value);   // Put operation on the local hash table
  std::optional<uint32_t> get_local(const uint32_t &key) const; // get operation on the local hash table
  GetResponse get_remote(const uint32_t &key, const NodeConfig &target); // Remote get operation

  void handle_client(int client_socket); // handle incoming requests from the client on the detached thread and send it to handle_request()
  void listen_loop();                    // listen for incoming clients and dispatch threads to handle_client()

public:
  explicit StaticClusterDHTNode(std::vector<NodeConfig> map, NodeConfig self, int hash_table_size, int num_locks);
  ~StaticClusterDHTNode();

  mutable NodeStats stats;                                    // stats for bechmarking

  void start();        // start the server
  void warmup_network(int connections_per_peer); // Warm up the connection_pool
  void stop();         // stop the server
  void print_status(); // print the stats

  PutResult put(const uint32_t &key, const uint32_t &value); // Put a KV pair in DHT, returns PutResult (Inserted = 0, Updated = 1, Failed = 0)
  GetResponse get(const uint32_t &key);                      // Get a value from DHT, returns GetResult (GetResult.status: Found = 1 (GetResult.value), Not Found = 2, Error = 0)
  void wait_for_barrier();                                   // Distributed barrier to start benchmarking
};

// returns raw hash value
inline uint64_t StaticClusterDHTNode::hash_key(const std::string& key) const {
  return XXHash64::hash(key.data(), key.length(), 0);
}

// Overload for int types
inline uint64_t StaticClusterDHTNode::hash_key(const uint32_t& key) const {
  return XXHash64::hash(&key, sizeof(key), 0);
}

// Get the correct node for the key
inline const NodeConfig& StaticClusterDHTNode::get_target_node(const uint32_t& key) const {
  uint64_t hash_val = hash_key(key);

  size_t target_index = hash_val % cluster_map.size();
  return cluster_map[target_index];
}