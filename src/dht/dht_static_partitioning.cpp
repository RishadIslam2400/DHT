#include "dht/dht_static_partitioning.h"
#include "common/xxHash64.h"
#include "common/utils.h"

#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <endian.h>

StaticClusterDHTNode::StaticClusterDHTNode(std::vector<NodeConfig> map, NodeConfig self,
                                           int hash_table_size, int num_locks, int rep_deg)
  : cluster_map{std::move(map)}, 
    self_config{std::move(self)},
    storage{hash_table_size, num_locks},
    replication_degree{rep_deg},
    server_fd{-1},
    running{false},
    connection_pool(cluster_map.size()),
    benchmark_ready{false}
{
  // Pre-allocate to prevent heap fragmentation during runtime
  logically_locked_keys.reserve(1024);
  staging_area.reserve(128);

  std::cout << "Booted Node " << self_config.id
            << " (" << self_config.ip << ":" << self_config.port << ")" << std::endl;
}

StaticClusterDHTNode::~StaticClusterDHTNode() {
  stop();
}

void StaticClusterDHTNode::start() {
  running = true;
  // Detach the listener loop to its own thread to asynchronously accept incoming TCP connections
  listener_thread = std::thread(&StaticClusterDHTNode::listen_loop, this);
}

// Establishes persistent TCP connections to avoid 3-way handshake latency during the benchmark
void StaticClusterDHTNode::warmup_network(int connections_per_peer) {
  std::cout << "[Node " << self_config.id << "] Pre-warming " 
            << connections_per_peer << " connections to each peer..." << std::endl;
  
  for (NodeConfig& peer : cluster_map) {
    if (peer.id == self_config.id)
      continue;

    connection_pool.pre_warm(peer.id, peer.ip, peer.port, connections_per_peer);
  }

  std::cout << "[Node " << self_config.id << "] Network pre-warm complete." << std::endl;
}

// Graceful shutdown sequence. Safely terminates active sockets to prevent data corruption.
void StaticClusterDHTNode::stop() {
  if (!running)
    return;
    
  std::cout << "Stopping Node..." << std::endl;
  running = false;
  
  // Wake up listener thread (blocked in accept)
  if (server_fd > 0) {
    shutdown(server_fd, SHUT_RDWR);
    close(server_fd);
    server_fd = -1;
  }

  // Wake up all client threads (blocked in recv)
  {
    std::lock_guard<std::mutex> lock(thread_mutex);
    for (int sock : active_sockets) {
      shutdown(sock, SHUT_RDWR); 
      close(sock);
    }
    active_sockets.clear();
  }

  // Join listener thread
  if (listener_thread.joinable()) {
    listener_thread.join();
  }

  // Join all client threads
  std::vector<std::thread> threads_to_join;
  {
    std::lock_guard<std::mutex> lock(thread_mutex);
    threads_to_join.swap(client_threads);
  }

  for (std::thread &t : threads_to_join) {
    if (t.joinable()) {
      t.join();
    }
  }
  
  std::cout << "Node stopped gracefully." << std::endl;
}

void StaticClusterDHTNode::print_status() {    
  // Aggregate base metrics
  uint32_t total_local_puts = stats.local_puts_inserted + stats.local_puts_updated
                              + stats.local_puts_dropped;
  uint32_t total_local_gets = stats.local_gets_found + stats.local_gets_not_found;
  uint32_t total_tx_coordinated = stats.local_tx_committed + stats.local_tx_aborted;

  std::cout << "\nNode " << self_config.id << " Statistics\n";
  std::cout << "[Local Hash Table Metrics]\n";
  std::cout << "  Total Local PUTs:           " << total_local_puts << "\n";
  std::cout << "    - Inserted:               " << stats.local_puts_inserted << "\n";
  std::cout << "    - Updated:                " << stats.local_puts_updated << "\n";
  std::cout << "    - Dropped:                " << stats.local_puts_dropped << "\n\n";
  
  std::cout << "  Total Local GETs:           " << total_local_gets << "\n";
  std::cout << "    - Found:                  " << stats.local_gets_found << "\n";
  std::cout << "    - Not Found:              " << stats.local_gets_not_found << "\n\n";
  
  std::cout << "[Remote Operations Metrics]\n";
  std::cout << "  Remote PUT Requests:\n";
  std::cout << "    - Success:                " << stats.remote_puts_success << "\n";
  std::cout << "    - Failed:                 " << stats.remote_puts_failed << "\n\n";

  std::cout << "  Remote GET Requests:\n";
  std::cout << "    - Success:                " << stats.remote_gets_success << "\n";
  std::cout << "    - Failed:                 " << stats.remote_gets_failed << "\n\n";

  std::cout << "[Distributed Transactions (2PC)]\n";
  std::cout << "  Coordinator:\n";
  std::cout << "    - TX Initiated:           " << total_tx_coordinated << "\n";
  std::cout << "    - TX Committed:           " << stats.local_tx_committed << "\n";
  std::cout << "    - TX Aborted:             " << stats.local_tx_aborted << "\n\n";
  
  std::cout << "  Cohort:\n";
  std::cout << "    - Rejected (Locked):      " << stats.tx_prepare_rejected_locked << "\n";
  std::cout << "    - Rejected (Obsolete):    " << stats.tx_prepare_rejected_obsolete << "\n";
}

// Guarantees all nodes start the benchmark simultaneously. 
// Node 0 acts as the Central Coordinator.
void StaticClusterDHTNode::wait_for_barrier() {
  if (self_config.id == 0) {
    std::cout << "[Coordinator] Barrier initiated. Waiting for pings from peers...\n";
    size_t total_peers = cluster_map.size() - 1;

    // Block and wait for N-1 workers to check in
    {
      std::unique_lock<std::mutex> lock(barrier_mtx);
      barrier_cv.wait(lock, [&] {
        return barrier_checkins.size() >= total_peers || !running;
      });
    }

    if (!running)
      return;

    // Broadcast CMD_GO signal to all workers via pre-warmed sockets
    std::cout << "[Coordinator] All peers checked in. Fetching sockets...\n";

    std::vector<std::pair<int, int>> barrier_sockets;
    barrier_sockets.reserve(total_peers);

    for (const NodeConfig& node : cluster_map) {
      if (node.id == 0)
        continue;

      int sock = connection_pool.get_connection(node.id, node.ip, node.port);
      if (sock != -1) {
        barrier_sockets.push_back({node.id, sock});
      } else {
        std::cerr << "[Coordinator] Failed to get pre-warmed socket for Node "
                  << node.id << "\n";
      }
    }

    std::cout << "[Coordinator] Broadcasting GO signal..." << std::endl;
    uint8_t go_signal = static_cast<uint8_t>(CommandType::CMD_GO);
    for (const auto& peer : barrier_sockets) {
      if (send(peer.second, &go_signal, 1, MSG_NOSIGNAL) != 1) {
        log_error("Failed to send go signal", errno);
      }
    }

    // Wait to receive the 1-byte ACK to clear the TCP buffer before pooling the socket
    for (const auto& peer : barrier_sockets) {
      uint8_t ack_buf;

      if (!recv_n_bytes(peer.second, &ack_buf, 1)) {
        log_error("Failed to receive GO ack", errno);
        connection_pool.return_connection(peer.first, peer.second, true);
        continue;
      }

      connection_pool.return_connection(peer.first, peer.second, false);
    }

    benchmark_ready.store(true, std::memory_order_release);
  } else {
    // Worker Node Logic: Ping Node 0 and wait for GO
    std::cout << "[Worker] Reached barrier. Notifying Coordinator...\n";

    const NodeConfig& coord = cluster_map[0];
    uint8_t response = 0;

    uint32_t net_id = htonl(self_config.id);
    uint8_t buf[4];
    std::memcpy(buf, &net_id, 4);

    bool success = false;
    while (running) {
      success = send_single_request(coord.id, coord.ip, coord.port,
                                    CommandType::CMD_BARRIER, buf, 4, &response, 1);

      if (success && response == 1) {
        std::cout << "[Worker] Coordinator got checkin notification. Waiting for GO..." << std::endl;
        break;
      }

      std::cout << "[Worker] Coordinator busy. Retrying...\n";
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    // Spin-wait until CMD_GO is received and processed by handle_client 
    while (running && !benchmark_ready.load(std::memory_order_acquire)) {
      #if defined(__x86_64__)
        __builtin_ia32_pause();
      #else
        std::this_thread::yield();
      #endif
    }
  }
}

// All-to-all broadcast barrier to ensure no node shuts down its listener socket until 
// all peers have finished their workload
void StaticClusterDHTNode::wait_for_exit_barrier() {
  size_t expected_peers = cluster_map.size() - 1;
  if (expected_peers == 0)
    return;

  // Pack node_id into a 4-byte buffer
  uint32_t net_id = htonl(self_config.id);
  uint8_t req_buf[4];
  std::memcpy(req_buf, &net_id, 4);

  // Track the peers we still need to successfully notify
  std::set<int> unacknowledged_peers;
  for (const NodeConfig& peer : cluster_map) {
    if (peer.id != self_config.id) {
      unacknowledged_peers.insert(peer.id);
    }
  }

  // Active Gossiping Loop: Continuously notify missing peers while waiting
  while (true) {
    // Attempt to deliver the exit signal to anyone who hasn't ACKed us yet
    for (auto it = unacknowledged_peers.begin(); it != unacknowledged_peers.end(); ) {
      int peer_id = *it;
      
      // Locate the peer's configuration
      const NodeConfig* target = nullptr;
      for (const auto& p : cluster_map) {
        if (p.id == peer_id) {
          target = &p;
          break;
        }
      }

      uint8_t ack = 0;
      bool success = send_single_request(target->id, target->ip, target->port, 
                                         CommandType::CMD_EXIT_BARRIER, 
                                         req_buf, 4, 
                                         &ack, 1);
      
      if (success) {
        // TCP delivery confirmed. Remove from the pending list.
        it = unacknowledged_peers.erase(it); 
      } else {
        // Delivery failed (peer backlog is full). Leave in the set and retry on the next tick.
        ++it; 
      }
    }

    // Check the bidirectional exit condition
    {
      std::lock_guard<std::mutex> lock(exit_mtx);
      // A node can only safely terminate its listener socket if:
      // A) Every peer has informed us they are done running benchmarks.
      // B) We have successfully informed every peer that we are done.
      if (exited_peers.size() >= expected_peers && unacknowledged_peers.empty()) {
        break;
      }
    }

    // Throttle the gossip to prevent network flooding while waiting
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Hardware propagation delay to ensure final TCP ACKs clear the NIC
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
} 

// -------------------------------Utility Functions------------------------------------

bool StaticClusterDHTNode::recv_n_bytes(const int sock, void* buffer, const size_t n) {
  size_t total_read = 0;
  char *buf_ptr = static_cast<char *>(buffer);

  while (total_read < n) {
    // MSG_WAITALL forces the kernel to block until exactly 'n' bytes are read.
    ssize_t received = recv(sock, buf_ptr + total_read, n - total_read, MSG_WAITALL);

    if (received < 0) {
      if (errno == EINTR)
        continue; // Retry if interrupted by system signal

      return false;
    }

    if (received == 0) {
      // EOF detected
      errno = ECONNRESET; // "Connection reset by peer"
      return false;
    }

    total_read += received;
  }
  return true;
}

// Low-level Scatter-Gather I/O execution
bool StaticClusterDHTNode::perform_rpc_single_request(const int sock, CommandType cmd,
                                                      const uint8_t* request,
                                                      size_t request_size,
                                                      uint8_t* response,
                                                      size_t response_size)
{
  // Writev completely eliminates memcpy overhead by passing two pointers directly to the kernel
  struct iovec iov[2];

  uint8_t cmd_byte = static_cast<uint8_t>(cmd);
  iov[0].iov_base = &cmd_byte;
  iov[0].iov_len = 1;

  int iov_count = 1;
  if (request_size > 0) [[likely]] {
    iov[1].iov_base = const_cast<uint8_t *>(request);
    iov[1].iov_len = request_size;
    iov_count = 2;
  }
  
  if (writev(sock, iov, iov_count) != static_cast<ssize_t>(1 + request_size)) {
    log_error("Error sending single request via writev", errno);
    return false;
  }

  // Process the fixed-length response header
  if (!recv_n_bytes(sock, response, response_size)) {
    log_error("Error receiving response from single request", errno);
    return false;
  }

  // Handle variable length response for CMD_GET
  // If GET was successful, trigger a second read to fetch the 4-byte value
  if (cmd == CommandType::CMD_GET && response[0] == static_cast<uint8_t>(GetStatus::Found)) {
    if (!recv_n_bytes(sock, response + response_size, 4)) {
      log_error("Error recieving the value", errno);
      return false;
    }
  }

  return true;
}

// Wraps the RPC in a bounded retry loop, managing socket checkout and return 
// against the Thread-Safe Connection Pool
bool StaticClusterDHTNode::send_single_request(const int target_id,
                                               const std::string& target_ip,
                                               const int target_port,
                                               CommandType cmd,
                                               const uint8_t* request,
                                               size_t request_size,
                                               uint8_t* response,
                                               size_t response_size)
{
  // Get a active connection from the pool or create a new connection
  int sock = connection_pool.get_connection(target_id, target_ip, target_port);
  if (sock < 0) {
    // log_error("Invalid Socket", errno);
    return false;
  }

  bool success = perform_rpc_single_request(sock, cmd,
                                            request, request_size,
                                            response, response_size);

  if (success) {
    connection_pool.return_connection(target_id, sock, false); // Return to pool
    return true;
  }

  // The socket might have gone stale.
  // Destroy the broken socket and try exactly one more time with a freshly dialed TCP connection.
  connection_pool.return_connection(target_id, sock, true);

  sock = connection_pool.get_connection(target_id, target_ip, target_port);
  if (sock < 0) {
    // log_error("Invalid Socket", errno);
    return false;
  }

  success = perform_rpc_single_request(sock, cmd,
                                       request, request_size,
                                       response, response_size);
  if (success) {
    connection_pool.return_connection(target_id, sock, false); // New socket is good, pool it
    return true;
  }

  connection_pool.return_connection(target_id, sock, true); // Failed twice, give up
  return false;
}

bool StaticClusterDHTNode::send_batch(const int target_id,
                                      const std::string& target_ip,
                                      const int target_port,
                                      const std::vector<PutRequest>& batch_requests)
{
  #ifndef NDEBUG
    if (batch_requests.empty() || batch_requests.size() > BATCH_SIZE) {
      return false;
    }
  #endif

  constexpr size_t MAX_SEND_BYTES = 3 + (BATCH_SIZE * 16);
  uint8_t send_buffer[MAX_SEND_BYTES];
  uint8_t recv_buffer[BATCH_SIZE];

  // Serialize the request count into the header
  uint16_t batch_size = static_cast<uint16_t>(batch_requests.size());
  uint16_t batch_size_net = htons(batch_size);

  send_buffer[0] = static_cast<uint8_t>(CommandType::CMD_BATCH_PUT);
  std::memcpy(&send_buffer[1], &batch_size_net, 2);
  uint8_t *buf_ptr = &send_buffer[3];

  // Copy all requests to buffer
  for (const PutRequest& request : batch_requests) {
    uint32_t net_key = htonl(request.key);
    uint32_t net_val = htonl(request.value);
    uint64_t net_ts = htobe64(request.timestamp);

    std::memcpy(buf_ptr, &net_key, 4);
    std::memcpy(buf_ptr + 4, &net_val, 4);
    std::memcpy(buf_ptr + 8, &net_ts, 8);
    buf_ptr += 16;
  }

  // Send the entire batch and recieve response
  size_t total_send_bytes = 3 + (batch_size * 16);
  size_t expected_response_bytes = batch_size;

  //  Retry once
  for (int attempt = 1; attempt <= 2; ++attempt) {
    int sock = connection_pool.get_connection(target_id, target_ip, target_port);
    if (sock < 0) {
      log_error("Invalid socket", errno);
      return false;
    }

    // Attempt send and receive
    bool success = false;
    if (send(sock, send_buffer, total_send_bytes, MSG_NOSIGNAL) == static_cast<ssize_t>(total_send_bytes)) {
      if (recv_n_bytes(sock, recv_buffer, expected_response_bytes)) {
        success = true;
      }
    }

    if (success) {
      // return healthy connection to connection pool
      connection_pool.return_connection(target_id, sock, false);

      stats.remote_puts_success.fetch_add(1, std::memory_order_relaxed);
      return true;
    }

    // Destroy the stale/broken socket and loop again
    connection_pool.return_connection(target_id, sock, true);
  }

  // Failed all attempts
  stats.remote_puts_failed.fetch_add(1, std::memory_order_relaxed);
  log_error("Batch request failed after retries", errno);
  return false;
}

// Evaluates incoming network timestamps and fast-forwards the local Lamport clock
void StaticClusterDHTNode::synchronize_clock(const uint64_t incoming_ts) {
  // Read the current clock value
  uint64_t current_clock = logical_clock.load(std::memory_order_relaxed);

  // Loop until current clock is >= incoming_ts, or we successfully update it
  while (current_clock < incoming_ts) {
    // If logical_clock still equals current_clock, it updates to incoming_ts.
    // If it fails (another thread updated it), current_clock is automatically refreshed.
    if (logical_clock.compare_exchange_weak(current_clock, incoming_ts, std::memory_order_relaxed)) {
      break;
    }
  }
}

PutResult StaticClusterDHTNode::put_local(const uint32_t& key, const uint32_t& value,
                                          const uint64_t &timestamp) {
  constexpr int MAX_RETRIES = 1000;
  uint16_t attempt = 0;

  // Bounded Spin-Wait: Pauses physical insertion if the key is currently 
  // logically locked by an active 2PC Phase 1 transaction.
  while (true) {
    bool is_locked = false;
    {
      std::lock_guard<Spinlock> lock(tx_spinlock);
      if (std::ranges::find(logically_locked_keys, key) != logically_locked_keys.end()) {
        is_locked = true;
      }
    }

    if (!is_locked) {
      break;
    }

    attempt++;
    if (attempt >= MAX_RETRIES) {
      return PutResult::Failed; // Prevent permanent thread starvation
    }

    if (attempt < 100) {
      #if defined(__x86_64__)
        __builtin_ia32_pause(); // Hint to CPU to optimize pipeline during spin
      #endif
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(5)); // Yield to OS scheduler
    }
  }

  // Physical insertion utilizing the strict Last-Write-Wins (LWW) conflict resolver
  PutResult result = storage.put(key, value, timestamp);

  // Update stats
  if (result == PutResult::Inserted) {
    stats.local_puts_inserted.fetch_add(1, std::memory_order_relaxed);
  } else if (result == PutResult::Updated) {
    stats.local_puts_updated.fetch_add(1, std::memory_order_relaxed);
  } else if (result == PutResult::Dropped) {
    stats.local_puts_dropped.fetch_add(1, std::memory_order_relaxed);
  }

  return result;
}

std::optional<uint32_t> StaticClusterDHTNode::get_local(const uint32_t& key) const {
  // Reads bypass the tx_mutex completely (Read-Committed isolation)
  std::optional<uint32_t> val = storage.get(key);

  if (val.has_value()) {
    stats.local_gets_found.fetch_add(1, std::memory_order_relaxed);
  } else {
    stats.local_gets_not_found.fetch_add(1, std::memory_order_relaxed);
  }

  return val;
}

PutResult StaticClusterDHTNode::put_remote(const uint32_t &key, const uint32_t &value,
                                           const uint64_t &timestamp, const NodeConfig &target)
{
  // Allocate buffers for request
  uint8_t request_buf[16];

  uint32_t net_k = htonl(key);
  uint32_t net_v = htonl(value);
  uint64_t net_ts = htobe64(timestamp);

  std::memcpy(request_buf, &net_k, 4);
  std::memcpy(request_buf + 4, &net_v, 4);
  std::memcpy(request_buf + 8, &net_ts, 8);

  uint8_t response_byte = 0;

  bool success = send_single_request(target.id, target.ip, target.port,
                                     CommandType::CMD_PUT,
                                     request_buf, 16,
                                     &response_byte, 1);

  if (success) {
    stats.remote_puts_success.fetch_add(1, std::memory_order_relaxed);
    return static_cast<PutResult>(response_byte);
  }

  // If the network completely failed after all retries, return Failed
  stats.remote_puts_failed.fetch_add(1, std::memory_order_relaxed);
  return PutResult::Failed;
}

GetResponse StaticClusterDHTNode::get_remote(const uint32_t &key, const uint64_t& read_ts,
                                             const NodeConfig &target)
{
  // 12-byte request: Key + Timestamp
  uint8_t request_buf[12];
  uint32_t net_key = htonl(key);
  uint64_t net_ts = htobe64(read_ts);

  std::memcpy(request_buf, &net_key, 4);
  std::memcpy(request_buf + 4, &net_ts, 8);

  // Variable-Length Response Buffer (1 byte status + 4 byte value)
  uint8_t response[5];

  bool success = send_single_request(target.id, target.ip, target.port, 
                                     CommandType::CMD_GET, 
                                     request_buf, 12, 
                                     response, 1); 

  if (!success) {
    stats.remote_gets_failed.fetch_add(1, std::memory_order_relaxed);
    return GetResponse::error();
  }

  // Process the response
  GetStatus status = static_cast<GetStatus>(response[0]);
  switch (status) {
    case GetStatus::Found: {
      stats.remote_gets_success.fetch_add(1, std::memory_order_relaxed);

      uint32_t net_val;
      std::memcpy(&net_val, response + 1, 4);
      return GetResponse::success(ntohl(net_val));
    }

    case GetStatus::NotFound:
      stats.remote_gets_success.fetch_add(1, std::memory_order_relaxed);
      return GetResponse::not_found();

    case GetStatus::NetworkError:
    default:
      stats.remote_gets_failed.fetch_add(1, std::memory_order_relaxed);
      return GetResponse::error();
  }
}

/// Two-Phase Commit (2PC) Server State Machine
// PHASE 1: Optimistic Concurrency Control (OCC) Validation
bool StaticClusterDHTNode::local_tx_prepare(const uint64_t &tx_timestamp,
                                            const std::vector<std::pair<uint32_t, uint32_t>> &batch)
{
  // Acquire global spinlock to check for overlapping active transactions
  {
    std::lock_guard<Spinlock> lock(tx_spinlock);

    // Lock collision check
    for (const auto& kv : batch) {
      if (std::ranges::find(logically_locked_keys, kv.first) != logically_locked_keys.end()) {
        stats.tx_prepare_rejected_locked.fetch_add(1, std::memory_order_relaxed);
        return false; // Key is locked
      }
    }

    // Optimistically claim the locks so other threads/network requests cannot touch these keys
    for (const auto& kv : batch) {
      logically_locked_keys.push_back(kv.first);
    }
  } // release spinlock

  // Validate LWW Timestamps against the physical storage layer
  bool is_obsolete = false;
  for (const auto& kv : batch) {
    uint64_t current_ts = storage.get_timestamp(kv.first);
    if (tx_timestamp <= current_ts) {
      is_obsolete = true;
      break;
    }
  }

  // Rollback logical locks if LWW validation failed
  if (is_obsolete) {
    std::lock_guard<Spinlock> lock(tx_spinlock);
    for (const auto& kv : batch) {
      auto it = std::ranges::find(logically_locked_keys, kv.first);
      if (it != logically_locked_keys.end()) {
        if (it != logically_locked_keys.end() - 1) {
          *it = logically_locked_keys.back();
        }
        logically_locked_keys.pop_back();
      }
    }

    stats.tx_prepare_rejected_obsolete.fetch_add(1, std::memory_order_relaxed);
    return false;
  }

  // Validation passed. Move payload to Staging Area awaiting Phase 2 Commit.
  {
    std::lock_guard<Spinlock> lock(tx_spinlock);
    staging_area.push_back({tx_timestamp, batch});
  }

  return true;
}

// PHASE 2 (Success): Extract from staging area and apply to physical storage
void StaticClusterDHTNode::local_tx_commit(const uint64_t &tx_timestamp) {
  std::vector<std::pair<uint32_t, uint32_t>> batch_to_commit;
  
  // Acquire the spinlock to steal the transaction from the staging area
  {
    std::lock_guard<Spinlock> lock(tx_spinlock);

    auto it = std::ranges::find_if(staging_area, [&tx_timestamp](const StagedTx& tx) {
      return tx.tx_timestamp == tx_timestamp;
    });

    if (it != staging_area.end()) [[likely]] {
      batch_to_commit = std::move(it->batch); 

      if (it != staging_area.end() - 1) {
        *it = std::move(staging_area.back());
      }
      staging_area.pop_back();
    }
  } // Release spinlock

  if (batch_to_commit.empty()) [[unlikely]] {
    return; // Safety guard against duplicate commits
  }

  // Insert into local storage
  // This operation is guaranteed safe from data races because the keys 
  // are still held inside logically_locked_keys
  if (batch_to_commit.size() == 1) {
    storage.put(batch_to_commit[0].first, batch_to_commit[0].second, tx_timestamp);
  } else {
    storage.multi_put(batch_to_commit, tx_timestamp);
  }

  // Release the logical locks to allow new transactions
  {
    std::lock_guard<Spinlock> lock(tx_spinlock);
    
    for (const auto& kv : batch_to_commit) {
      auto lock_it = std::ranges::find(logically_locked_keys, kv.first);
      if (lock_it != logically_locked_keys.end()) {
        *lock_it = logically_locked_keys.back();
        logically_locked_keys.pop_back();
      }
    }
  }

  stats.local_tx_committed.fetch_add(1, std::memory_order_relaxed);
}

// PHASE 2 (Failure): Coordinator ordered rollback
void StaticClusterDHTNode::local_tx_abort(const uint64_t &tx_timestamp) {
  std::vector<std::pair<uint32_t, uint32_t>> discarded_batch;

  {
    std::lock_guard<Spinlock> lock(tx_spinlock);

    auto it = std::ranges::find_if(staging_area, [&tx_timestamp](const StagedTx& tx) {
      return tx.tx_timestamp == tx_timestamp;
    });

    if (it != staging_area.end()) {
      // Steal the transaction batch
      discarded_batch = std::move(it->batch);

      // Roll back the tx from the staging area
      if (it != staging_area.end() - 1) {
        *it = std::move(staging_area.back());
      }
      staging_area.pop_back();

      // Release logical locks on the keys
      for (const auto& kv : discarded_batch) {
        auto lock_it = std::ranges::find(logically_locked_keys, kv.first);
        if (lock_it != logically_locked_keys.end()) {
          if (lock_it != logically_locked_keys.end() - 1) {
            *lock_it = logically_locked_keys.back();
          }
          logically_locked_keys.pop_back();
        }
      }
    }
  } // release spinlock

  stats.local_tx_aborted.fetch_add(1, std::memory_order_relaxed);
  // discarded_batch vector goes out of scope
}

/// 2PC Coordinator Network Execution
// Sends PREPARE message and awaits Cohort's vote
bool StaticClusterDHTNode::send_tx_prepare(const int target_id,
                                           const uint64_t tx_timestamp,
                                           const std::vector<std::pair<uint32_t, uint32_t>> &batch)
{
  // Locate target node details
  if (target_id < 0 || target_id >= static_cast<int>(cluster_map.size())) [[unlikely]] {
    return false;
  }
  const NodeConfig &target = cluster_map[target_id];

  uint16_t batch_size = batch.size();
  #ifndef NDEBUG
    if (batch_size > BATCH_SIZE) return false;
  #endif

  // Calculate strict buffer sizes to avoid memory overallocation
  constexpr size_t MAX_BUF_SIZE = 10 + (BATCH_SIZE * 8);
  size_t actual_request_size = 10 + (batch_size * 8);
  uint8_t request_buf[MAX_BUF_SIZE];

  // Serialize Header: [Timestamp:8][BatchSize:2]
  uint64_t net_ts = htobe64(tx_timestamp);
  uint16_t net_batch_size = htons(batch_size);
  std::memcpy(request_buf, &net_ts, 8);
  std::memcpy(request_buf + 8, &net_batch_size, 2);

  // Serialize Payload: [Key:4][Value:4]
  size_t offset = 10;
  for (const auto& kv : batch) {
    uint32_t net_k = htonl(kv.first);
    uint32_t net_v = htonl(kv.second);
    std::memcpy(request_buf + offset, &net_k, 4);
    std::memcpy(request_buf + offset + 4, &net_v, 4);
    offset += 8;
  }

  // Transmit and wait for Cohort's 9-byte response: [Vote:1][CurrentClock:8]
  uint8_t response_buf[9];
  bool success = send_single_request(target.id, target.ip, target.port, 
                                     CommandType::CMD_TX_PREPARE, 
                                     request_buf, actual_request_size, 
                                     response_buf, 9);

  if (success) {
    // Extract the remote server's clock and instantly catch up
    uint64_t remote_clock;
    std::memcpy(&remote_clock, response_buf + 1, 8);
    synchronize_clock(be64toh(remote_clock));
    
    return response_buf[0] == 1; // Return true only if the server voted YES
  }

  return false;
}

// Send Phase 2 COMMIT message
bool StaticClusterDHTNode::send_tx_commit(const int target_id, const uint64_t tx_timestamp) {
  if (target_id < 0 || target_id >= static_cast<int>(cluster_map.size())) [[unlikely]] {
    return false;
  }
  const NodeConfig &target = cluster_map[target_id];

  uint64_t net_ts = htobe64(tx_timestamp);
  uint8_t response = 0;

  // Direct memory reinterpretation completely bypasses memcpy
  bool success = send_single_request(target.id, target.ip, target.port,
                                     CommandType::CMD_TX_COMMIT,
                                     reinterpret_cast<uint8_t *>(&net_ts), 8,
                                     &response, 1);
  
  #ifndef NDEBUG
    if (!success) {
      std::cerr << "[Coordinator] WARNING: Failed to deliver TX_COMMIT to Node " << target_id << "\n";
    }
  #endif

  return success;
}

// Send Phase 2 ABORT message
bool StaticClusterDHTNode::send_tx_abort(const int target_id, const uint64_t tx_timestamp) {
  if (target_id < 0 || target_id >= static_cast<int>(cluster_map.size())) [[unlikely]] {
    return false;
  }
  const NodeConfig &target = cluster_map[target_id];

  uint64_t net_ts = htobe64(tx_timestamp);
  uint8_t response = 0;

  bool success = send_single_request(target.id, target.ip, target.port,
                                     CommandType::CMD_TX_ABORT,
                                     reinterpret_cast<uint8_t *>(&net_ts), 8,
                                     &response, 1);
  #ifndef NDEBUG
    if (!success) {
      std::cerr << "[Coordinator] WARNING: Failed to deliver TX_ABORT to Node " << target_id << "\n";
    }
  #endif

    return success;
}

// Thread-per-connection model. Persistently reads from a specific client socket until EOF
void StaticClusterDHTNode::handle_client(int client_socket) {
  // Set a 60-second receive timeout to prevent dead or partitioned connections 
  // from permanently consuming the server's thread pool.
  // struct timeval tv;
  // tv.tv_sec = 300;
  // tv.tv_usec = 0;
  // setsockopt(client_socket, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof tv);

  uint8_t cmd_buf;

  // Pre-allocate dynamic buffers to prevent heap fragmentation during rapid parsing
  std::vector<uint8_t> tx_batch_buf;
  tx_batch_buf.reserve(BATCH_SIZE * 8);

  std::vector<std::pair<uint32_t, uint32_t>> tx_batch;
  tx_batch.reserve(BATCH_SIZE);

  while (running) {
    // Both the batch request and single request has a command type in the first byte
    if (!recv_n_bytes(client_socket, &cmd_buf, 1)) [[unlikely]] {
      #ifndef NDEBUG
        if (running && errno != 0 && errno != ECONNRESET) {
          log_error("Could not read command byte", errno);
        }
      #endif
      break;
    }

    CommandType cmd = static_cast<CommandType>(cmd_buf);

    // Clients wants to disconnect
    if (cmd == CommandType::CMD_QUIT) [[unlikely]] {
      std::cout << "Client requested disconnect.\n";
      break;
    }

    switch(cmd) {
      case CommandType::CMD_PUT: {
        // Read 16 Bytes: [Key:4][Value:4][Timestamp:8]
        uint8_t buf[16];
        if (!recv_n_bytes(client_socket, buf, 16)) [[unlikely]] {
          log_error("Failed to read PUT request", errno);
          goto cleanup;
        }

        uint32_t net_key, net_value;
        uint64_t net_ts;
        std::memcpy(&net_key, buf, 4);
        std::memcpy(&net_value, buf + 4, 4);
        std::memcpy(&net_ts, buf + 8, 8);

        uint32_t key = ntohl(net_key);
        uint32_t value = ntohl(net_value);
        uint64_t timestamp = be64toh(net_ts);

        synchronize_clock(timestamp);

        PutResult result = put_local(key, value, timestamp);

        // Send 1 byte response
        if (send(client_socket, &result, 1, MSG_NOSIGNAL) != 1) [[unlikely]] {
          log_error("Failed to send the response to PUT", errno);
          goto cleanup;
        }

        break; // exit the switch
      }

      case CommandType::CMD_GET: {
        // Read 12 bytes: [Key:4][Read_Timestamp:8]
        uint8_t buf[12];
        if (!recv_n_bytes(client_socket, buf, 12)) [[unlikely]] {
          log_error("Failed to read GET request", errno);
          goto cleanup;
        }

        uint32_t net_key;
        uint64_t net_ts;
        std::memcpy(&net_key, buf, 4);
        std::memcpy(&net_ts, buf + 4, 8);

        uint32_t key = ntohl(net_key);
        uint64_t read_ts = be64toh(net_ts);

        // Sync clock to ensure Causal Consistency for the read
        synchronize_clock(read_ts);

        std::optional<uint32_t> res = get_local(key);
        GetResponse result = res.has_value() ? GetResponse::success(res.value())
                                             : GetResponse::not_found();
        
        if (result.status == GetStatus::Found) {
          // Send 5 Bytes: [Status:1][Value:4]
          uint8_t out_buf[5];
          out_buf[0] = static_cast<uint8_t>(result.status);
          uint32_t net_val = htonl(result.value);
          std::memcpy(out_buf + 1, &net_val, 4);

          if (send(client_socket, out_buf, 5, MSG_NOSIGNAL) != 5) [[unlikely]] {
            log_error("Failed to send GET found response", errno);
            goto cleanup;
          }
        } else {
          // Send 1 Byte: [Status]
          uint8_t status = static_cast<uint8_t>(result.status);
          if (send(client_socket, &status, 1, MSG_NOSIGNAL) != 1) [[unlikely]] {
            log_error("Failed to send GET status response", errno);
            goto cleanup;
          }
        }

        break;
      }

      case CommandType::CMD_BATCH_PUT: {
        // Array of async puts flushed from a client buffer
        uint8_t request_buffer[BATCH_SIZE * 16];
        uint8_t response_buffer[BATCH_SIZE];

        // Read Batch Count (2 bytes)
        uint16_t batch_count_net;
        if (!recv_n_bytes(client_socket, &batch_count_net, 2)) [[unlikely]] {
          log_error("Failed to read batch count", errno);
          goto cleanup;
        }

        uint16_t batch_count = ntohs(batch_count_net);
        if (batch_count > BATCH_SIZE) [[unlikely]] {
          std::cerr << "Invalid batch size: " << batch_count << "\n";
          goto cleanup;
        }

        // Read batch requests
        size_t total_req_bytes = batch_count * 16;
        if (!recv_n_bytes(client_socket, request_buffer, total_req_bytes)) [[unlikely]] {
          log_error("Failed to read batched requests", errno);
          goto cleanup;
        }

        uint8_t* req_ptr = request_buffer;
        uint8_t* resp_ptr = response_buffer;

        for (int i = 0; i < batch_count; ++i) {
          uint32_t net_k, net_v;
          uint64_t net_ts;
          std::memcpy(&net_k, req_ptr, 4);
          std::memcpy(&net_v, req_ptr + 4, 4);
          std::memcpy(&net_ts, req_ptr + 8, 8);
          
          uint64_t ts = be64toh(net_ts);
          synchronize_clock(ts);
          
          PutResult res = put_local(ntohl(net_k), ntohl(net_v), ts);
          *resp_ptr = static_cast<uint8_t>(res);

          req_ptr += 16;
          resp_ptr++;
        }

        // Send Batch Response
        if (send(client_socket, response_buffer, batch_count, MSG_NOSIGNAL)
            != batch_count) [[unlikely]] {
          log_error("Failed to send batched response", errno);
          goto cleanup;
        }

        break;
      }

      case CommandType::CMD_TX_PREPARE: {
        // 2PC Phase 1: Lock keys and validate LWW timestamps
        // Read the 10 byte header
        uint8_t header_buf[10];
        if (!recv_n_bytes(client_socket, &header_buf, 10)) [[unlikely]] {
          log_error("Failed to read TX_PREPARE header", errno);
          goto cleanup;
        }

        uint64_t net_tx_ts;
        uint16_t net_batch_size;
        std::memcpy(&net_tx_ts, header_buf, 8);
        std::memcpy(&net_batch_size, header_buf + 8, 2);

        uint64_t tx_timestamp = be64toh(net_tx_ts);
        uint16_t batch_size = ntohs(net_batch_size);

        synchronize_clock(tx_timestamp);

        // Read the batch data [Key:4][Value:4] per item
        tx_batch_buf.resize(batch_size * 8);
        if (!recv_n_bytes(client_socket, tx_batch_buf.data(), batch_size * 8))
        [[unlikely]] {
          log_error("Failed to read Tx batch", errno);
          goto cleanup;
        }

        tx_batch.clear(); // Resets size to 0 without freeing memory
        for (size_t i = 0; i < batch_size; ++i) {
          uint32_t net_k, net_v;
          std::memcpy(&net_k, tx_batch_buf.data() + (i * 8), 4);
          std::memcpy(&net_v, tx_batch_buf.data() + (i * 8) + 4, 4);
          tx_batch.emplace_back(ntohl(net_k), ntohl(net_v));
        }

        // Execute Phase 1 Validation
        bool prepared = local_tx_prepare(tx_timestamp, tx_batch);

        // Pack 9-byte response: [Vote:1][CurrentClock:8]
        uint8_t response_buf[9];
        response_buf[0] = prepared ? 1 : 0;

        // Read our current clock and append it to the response
        uint64_t current_clock = htobe64(logical_clock.load(std::memory_order_relaxed));
        std::memcpy(response_buf + 1, &current_clock, 8);

        if (send(client_socket, response_buf, 9, MSG_NOSIGNAL) != 9) [[unlikely]] {
          log_error("Failed to vote for 2PC Phase 1", errno);
          goto cleanup;
        }
        break;
      }

      case CommandType::CMD_TX_COMMIT: {
        uint8_t ts_buf[8];
        if (!recv_n_bytes(client_socket, ts_buf, 8)) [[unlikely]] {
          log_error("Failed to read TX_COMMIT timestamp", errno);
          goto cleanup;
        }

        uint64_t net_tx_ts;
        std::memcpy(&net_tx_ts, ts_buf, 8);
        uint64_t tx_timestamp = be64toh(net_tx_ts);

        synchronize_clock(tx_timestamp);
        local_tx_commit(tx_timestamp);
        
        uint8_t ack = 1;
        if (send(client_socket, &ack, 1, MSG_NOSIGNAL) != 1) [[unlikely]] {
          log_error("Failed to send TX_COMMIT ack", errno);
          goto cleanup;
        }
        break;
      }

      case CommandType::CMD_TX_ABORT: {
        uint8_t ts_buf[8];
        if (!recv_n_bytes(client_socket, ts_buf, 8)) [[unlikely]] {
          log_error("Failed to read TX_ABORT timestamp", errno);
          goto cleanup;
        }
        
        uint64_t net_tx_ts;
        std::memcpy(&net_tx_ts, ts_buf, 8);
        uint64_t tx_timestamp = be64toh(net_tx_ts);

        synchronize_clock(tx_timestamp);        
        local_tx_abort(tx_timestamp);
        
        uint8_t ack = 1;
        if (send(client_socket, &ack, 1, MSG_NOSIGNAL) != 1) [[unlikely]] {
          log_error("Failed to send TX_ABORT ack", errno);
          goto cleanup;
        }
        break;
      }

      case CommandType::CMD_BARRIER: {
        // Read the node id of the worker node
        uint8_t buf[4];
        if (!recv_n_bytes(client_socket, buf, 4)) [[unlikely]]  {
          log_error("Failed to read node id for barrier", errno);
          goto cleanup;
        }

        uint32_t net_id;
        std::memcpy(&net_id, buf, 4);
        uint32_t worker_id = ntohl(net_id);

        // Add to checkins
        {
          std::lock_guard<std::mutex> lock(barrier_mtx);
          barrier_checkins.insert(worker_id);
        }

        barrier_cv.notify_all(); // wake up coordinator's wait_for_barrier()

        // Send 1 byte ACK
        uint8_t ack = 1; 
        if (send(client_socket, &ack, 1, MSG_NOSIGNAL) != 1) [[unlikely]] {
          log_error("Failed to send ack signal for barrier", errno);
          goto cleanup;
        }

        break;
      }

      case CommandType::CMD_PING: {
        // Simple Echo/ACK
        uint8_t ack = 1; 
        if (send(client_socket, &ack, 1, MSG_NOSIGNAL) != 1) [[unlikely]] {
          log_error("Failed to response to ping", errno);
          goto cleanup;
        }

        break;
      }

      case CommandType::CMD_GO: {
        // Used to signal start of benchmark
        uint8_t ack = 1;
        if (send(client_socket, &ack, 1, MSG_NOSIGNAL) != 1) [[unlikely]] {
          log_error("Failed to send bechmark signal", errno);
          goto cleanup;
        }

        benchmark_ready.store(true, std::memory_order_release);
        break;
      }

      case CommandType::CMD_EXIT_BARRIER: {
        // Read the 4-byte payload containing the sender's node ID
        uint8_t buf[4];
        if (!recv_n_bytes(client_socket, buf, 4)) [[unlikely]] {
          log_error("Failed to read node id for exit barrier", errno);
          goto cleanup;
        }

        uint32_t net_id;
        std::memcpy(&net_id, buf, 4);
        uint32_t worker_id = ntohl(net_id);

        // Idempotent insertion: if it retries, it just overwrites the same ID
        {
          std::lock_guard<std::mutex> lock(exit_mtx);
          exited_peers.insert(worker_id);
        }

        uint8_t ack = 1;
        if (send(client_socket, &ack, 1, MSG_NOSIGNAL) != 1) [[unlikely]] {
          log_error("Failed to send exit barrier ack", errno);
          goto cleanup;
        }
        break;
      }
    }
  }

  cleanup:
    close(client_socket);
}

// Listener thread creates a server socket which is listening for incoming connections
// This model only works for the assignment but not real world with millions of connections
void StaticClusterDHTNode::listen_loop() {
  server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    log_error("Error making server socket: ", errno);
    return;
  }

  // set socket options to use address in use
  int opt = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
    log_error("setsockopt(SO_REUSEADDR) failed: ", errno);
    return;
  }

  // create the address to bind with server socket
  sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  server_addr.sin_port = htons(self_config.port);

  // bind the socket to the address
  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    log_error("Error binding socket to local address: ", errno);
    return;
  }

  // mark the socket to listen state
  if (listen(server_fd, SOMAXCONN) < 0) {
    log_error("Error listening on socket: ", errno);
    return;
  }

  std::cout << "Node listening on port " << self_config.port << "..." << std::endl;

  // server_fd socket accepts incoming connections in a persistent loop
  while (true) {
    // initialize client address to store client information
    sockaddr_in client_addr = {0};
    socklen_t client_addr_len = sizeof(client_addr);

    // accept a incoming connection that returns a new_socket to communicate with the 
    // client listener thread is blocked on accept
    int new_socket = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
    if (new_socket < 0) {
      // Check if we are shutting down
      if (errno == EINVAL || errno == EBADF || !running) {
          break; 
      }

      if (errno == EMFILE || errno == ENFILE) {
          log_error("CRITICAL: Node ran out of File Descriptors!", errno);
          // Sleep to let the OS naturally close old TIME_WAIT sockets and free up FDs
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
          continue;
      }

      // Log other errors but keep listening
      log_error("Accept failed: ", errno);
      continue;
    }

    // disable Nagle's algorithm on this new socket
    if (setsockopt(new_socket, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0) {
      log_error("Disabling Nagle's Algorithm failed: ", errno);
      close(new_socket);
      continue;
    }

    char clientname[1024];
    std::cout << "Connected to "
              << inet_ntop(AF_INET, &client_addr.sin_addr, clientname, sizeof(clientname))
              << std::endl;

    std::thread client_thread(&StaticClusterDHTNode::handle_client, this, new_socket);
    
    // Safely track the socket and thread for graceful shutdown
    {
      std::lock_guard<std::mutex> lock(thread_mutex);
      active_sockets.push_back(new_socket);
      client_threads.push_back(std::move(client_thread));
    }
  }
}