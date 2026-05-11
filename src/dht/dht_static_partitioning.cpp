#include "dht/dht_static_partitioning.h"
#include "common/xxHash64.h"
#include "common/utils.h"
#include "network/buffered_socket.h"
#include "dht/consensus_interface.h"

#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <endian.h>
#include <bit>

StaticClusterDHTNode::StaticClusterDHTNode(std::vector<NodeConfig> map, NodeConfig self,
                                           int hash_table_size, int num_locks, int rep_deg,
                                          std::unique_ptr<IConsensusEngine> engine)
  : cluster_map{std::move(map)}, 
    self_config{std::move(self)},
    storage{hash_table_size, num_locks},
    replication_degree{rep_deg},
    server_fd{-1},
    running{false},
    connection_pool(cluster_map.size()),
    benchmark_ready{false},
    consensus_engine(std::move(engine))
{
  // Calculate optimal logical stripes based on key_range (hash_table_size)
  // Cap at 4096 to prevent allocating excessive memory for massive key ranges.
  size_t target_stripes = std::min(static_cast<size_t>(2 * hash_table_size), static_cast<size_t>(4096));

  // key range 10, 100, 1000, 100000: 32, 256, 2048, 4096
  num_logical_stripes = std::bit_ceil(std::max<size_t>(1, target_stripes)); 
  logical_stripe_mask = num_logical_stripes - 1;

  stripe_locks = std::make_unique<AlignedSpinlock[]>(num_logical_stripes);
  logically_locked_stripes.resize(num_logical_stripes);

  staging_locks = std::make_unique<AlignedSpinlock[]>(num_logical_stripes);
  staging_stripes.resize(num_logical_stripes);

  std::cout << "Booted Node " << self_config.id
            << " (" << self_config.ip << ":" << self_config.port << ")" << std::endl;
}

StaticClusterDHTNode::~StaticClusterDHTNode() {
  stop();
}

void StaticClusterDHTNode::start() {
  running.store(true, std::memory_order_release);
  listener_thread = std::thread(&StaticClusterDHTNode::listen_loop, this);
  if (consensus_engine) {
    consensus_engine->start(this);
  }
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
  std::cout << "Stopping Node..." << std::endl;
  if (!running.exchange(false, std::memory_order_acq_rel)) {
    return;
  }

  // Halt Consensus Engine
  if (consensus_engine) {
    consensus_engine->stop();
  }
  
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

  // Latency calculations
  uint32_t remote_success = stats.remote_puts_success.load();
  double avg_remote_put_us = remote_success > 0 ? 
      (static_cast<double>(stats.remote_puts_total_ns.load()) / remote_success) / 1000.0 : 0.0;

  uint32_t local_commits = stats.local_tx_committed.load();
  double avg_tx_commit_us = local_commits > 0 ? 
      (static_cast<double>(stats.local_tx_commit_total_ns.load()) / local_commits) / 1000.0 : 0.0;

  std::cout << "\n============================================\n";
  std::cout << " Node " << self_config.id << " Statistics (" << self_config.ip << ":" << self_config.port << ")\n";
  std::cout << "============================================\n";
  
  std::cout << "[Local Hash Table Metrics]\n";
  std::cout << "  Total Local PUTs:           " << total_local_puts << "\n";
  std::cout << "    - Inserted:               " << stats.local_puts_inserted << "\n";
  std::cout << "    - Updated:                " << stats.local_puts_updated << "\n";
  std::cout << "    - Dropped (Obsolete):     " << stats.local_puts_dropped << "\n\n";
  
  std::cout << "  Total Local GETs:           " << total_local_gets << "\n";
  std::cout << "    - Found:                  " << stats.local_gets_found << "\n";
  std::cout << "    - Not Found:              " << stats.local_gets_not_found << "\n\n";
  
  std::cout << "[Remote Operations Metrics]\n";
  std::cout << "  Remote PUT Requests:\n";
  std::cout << "    - Success:                " << stats.remote_puts_success << "\n";
  std::cout << "    - Failed:                 " << stats.remote_puts_failed << "\n";
  std::cout << "    - Avg Latency:            " << avg_remote_put_us << " us\n\n";

  std::cout << "  Remote GET Requests:\n";
  std::cout << "    - Success:                " << stats.remote_gets_success << "\n";
  std::cout << "    - Failed:                 " << stats.remote_gets_failed << "\n\n";

  std::cout << "[Distributed Transactions (2PC)]\n";
  std::cout << "  Coordinator:\n";
  std::cout << "    - TX Committed:           " << stats.coordinator_tx_committed << "\n";
  std::cout << "    - TX Failed/Aborted:      " << stats.coordinator_tx_failed << "\n";
  std::cout << "    - Phase 1 Retries:        " << stats.coordinator_tx_retries << "\n";
  std::cout << "    - Phase 2 Retries:        " << stats.coordinator_phase2_retries << "\n\n";
  
  std::cout << "  Cohort (Server-side):\n";
  std::cout << "    - Local TX Committed:     " << local_commits << "\n";
  std::cout << "    - Local TX Aborted:       " << stats.local_tx_aborted << "\n";
  std::cout << "    - Rejected (Locked):      " << stats.tx_prepare_rejected_locked << "\n";
  std::cout << "    - Rejected (Obsolete):    " << stats.tx_prepare_rejected_obsolete << "\n";
  std::cout << "    - Avg Commit Latency:     " << avg_tx_commit_us << " us\n\n";

  std::cout << "[Consensus Engine Metrics]\n";
  std::cout << "  Leadership & Stability:\n";
  std::cout << "    - Elections Started:      " << stats.consensus_elections_started << "\n";
  std::cout << "    - Term/Ballot Changes:    " << stats.consensus_term_changes << "\n\n";
  
  std::cout << "  Replication State:\n";
  std::cout << "    - Logs Appended:          " << stats.consensus_log_entries_appended << "\n";
  std::cout << "    - Logs Truncated (Rollback): " << stats.consensus_log_truncations << "\n";
  std::cout << "    - State Machine Applied:  " << stats.consensus_state_machine_applied << "\n";
  std::cout << "============================================\n\n";
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
    NetworkEnvelope go_env;
    go_env.protocol_type = ProtocolType::ClientDht;
    go_env.command_type = static_cast<uint8_t>(DhtCommand::Go);
    go_env.payload_size = 0;
    go_env.sender_id = self_config.id;
    for (const auto& peer : barrier_sockets) {
      if (send(peer.second, &go_env, sizeof(NetworkEnvelope), MSG_NOSIGNAL) != sizeof(NetworkEnvelope)) {
        log_error("Failed to send go signal", errno);
        connection_pool.return_connection(peer.first, peer.second, true); // Destroy on fail
      } else {
        // Immediately return the socket to the pool
        connection_pool.return_connection(peer.first, peer.second, false);
      }
    }

    // Do not wait for ACKs
    benchmark_ready.store(true, std::memory_order_release);
  } else {
    // Ping Node 0 and wait for GO
    std::cout << "[Worker] Reached barrier. Notifying Coordinator...\n";

    const NodeConfig& coord = cluster_map[0];
    uint8_t response = 0;
    bool success = false;
    while (running) {
      success = send_single_request(coord.id, coord.ip, coord.port,
                                    ProtocolType::ClientDht, 
                                    static_cast<uint8_t>(DhtCommand::Barrier), 
                                    nullptr, 0,  // No payload needed, envelope has sender_id
                                    &response, 1);

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

  // Track the peers we still need to successfully notify
  std::vector<const NodeConfig*> unacknowledged_peers;
  unacknowledged_peers.reserve(expected_peers);

  for (const NodeConfig& peer : cluster_map) {
    if (peer.id != self_config.id) {
      unacknowledged_peers.push_back(&peer);
    }
  }

  // Continuously notify missing peers while waiting
  while (true) {
    // Attempt to deliver the exit signal to anyone who hasn't ACKed us yet
    for (auto it = unacknowledged_peers.begin(); it != unacknowledged_peers.end(); ) {
      const NodeConfig* target = *it;

      uint8_t ack = 0;
      bool success = send_single_request(target->id, target->ip, target->port,
                                         ProtocolType::ClientDht,
                                         static_cast<uint8_t>(DhtCommand::ExitBarrier), 
                                         nullptr, 0, &ack, 1);
      
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
      std::lock_guard<Spinlock> lock(exit_mtx.mutex);
      // A node can only safely terminate its listener socket if:
      // Every peer has informed us they are done running benchmarks and
      // We have successfully informed every peer that we are done.
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

      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        log_error("Socket read timed out", errno);
        return false; 
      }

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

// Scatter-Gather I/O execution
RpcResult StaticClusterDHTNode::perform_rpc_single_request(const int sock, ProtocolType proto,
                                                      uint8_t cmd, const uint8_t *request,
                                                      const size_t request_size, 
                                                      uint8_t *response,
                                                      const size_t response_size)
{
  // Construct the 8-byte header
  NetworkEnvelope env;
  env.protocol_type = proto;
  env.command_type = cmd;
  env.payload_size = static_cast<uint16_t>(request_size);
  env.sender_id = self_config.id;

  // Writev eliminates memcpy overhead by passing two pointers directly to the kernel
  struct iovec iov[2];
  iov[0].iov_base = &env;
  iov[0].iov_len = sizeof(NetworkEnvelope);

  int iov_count = 1;
  if (request_size > 0) [[likely]] {
    iov[1].iov_base = const_cast<uint8_t *>(request);
    iov[1].iov_len = request_size;
    iov_count = 2;
  }

  const size_t total_expected = sizeof(NetworkEnvelope) + request_size;
  struct msghdr msg;
  msg.msg_iov = iov;
  msg.msg_iovlen = iov_count;

  // Handle partial writes
  size_t total_sent = 0;
  while (total_sent < total_expected) {
    ssize_t sent = sendmsg(sock, &msg, MSG_NOSIGNAL);
    
    if (sent < 0) {
      if (errno == EINTR)
        continue;
      
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        log_error("Socket buffer full (EAGAIN) during sendmsg", errno);
        return RpcResult::SendFailed; // Safe to retry the request
      }

      log_error("Error sending RPC request via sendmsg", errno);
      return RpcResult::SendFailed; // Safe to retry the request
    }

    total_sent += sent;

    // Advance the iovec array pointers if we experienced a partial write
    if (total_sent < total_expected) {
      size_t advanced = sent;
      for (int i = 0; i < msg.msg_iovlen; ++i) {
        if (advanced >= msg.msg_iov[i].iov_len) {
          advanced -= msg.msg_iov[i].iov_len;
          msg.msg_iov[i].iov_len = 0;
        } else {
          msg.msg_iov[i].iov_base = static_cast<char*>(msg.msg_iov[i].iov_base) + advanced;
          msg.msg_iov[i].iov_len -= advanced;
          break;
        }
      }
    }
  }

  // Process the fixed-length response (if expected)
  if (response_size > 0) {
    if (!recv_n_bytes(sock, response, response_size)) {
      log_error("Error receiving response from single request", errno);
      return RpcResult::RecvFailed; // Unsafe to retry
    }
  }

  return RpcResult::Success;
}

// Wraps the RPC in a bounded retry loop, managing socket checkout and return 
// against the Thread-Safe Connection Pool
bool StaticClusterDHTNode::send_single_request(const int target_id,
                                               const std::string &target_ip,
                                               const int target_port, ProtocolType proto,
                                               uint8_t cmd, const uint8_t *request,
                                               size_t request_size, uint8_t *response,
                                               size_t response_size)
{
  const int max_attempts = 2;

  for (int attempt = 0; attempt < max_attempts; ++attempt) {
    // Get a active connection from the pool or create a new connection
    int sock = connection_pool.get_connection(target_id, target_ip, target_port);
    if (sock < 0) {
      // Hardware/OS failure: Out of file descriptors
      return false;
    }

    RpcResult result = perform_rpc_single_request(sock, proto, cmd, request,
                                                   request_size, response, response_size);

    if (result == RpcResult::Success) {
      connection_pool.return_connection(target_id, sock, false);
      return true;
    }

    // Regardless of the error type, the socket is now in an undefined/broken state. 
    connection_pool.return_connection(target_id, sock, true);

    if (result == RpcResult::RecvFailed) {
      // Unsafe to retry
      return false; 
    }

    // result == RpcResult::SendFailed, safe to retry
  }

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

  size_t batch_size = batch_requests.size();
  size_t payload_size = batch_size * sizeof(PutRequest);

  uint8_t recv_buffer[BATCH_SIZE];

  auto start_time = std::chrono::high_resolution_clock::now();

  bool success = send_single_request(target_id, target_ip, target_port, 
                                     ProtocolType::ClientDht, 
                                     static_cast<uint8_t>(DhtCommand::BatchPut),
                                     reinterpret_cast<const uint8_t*>(batch_requests.data()),
                                     payload_size, recv_buffer, batch_size);

  if (success) {
    // Stop the timer and accumulate
    auto end_time = std::chrono::high_resolution_clock::now();
    uint64_t duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    stats.remote_puts_total_ns.fetch_add(duration_ns, std::memory_order_relaxed);

    // Map the PutResult enum values (0, 1, 2, 3) to either 0 (fail) or 1 (success)
    // Failed = 0, Inserted = 1, Updated = 1, Dropped = 0
    static constexpr uint8_t is_success_map[4] = {0, 1, 1, 0};
    uint32_t successful_puts = 0;

    for (size_t i = 0; i < batch_size; ++i) {
      successful_puts += is_success_map[recv_buffer[i]];
    }

    uint32_t failed_puts = batch_size - successful_puts;

    if (successful_puts > 0) {
      stats.remote_puts_success.fetch_add(successful_puts, std::memory_order_relaxed);
    }
    if (failed_puts > 0) {
      stats.remote_puts_failed.fetch_add(failed_puts, std::memory_order_relaxed);
    }
    return true;
  }

  stats.remote_puts_failed.fetch_add(batch_size, std::memory_order_relaxed);
  log_error("Batch request failed after retries", errno);
  return false;
}

// Evaluates incoming network timestamps and fast-forwards the local Lamport clock
void StaticClusterDHTNode::synchronize_clock(const uint64_t incoming_ts) {
  uint64_t current_clock = logical_clock.load(std::memory_order_relaxed);

  // Loop until current clock is >= incoming_ts, or we successfully update it
  while (current_clock < incoming_ts) {
    // If logical_clock still equals current_clock, it updates to incoming_ts.
    // If it fails (another thread updated it), current_clock is automatically refreshed.
    if (logical_clock.compare_exchange_weak(current_clock, incoming_ts + 1, std::memory_order_relaxed)) {
      break;
    }
  }
}

PutResult StaticClusterDHTNode::put_local(const uint32_t& key, const uint32_t& value,
                                          const uint64_t &timestamp) {
  constexpr int MAX_RETRIES = 1000;
  uint16_t attempt = 0;

  // Use the node's internal XXHash64 hash function
  size_t stripe = hash_key(key) & logical_stripe_mask;
  PutResult result;

  // Pauses physical insertion if the key is currently logically locked by an active 2PC 
  // Phase 1 transaction.
  while (true) {
    {
      // Acquire the logical stripe Spinlock
      std::lock_guard<Spinlock> lock(stripe_locks[stripe].mutex);

      // Check for 2PC logical locks
      if (logically_locked_stripes[stripe].find(key) == logically_locked_stripes[stripe].end()) {
        // Execute the physical write while holding the stripe lock.
        // This guarantees a 2PC transaction cannot lock the key during put.
        result = storage.put(key, value, timestamp);
        break;
      }
    } // Logical lock released here

    // Backoff strategy if the key is logically locked by an active 2PC
    attempt++;
    if (attempt >= MAX_RETRIES) {
      return PutResult::Failed; // Prevent permanent thread starvation
    }

    if (attempt < 100) {
      #if defined(__x86_64__) || defined(_M_X64)
        __builtin_ia32_pause(); 
      #elif defined(__aarch64__) || defined(__arm__)
        // Hardware-level pipeline yield for ARM (does not context switch)
        asm volatile("yield" ::: "memory"); 
      #else
        // If an unknown architecture, spin quietly
      #endif
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(5)); // Yield to OS scheduler
    }
  }

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
  // Reads bypass the logical locks completely (Read-Committed isolation)
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
  PutRequest req{key, value, timestamp};
  uint8_t response_byte = 0;

  auto start_time = std::chrono::high_resolution_clock::now();
  bool success = send_single_request(target.id, target.ip, target.port,
                                     ProtocolType::ClientDht,
                                     static_cast<uint8_t>(DhtCommand::Put),
                                     reinterpret_cast<const uint8_t*>(&req),
                                     sizeof(PutRequest),
                                     &response_byte, 1);

  if (success) {
    // Stop timer and accumulate telemetry
    auto end_time = std::chrono::high_resolution_clock::now();
    uint64_t duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    stats.remote_puts_total_ns.fetch_add(duration_ns, std::memory_order_relaxed);

    // Evaluate response
    PutResult result = static_cast<PutResult>(response_byte);
    
    if (result == PutResult::Inserted || result == PutResult::Updated) {
      stats.remote_puts_success.fetch_add(1, std::memory_order_relaxed);
    } else {
      // The network worked, but the database rejected the write (Dropped or 2PC Failed)
      stats.remote_puts_failed.fetch_add(1, std::memory_order_relaxed);
    }
    
    return result;
  }

  // If the network completely failed after all retries, return Failed
  stats.remote_puts_failed.fetch_add(1, std::memory_order_relaxed);
  return PutResult::Failed;
}

GetResponse StaticClusterDHTNode::get_remote(const uint32_t &key, const uint64_t& read_ts,
                                             const NodeConfig &target)
{
  GetRequest req{key, read_ts};
  GetResponse response;

  auto start_time = std::chrono::high_resolution_clock::now();
  bool success = send_single_request(target.id, target.ip, target.port,
                                     ProtocolType::ClientDht,
                                     static_cast<uint8_t>(DhtCommand::Get),
                                     reinterpret_cast<const uint8_t*>(&req),
                                     sizeof(GetRequest),
                                     reinterpret_cast<uint8_t*>(&response),
                                     sizeof(GetResponse));

  if (!success) {
    stats.remote_gets_failed.fetch_add(1, std::memory_order_relaxed);
    return GetResponse::error();
  }

  // Process the response
  if (response.status == GetStatus::Found || response.status == GetStatus::NotFound) {
    auto end_time = std::chrono::high_resolution_clock::now();
    uint64_t duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    stats.remote_gets_total_ns.fetch_add(duration_ns, std::memory_order_relaxed);
    
    stats.remote_gets_success.fetch_add(1, std::memory_order_relaxed);
    return response; 
  }

  stats.remote_gets_failed.fetch_add(1, std::memory_order_relaxed);
  return GetResponse::error();
}

/// 2PC Server State Machine
// PHASE 1: Optimistic Concurrency Control (OCC) Validation
bool StaticClusterDHTNode::local_tx_prepare(const uint64_t &tx_timestamp,
                                            const std::vector<std::pair<uint32_t, uint32_t>> &batch)
{
  auto start_time = std::chrono::high_resolution_clock::now();

  // Map keys to their required lock stripes
  std::vector<size_t> required_stripes;
  required_stripes.reserve(batch.size());
  for (const auto& kv : batch) {
    required_stripes.push_back(hash_key(kv.first) & logical_stripe_mask);
  }

  // Global order for the stripes
  std::sort(required_stripes.begin(), required_stripes.end());
  required_stripes.erase(std::unique(required_stripes.begin(), required_stripes.end()), required_stripes.end());

  // Acquire stripe locks in strict ascending order
  for (size_t stripe : required_stripes) {
    stripe_locks[stripe].mutex.lock();
  }

  // Validate the keys
  bool validation_failed = false;
  for (const auto& kv : batch) {
    size_t stripe = hash_key(kv.first) & logical_stripe_mask;

    // Lock collision check
    if (logically_locked_stripes[stripe].find(kv.first) != logically_locked_stripes[stripe].end()) {
      stats.tx_prepare_rejected_locked.fetch_add(1, std::memory_order_relaxed);
      validation_failed = true;
      break;
    }

    // LWW Timestamp check
    if (tx_timestamp <= storage.get_timestamp(kv.first)) {
      stats.tx_prepare_rejected_obsolete.fetch_add(1, std::memory_order_relaxed);
      validation_failed = true;
      break;
    }
  }

  // Rollback immediately if any validation failed
  if (validation_failed) {
    for (auto it = required_stripes.rbegin(); it != required_stripes.rend(); ++it) {
      stripe_locks[*it].mutex.unlock();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    uint64_t duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    stats.local_tx_prepare_total_ns.fetch_add(duration_ns, std::memory_order_relaxed);

    return false;
  }

  // Optimistically claim the logical locks
  for (const auto& kv : batch) {
    size_t stripe = hash_key(kv.first) & logical_stripe_mask;
    logically_locked_stripes[stripe].insert(kv.first);
  }

  // Push to the sharded staging area
  // We shard by tx_timestamp so Phase 2 commits do not bottleneck on key lock contention
  size_t staging_stripe = tx_timestamp & logical_stripe_mask;
  {
    std::lock_guard<Spinlock> stage_lock(staging_locks[staging_stripe].mutex);
    staging_stripes[staging_stripe].push_back({tx_timestamp, batch});
  }

  // Release striped key locks in reverse order
  for (auto it = required_stripes.rbegin(); it != required_stripes.rend(); ++it) {
    stripe_locks[*it].mutex.unlock();
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  uint64_t duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  stats.local_tx_prepare_total_ns.fetch_add(duration_ns, std::memory_order_relaxed);

  return true;
}

// PHASE 2 (Success): Extract from staging area and apply to physical storage
void StaticClusterDHTNode::local_tx_commit(const uint64_t &tx_timestamp) {
  auto start_time = std::chrono::high_resolution_clock::now();

  // Route to the correct staging shard
  size_t staging_stripe = tx_timestamp & logical_stripe_mask;
  std::vector<std::pair<uint32_t, uint32_t>> batch_to_commit;

  // Extract the transaction from the staging area
  {
    std::lock_guard<Spinlock> stage_lock(staging_locks[staging_stripe].mutex);
    auto &stripe_vector = staging_stripes[staging_stripe];

    for (auto it = stripe_vector.begin(); it != stripe_vector.end(); ++it) {
      if (it->tx_timestamp == tx_timestamp) {
        batch_to_commit = std::move(it->batch);

        if (it != stripe_vector.end() - 1) {
          *it = std::move(stripe_vector.back());
        }

        stripe_vector.pop_back();
        break;
      }
    }
  } // Release stage lock

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
  for (const auto& kv : batch_to_commit) {
    size_t lock_stripe = hash_key(kv.first) & logical_stripe_mask;
    
    std::lock_guard<Spinlock> lock(stripe_locks[lock_stripe].mutex);
    logically_locked_stripes[lock_stripe].erase(kv.first);
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  uint64_t duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

  stats.local_tx_commit_total_ns.fetch_add(duration_ns, std::memory_order_relaxed);
  stats.local_tx_committed.fetch_add(1, std::memory_order_relaxed);
}

// PHASE 2 (Failure): Coordinator ordered rollback
void StaticClusterDHTNode::local_tx_abort(const uint64_t &tx_timestamp) {
  auto start_time = std::chrono::high_resolution_clock::now();

  size_t staging_stripe = tx_timestamp & logical_stripe_mask;
  std::vector<std::pair<uint32_t, uint32_t>> batch_to_abort;

  // Extract and remove the transaction from the staging area
  {
    std::lock_guard<Spinlock> stage_lock(staging_locks[staging_stripe].mutex);
    auto& stripe_vector = staging_stripes[staging_stripe];
    
    for (auto it = stripe_vector.begin(); it != stripe_vector.end(); ++it) {
      if (it->tx_timestamp == tx_timestamp) {
        batch_to_abort = std::move(it->batch);
        
        if (it != stripe_vector.end() - 1) {
          *it = std::move(stripe_vector.back());
        }
        stripe_vector.pop_back();
        break;
      }
    }
  } // release stage lock

  if (batch_to_abort.empty()) [[unlikely]] {
    return; // Safety guard against duplicate aborts
  }

  // Release logical locks
  for (const auto& kv : batch_to_abort) {
    size_t lock_stripe = hash_key(kv.first) & logical_stripe_mask;
    
    std::lock_guard<Spinlock> lock(stripe_locks[lock_stripe].mutex);
    logically_locked_stripes[lock_stripe].erase(kv.first);
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  uint64_t duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  
  stats.local_tx_abort_total_ns.fetch_add(duration_ns, std::memory_order_relaxed);
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

  size_t batch_size = batch.size();
  #ifndef NDEBUG
    if (batch_size > BATCH_SIZE) return false;
  #endif

  TxPrepareHeader header = {
    .tx_timestamp = tx_timestamp,
    .batch_size = static_cast<uint16_t>(batch_size)
  };

  // Allocate buffer and copy
  size_t payload_size = sizeof(TxPrepareHeader) + (batch_size * 8); // pair<uint32_t, uint32_t> is 8 bytes
  alignas(16) uint8_t payload_buffer[sizeof(TxPrepareHeader) + (BATCH_SIZE * 8)];
  std::memcpy(payload_buffer, &header, sizeof(TxPrepareHeader));
  std::memcpy(payload_buffer + sizeof(TxPrepareHeader), batch.data(), batch_size * 8);

  // Prepare the response
  TxPrepareResponse response;

  bool success = send_single_request(target.id, target.ip, target.port,
                                     ProtocolType::TwoPhaseCommit,
                                     static_cast<uint8_t>(TwoPhaseCommitCommand::Prepare),
                                     payload_buffer, payload_size,
                                     reinterpret_cast<uint8_t*>(&response),
                                     sizeof(TxPrepareResponse));

  if (success) {
    // Extract the remote server's clock and instantly catch up
    synchronize_clock(response.remote_clock);
    return response.vote == 1; // Return true only if the server voted yes
  }

  return false;
}

// Send Phase 2 COMMIT message
bool StaticClusterDHTNode::send_tx_commit(const int target_id, const uint64_t tx_timestamp) {
  if (target_id < 0 || target_id >= static_cast<int>(cluster_map.size())) [[unlikely]] {
    return false;
  }
  const NodeConfig &target = cluster_map[target_id];

  // Instantiate the header
  TxCommitHeader header = {
    .tx_timestamp = tx_timestamp
  };

  // 1 byte ack
  uint8_t response = 0;

  // Captures transient failures
  constexpr int MAX_INLINE_RETRIES = 3;
  const int BASE_BACKOFF_US = 50;

  for (int attempt = 0; attempt <= MAX_INLINE_RETRIES; ++attempt) {
    bool success = send_single_request(target.id, target.ip, target.port,
                                       ProtocolType::TwoPhaseCommit,
                                       static_cast<uint8_t>(TwoPhaseCommitCommand::Commit),
                                       reinterpret_cast<const uint8_t *>(&header),
                                       sizeof(TxCommitHeader), &response, 1);
    
    if (success) {
      return true; // Successfully delivered and acknowledged by the cohort
    }

    // Telemetry for network instability
    stats.coordinator_phase2_retries.fetch_add(1, std::memory_order_relaxed);

    if (attempt < MAX_INLINE_RETRIES) {
      // Exponential backoff
      std::this_thread::sleep_for(std::chrono::microseconds(BASE_BACKOFF_US * (1 << attempt)));
    }
  }

  // Asynchronous Handoff
  // The target is completely unreachable. We yield the thread back to the client.
  #ifndef NDEBUG
    std::cerr << "[Coordinator] Target " << target_id 
              << " unreachable. Offloading TX_COMMIT (" << tx_timestamp 
              << ") to async recovery queue.\n";
  #endif

  enqueue_for_async_recovery(target_id, TwoPhaseCommitCommand::Commit, tx_timestamp);

  return true;
}

// Send Phase 2 ABORT message
bool StaticClusterDHTNode::send_tx_abort(const int target_id, const uint64_t tx_timestamp) {
  if (target_id < 0 || target_id >= static_cast<int>(cluster_map.size())) [[unlikely]] {
    return false;
  }
  const NodeConfig &target = cluster_map[target_id];

  TxCommitHeader header = {
    .tx_timestamp = tx_timestamp
  };

  uint8_t response = 0;

  // Capture transient failures
  constexpr int MAX_INLINE_RETRIES = 3;
  const int BASE_BACKOFF_US = 50;

  for (int attempt = 0; attempt <= MAX_INLINE_RETRIES; ++attempt) {
    bool success = send_single_request(target.id, target.ip, target.port,
                                       ProtocolType::TwoPhaseCommit,
                                       static_cast<uint8_t>(TwoPhaseCommitCommand::Abort),
                                       reinterpret_cast<const uint8_t *>(&header),
                                       sizeof(TxCommitHeader), &response, 1);
    
    if (success) {
      return true; // Successfully delivered and acknowledged by the cohort
    }

    // Telemetry for network instability
    stats.coordinator_phase2_retries.fetch_add(1, std::memory_order_relaxed);

    if (attempt < MAX_INLINE_RETRIES) {
      // Exponential backoff
      std::this_thread::sleep_for(std::chrono::microseconds(BASE_BACKOFF_US * (1 << attempt)));
    }
  }

  // Asynchronous Handoff
  // The target is completely unreachable. We yield the thread back to the client.
  #ifndef NDEBUG
    std::cerr << "[Coordinator] Target " << target_id 
              << " unreachable. Offloading TX_COMMIT (" << tx_timestamp 
              << ") to async recovery queue.\n";
  #endif

  enqueue_for_async_recovery(target_id, TwoPhaseCommitCommand::Abort, tx_timestamp);

  return true;
}

// Thread-per-connection model. Persistently reads from a specific client socket until EOF
void StaticClusterDHTNode::handle_client(int client_socket) {  
  // Wrap the raw socket in User-Space buffer
  BufferedSocket buffered_sock(client_socket);

  // Pre-allocate buffers
  std::vector<std::pair<uint32_t, uint32_t>> tx_batch;
  tx_batch.reserve(BATCH_SIZE);

  while (running.load(std::memory_order_relaxed)) {
    // Read the universal 8-byte network envelope
    uint8_t* env_ptr = buffered_sock.read_ptr(sizeof(NetworkEnvelope));
    if (!env_ptr) [[unlikely]] {
      #ifndef NDEBUG
        if (running && errno != 0 && errno != ECONNRESET) {
          log_error("Could not read NetworkEnvelope", errno);
        }
      #endif
      break; // EOF or connection dropped
    }

    // Extract the network envelope
    NetworkEnvelope env;
    std::memcpy(&env, env_ptr, sizeof(NetworkEnvelope));

    // Read the payload
    uint8_t* payload_ptr = nullptr;
    if (env.payload_size > 0) {
      payload_ptr = buffered_sock.read_ptr(env.payload_size);
      if (!payload_ptr) [[unlikely]] {
        log_error("Failed to read payload bytes", errno);
        goto cleanup;
      }
    }

    switch(env.protocol_type) {
      // DHT commands
      case ProtocolType::ClientDht: {
        DhtCommand cmd = static_cast<DhtCommand>(env.command_type);

        if (cmd == DhtCommand::Quit) [[unlikely]] {
          std::cout << "Client requested disconnect.\n";
          goto cleanup;
        }

        switch (cmd) {
          case DhtCommand::Put: {
            const PutRequest* req = reinterpret_cast<const PutRequest*>(payload_ptr);

            synchronize_clock(req->timestamp);
            PutResult result = put_local(req->key, req->value, req->timestamp);

            if (send(client_socket, &result, 1, MSG_NOSIGNAL) != 1) [[unlikely]]
              goto cleanup;
            break; 
          }

          case DhtCommand::Get: {
            const GetRequest* req = reinterpret_cast<const GetRequest*>(payload_ptr);
            synchronize_clock(req->timestamp);

            std::optional<uint32_t> res = get_local(req->key);
            GetResponse response = res.has_value() ? GetResponse::success(res.value())
                                                   : GetResponse::not_found();

            if (send(client_socket, &response, sizeof(GetResponse), MSG_NOSIGNAL) != sizeof(GetResponse)) [[unlikely]]
              goto cleanup;
            break;
          }

          case DhtCommand::BatchPut: {
            // Determine batch size
            uint16_t batch_count = env.payload_size / sizeof(PutRequest);
            uint8_t response_buffer[BATCH_SIZE];

            const PutRequest* requests = reinterpret_cast<const PutRequest*>(payload_ptr);
            for (uint16_t i = 0; i < batch_count; ++i) {
              synchronize_clock(requests[i].timestamp);
              PutResult res = put_local(requests[i].key, requests[i].value, requests[i].timestamp);
              response_buffer[i] = static_cast<uint8_t>(res);
            }

            if (send(client_socket, response_buffer, batch_count, MSG_NOSIGNAL) != batch_count) [[unlikely]]
              goto cleanup;
            break;
          }

          case DhtCommand::Barrier: {
            {
              std::lock_guard<std::mutex> lock(barrier_mtx);
              barrier_checkins.insert(env.sender_id);
            }
            barrier_cv.notify_all();

            uint8_t ack = 1; 
            if (send(client_socket, &ack, 1, MSG_NOSIGNAL) != 1) [[unlikely]]
              goto cleanup;
            break;
          }

          case DhtCommand::ExitBarrier: {
            {
              std::lock_guard<Spinlock> lock(exit_mtx.mutex);
              exited_peers.insert(env.sender_id);
            }

            uint8_t ack = 1;
            if (send(client_socket, &ack, 1, MSG_NOSIGNAL) != 1) [[unlikely]]
              goto cleanup;
            break;
          }

          case DhtCommand::Ping:
          case DhtCommand::Go: {
            if (cmd == DhtCommand::Go)
              benchmark_ready.store(true, std::memory_order_release);
            
            uint8_t ack = 1; 
            if (send(client_socket, &ack, 1, MSG_NOSIGNAL) != 1) [[unlikely]]
              goto cleanup;
            break;
          }

          default: break;
        }
        break; // End ClientDht
      }

      // 2PC commands
      case ProtocolType::TwoPhaseCommit: {
        TwoPhaseCommitCommand cmd = static_cast<TwoPhaseCommitCommand>(env.command_type);

        switch (cmd) {
          case TwoPhaseCommitCommand::Prepare: {
            const TxPrepareHeader* header = reinterpret_cast<const TxPrepareHeader*>(payload_ptr);
            synchronize_clock(header->tx_timestamp);

            const std::pair<uint32_t, uint32_t>* batch_data = reinterpret_cast<const std::pair<uint32_t, uint32_t>*>(payload_ptr + sizeof(TxPrepareHeader));
            tx_batch.assign(batch_data, batch_data + header->batch_size);

            bool prepared = local_tx_prepare(header->tx_timestamp, tx_batch);

            TxPrepareResponse response = {
              .remote_clock = logical_clock.load(std::memory_order_relaxed),
              .vote = static_cast<uint8_t>(prepared ? 1 : 0)
            };

            if (send(client_socket, &response, sizeof(TxPrepareResponse), MSG_NOSIGNAL) != sizeof(TxPrepareResponse)) [[unlikely]]
              goto cleanup;
            break;
          }

          case TwoPhaseCommitCommand::Commit:
          case TwoPhaseCommitCommand::Abort: {
            const TxCommitHeader* header = reinterpret_cast<const TxCommitHeader*>(payload_ptr);
            synchronize_clock(header->tx_timestamp);

            if (cmd == TwoPhaseCommitCommand::Commit) {
              local_tx_commit(header->tx_timestamp);
            } else {
              local_tx_abort(header->tx_timestamp);
            }

            uint8_t ack = 1;
            if (send(client_socket, &ack, 1, MSG_NOSIGNAL) != 1) [[unlikely]]
              goto cleanup;
            break;
          }
        }
        break; // End TwoPhaseCommit
      }

      // Consensus Engine
      case ProtocolType::Paxos:
      case ProtocolType::Raft: {
        // Offload to consensus engine
        if (consensus_engine) {
          consensus_engine->on_network_message(env.sender_id, env.command_type, payload_ptr, env.payload_size);
        }
        break; // End Consensus
      }

      default:
        std::cerr << "[Network] Unknown Protocol Type: " << static_cast<int>(env.protocol_type) << "\n";
        break;
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

  // set socket options to reuse address
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

  // server_fd socket accepts incoming connections in a persistent loop until the system is running
  while (running.load(std::memory_order_relaxed)) {
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

    #ifdef __linux__
    // Disable Delayed ACKs
    if (setsockopt(new_socket, IPPROTO_TCP, TCP_QUICKACK, &opt, sizeof(opt)) < 0) {
      log_error("Enabling TCP_QUICKACK failed: ", errno);
    }
    #endif

    // Expand OS Buffers to handle massive BatchPuts and Raft AppendEntries
    int buf_size = 1024 * 1024 * 2; // 2 Megabytes
    setsockopt(new_socket, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
    setsockopt(new_socket, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));

#ifndef NDEBUG
    char clientname[1024];
    std::cout << "Connected to "
              << inet_ntop(AF_INET, &client_addr.sin_addr, clientname, sizeof(clientname))
              << std::endl;
#endif

    try {
      // Fire-and-forget lambda wrapper for automatic socket cleanup
      std::thread ([this, new_socket]() {
        // Thread registers its own socket
        {
          std::lock_guard<std::mutex> lock(thread_mutex);
          active_sockets.insert(new_socket);
        }

        // Process the client requests until disconnect
        this->handle_client(new_socket);

        // Thread cleans up its own socket upon exit to prevent memory leaks
        {
          std::lock_guard<std::mutex> lock(this->thread_mutex);
          this->active_sockets.erase(new_socket);
        }
      }).detach(); // Detach allows the OS to immediately reclaim the thread's stack memory
    } catch (const std::system_error& e) {
      log_error("CRITICAL: OS refused to spawn more threads! Dropping connection.", errno);
      close(new_socket);
    }
  }
}