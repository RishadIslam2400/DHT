#include "dht/dht_static_partitioning.h"
#include "dht/consensus_interface.h"

#include <unistd.h>
#include <sys/socket.h>
#include <bit>
#include <iostream>

StaticClusterDHTNode::StaticClusterDHTNode(
  std::vector<NodeConfig> map, NodeConfig self, size_t hash_table_size, size_t num_locks,
  int rep_deg)
  : cluster_map{std::move(map)}, 
    self_config{std::move(self)},
    storage{hash_table_size, num_locks},
    replication_degree{rep_deg},
    server_fd{-1},
    running{false},
    connection_pool(map.empty() ? 0 : map.size()), 
    benchmark_ready{false},
    thread_pool(16) // Fixed number for now
{
  size_t target_stripes = std::min(2 * hash_table_size, static_cast<size_t>(4096));
  num_logical_stripes = std::bit_ceil(std::max<size_t>(1, target_stripes)); 
  logical_stripe_mask = num_logical_stripes - 1;

  stripe_locks = std::make_unique<AlignedSpinlock[]>(num_logical_stripes);
  logically_locked_stripes.resize(num_logical_stripes);
  
  staging_locks = std::make_unique<AlignedSpinlock[]>(num_logical_stripes);
  staging_stripes.resize(num_logical_stripes);

  for (size_t i = 0; i < num_logical_stripes; ++i) {
    logically_locked_stripes[i].reserve(8);
    // Pre-allocate the staging vectors
    staging_stripes[i].reserve(8); 
  }

  // Safe reserve that explicitly prevents unsigned integer underflow
  if (cluster_map.size() > 1) {
    cached_peer_ids.reserve(cluster_map.size() - 1);
  }

  for (const auto& peer : cluster_map) {
    if (peer.id != self_config.id) {
      cached_peer_ids.push_back(peer.id);
    }
  }

  this->consensus_engine = std::make_unique<RaftEngine>(this, this);

  std::cout << "Booted Node " << self_config.id
            << " (" << self_config.ip << ":" << self_config.port << ")" << std::endl;
}

StaticClusterDHTNode::~StaticClusterDHTNode() {
  stop();
}

void StaticClusterDHTNode::start() {
  running.store(true, std::memory_order_release);
  if (consensus_engine) {
    consensus_engine->start();
  }

  listener_thread = std::thread(&StaticClusterDHTNode::listen_loop, this);
  dispatcher_thread = std::thread(&StaticClusterDHTNode::recovery_dispatcher_loop, this);
  sweeper_thread = std::thread(&StaticClusterDHTNode::stale_lock_sweeper_loop, this);
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

  // Join all background threads
  if (listener_thread.joinable()) listener_thread.join();
  if (dispatcher_thread.joinable()) dispatcher_thread.join();
  if (sweeper_thread.joinable()) sweeper_thread.join();

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

    std::cout << "[Coordinator] Broadcasting GO signal..." << std::endl;
    for (const NodeConfig& node : cluster_map) {
      if (node.id == 0)
        continue;
      
      uint8_t ack_response = 0;
      send_single_request(node.id, node.ip, node.port,  ProtocolType::ClientDht,
                          static_cast<uint8_t>(DhtCommand::Go), nullptr, 0,
                          &ack_response, 1);
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
                                    nullptr, 0, &response, 1);

      if (success && response == 1) {
        std::cout << "[Worker] Coordinator got checkin notification. Waiting for GO..." << std::endl;
        break;
      }

      std::cout << "[Worker] Coordinator busy. Retrying...\n";
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    // Spin-wait until CMD_GO is received and processed by handle_client 
    while (running && !benchmark_ready.load(std::memory_order_acquire)) {
      #if defined(__x86_64__) || defined(_M_X64)
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

  // Dead peer safety timeout
  auto start_time = std::chrono::steady_clock::now();
  const auto MAX_WAIT = std::chrono::seconds(30);

  // Continuously notify missing peers while waiting
  while (true) {
    if (std::chrono::steady_clock::now() - start_time > MAX_WAIT) {
      std::cerr << "[Warning] Exit barrier timed out! Forcing shutdown.\n";
      break; // Escape the hang
    }

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
        if (it != unacknowledged_peers.end() - 1) {
          *it = unacknowledged_peers.back();
        }
        unacknowledged_peers.pop_back(); 
      } else {
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

void StaticClusterDHTNode::print_status() {    
  // Local Hash Table
  uint32_t puts_inserted = stats.local_puts_inserted.load(std::memory_order_relaxed);
  uint32_t puts_updated  = stats.local_puts_updated.load(std::memory_order_relaxed);
  uint32_t puts_dropped  = stats.local_puts_dropped.load(std::memory_order_relaxed);
  uint32_t gets_found    = stats.local_gets_found.load(std::memory_order_relaxed);
  uint32_t gets_not_found= stats.local_gets_not_found.load(std::memory_order_relaxed);
  
  // Remote RPCs
  uint32_t remote_put_ok = stats.remote_puts_success.load(std::memory_order_relaxed);
  uint32_t remote_put_fail = stats.remote_puts_failed.load(std::memory_order_relaxed);
  uint32_t remote_get_ok = stats.remote_gets_success.load(std::memory_order_relaxed);
  uint32_t remote_get_fail = stats.remote_gets_failed.load(std::memory_order_relaxed);

  // 2PC Coordinator
  uint32_t coord_tx_commit = stats.coordinator_tx_committed.load(std::memory_order_relaxed);
  uint32_t coord_tx_fail   = stats.coordinator_tx_failed.load(std::memory_order_relaxed);

  // 2PC Cohort
  uint32_t local_commits = stats.local_tx_committed.load(std::memory_order_relaxed);
  uint32_t local_aborts  = stats.local_tx_aborted.load(std::memory_order_relaxed);
  uint32_t rejected_locked = stats.tx_prepare_rejected_locked.load(std::memory_order_relaxed);
  uint32_t rejected_obsolete = stats.tx_prepare_rejected_obsolete.load(std::memory_order_relaxed);
  uint32_t total_prepares = local_commits + local_aborts + rejected_locked + rejected_obsolete;

  // Aggregate Totals
  uint32_t total_local_puts = puts_inserted + puts_updated + puts_dropped;
  uint32_t total_local_gets = gets_found + gets_not_found;

  // Compute Latencies
  auto calc_avg_us = [](uint64_t total_ns, uint32_t count) -> double {
      return count > 0 ? (static_cast<double>(total_ns) / count) / 1000.0 : 0.0;
  };

  double avg_remote_put_us  = calc_avg_us(stats.remote_puts_total_ns.load(std::memory_order_relaxed), remote_put_ok);
  double avg_remote_get_us  = calc_avg_us(stats.remote_gets_total_ns.load(std::memory_order_relaxed), remote_get_ok);
  
  double avg_coord_tx_us    = calc_avg_us(stats.coordinator_tx_total_ns.load(std::memory_order_relaxed), coord_tx_commit);
  double avg_tx_prepare_us  = calc_avg_us(stats.local_tx_prepare_total_ns.load(std::memory_order_relaxed), total_prepares);
  double avg_tx_commit_us   = calc_avg_us(stats.local_tx_commit_total_ns.load(std::memory_order_relaxed), local_commits);
  double avg_tx_abort_us    = calc_avg_us(stats.local_tx_abort_total_ns.load(std::memory_order_relaxed), local_aborts);

  // Print Dashboard
  std::cout << "\n============================================\n";
  std::cout << " Node " << self_config.id << " Statistics (" << self_config.ip << ":" << self_config.port << ")\n";
  std::cout << "============================================\n";
  
  std::cout << "[Local Hash Table Metrics]\n";
  std::cout << "  Total Local PUTs:           " << total_local_puts << "\n";
  std::cout << "    - Inserted:               " << puts_inserted << "\n";
  std::cout << "    - Updated:                " << puts_updated << "\n";
  std::cout << "    - Dropped (Obsolete):     " << puts_dropped << "\n\n";
  
  std::cout << "  Total Local GETs:           " << total_local_gets << "\n";
  std::cout << "    - Found:                  " << gets_found << "\n";
  std::cout << "    - Not Found:              " << gets_not_found << "\n\n";
  
  std::cout << "[Remote Operations Metrics]\n";
  std::cout << "  Remote PUT Requests:\n";
  std::cout << "    - Success:                " << remote_put_ok << "\n";
  std::cout << "    - Failed:                 " << remote_put_fail << "\n";
  std::cout << "    - Avg Latency:            " << avg_remote_put_us << " us\n\n";

  std::cout << "  Remote GET Requests:\n";
  std::cout << "    - Success:                " << remote_get_ok << "\n";
  std::cout << "    - Failed:                 " << remote_get_fail << "\n";
  std::cout << "    - Avg Latency:            " << avg_remote_get_us << " us\n\n";

  std::cout << "[Distributed Transactions (2PC)]\n";
  std::cout << "  Coordinator:\n";
  std::cout << "    - TX Committed:           " << coord_tx_commit << "\n";
  std::cout << "    - TX Failed/Aborted:      " << coord_tx_fail << "\n";
  std::cout << "    - Phase 1 Retries:        " << stats.coordinator_tx_retries.load(std::memory_order_relaxed) << "\n";
  std::cout << "    - Phase 2 Retries:        " << stats.coordinator_phase2_retries.load(std::memory_order_relaxed) << "\n";
  std::cout << "    - Avg End-to-End Latency: " << avg_coord_tx_us << " us\n\n";
  
  std::cout << "  Cohort (Server-side):\n";
  std::cout << "    - Local TX Committed:     " << local_commits << "\n";
  std::cout << "    - Local TX Aborted:       " << local_aborts << "\n";
  std::cout << "    - Rejected (Locked):      " << rejected_locked << "\n";
  std::cout << "    - Rejected (Obsolete):    " << rejected_obsolete << "\n";
  std::cout << "    - Avg Prepare Latency:    " << avg_tx_prepare_us << " us\n";
  std::cout << "    - Avg Commit Latency:     " << avg_tx_commit_us << " us\n";
  std::cout << "    - Avg Abort Latency:      " << avg_tx_abort_us << " us\n\n";

  std::cout << "[Consensus Engine Metrics]\n";
  std::cout << "  Leadership & Stability:\n";
  std::cout << "    - Elections Started:      " << stats.consensus_elections_started.load(std::memory_order_relaxed) << "\n";
  std::cout << "    - Term/Ballot Changes:    " << stats.consensus_term_changes.load(std::memory_order_relaxed) << "\n\n";
  
  std::cout << "  Replication State:\n";
  std::cout << "    - Logs Appended:          " << stats.consensus_log_entries_appended.load(std::memory_order_relaxed) << "\n";
  std::cout << "    - Logs Truncated:         " << stats.consensus_log_truncations.load(std::memory_order_relaxed) << "\n";
  std::cout << "    - State Machine Applied:  " << stats.consensus_state_machine_applied.load(std::memory_order_relaxed) << "\n";
  std::cout << "============================================\n\n";
}