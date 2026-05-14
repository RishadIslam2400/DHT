#include "dht/raft_engine.h"

#include <iostream>
#include <algorithm>

// Constructor
RaftEngine::RaftEngine(IStateMachine *sm, INetworkTransport *nt)
: state_machine(sm), transport(nt)
{ 
  // Cahce the network topology
  peer_ids = transport->get_peer_ids();
  total_nodes = peer_ids.size() + 1; // peer + self
  quorum_size = (total_nodes / 2) + 1;

  next_index.resize(total_nodes);
  match_index.resize(total_nodes);

  log.reserve(100000);
  log_mempool.reserve(1024 * 1024 * 16);

  // Insert a dummy log entry
  log.push_back({0, 0, 0});
}

// Destructor
RaftEngine::~RaftEngine() {
  // Ensure we don't leak threads if the Engine is destroyed before stop() is explicitly called
  stop(); 
}

void RaftEngine::start() {
  bool expected = false;
  
  // Atomically check and set to prevent double start
  if (!running.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
    return;
  }

  // Reset the heartbeat timer so the node doesn't instantly trigger an election
  last_heartbeat_received = std::chrono::steady_clock::now();

  // Spawn the background worker threads
  timer_thread = std::thread(&RaftEngine::timer_loop, this);
  applier_thread = std::thread(&RaftEngine::applier_loop, this);

  #ifndef NDEBUG
    std::cout << "[RaftEngine] Started on Node " << transport->get_self_id() 
              << ". Quorum size: " << quorum_size << "/" << total_nodes << "\n";
  #endif
}

void RaftEngine::stop() {
  // Atomically flip the running flag
  if (!running.exchange(false, std::memory_order_acq_rel)) {
    return;
  }

  // Wake up the timer thread
  {
    std::lock_guard<std::mutex> lock(timer_mutex);
    timer_cv.notify_all();
  }

  // Wake up the applier thread
  {
    std::lock_guard<std::mutex> lock(apply_mutex);
    apply_cv.notify_all();
  }

  // Wake up application threads blocked in propose_command()
  // If the engine shuts down while a client is waiting for a quorum, 
  // we must unblock them so they can return an Abort to the user.
  commit_cv.notify_all();

  if (timer_thread.joinable()) {
      timer_thread.join();
  }
  
  if (applier_thread.joinable()) {
      applier_thread.join();
  }
}