#include "dht/raft_engine.h"
#include "common/utils.h"
#include "dht/dht_static_partitioning.h"

#include <iostream>
#include <algorithm>
#include <random>

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

  pin_thread_to_control_cores(timer_thread);
  pin_thread_to_control_cores(applier_thread);

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

bool RaftEngine::propose_command(const uint8_t *data, size_t data_len) {
  if (data == nullptr || data_len == 0) [[unlikely]]
    return true; // Trivial

  uint64_t target_index = 0;
  uint64_t expected_term = 0;

  // Exclusive writer lock
  std::unique_lock<std::shared_mutex> lock(raft_mutex);

  // Verify leadership status
  if (role != ConsensusRole::LEADER || !running.load(std::memory_order_relaxed)) {
    return false;
  }

  // Deep copy into global memory pool
  size_t offset = log_mempool.size();
  log_mempool.insert(log_mempool.end(), data, data + data_len);

  // Construct the Raft log entry
  expected_term = current_term;

  RaftLogEntry entry{};
  entry.term = expected_term;
  entry.data_offset = offset;
  entry.data_size = data_len;

  log.push_back(entry);
  target_index = log.size() - 1;

  // Update the leaders own match index
  int self_id = transport->get_self_id();
  if (self_id >= 0 && self_id < total_nodes) {
    match_index[self_id] = target_index;
  }

  // Group commit logger
  timer_cv.notify_one();

  // Block the Client Thread Until Quorum is Achieved
  commit_cv.wait(lock, [this, target_index, expected_term]() {
    return (commit_index >= target_index) ||             
           (role != ConsensusRole::LEADER) ||            
           (current_term != expected_term) ||            
           (!running.load(std::memory_order_relaxed));   
  });

  // Final Evaluation
  return (commit_index >= target_index) && (current_term == expected_term);
}

void RaftEngine::on_network_message(uint32_t sender_id, uint8_t command_type, 
                                    const uint8_t* payload, uint16_t payload_size)
{
  if (payload == nullptr || payload_size == 0) [[unlikely]] {
    return;
  }

  switch (command_type) {
    case static_cast<uint8_t>(RaftCommand::AppendEntries): [[likely]]
      handle_append_entries(sender_id, payload, payload_size);
      break;

    case static_cast<uint8_t>(RaftCommand::AppendReply): [[likely]]
      handle_append_reply(sender_id, payload, payload_size);
      break;

    case static_cast<uint8_t>(RaftCommand::RequestVote): [[unlikely]]
      handle_request_vote(sender_id, payload, payload_size);
      break;

    case static_cast<uint8_t>(RaftCommand::VoteReply): [[unlikely]]
      handle_vote_reply(sender_id, payload, payload_size);
      break;

    default: [[unlikely]]
      #ifndef NDEBUG
        std::cerr << "[RaftEngine] Dropped unknown network command: " 
                  << static_cast<int>(command_type) << " from Node " << sender_id << "\n";
      #endif
      break;
  }
}

ConsensusRole RaftEngine::get_role() const {
  std::shared_lock<std::shared_mutex> lock(raft_mutex);
  return role;
}

int RaftEngine::get_leader_id() const {
  std::shared_lock<std::shared_mutex> lock(raft_mutex);
  return current_leader_id;
}

uint64_t RaftEngine::get_commit_index() const {
  std::shared_lock<std::shared_mutex> lock(raft_mutex);
  return commit_index;
}

// ---------------------------------------------------------------------------------------
// Internal handlers

void RaftEngine::timer_loop() {
  // RNG for election timeouts
  std::random_device rd;
  std::mt19937 gen(rd());

  while (running.load(std::memory_order_relaxed)) {
    ConsensusRole current_role;
    int current_timeout;

    {
      std::shared_lock<std::shared_mutex> lock(raft_mutex);
      current_role = role;
    }

    // Determine the correct sleep duration based on Role
    if (current_role == ConsensusRole::LEADER) {
      current_timeout = heartbeat_interval_ms;
    } else {
      // Followers and Candidates uses randomized timeouts to prevent infinite split-vote
      std::uniform_int_distribution<> dist(election_timeout_min_ms, election_timeout_max_ms);
      current_timeout = dist(gen);
    }

    std::cv_status wakeup_status;
    {
      std::unique_lock<std::mutex> timer_lock(timer_mutex);
      wakeup_status = timer_cv.wait_for(timer_lock, std::chrono::milliseconds(current_timeout));
    }

    // Exit immediately if the server is shutting down
    if (!running.load(std::memory_order_relaxed)) {
      break;
    }

    // Execution Phase
    if (current_role == ConsensusRole::LEADER) {
      if (wakeup_status == std::cv_status::no_timeout) {
         // Awoken early by a client calling propose_command().
         // Yield for batch to fill up
         std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }

      // Send heartbeat network
      send_heartbeat();

    } else {
      // FOLLOWER / CANDIDATE: Check if we starved
      auto now = std::chrono::steady_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       now - last_heartbeat_received).count();

      if (elapsed >= current_timeout) {
        #ifndef NDEBUG
          std::cout << "[RaftEngine] Node " << transport->get_self_id() 
                    << " starved for " << elapsed << "ms. Starting election.\n";
        #endif
        
        start_election();
      }
    }
  }
}

void RaftEngine::applier_loop() {
  while (running.load(std::memory_order_relaxed)) {    
    // Awoken by advance_commit_index() or shutdown()
    {
      std::unique_lock<std::mutex> lock(apply_mutex);
      apply_cv.wait(lock, [this]() {
        return (get_commit_index() > last_applied) || !running.load(std::memory_order_relaxed);
      });
    }

    if (!running.load(std::memory_order_relaxed)) {
      break;
    }

    // Gather the Batch
    std::vector<std::pair<uint64_t, std::vector<uint8_t>>> batch;
    uint64_t current_commit;

    {
      std::shared_lock<std::shared_mutex> r_lock(raft_mutex);
      
      current_commit = commit_index;
      if (current_commit > last_applied) {
        batch.reserve(current_commit - last_applied);
        
        for (uint64_t i = last_applied + 1; i <= current_commit; ++i) {
          const auto& entry = log[i];
          if (entry.data_size > 0) {
            const uint8_t* payload_ptr = log_mempool.data() + entry.data_offset;
            batch.push_back({i, std::vector<uint8_t>(payload_ptr, payload_ptr + entry.data_size)});
          }
        }
      }
    }

    // Execute the state machine
    if (!batch.empty() && state_machine) {
      state_machine->apply_committed_log(batch);
    }

    // Update the local tracking index
    {
      std::unique_lock<std::shared_mutex> w_lock(raft_mutex);
      last_applied = current_commit;
    }
  }
}