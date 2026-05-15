#include "dht/raft_engine.h"
#include "common/utils.h"
#include "dht/dht_static_partitioning.h"

#include <iostream>
#include <algorithm>
#include <random>
#include <functional>

// Constructor
RaftEngine::RaftEngine(INetworkTransport* nt, IStateMachine* sm)
: transport(nt), state_machine(sm)
{ 
  // Cahce the network topology
  peer_ids = transport->get_peer_ids();
  total_nodes = peer_ids.size() + 1; // peer + self
  quorum_size = (total_nodes / 2) + 1;

  next_index.resize(total_nodes);
  match_index.resize(total_nodes);
  peers_voted.resize(total_nodes, false);

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

void RaftEngine::start_election() {
  int self_id = transport->get_self_id();
  
  // Variables to hold a snapshot of our state for the network packets
  uint64_t election_term;
  uint64_t last_log_index;
  uint64_t last_log_term;

  // State Transition
  {
    std::unique_lock<std::shared_mutex> lock(raft_mutex);
    
    if (!running.load(std::memory_order_relaxed)) {
      return;
    }

    // Upgrade state to Candidate
    role = ConsensusRole::CANDIDATE;
    current_term++;
    voted_for = self_id;

    // Always vote for yourself first
    std::fill(peers_voted.begin(), peers_voted.end(), false);
    if (self_id >= 0 && self_id < total_nodes) [[likely]] {
      peers_voted[self_id] = true;
    }
    votes_received = 1;

    // Snapshot the log state required by the Raft safety protocol
    election_term = current_term;
    last_log_index = log.size() - 1;
    last_log_term = log.back().term;

    // Reset the physical clock to prevent immediate re-timeouts
    last_heartbeat_received = std::chrono::steady_clock::now();

    #ifndef NDEBUG
      std::cout << "[RaftEngine] Node " << self_id 
                << " starting election for Term " << election_term << "\n";
    #endif
  }

  // Prepare the network payload
  RaftRequestVoteHeader header{};
  header.term = election_term;
  header.candidate_id = static_cast<uint32_t>(self_id);
  header.last_log_index = last_log_index;
  header.last_log_term = last_log_term;

  // Asynchronous network meassage
  for (int peer_id : peer_ids) {
    if (peer_id == self_id) continue;

    transport->send_message(
      peer_id, ProtocolType::Raft, static_cast<uint8_t>(RaftCommand::RequestVote), 
      reinterpret_cast<const uint8_t*>(&header), sizeof(RaftRequestVoteHeader)
    );
  }
}

#pragma pack(push, 1)
struct RaftSerializedLogHeader {
  uint64_t term;
  uint16_t payload_size;
};
#pragma pack(pop)

void RaftEngine::send_heartbeat() {
  std::vector<uint8_t> packet_buffer;
  packet_buffer.reserve(65535);

  std::shared_lock<std::shared_mutex> lock(raft_mutex);

  if (role != ConsensusRole::LEADER || !running.load(std::memory_order_relaxed)) {
    return;
  }

  int self_id = transport->get_self_id();
  uint64_t current_log_size = log.size();

  // Broadcast loop
  for (int peer_id : peer_ids) {
    if (peer_id == self_id) continue;

    packet_buffer.clear();

    // Determine the log size for this peer
    uint64_t next_idx = next_index[peer_id];
    uint64_t prev_idx = next_idx > 0 ? next_idx - 1 : 0;

    if (prev_idx >= current_log_size) {
      prev_idx = current_log_size - 1;
      next_idx = current_log_size;
    }

    RaftAppendEntriesHeader header{};
    header.term = current_term;
    header.leader_id = static_cast<uint32_t>(self_id);
    header.prev_log_index = prev_idx;
    header.prev_log_term = log[prev_idx].term; 
    header.leader_commit = commit_index;
    header.batch_size = 0;

    packet_buffer.insert(packet_buffer.end(), reinterpret_cast<uint8_t*>(&header), 
                         reinterpret_cast<uint8_t*>(&header) + sizeof(RaftAppendEntriesHeader));

    // Group Commit Packer
    uint16_t entries_packed = 0;
    
    for (uint64_t i = next_idx; i < current_log_size; ++i) {
      const auto& entry = log[i];
      
      // Calculate total byte size of the entry
      size_t required_space = sizeof(RaftSerializedLogHeader) + entry.data_size;
      
      if (packet_buffer.size() + required_space >= 65000) [[unlikely]] {
        break; 
      }

      // Append sub-header for this specific log
      RaftSerializedLogHeader s_header;
      s_header.term = entry.term;
      s_header.payload_size = static_cast<uint16_t>(entry.data_size);

      packet_buffer.insert(packet_buffer.end(), reinterpret_cast<uint8_t*>(&s_header), 
                           reinterpret_cast<uint8_t*>(&s_header) + sizeof(RaftSerializedLogHeader));

      // Append raw payload
      if (entry.data_size > 0) {
        const uint8_t* payload_ptr = log_mempool.data() + entry.data_offset;
        packet_buffer.insert(packet_buffer.end(), payload_ptr, payload_ptr + entry.data_size);
      }
      
      entries_packed++;
    }

    reinterpret_cast<RaftAppendEntriesHeader*>(packet_buffer.data())->batch_size = entries_packed;

    // Dispatch the payload
    transport->send_message(
      peer_id, ProtocolType::Raft, static_cast<uint8_t>(RaftCommand::AppendEntries), 
      packet_buffer.data(), packet_buffer.size()
    );
  }
}

void RaftEngine::handle_request_vote(uint32_t sender_id, const uint8_t* payload, size_t size) {
  if (size < sizeof(RaftRequestVoteHeader)) [[unlikely]] {
    return;
  }

  const auto* req = reinterpret_cast<const RaftRequestVoteHeader*>(payload);
  
  // Exclusive writer lock to update current_term and voted_for
  std::unique_lock<std::shared_mutex> lock(raft_mutex);

  RaftVoteReplyHeader reply{};
  reply.term = current_term;
  reply.vote_granted = 0;

  // Stale term rejection
  if (req->term < current_term) {
    // A partitioned node that didn't know an election already happened 
    // is asking for a vote. Reject it immediately.
    dispatch_vote_reply(sender_id, reply);
    return;
  }

  // Candidate steps down
  if (req->term > current_term) {
    // Current node is behind, or a valid new election has started.
    // Step down to Follower and adopt this new term.
    current_term = req->term;
    role = ConsensusRole::FOLLOWER;
    voted_for = -1; // Reset our vote record for this new, higher term
  }

  // Update reply to match equal or higher term
  reply.term = current_term;

  // Check log index
  uint64_t my_last_index = log.size() - 1;
  uint64_t my_last_term = log.back().term;

  bool is_up_to_date = false;

  // If the logs end with different terms, the log with the larger term is more up-to-date.
  if (req->last_log_term > my_last_term) {
    is_up_to_date = true;
  } 
  // If the logs end with the same term, the longer log is more up-to-date.
  else if (req->last_log_term == my_last_term && req->last_log_index >= my_last_index) {
    is_up_to_date = true;
  }

  // Decision on the vote
  
  // Only vote if current node didn't already voted for someone else in this term,
  // And the candidate's log is as fresh (or fresher) than curernt node.
  if ((voted_for == -1 || voted_for == static_cast<int>(req->candidate_id)) && is_up_to_date) {
    reply.vote_granted = 1;
    voted_for = req->candidate_id;
    
    // Only reset the election timer if the vote is granted.
    // If we reject the vote, let the timer keep running so we can start our own election.
    last_heartbeat_received = std::chrono::steady_clock::now();
  }

  dispatch_vote_reply(sender_id, reply);
}

void RaftEngine::dispatch_vote_reply(uint32_t target_id, const RaftVoteReplyHeader& reply) {
  transport->send_message(
    target_id, ProtocolType::Raft, static_cast<uint8_t>(RaftCommand::VoteReply), 
    reinterpret_cast<const uint8_t*>(&reply), sizeof(RaftVoteReplyHeader)
  );
}

void RaftEngine::handle_vote_reply(uint32_t sender_id, const uint8_t* payload, size_t size) {
  if (size < sizeof(RaftVoteReplyHeader)) [[unlikely]] {
    return;
  }

  const auto* reply = reinterpret_cast<const RaftVoteReplyHeader*>(payload);
  bool became_leader = false;

  // Acquire exclusive lock
  {
    std::unique_lock<std::shared_mutex> lock(raft_mutex);

    // If the current node is no longer a candidate,
    // or if this vote belongs to a previous election term, drop the packet.
    if (role != ConsensusRole::CANDIDATE || reply->term < current_term) {
      return;
    }

    // The voter replied with a term higher than the current node.
    // Another node has already advanced the cluster's term. Abort candidacy.
    if (reply->term > current_term) {
      current_term = reply->term;
      role = ConsensusRole::FOLLOWER;
      voted_for = -1;
      return;
    }

    // Count vote
    if (reply->vote_granted) {
      if (sender_id < static_cast<uint32_t>(total_nodes)) [[likely]] {        
        // Check if we already received a vote from this node
        if (peers_voted[sender_id]) {
          return;
        }
        peers_voted[sender_id] = true;
        votes_received++;
      }

      // Check if quorum achieved
      if (votes_received >= quorum_size) {
        role = ConsensusRole::LEADER;
        current_leader_id = transport->get_self_id();
        became_leader = true;

        // The Leader must reinitialize its volatile state arrays instantly after winning election 
        uint64_t current_log_size = log.size();
        
        for (int i = 0; i < total_nodes; ++i) {
          // Optimistically assume all followers have the exact same log length as current node
          next_index[i] = current_log_size; 
          
          // Pessimistically assume no followers have matched anything yet
          match_index[i] = 0;               
        }

        #ifndef NDEBUG
          std::cout << "[RaftEngine] Node " << current_leader_id 
                    << " WON the election for Term " << current_term 
                    << " with " << votes_received << " votes!\n";
        #endif
      }
    }
  } // Relesse exclusive lock

  if (became_leader) {
    // Instantly broadcast empty AppendEntries to the cluster.
    send_heartbeat(); 
  }
}

void RaftEngine::handle_append_entries(uint32_t sender_id, const uint8_t* payload, size_t size) {
  if (size < sizeof(RaftAppendEntriesHeader)) [[unlikely]] {
    return;
  }

  const auto *header = reinterpret_cast<const RaftAppendEntriesHeader *>(payload);

  // Exclusive lock to modify WAL
  std::unique_lock<std::shared_mutex> lock(raft_mutex);

  // Initialize the reply
  RaftAppendReplyHeader reply{};
  reply.term = current_term;
  reply.success = 0;
  reply.match_index = 0;

  // Term check
  if (header->term < current_term) {
    // A deposed leader partitioned from the network is trying to send us data.
    // We reject it immediately so it realizes it is no longer the leader.
    dispatch_append_reply(sender_id, reply);
    return;
  }

  // If the leader's term is newer or equal, acknowledge them as the valid leader
  current_term = header->term;
  role = ConsensusRole::FOLLOWER;
  current_leader_id = header->leader_id;
  last_heartbeat_received = std::chrono::steady_clock::now();

  // Update our reply term to match the new current term
  reply.term = current_term;

  // Log matching check
  // Case A: Follower log is behined leader log (contains gap >= 1)
  if (header->prev_log_index >= log.size()) {
    dispatch_append_reply(sender_id, reply);
    return;
  }

  // Case B: Index matches but the term associated with the index does not match the leader
  if (log[header->prev_log_index].term != header->prev_log_term) {
    // Follower has corrupted/uncommitted logs from an old split vote election.
    // Delete this index and all indexes that follow it.

    truncate_log(header->prev_log_index);

    // Reject the packet so the Leader decrements next_index and probes backward
    dispatch_append_reply(sender_id, reply);
    return;
  }

  // Success
  // Read after the append entries header
  const uint8_t* read_ptr = payload + sizeof(RaftAppendEntriesHeader);
  size_t bytes_remaining = size - sizeof(RaftAppendEntriesHeader);

  for (uint16_t i = 0; i < header->batch_size; ++i) {
    if (bytes_remaining < sizeof(RaftSerializedLogHeader)) [[unlikely]]
      break;

    const auto *s_header = reinterpret_cast<const RaftSerializedLogHeader *>(read_ptr);
    read_ptr += sizeof(RaftSerializedLogHeader);
    bytes_remaining -= sizeof(RaftSerializedLogHeader);

    if (bytes_remaining < s_header->payload_size) [[unlikely]]
      break;

    uint64_t target_idx = header->prev_log_index + 1 + i;

    // Conflict resolution
    if (target_idx < log.size()) {
      if (log[target_idx].term == s_header->term) {
        // The already exists in this follower node
        read_ptr += s_header->payload_size;
        bytes_remaining -= s_header->payload_size;
        continue;
      } else {
        // An uncommitted log exists here with a different term
        // Truncate the local log from this point forward to match the current leader
        truncate_log(log[target_idx].data_offset);
      }
    }

    // Append the new log
    RaftLogEntry new_entry{};
    new_entry.term = s_header->term;
    new_entry.data_offset = log_mempool.size();
    new_entry.data_size = s_header->payload_size;

    if (s_header->payload_size > 0) {
      log_mempool.insert(log_mempool.end(), read_ptr, read_ptr + s_header->payload_size);
    }

    log.push_back(new_entry);

    // Advance the pointer for next iteration
    read_ptr += s_header->payload_size;
    bytes_remaining -= s_header->payload_size;
  }

  // Advance commit index
  uint64_t index_of_last_new_entry = log.size() - 1;

  if (header->leader_commit > commit_index) {
    commit_index = std::min(header->leader_commit, index_of_last_new_entry);
    apply_cv.notify_one();
  }

  // Dispatch the success reply
  reply.success = 1;
  reply.match_index = index_of_last_new_entry;
  dispatch_append_reply(sender_id, reply);
}

void RaftEngine::dispatch_append_reply(uint32_t target_id, const RaftAppendReplyHeader& reply) {
  transport->send_message(
    target_id, ProtocolType::Raft, static_cast<uint8_t>(RaftCommand::AppendReply), 
    reinterpret_cast<const uint8_t*>(&reply), sizeof(RaftAppendReplyHeader)
  );
}

void RaftEngine::handle_append_reply(uint32_t sender_id, const uint8_t* payload, size_t size) {
  if (size < sizeof(RaftAppendReplyHeader)) [[unlikely]] {
    return;
  }

  const auto* reply = reinterpret_cast<const RaftAppendReplyHeader*>(payload);

  bool commit_advanced = false;
  bool needs_retry = false;

  // Acquire exclusive lock
  {
    std::unique_lock<std::shared_mutex> lock(raft_mutex);

    // If leader changed before recieving reply, ignore
    if (role != ConsensusRole::LEADER || reply->term < current_term) {
      return;
    }

    // A network partition healed, and a Follower replied with a newer term.
    if (reply->term > current_term) {
      current_term = reply->term;
      role = ConsensusRole::FOLLOWER; // Cluster moved forward, step down as leader
      voted_for = -1;
      
      // Retry the transaction
      commit_cv.notify_all();
      return;
    }

    // Process rejection
    if (reply->success == 0) {
      // The follower rejected our prev_log_index
      if (next_index[sender_id] > 1) {
        next_index[sender_id]--;
        needs_retry = true;
      }
    } 
    // Process success & calculate quorum
    else {
      // Only update if the reply indicates a forward progression.
      if (reply->match_index > match_index[sender_id]) {
        match_index[sender_id] = reply->match_index;
        next_index[sender_id] = reply->match_index + 1;
        
        // Sort the match index array
        std::vector<uint64_t> matches = match_index;
        
        // The Leader matches its own absolute latest log
        matches[transport->get_self_id()] = log.size() - 1; 

        // Sort descending to find the median (quorum boundary).
        std::sort(matches.begin(), matches.end(), std::greater<uint64_t>());
        uint64_t majority_index = matches[quorum_size - 1];

        // The Leader can only advance the commit index if the log entry 
        // at the majority_index was written in its current term.
        if (majority_index > commit_index && log[majority_index].term == current_term) {
          commit_index = majority_index;
          commit_advanced = true;
        }
      }
    }
  } // Release exclusive lock

  if (commit_advanced) {
    // Wake up waiting client threads in propose_command()
    commit_cv.notify_all();
    
    // Wake up the local state machine applier_loop
    apply_cv.notify_one();
  } else if (needs_retry) {
    // Instantly ping the timer_loop to send a backward-probing heartbeat
    timer_cv.notify_one();
  }
}

void RaftEngine::truncate_log(size_t new_size) {
  if (new_size >= log.size()) {
    return;
  }

  size_t cutoff_offset = log[new_size].data_offset;
  if (cutoff_offset < log_mempool.size()) {
    log_mempool.resize(cutoff_offset);
  }
  log.resize(new_size);
}