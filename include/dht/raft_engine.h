#pragma once

#include <vector>
#include <shared_mutex>
#include <mutex>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <chrono>

#include "consensus_interface.h"
#include "common/dht_common.h"
#include "common/spin_lock.h"

struct RaftLogEntry {
  uint64_t term;
  size_t data_offset;
  size_t data_size;
};

// Forward declaration
struct NodeStats;

class RaftEngine : public IConsensusEngine {
private:
  INetworkTransport *transport;
  IStateMachine *state_machine;
  NodeStats* stats;

  // Cached Topology
  std::vector<int> peer_ids;
  int total_nodes{0};
  int quorum_size{0};

  // Global State Lock
  mutable std::shared_mutex raft_mutex;

  // Memory pool for log
  std::vector<RaftLogEntry> log;
  std::vector<uint8_t> log_mempool;

  // ====================================================================
  // RAFT STATE
  // ====================================================================

  // Persistent state on all servers
  uint64_t current_term{0};
  int voted_for{-1};

  // Volatile state on all servers
  ConsensusRole role{ConsensusRole::FOLLOWER};
  int current_leader_id{-1};
  uint64_t commit_index{0};
  uint64_t last_applied{0};

  // Volatile state on all leaders
  std::vector<uint64_t> next_index;
  std::vector<uint64_t> match_index;

  // Volatile state on Candidates
  int votes_received{0};
  std::vector<bool> peers_voted;

  // ====================================================================
  // CONCURRENCY
  // ====================================================================
  std::atomic<bool> running{false};

  // Timer Thread (Leader Election & Heartbeats)
  std::thread timer_thread;
  std::condition_variable timer_cv;
  std::mutex timer_mutex; // Required for timer_cv
  std::chrono::steady_clock::time_point last_heartbeat_received;

  // Applier Thread (Pushes committed logs to the Hash Table)
  std::thread applier_thread;
  std::condition_variable apply_cv;
  std::mutex apply_mutex;

  // Commit Awaiter (Blocks propose_command until quorum is reached)
  std::condition_variable_any commit_cv;

  // Configuration
  int election_timeout_min_ms{150};
  int election_timeout_max_ms{300};
  int heartbeat_interval_ms{50};
  
  // ====================================================================
  // INTERNAL HANDLERS
  // ====================================================================
  void timer_loop();
  void applier_loop();
  void start_election();
  void send_heartbeat();

  void handle_request_vote(uint32_t sender_id, const uint8_t *payload, size_t size);
  void handle_vote_reply(uint32_t sender_id, const uint8_t* payload, size_t size);
  void handle_append_entries(uint32_t sender_id, const uint8_t* payload, size_t size);
  void handle_append_reply(uint32_t sender_id, const uint8_t* payload, size_t size);

  // Resolve conflicting entries
  void truncate_log(size_t new_size);

  void dispatch_append_reply(uint32_t target_id, const RaftAppendReplyHeader &reply);
  void dispatch_vote_reply(uint32_t target_id, const RaftVoteReplyHeader& reply);

public:
  explicit RaftEngine(INetworkTransport* nt, IStateMachine* sm, NodeStats* node_stats);
  ~RaftEngine() override;

  // IConsensusEngine interface
  void start() override;
  void stop() override;

  // Blocks calling thread until quorum replication or leadership loss
  bool propose_command(const uint8_t* data, size_t data_len) override;
  
  void on_network_message(uint32_t sender_id, uint8_t command_type, 
                          const uint8_t* payload, uint16_t payload_size) override;

  ConsensusRole get_role() const override;
  int get_leader_id() const override;
  uint64_t get_commit_index() const override;
};