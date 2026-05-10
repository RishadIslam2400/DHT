#pragma once

#include <vector>
#include <shared_mutex>
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

class RaftEngine : public IConsensusEngine {
private:
  INetworkTransport *transport;
  IStateMachine *state_machine;

  mutable std::shared_mutex raft_mutex;

  // Memory pool for log
  std::vector<RaftLogEntry> log;
  std::vector<uint8_t> log_mempool;

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

  // Concurrency
  std::atomic<bool> running{false};

  // Handles Leader Election and Heartbeats
  std::thread timer_thread;
  std::condition_variable timer_cv;
  std::chrono::steady_clock::time_point last_heartbeat_recieved;

  // Background applier thread
  // Wakes up when commit_index > last_applied and executes the Hash Table writes
  std::thread applier_thread;
  std::condition_variable apply_cv;
  std::mutex apply_mutex;

  // Configuration
  int election_timeout_min_ms{150};
  int election_timeout_max_ms{300};
  int heartbeat_interval_ms{50};
  
  // Handlers
  void timer_loop();
  void applier_loop();
  void start_election();
  void send_heartbeat();

  void handle_request_vote(uint32_t sender_id, const uint8_t *payload, size_t size);
  void handle_vote_reply(uint32_t sender_id, const uint8_t* payload, size_t size);
  void handle_append_entries(uint32_t sender_id, const uint8_t* payload, size_t size);
  void handle_append_reply(uint32_t sender_id, const uint8_t* payload, size_t size);

  // Checks if a quorum has been reached to safely advance the commit_index
  void advance_commit_index();

  // Resolve conflicting entries
  void truncate_log(size_t new_size);

public:
  explicit RaftEngine(INetworkTransport *transport_layer);
  ~RaftEngine() override;

  // IConsensusEngine interface
  void start(IStateMachine* state_machine) override;
  void stop() override;

  bool propose_command(const uint8_t* data, size_t data_len) override;
  void on_network_message(uint32_t sender_id, uint8_t command_type, 
                          const uint8_t* payload, uint16_t payload_size) override;

  ConsensusRole get_role() const override;
  int get_leader_id() const override;
  uint64_t get_commit_index() const override;
};