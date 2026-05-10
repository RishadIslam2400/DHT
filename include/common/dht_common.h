#pragma once

#include <cstdint>

constexpr int BATCH_SIZE = 64;

enum class ProtocolType : uint8_t {
  ClientDht = 0,
  TwoPhaseCommit = 1,
  Paxos = 2,
  Raft = 3
};

enum class DhtCommand : uint8_t {
  Ping = 0,         // Cluster health check
  Go = 1,           // Benchmark synchronization barrier
  Put = 2,          // Single strictly consistent put
  Get = 3,          // Single read operation
  BatchPut = 4,     // Array of async puts
  Barrier = 5,      // Coordinator sync point
  Quit = 6,         // Shutdown signal
  ExitBarrier = 7   // Exit sync
};

enum class TwoPhaseCommitCommand : uint8_t {
  Prepare = 0,      // Phase 1: Lock keys and validate
  Commit = 1,       // Phase 2: Apply writes to storage
  Abort = 2         // Phase 2: Discard staging area writes
};

enum class PaxosCommand : uint8_t {
  Prepare = 0,      // Leader Election: Propose new ballot
  Promise = 1,      // Follower replies to Prepare
  Accept = 2,       // Leader asks to append entry
  Accepted = 3      // Follower confirms log append
};

enum class RaftCommand : uint8_t {
  RequestVote = 0,  // Leader Election phase
  VoteReply = 1,    // Vote result
  AppendEntries = 2,// Log replication & heartbeat
  AppendReply = 3   // Acknowledgment of append
};

struct NetworkEnvelope {
  ProtocolType protocol_type;
  uint8_t command_type;
  uint16_t payload_size;
  uint32_t sender_id;
};

enum class ConsensusRole : uint8_t {
  FOLLOWER = 0,
  CANDIDATE = 1,
  LEADER = 2
};

struct PutRequest {
  uint64_t timestamp;
  uint32_t key; 
  uint32_t value;

  PutRequest() : timestamp(0), key(0), value(0) {}
  PutRequest(uint32_t k, uint32_t v, uint64_t ts) : timestamp(ts), key(k), value(v) {}
};

// Return outcomes for any PUT operation.
enum class PutResult : uint8_t {
  Failed = 0,           // Network failure, timeout, or blocked by an active 2PC logical lock
  Inserted = 1,         // Key was new, successfully written
  Updated = 2,          // Key existed, successfully overwritten (incoming TS > current TS)
  Dropped = 3           // Network success, but safely ignored (incoming TS <= current TS)
};

// Wrapper for returning the PutResult over the TCP socket.
struct PutResponse {
  PutResult status;
};

struct GetRequest {
  uint64_t timestamp;
  uint32_t key;
  uint32_t _padding;

  GetRequest() : timestamp(0), key(0), _padding(0) {}
  GetRequest(uint32_t k, uint64_t ts) : timestamp(ts), key(k), _padding(0) {}
};

// Return outcomes for GET operations.
enum class GetStatus : uint8_t {
  NetworkError = 0, // RPC failed or timed out
  Found = 1,        // Key exists, payload contains the valid value
  NotFound = 2      // Key does not exist in the hash table
};

// Wrapper for GET responses. 
struct GetResponse {
  uint32_t value;      // Only valid if status == Found
  GetStatus status;
  uint8_t _padding[3];

  static GetResponse success(uint32_t v) { return {v, GetStatus::Found, {0}}; }
  static GetResponse not_found()    { return {0, GetStatus::NotFound, {0}}; }
  static GetResponse error()        { return {0, GetStatus::NetworkError, {0}}; }
};

// Header sent by the Coordinator during 2PC PREPARE Phase
struct TxPrepareHeader {
  uint64_t tx_timestamp;
  uint16_t batch_size;   // number of PUT requests
  uint8_t  _padding[6];  // 6 bytes explicit padding
};

// Header sent by Coordinator during 2PC COMMIT Phase
// Used to identify which transaction in the server's staging_area to commit or abort.
struct TxCommandHeader {
  uint64_t tx_timestamp;
};

// Header sent during CMD_PAXOS_PREPARE
struct PaxosPrepareHeader {
  uint64_t ballot_number;
  uint64_t next_slot_index; // Tells followers where the new leader wants to start
};

// Header sent during CMD_PAXOS_ACCEPT
struct PaxosAcceptHeader {
  uint64_t slot_index;
  uint64_t ballot_number;
  uint16_t batch_size;    // How many PutRequests are inside the LogEntry payload
  uint8_t  _padding[6];   // 6 bytes padding
};

// Acknowledgment sent back to the leader (CMD_PAXOS_ACCEPTED)
struct PaxosAcceptedHeader {
  uint64_t slot_index;
  uint64_t ballot_number;
  uint32_t ack_node_id;
  uint32_t _padding;      // 4 bytes padding
};

// Header sent during RaftCommand::RequestVote
struct RaftRequestVoteHeader {
  uint64_t term;
  uint64_t last_log_index;
  uint64_t last_log_term;
  uint32_t candidate_id;
  uint32_t _padding;
};

// Reply to a RequestVote
struct RaftVoteReplyHeader {
  uint64_t term;
  uint8_t  vote_granted; // 1 for true, 0 for false
  uint8_t  _padding[7];
};

// Header sent during RaftCommand::AppendEntries
struct RaftAppendEntriesHeader {
  uint64_t term;
  uint64_t prev_log_index;
  uint64_t prev_log_term;
  uint64_t leader_commit;
  uint32_t leader_id;
  uint16_t batch_size;
  uint16_t _padding;
};

// Reply to an AppendEntries
struct RaftAppendReplyHeader {
  uint64_t term;
  uint64_t match_index; // If successful, the index the follower appended
  uint8_t  success;     // 1 for true, 0 for false
  uint8_t  _padding[7];
};