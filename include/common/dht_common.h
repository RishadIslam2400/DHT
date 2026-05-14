#pragma once

#include <cstdint>

constexpr int BATCH_SIZE = 64;

enum class ProtocolType : uint8_t {
  ClientDht = 0,
  TwoPhaseCommit = 1,
  Paxos = 2,
  Raft = 3
};

// 0x00 - 0x0F: DHT Commands
enum class DhtCommand : uint8_t {
  Ping = 0x00,        // Cluster health check
  Go = 0x01,          // Benchmark synchronization barrier
  Put = 0x02,         // Single strictly consistent put
  Get = 0x03,         // Single read operation
  Barrier = 0x05,     // Distributed barrier to start benchmark
  Quit = 0x06,        // Shutdown signal
  ExitBarrier = 0x07  // Exit sync
};

// 0x10 - 0x1F: 2PC Commands
enum class TwoPhaseCommitCommand : uint8_t {
  Prepare = 0x10,     // Phase 1: Lock keys and validate
  Commit = 0x11,      // Phase 2: Apply writes to hash table
  Abort = 0x12,       // Phase 2: Discard staging area writes
  StatusCheck = 0x13  // Cohort enquires about the status of coordinator after timeout
};

// 0x20 - 0x2F: Paxos Commands
enum class PaxosCommand : uint8_t {
  Prepare = 0x20,     // Leader Election: Propose new ballot
  Promise = 0x21,     // Follower replies to Prepare
  Accept = 0x22,      // Leader asks to append entry
  Accepted = 0x23     // Follower confirms log append
};

// 0x30 - 0x3F: Raft Commands
enum class RaftCommand : uint8_t {
  RequestVote = 0x30,   // Leader Election phase
  VoteReply = 0x31,     // Vote result
  AppendEntries = 0x32, // Log replication & heartbeat
  AppendReply = 0x33    // Acknowledgment of append
};

enum class RpcResult : uint8_t {
  Success = 0,
  SendFailed = 1,      // The TCP write failed. The peer never saw it. Safe to retry.
  RecvFailed = 2,      // The TCP read failed/timed out. Peer might have executed it. Unsafe to retry.
  InvalidPayload = 3   // Request exceeded size limits.
};

enum class PutResult : uint8_t {
  Failed = 0,          // Network failure, timeout, or blocked by an active 2PC logical lock
  Inserted = 1,        // Key was new, successfully written
  Updated = 2,         // Key existed, successfully overwritten (incoming TS > current TS)
  Dropped = 3          // Network success, but safely ignored (incoming TS <= current TS)
};

enum class GetStatus : uint8_t {
  NetworkError = 0, // RPC failed or timed out
  Found = 1,        // Key exists, payload contains the valid value
  NotFound = 2      // Key does not exist in the hash table
};

enum class ConsensusRole : uint8_t {
  FOLLOWER = 0,
  CANDIDATE = 1,
  LEADER = 2
};

enum class TransactionResult : uint8_t {
  Committed = 0,
  Aborted = 1,
  NotLeader = 2  // Indicates the network layer must forward this!
};

struct LocalValue {
  uint32_t value;
  uint64_t timestamp;
};

struct NetworkEnvelope {
  ProtocolType protocol_type;
  uint8_t command_type;
  uint16_t payload_size;
  uint32_t sender_id;
};

struct PutRequest {
  uint64_t timestamp;
  uint32_t key; 
  uint32_t value;

  PutRequest() : timestamp(0), key(0), value(0) {}
  PutRequest(uint32_t k, uint32_t v, uint64_t ts) : timestamp(ts), key(k), value(v) {}
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

// Wrapper for GET responses. 
struct GetResponse {
  uint64_t timestamp;
  uint32_t value; // Only valid if status == Found
  GetStatus status;
  uint8_t _padding[3] = {0};

  static GetResponse success(uint32_t v, uint64_t ts) { 
      return {ts, v, GetStatus::Found, {0, 0, 0}}; 
  }
  static GetResponse not_found(uint64_t ts) { 
      return {ts, 0, GetStatus::NotFound, {0, 0, 0}}; 
  }
  
  static GetResponse error() { 
      return {0, 0, GetStatus::NetworkError, {0, 0, 0}}; 
  }
};

// Header sent by the Coordinator during 2PC prepare Phase
struct TxPrepareHeader {
  uint64_t tx_timestamp;
  uint16_t batch_size;
  int16_t coordinator_id;
  uint32_t _padding{0};
};

// Response sent by cohorts during 2PC prepare Phase
struct TxPrepareResponse {
  uint64_t remote_clock;
  uint8_t vote;
  uint8_t  _padding[7] = {0};
};

// Header sent by Coordinator during 2PC COMMIT/ABORT Phase
// Used to identify which transaction in the server's staging_area to commit or abort.
struct TxCommitHeader {
  uint64_t tx_timestamp;
};

// Handle coordinator crash during phase 2 of 2PC using Raft
struct CoordinatorCommitIntent {
  uint64_t tx_timestamp;
  uint8_t  decision;         // 1 = COMMIT, 0 = ABORT
  uint8_t  num_shards;
  uint8_t  target_shards[6]; // IDs of the cohorts involved
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
  uint8_t  _padding[6] = {0}; 
};

// Acknowledgment sent back to the leader (CMD_PAXOS_ACCEPTED)
struct PaxosAcceptedHeader {
  uint64_t slot_index;
  uint64_t ballot_number;
  uint32_t ack_node_id;
  uint32_t _padding = 0;      
};

// Header sent during RaftCommand::RequestVote
struct RaftRequestVoteHeader {
  uint64_t term;
  uint64_t last_log_index;
  uint64_t last_log_term;
  uint32_t candidate_id;
  uint32_t _padding = 0;
};

// Reply to a RequestVote
struct RaftVoteReplyHeader {
  uint64_t term;
  uint8_t  vote_granted; // 1 for true, 0 for false
  uint8_t  _padding[7] = {0};
};

// Header sent during RaftCommand::AppendEntries
struct RaftAppendEntriesHeader {
  uint64_t term;
  uint64_t prev_log_index;
  uint64_t prev_log_term;
  uint64_t leader_commit;
  uint32_t leader_id;
  uint16_t batch_size;
  uint16_t _padding = 0;
};

// Reply to an AppendEntries
struct RaftAppendReplyHeader {
  uint64_t term;
  uint64_t match_index; // If successful, the index the follower appended
  uint8_t  success;     // 1 for true, 0 for false
  uint8_t  _padding[7] = {0};
};

// Compile time check of struct sizes
static_assert(sizeof(NetworkEnvelope) == 8, "NetworkEnvelope alignment error");
static_assert(sizeof(PutRequest) == 16, "PutRequest alignment error");
static_assert(sizeof(GetRequest) == 16, "GetRequest alignment error");
static_assert(sizeof(GetResponse) == 16, "GetResponse alignment error");
static_assert(sizeof(TxPrepareHeader) == 16, "TxPrepareHeader alignment error");
static_assert(sizeof(TxPrepareResponse) == 16, "TxPrepareResponse alignment error");
static_assert(sizeof(TxCommitHeader) == 8, "TxCommitHeader alignment error");
static_assert(sizeof(CoordinatorCommitIntent) == 16, "CoordinatorCommitIntent alignment error");
static_assert(sizeof(PaxosPrepareHeader) == 16, "PaxosPrepareHeader alignment error");
static_assert(sizeof(PaxosAcceptHeader) == 24, "PaxosAcceptHeader alignment error");
static_assert(sizeof(PaxosAcceptedHeader) == 24, "PaxosAcceptedHeader alignment error");
static_assert(sizeof(RaftRequestVoteHeader) == 32, "RaftRequestVoteHeader alignment error");
static_assert(sizeof(RaftVoteReplyHeader) == 16, "RaftVoteReplyHeader alignment error");
static_assert(sizeof(RaftAppendEntriesHeader) == 40, "RaftAppendEntriesHeader alignment error");
static_assert(sizeof(RaftAppendReplyHeader) == 24, "RaftAppendReplyHeader alignment error");