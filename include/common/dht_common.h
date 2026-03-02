#pragma once

#include <cstdint>

// Number of async puts to buffer before triggering a network flush.
// Balances network throughput vs. staleness of uncommitted data.
constexpr int BATCH_SIZE = 64;

// Defines the network protocol instructions the server socket listens for.
enum class CommandType : uint8_t {
  CMD_PING = 0,       // Cluster health check
  CMD_GO = 1,         // Benchmark synchronization barrier
  CMD_PUT = 2,        // Single strictly consistent put (put_sync)
  CMD_GET = 3,        // Single read operation
  CMD_BATCH_PUT = 4,  // Array of async puts flushed from a client buffer
  CMD_BARRIER = 5,    // Coordinator sync point
  CMD_QUIT = 6,       // Shutdown signal
  CMD_TX_PREPARE = 7, // 2PC Phase 1: Lock keys and validate LWW timestamps
  CMD_TX_COMMIT = 8,  // 2PC Phase 2: Apply writes to storage and unlock
  CMD_TX_ABORT = 9    // 2PC Phase 2: Discard staging area writes and unlock
};

// Payload for single or batched put operations over the network.
struct alignas(8) PutRequest {
  uint32_t key;
  uint32_t value;
  uint64_t timestamp;

  PutRequest() : key(0), value(0), timestamp(0) {}
  PutRequest(uint32_t k, uint32_t v, uint64_t ts) : key(k), value(v), timestamp(ts) {}
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

// Payload for requesting a value over the network.
struct alignas(8) GetRequest {
  uint32_t key;
  uint64_t timestamp;

  GetRequest() : key(0), timestamp(0) {}
  GetRequest(uint32_t k, uint64_t ts) : key(k), timestamp(ts) {}
};

// Return outcomes for GET operations.
enum class GetStatus : uint8_t {
  NetworkError = 0, // RPC failed or timed out
  Found = 1,        // Key exists, payload contains the valid value
  NotFound = 2      // Key does not exist in the hash table
};

// Wrapper for GET responses. 
struct GetResponse {
  GetStatus status;
  uint32_t value; // Only valid if status == Found

  static GetResponse success(uint32_t v) { return {GetStatus::Found, v}; }
  static GetResponse not_found()    { return {GetStatus::NotFound, 0}; }
  static GetResponse error()        { return {GetStatus::NetworkError, 0}; }
};

// Header sent by the Coordinator during PREPARE Phase.
struct __attribute__((packed)) TxPrepareHeader {
  uint64_t tx_timestamp; // The single Lamport clock value defining the transaction's age
  uint16_t batch_size;   // How many PutRequests immediately follow this header in the stream
};

// Header sent by Coordinator during COMMIT Phase 2.
// Used to identify which transaction in the server's staging_area to commit or abort.
struct TxCommandHeader {
  uint64_t tx_timestamp;
};