#pragma once

#include <cstdint>

constexpr int BATCH_SIZE = 160;

enum class CommandType : uint8_t {
  CMD_PING = 0,
  CMD_GO = 1,
  CMD_PUT = 2,
  CMD_GET = 3,
  CMD_BATCH_PUT = 4,
  CMD_BARRIER = 5,
  CMD_QUIT = 6
};

// Request sent from client to server
struct PutRequest {
  uint32_t key;
  uint32_t value;
  PutRequest() {}
  PutRequest(uint32_t k, uint32_t v) : key(k), value(v) {}
};

// Return value of PUT public API
enum class PutResult : uint8_t {
  Failed = 0,   // Network error or server error
  Inserted = 1, // Key did not exist, new entry created
  Updated = 2   // Key existed, value updated
};

// Response sent to client from server after processing
struct PutResponse {
  PutResult status;
};

// Request sent from client to server
struct GetRequest {
  uint32_t key;
  GetRequest() {}
  GetRequest(uint32_t k) : key(k) {}
};

// Status of the Get operation
enum class GetStatus : uint8_t {
  NetworkError = 0, // RPC failed (Failure)
  Found = 1,        // Key exists, value returned
  NotFound = 2      // Key does not exist (Success)
};

// Response sent to client from server after processing
struct GetResponse {
  GetStatus status;
  uint32_t value; // Only valid if status == Found

  static GetResponse success(uint32_t v) { return {GetStatus::Found, v}; }
  static GetResponse not_found()    { return {GetStatus::NotFound, 0}; }
  static GetResponse error()        { return {GetStatus::NetworkError, 0}; }
};
