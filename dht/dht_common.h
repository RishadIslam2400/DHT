#pragma once

#include <cstdint>

constexpr int BATCH_SIZE = 128;

enum CommandType : uint8_t {
    CMD_PING = 0,
    CMD_GO = 1,
    CMD_PUT = 2,
    CMD_GET = 3,
    CMD_BATCH = 4
};

struct RequestMsg {
    CommandType cmd;
    int32_t key;
    int32_t value;
};

struct ResponseMsg {
    uint8_t status;
    int32_t value;
};