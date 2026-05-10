#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>

#include "common/dht_common.h"

// Define an interface for the DHT to receive committed logs
class IStateMachine {
public:
    virtual ~IStateMachine() = default;
    
    // The Consensus engine calls this when a log is safely replicated
    virtual void apply_committed_log(uint64_t commit_index, const uint8_t* data, size_t data_len) = 0;
};

class IConsensusEngine {
public:
    virtual ~IConsensusEngine() = default;

    // Pass a pointer to the DHT's state machine so the engine can call back
    virtual void start(IStateMachine* state_machine) = 0;
    virtual void stop() = 0;

    virtual bool propose_command(const uint8_t* data, size_t data_len) = 0;

    virtual void on_network_message(uint32_t sender_id, uint8_t command_type, 
                                    const uint8_t* payload, uint16_t payload_size) = 0;

    // State accessors
    virtual ConsensusRole get_role() const = 0;
    virtual int get_leader_id() const = 0;
    virtual uint64_t get_commit_index() const = 0;
};

// Interface for the consensus engine to send network packets 
// without knowing how the sockets actually work.
class INetworkTransport {
public:
    virtual ~INetworkTransport() = default;

    virtual void send_message(int target_node_id, ProtocolType proto, uint8_t command_type, 
                              const uint8_t* payload, size_t payload_size) = 0;

    // Topology queries
    virtual std::vector<int> get_peer_ids() const = 0;
    virtual int get_self_id() const = 0;
};