#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>

#include "common/dht_common.h"

// Define an interface for the DHT to apply commited log to the hashtable
class IStateMachine {
public:
    virtual ~IStateMachine() = default;
    
    // The Consensus engine calls this only when a log is safely replicated to a quorum
    virtual void apply_committed_log(const std::vector<std::pair<uint64_t, std::vector<uint8_t>>>& committed_batch) = 0;
};

// Define an interfor the DHT to send packets
class INetworkTransport {
public:
    virtual ~INetworkTransport() = default;

    // Must be implemented as a non-blocking operation.
    // Raft relies on fast timeouts; blocking here will destroy cluster stability.
    virtual void send_message(int target_node_id, ProtocolType proto, uint8_t command_type, 
                              const uint8_t* payload, size_t payload_size) = 0;

    // Topology queries
    virtual std::vector<int> get_peer_ids() const = 0;
    virtual int get_self_id() const = 0;
};

class IConsensusEngine {
public:
    virtual ~IConsensusEngine() = default;

    virtual void start() = 0;
    virtual void stop() = 0;

    // Block the calling thread until the log is replicated to a majority
    // Returns true if committed, false if leadership was lost during replication.
    virtual bool propose_command(const uint8_t* data, size_t data_len) = 0;

    // Handle client requests
    virtual void on_network_message(uint32_t sender_id, uint8_t command_type, 
                                    const uint8_t* payload, uint16_t payload_size) = 0;

    // State accessors
    virtual ConsensusRole get_role() const = 0;
    virtual int get_leader_id() const = 0;
    virtual uint64_t get_commit_index() const = 0;
};