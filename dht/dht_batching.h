#pragma once

// We want each thread to have their own buffer. Instead of having a single buffer
// for all the threads. For a single buffer we have to synchronize access to the buffer
// from different threads. The constructor for the batcher will take the DHT class as a
// parameter. So all the threads will operate on the same DHT instead of a separate DHT
// object for each thread. We batch the put request but not the get as we need the get
// result instantly. For get operation latency matters.

#include "dht_static_partitioning.h"
#include "dht_consistent_hashing.h"

class StaticPartitioningBatching {
private:
    StaticClusterDHTNode &dht_node;
    size_t batch_limit;
    
    // Caching information for each node including requests
    struct NodeBatch {
        std::string ip;
        int port;
        int id;

        std::vector<RequestMsg> requests;

        NodeBatch() : port(-1), id(-1) { requests.reserve(128); }
    };
    std::unordered_map<int, NodeBatch> buffers;

    // Check if the key is present in the buffer
    bool is_key_pending(const std::vector<RequestMsg>& batch, int key) {
        int32_t net_key = htonl(key);

        for (const RequestMsg& req : batch) {
            if (req.key == net_key) {
                return true;
            }
        }
        return false;
    }

    // Flush the buffer for a node
    void flush_node(NodeBatch& node_batch) {
        if (node_batch.requests.empty())
            return;

        dht_node.stats.remote_puts_success += node_batch.requests.size();

        bool success = dht_node.send_batch(node_batch.id, node_batch.ip, node_batch.port, node_batch.requests);

        // In future implement a backoff strategy to retry the failed batch
        if (!success) {
            dht_node.stats.remote_puts_failed += node_batch.requests.size();
        }
        
        // clear the buffer, drop failed requests
        node_batch.requests.clear();
    }

public:
    StaticPartitioningBatching(StaticClusterDHTNode &node, size_t limit = 128)
        : dht_node(node), batch_limit(limit) { }
    
    ~StaticPartitioningBatching() {
        flush_all();
    }

    // This PUT API is for batched insert
    void put(int key, int value) {
        NodeConfig target = dht_node.get_target_node(key);

        // if local no need to batch the reqeuest
        if (target.id == dht_node.self_config.id) {
            dht_node.put_local(key, value);
            return;
        }

        // lazy initialization
        NodeBatch &node_batch = buffers[target.id];
        if (node_batch.id == -1) {
            node_batch.id = target.id;
            node_batch.ip = target.ip;
            node_batch.port = target.port;
        }

        // prepare request
        RequestMsg request;
        request.cmd = CommandType::CMD_PUT;
        request.key = htonl(key);
        request.value = htonl(value);

        // put it into the buffer for target node
        node_batch.requests.push_back(request);

        // flush the node if the buffer reaches limit
        if (node_batch.requests.size() >= batch_limit) {
            flush_node(node_batch);
        }
    }

    // Get requests are not batched, but put in here for consistency
    std::optional<int> get(int key) {
        NodeConfig target = dht_node.get_target_node(key);

        if (target.id == dht_node.self_config.id) {
            return dht_node.get_local(key);
        }

        // Remote Read: enforce "Read-Your-Writes" consistency
        auto it = buffers.find(target.id); // check if the buffer for the node exists, does not create a new entry
        if (it != buffers.end()) {
            NodeBatch &node_batch = it->second;
            
            // Check if the key exists in the buffer, if yes flush the buffer
            if (!node_batch.requests.empty()) {
                if (is_key_pending(node_batch.requests, key)) {
                    flush_node(node_batch);
                }
            }
        }

        // Perform the standard GET
        return dht_node.get(key);
    }

    // flush buffers for all the nodes
    void flush_all() {
        for (auto& entry : buffers) {
            flush_node(entry.second);
        }
    }
};

class ConsistentHashingBatching {
private:
    ConsistentHashingDHTNode &dht_node;
    size_t batch_limit;
    
    // Caching information for each node including requests
    struct NodeBatch {
        std::string ip;
        int port;
        int id;

        std::vector<RequestMsg> requests;

        NodeBatch() : port(-1), id(-1) { requests.reserve(128); }
    };
    std::unordered_map<int, NodeBatch> buffers;

    // Check if the key is present in the buffer
    bool is_key_pending(const std::vector<RequestMsg>& batch, int key) {
        int32_t net_key = htonl(key);

        for (const RequestMsg& req : batch) {
            if (req.key == net_key) {
                return true;
            }
        }
        return false;
    }

    // Flush the buffer for a node
    void flush_node(NodeBatch& node_batch) {
        if (node_batch.requests.empty())
            return;

        dht_node.stats.remote_puts_success += node_batch.requests.size();

        bool success = dht_node.send_batch(node_batch.id, node_batch.ip, node_batch.port, node_batch.requests);

        // In future implement a backoff strategy to retry the failed batch
        if (!success) {
            dht_node.stats.remote_puts_failed += node_batch.requests.size();
        }
        
        // clear the buffer, drop failed requests
        node_batch.requests.clear();
    }

public:
    ConsistentHashingBatching(ConsistentHashingDHTNode &node, size_t limit = 128)
        : dht_node(node), batch_limit(limit) { }
    
    ~ConsistentHashingBatching() {
        flush_all();
    }

    // This PUT API is for batched insert
    void put(int key, int value) {
        NodeConfig target = dht_node.find_successor(key);

        // if local no need to batch the reqeuest
        if (target.id == dht_node.self_config.id) {
            dht_node.put_local(key, value);
            return;
        }

        // lazy initialization
        NodeBatch &node_batch = buffers[target.id];
        if (node_batch.id == -1) {
            node_batch.id = target.id;
            node_batch.ip = target.ip;
            node_batch.port = target.port;
        }

        // prepare request
        RequestMsg request;
        request.cmd = CommandType::CMD_PUT;
        request.key = htonl(key);
        request.value = htonl(value);

        // put it into the buffer for target node
        node_batch.requests.push_back(request);

        // flush the node if the buffer reaches limit
        if (node_batch.requests.size() >= batch_limit) {
            flush_node(node_batch);
        }
    }

    // Get requests are not batched, but put in here for consistency
    std::optional<int> get(int key) {
        NodeConfig target = dht_node.find_successor(key);

        if (target.id == dht_node.self_config.id) {
            return dht_node.get_local(key);
        }

        // Remote Read: enforce "Read-Your-Writes" consistency
        auto it = buffers.find(target.id); // check if the buffer for the node exists, does not create a new entry
        if (it != buffers.end()) {
            NodeBatch &node_batch = it->second;
            
            // Check if the key exists in the buffer, if yes flush the buffer
            if (!node_batch.requests.empty()) {
                if (is_key_pending(node_batch.requests, key)) {
                    flush_node(node_batch);
                }
            }
        }

        // Perform the standard GET
        return dht_node.get(key);
    }

    // flush buffers for all the nodes
    void flush_all() {
        for (auto& entry : buffers) {
            flush_node(entry.second);
        }
    }
};