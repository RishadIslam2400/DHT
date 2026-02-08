#pragma once

#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <sstream>
#include <atomic>

struct NodeConfig {
    int id;
    std::string ip;
    int port;
    
    bool operator<(const NodeConfig& other) const { return id < other.id; }
    bool operator==(const NodeConfig& other) const { return id == other.id; }
};

struct NodeStats {
    std::atomic<uint32_t> local_puts_success{0};
    std::atomic<uint32_t> local_puts_failed{0};
    std::atomic<uint32_t> local_gets_success{0};
    std::atomic<uint32_t> local_gets_failed{0};

    std::atomic<uint32_t> remote_puts{0};
    std::atomic<uint32_t> remote_gets{0};
};

// format of the property file:
// id ip_address port
std::vector<NodeConfig> load_config(const std::string& filename) {
    std::vector<NodeConfig> nodes;
    std::ifstream file(filename);
    std::string line;
    
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#') continue;
        std::stringstream ss(line);
        NodeConfig node;
        ss >> node.id >> node.ip >> node.port;
        nodes.push_back(node);
    }
    
    std::sort(nodes.begin(), nodes.end());
    return nodes;
}