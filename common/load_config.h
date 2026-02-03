#pragma once

#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <sstream>

struct NodeConfig {
    int id;
    std::string ip;
    int port;
    
    bool operator<(const NodeConfig& other) const { return id < other.id; }
    bool operator==(const NodeConfig& other) const { return ip == other.ip; }
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