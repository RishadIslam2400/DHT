#include "dht/node_properties.h"
#include <fstream>
#include <sstream>
#include <algorithm>

// format of the property file:
// id ip_address port
inline std::vector<NodeConfig> load_config(const std::string& filename) {
    std::vector<NodeConfig> nodes;
    std::ifstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Could not open config file: " + filename);
    }

    std::string line;
    int line_num = 0;
    while (std::getline(file, line)) {
        line_num++;

        if (line.empty() || line[0] == '#')
            continue;
        
        std::stringstream ss(line);
        NodeConfig node;

        if (!(ss >> node.id >> node.ip >> node.port)) {
            throw std::runtime_error("Malformed config on line " + std::to_string(line_num));
        }
        
        nodes.push_back(std::move(node));
    }
    
    std::sort(nodes.begin(), nodes.end());
    return nodes;
}