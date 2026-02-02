#pragma once

#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <atomic>
#include <sstream>
#include <map>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <arpa/inet.h>

#include "../concurrent_hash_table/striped_lock_concurrent_hash_table.h"
#include "../common/MurmurHash3.h"

/**
 * The network topology has a ring structure. It is a sorted list (or map) of Node IDs.
 * Nodes are hashed onto the ring based on their ID (e.g., IP).
 * When a key comes in, we hash it.
 * We then scan the ring clockwise to find the first node that is >= to the key's hash.
 * That node is the owner.
 */

struct NodeInfo {
    std::string ip;
    bool operator==(const NodeInfo& other) const { return ip == other.ip; }
    bool operator!=(const NodeInfo& other) const { return ip != other.ip; }
};


class ConsistentHashingDHTNode {
private:
    StripedLockConcurrentHashTable<int, int> storage;

    std::map<uint64_t, NodeInfo> ring;
    mutable std::shared_mutex ring_mutex;
    NodeInfo self_info;

    int server_fd;
    int server_port;
    std::atomic<bool> running;
    std::thread listener_thread;
    std::vector<std::thread> client_threads;
    std::mutex thread_mutex;

    void log_error(const char* prefix, int err) {
        char buf[1024];
        std::cerr << "[Error] " << prefix << " " << strerror_r(err, buf, sizeof(buf)) << std::endl;
    }

    uint64_t hash_key(const std::string& key) {
        uint64_t hash_out[2];
        MurmurHash3_x86_128(key.c_str(), key.length(), 2400, hash_out);
        return hash_out[0];
    }

    uint64_t hash_key(int key) {
        return hash_key(std::to_string(key));
    }

    NodeInfo find_successor(uint64_t key_hash) {
        std::shared_lock<std::shared_mutex> lock(ring_mutex);
        if (ring.empty()) return self_info;

        auto it = ring.lower_bound(key_hash);
        if (it == ring.end()) {
            return ring.begin()->second; // Wrap around
        }
        return it->second;
    }

    std::string handle_request(const std::string& request) {
        std::stringstream ss(request);
        std::string command;
        int key, value;
        ss >> command;

        if (command == "SET_LOCAL") {
            if (ss >> key >> value) {
                storage.insert(key, value);
                return "Stored Locally\n";
            }
        } else if (command == "GET_LOCAL") {
            if (ss >> key) {
                auto result = storage.search(key);
                return result.has_value() ? std::to_string(result.value()) + "\n" : "NOT FOUND\n";
            }
        } else if (command == "PUT") {
            if (ss >> key >> value) return put(key, value);
        } else if (command == "GET") {
            if (ss >> key) return get(key);
        } else if (command == "JOIN_REQ") {
            std::string new_ip;
            ss >> new_ip;
            add_node_to_ring(new_ip); // This acquires Write Lock internally
            return "JOIN_ACK\n";
        }
        return "ERROR\n";
    }

    void listen_loop() {
        sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
        server_addr.sin_port = htons(server_port);

        // create socket
        server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd < 0) {
            log_error("Error making server socket: ", errno);
            return;
        }

        // set options
        int opt = 1;
        if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
            close(server_fd);
            log_error("setsockopt(SO_REUSEADDR) failed: ", errno);
            return;
        }

        // bind socket
        if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            close(server_fd);
            log_error("Error binding socket to local address: ", errno);
            return;
        }

        // listen
        if (listen(server_fd, 10) < 0) {
            close(server_fd);
            log_error("Error listening on socket: ", errno);
            return;
        }

        std::cout << "Node listening on port " << server_port << "..." << std::endl;

        while (running) {
            sockaddr_in client_addr;
            memset(&client_addr, 0, sizeof(client_addr));
            socklen_t client_addr_len = sizeof(client_addr);

            int new_socket = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
            if (new_socket < 0) {
                if (!running) break;
                log_error("Error accepting request from client: ", errno);
                continue;
            }

            // handle each client in a separate thread (basic concurrency)
            {
                std::lock_guard<std::mutex> lock(thread_mutex);
                client_threads.emplace_back([this, new_socket]() {
                    char buffer[1024] = {0};
                    int valread = read(new_socket, buffer, 1024);
                    if (valread > 0) {
                        std::string response = handle_request(std::string(buffer));
                        send(new_socket, response.c_str(), response.length(), 0);
                    }
                    close(new_socket);
                });
            }
        }
    }

public:
    ConsistentHashingDHTNode(std::string ip, int p = 1895) : running(false), server_port(p) {
        self_info = {ip};
        add_node_to_ring(ip); 
    }

    ~ConsistentHashingDHTNode() {
        stop();
    }

    void start() {
        running = true;
        listener_thread = std::thread(&ConsistentHashingDHTNode::listen_loop, this);
    }

    void stop() {
        running = false;
        if (server_fd > 0) close(server_fd);
        if (listener_thread.joinable()) listener_thread.join(); 

        std::lock_guard<std::mutex> lock(thread_mutex);
        for (auto& t : client_threads) {
            if (t.joinable()) t.join();
        }
    }

    void add_node_to_ring(const std::string& ip) {
        std::unique_lock<std::shared_mutex> lock(ring_mutex); // Write Lock
        uint64_t hash_val = hash_key(ip);
        ring[hash_val] = {ip};
        std::cout << "[Ring] Added " << ip << " at hash " << hash_val << "\n";
    }

    void send_join_request(const std::string& peer_ip) {
        add_node_to_ring(peer_ip);
        std::string msg = "JOIN_REQ " + self_info.ip;
        send_request(peer_ip, msg);
    }

    std::string put(int key, int value) {
        uint64_t key_hash = hash_key(key);
        NodeInfo owner = find_successor(key_hash);

        if (owner == self_info) {
            storage.insert(key, value);
            return "OK (stored locally)\n";
        } else {
            std::string msg = "SET_LOCAL " + std::to_string(key) + " " + std::to_string(value);
            return send_request(owner.ip, msg);
        }
    }

    std::string get(int key) {
        uint64_t key_hash = hash_key(key);
        NodeInfo owner = find_successor(key_hash);

        if (owner == self_info) {
            auto res = storage.search(key);
            return res.has_value() ? std::to_string(res.value()) + "\n" : "NOT FOUND\n";
        } else {
            return send_request(owner.ip, "GET_LOCAL " + std::to_string(key));
        }
    }

    static std::string send_request(const std::string& ip, const std::string& message, int target_port = 1895) {
        char buffer[1024] = {0};
        sockaddr_in node_addr;
        memset(&node_addr, 0, sizeof(node_addr));
        node_addr.sin_family = AF_INET;
        node_addr.sin_port = htons(target_port);
        if (inet_pton(AF_INET, ip.c_str(), &node_addr.sin_addr) <= 0) {
            return "ERROR: Invalid address";
        }

        // create socket
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            return "ERROR: Error creating socket to send";
        }

        // connect socket
        if (connect(sock, (struct sockaddr *)&node_addr, sizeof(node_addr)) < 0) {
            return "ERROR: Connection failed";
        }

        send(sock, message.c_str(), message.length(), 0);
        read(sock, buffer, 1024);
        close(sock);

        return std::string(buffer);
    }

    void print_status() {
        storage.print_table();
    }

    void print_ring() {
        std::shared_lock<std::shared_mutex> lock(ring_mutex);
        std::cout << "Ring Topology\n";
        for (auto &[hash, node] : ring) {
            std::cout << hash << " -> " << node.ip << (node == self_info ? " (SELF)" : "") << "\n";
        }
        std::cout << "\n";
    }
};