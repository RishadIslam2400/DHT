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
#include <mutex>
#include <shared_mutex>
#include <memory>

#include "concurrent_hash_table/striped_lock_concurrent_hash_table.h"
#include "common/MurmurHash3.h"
#include "common/load_config.h"
#include "common/threadpool.h"

/**
 * This is a Static Consistent Hashing architecture.
 * It combines the topology of Consistent Hashing (Ring) with the initialization phase
 * of a Static Cluster (Config File).
 */


class ConsistentHashingDHTNode {
private:
    StripedLockConcurrentHashTable<int, int> storage;

    std::vector<NodeConfig> physical_nodes;
    std::map<uint64_t, NodeConfig> ring; // virtual nodes
    // mutable std::shared_mutex ring_mutex;
    NodeConfig self_config;
    int num_virtual_nodes;

    int server_fd;
    std::atomic<bool> running;
    std::atomic<bool> benchmark_ready;
    std::thread listener_thread;

    std::unique_ptr<ThreadPool> thread_pool;

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

    NodeConfig find_successor(uint64_t key_hash) {
        // std::shared_lock<std::shared_mutex> lock(ring_mutex); // ring is fixed so no need for a lock
        if (ring.empty()) return self_config;

        // Find the first node with hash >= key_hash
        auto it = ring.lower_bound(key_hash);
        if (it == ring.end()) {
            return ring.begin()->second; // Wrap around
        }
        return it->second;
    }

    static bool recv_n_bytes(int sock, void* buffer, size_t n) {
        size_t total_read = 0;
        char *buf_ptr = static_cast<char *>(buffer);
        while (total_read < n) {
            ssize_t received = read(sock, buf_ptr + total_read, n - total_read);
            if (received < 0) {
                if (errno == EINTR)
                    continue;
                return false;
            }
            if (received == 0)
                return false;
            total_read += received;
        }
        return true;
    }

    static bool recv_framed(int sock, std::string& out_msg) {
        uint32_t len_net;
        if(!recv_n_bytes(sock, &len_net, sizeof(len_net)))
            return false;

        uint32_t len_host = ntohl(len_net);
        if (len_host > 10 * 1024 * 1024)
            return false;

        std::vector<char> buf(len_host);
        if (!recv_n_bytes(sock, buf.data(), len_host))
            return false;

        out_msg.assign(buf.data(), len_host);
        return true;
    }

    static bool send_framed(int sock, const std::string& message) {
        uint32_t len_host = static_cast<uint32_t>(message.length());
        uint32_t len_net = htonl(len_host);

        // Send header
        if (send(sock, &len_net, sizeof(len_net), MSG_NOSIGNAL) != sizeof(len_net))
            return false;
        
        // Send body
        size_t total_sent = 0;
        while (total_sent < len_host) {
            ssize_t sent = send(sock, message.data() + total_sent, len_host - total_sent, MSG_NOSIGNAL);
            if (sent < 0) {
                if (errno == EINTR)
                    continue;
                return false;
            }
            total_sent += sent;
        }

        return true;
    }

    std::string handle_request(const std::string& request) {
        std::stringstream ss(request);
        std::string command;
        int key, value;
        ss >> command;

        if (command == "PUT") {
            if (ss >> key >> value)
                return put(key, value);
        } else if (command == "GET") {
            if (ss >> key)
                return get(key);
        } else if (command == "PING") {
            return "READY";
        } else if (command == "GO") {
            benchmark_ready = true;
            return "OK";
        }
        return "ERROR";
    }

    void handle_client(int client_socket) {
        struct SockGuard {
            int s;
            ~SockGuard() {
                close(s);
            }
        };

        SockGuard guard;
        guard.s = client_socket;

        std::string request;
        while(recv_framed(client_socket, request)) {
            std::string response = handle_request(request);
            if (!send_framed(client_socket, response)) {
                break;
            }
        }
    }

    void listen_loop() {
        sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
        server_addr.sin_port = htons(self_config.port);

        // create socket
        server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd < 0) {
            log_error("Error making server socket: ", errno);
            return;
        }

        // set options
        int opt = 1;
        if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
            log_error("setsockopt(SO_REUSEADDR) failed: ", errno);
            return;
        }

        // bind socket
        if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            log_error("Error binding socket to local address: ", errno);
            return;
        }

        // listen
        if (listen(server_fd, 128) < 0) {
            log_error("Error listening on socket: ", errno);
            return;
        }

        std::cout << "Node listening on port " << self_config.port << "..." << std::endl;

        while (running) {
            sockaddr_in client_addr;
            memset(&client_addr, 0, sizeof(client_addr));
            socklen_t client_addr_len = sizeof(client_addr);

            int new_socket = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
            if (new_socket < 0) {
                if (!running) break;
                continue;
            }

            /* std::thread([this, new_socket]() {
                this->handle_client(new_socket);
            }).detach(); */

            try {
                thread_pool->enqueue([this, new_socket] {
                    this->handle_client(new_socket);
                });
            } catch (const std::exception& e) {
                std::cerr << "ThreadPool enqueue failed: " << e.what() << "\n";
                close(new_socket);
            }
        }
    }

public:
    ConsistentHashingDHTNode(std::string& filename, int my_id, int virtual_nodes = 100)
        : num_virtual_nodes(virtual_nodes), server_fd(-1), running(false), benchmark_ready(false)
    {
        physical_nodes = load_config(filename);

        bool found = false;
        for (const NodeConfig& node : physical_nodes) {
            if (node.id == my_id) {
                self_config = node;
                found = true;
            }

            // create virtual nodes for this physical node
            for (int i = 0; i < num_virtual_nodes; ++i) {
                std::string vnode_key = node.ip + ":" + std::to_string(node.port) + "-" + std::to_string(i);
                uint64_t vnode_hash = hash_key(vnode_key);
                ring[vnode_hash] = node;
            }
        }

        if (!found) {
            throw std::runtime_error("My ID not found in config file!");
        }

        // Initialize threadpool
        unsigned int threads = std::thread::hardware_concurrency();
        if (threads == 0)
            threads = 4;
        thread_pool = std::make_unique<ThreadPool>(threads);

        std::cout << "Booted Consistent Node " << my_id << " (" << self_config.ip << ")\n";
        std::cout << "Ring Topology: " << physical_nodes.size() << " Physical Nodes, " 
                  << ring.size() << " Virtual Nodes.\n";
    }

    ~ConsistentHashingDHTNode() {
        stop();
    }

    void start() {
        running = true;
        listener_thread = std::thread(&ConsistentHashingDHTNode::listen_loop, this);
    }

    void stop() {
        if (!running)
            return;
        running = false;
        
        if (server_fd > 0) {
            shutdown(server_fd, SHUT_RDWR);
            close(server_fd);
            server_fd = -1;
        }
        if (listener_thread.joinable()) listener_thread.join();
        thread_pool.reset();
    }

    void wait_for_barrier() {
        if (self_config.id == 0) {
            std::cout << "[Coordiator] Checking if ring peers are ready...\n";

            while (running) {
                int ready_count = 0;
                for (const NodeConfig& node : physical_nodes) {
                    if (node.id == 0)
                        continue;

                    std::string res = send_request(node.ip, "PING", node.port);
                    if (res == "READY")
                        ready_count++;
                }

                if (ready_count == physical_nodes.size() - 1) {
                    std::cout << "[Coordinator] All peers online. Sending GO signal!\n";
                    break;
                }

                std::cout << "[Coordinator] Waiting... (" << ready_count << "/" << physical_nodes.size()-1 << ")\n";
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }

            // Broadcast GO
            for (const NodeConfig& node : physical_nodes) {
                if (node.id == 0)
                    continue;
                send_request(node.ip, "GO", node.port);
            }

            benchmark_ready = true;
        } else {
            std::cout << "[Worker] Waiting for Coordinator...\n";
            while (running && !benchmark_ready) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            std::cout << "[Worker] GO received!\n";
        }
    }

    std::string put(int key, int value) {
        uint64_t key_hash = hash_key(key);

        // Find successor will land on a Virtual Node, which maps to a Physical Node
        NodeConfig owner = find_successor(key_hash);

        if (owner == self_config) {
            storage.insert(key, value);
            return "OK";
        } else {
            std::string msg = "PUT " + std::to_string(key) + " " + std::to_string(value);
            return send_request(owner.ip, msg, owner.port);
        }
    }

    std::string get(int key) {
        uint64_t key_hash = hash_key(key);
        NodeConfig owner = find_successor(key_hash);

        if (owner == self_config) {
            auto res = storage.search(key);
            return res.has_value() ? std::to_string(res.value()) : "NOT FOUND";
        } else {
            return send_request(owner.ip, "GET " + std::to_string(key), owner.port);
        }
    }

    static std::string send_request(const std::string& ip, const std::string& message, int target_port = 1895) {
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

        struct timeval timeout;
        timeout.tv_sec = 2; // 2 second timeout
        timeout.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

        // connect socket
        if (connect(sock, (struct sockaddr *)&node_addr, sizeof(node_addr)) < 0) {
            close(sock);
            return "ERROR: Connection failed";
        }

        if (!send_framed(sock, message)) {
            close(sock);
            return "ERROR: Send failed";
        }

        std::string response;
        if (!recv_framed(sock, response)) {
            close(sock);
            return "ERROR: Receive failed";
        }

        close(sock);
        return response;
    }

    void print_status() {
        storage.print_table();
    }

    void print_ring() {
        // std::shared_lock<std::shared_mutex> lock(ring_mutex);
        std::cout << "Ring Topology (" << ring.size() << " Virtual Nodes)\n";
        int count = 0;
        for(auto& kv : ring) {
             if(count++ < 20) // Only print first 20 to avoid spamming console
                std::cout << "Hash " << kv.first << " -> Physical Node " << kv.second.id << "\n";
        }
        if(ring.size() > 20) std::cout << "... (remaining hidden)\n";
        std::cout << "\n";
    }
};