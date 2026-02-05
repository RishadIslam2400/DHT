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
#include <netinet/tcp.h>
#include <unistd.h>
#include <cstring>
#include <arpa/inet.h>
#include <mutex>
#include <shared_mutex>
#include <memory>
#include <iomanip>

#include "concurrent_hash_table/striped_lock_concurrent_hash_table.h"
#include "common/MurmurHash3.h"
#include "common/load_config.h"
#include "common/threadpool.h"

/**
 * This is a Static Consistent Hashing architecture.
 * It combines the topology of Consistent Hashing (Ring) with the initialization phase
 * of a Static Cluster (Config File).
 */

struct NodeStats {
    std::atomic<uint32_t> local_puts_success{0};
    std::atomic<uint32_t> local_puts_failed{0};
    std::atomic<uint32_t> local_gets_success{0};
    std::atomic<uint32_t> local_gets_failed{0};

    std::atomic<uint32_t> remote_puts{0};
    std::atomic<uint32_t> remote_gets{0};
};

enum CommandType : uint8_t {
    CMD_PING = 0,
    CMD_GO = 1,
    CMD_PUT = 2,
    CMD_GET = 3
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


class ConsistentHashingDHTNode {
private:
    StripedLockConcurrentHashTable<int, int> storage;

    std::vector<NodeConfig> physical_nodes;
    std::map<uint64_t, NodeConfig> ring; // virtual nodes
    NodeConfig self_config;
    int num_virtual_nodes;

    int server_fd;
    std::atomic<bool> running;
    std::atomic<bool> benchmark_ready;
    std::thread listener_thread;

    // std::unique_ptr<ThreadPool> thread_pool;
    NodeStats stats;

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

    bool put_local(int key, int value) {
        bool success = storage.put(key, value);
        if (success) {
            stats.local_puts_success++; // new key inserted into storage
        } else {
            stats.local_puts_failed++; // key updated in storage
        }

        return success;
    }

    std::optional<int> get_local(int key) {
        auto res = storage.get(key);
        if (res.has_value()) {
            stats.local_gets_success++; // Key found
        } else {
            stats.local_gets_failed++; // Null
        }

        return res;
    }

    ResponseMsg handle_request(RequestMsg& request) {
        request.key = ntohl(request.key);
        request.value = ntohl(request.value);

        switch (request.cmd) {
            case CMD_PUT: {
                bool success = put_local(request.key, request.value);
                
                // status = 1 (RPC Success)
                // value  = 1 (Insert) or 0 (Update)
                return ResponseMsg{1, static_cast<int32_t>(htonl(success))};
            }
            case CMD_GET: {
                std::optional<int> res = get_local(request.key);
                if (res.has_value()) {
                    // status = 1 (Found), value = data
                    return ResponseMsg{1, static_cast<int32_t>(htonl(res.value()))};
                } else {
                    // status = 0 (Not Found / Error)
                    return ResponseMsg{0, 0};
                }
            }
            case CMD_PING:
            case CMD_GO:
                if (request.cmd == CMD_GO)
                    benchmark_ready = true;
                return ResponseMsg{1, 0};
        }

        return ResponseMsg{0, 0};
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

    void handle_client(int client_socket) {
        uint8_t buffer[9];

        while (recv_n_bytes(client_socket, buffer, 9)) {
            RequestMsg request;

            // Deserialize
            request.cmd = static_cast<CommandType>(buffer[0]);
            std::memcpy(&request.key, &buffer[1], 4);
            std::memcpy(&request.value, &buffer[5], 4);

            ResponseMsg response = handle_request(request);

            // Serialize response
            uint8_t response_buffer[5];
            response_buffer[0] = response.status;
            std::memcpy(&response_buffer[1], &response.value, 4);

            if (send(client_socket, response_buffer, 5, MSG_NOSIGNAL) != 5) {
                break;
            }
        }

        close(client_socket);
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
        if (setsockopt(server_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0) {
            log_error("Disabling Nagle's Algorithm failed: ", errno);
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

            if (setsockopt(new_socket, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0) {
                log_error("Disabling Nagle's Algorithm failed: ", errno);
                return;
            }

            std::thread([this, new_socket]() {
                this->handle_client(new_socket);
            }).detach();

            /* try {
                thread_pool->enqueue([this, new_socket] {
                    this->handle_client(new_socket);
                });
            } catch (const std::exception& e) {
                std::cerr << "ThreadPool enqueue failed: " << e.what() << "\n";
                close(new_socket);
            } */
        }
    }

public:
    ConsistentHashingDHTNode(std::string& filename, int my_id, int virtual_nodes = 100)
        : num_virtual_nodes(virtual_nodes), server_fd(-1), running(false), benchmark_ready(false)
    {
        physical_nodes = load_config(filename);

        for (const NodeConfig& node : physical_nodes) {
            if (node.id == my_id) {
                self_config = node;
            }

            // create virtual nodes for this physical node
            for (int i = 0; i < num_virtual_nodes; ++i) {
                std::string vnode_key = node.ip + ":" + std::to_string(node.port) + "-" + std::to_string(i);
                uint64_t vnode_hash = hash_key(vnode_key);
                ring[vnode_hash] = node;
            }
        }

        // Initialize threadpool
        /* unsigned int threads = std::thread::hardware_concurrency();
        if (threads == 0)
            threads = 4;
        thread_pool = std::make_unique<ThreadPool>(threads); */

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
        // thread_pool.reset();
    }

    void wait_for_barrier() {
        if (self_config.id == 0) {
            std::cout << "[Coordinator] Checking if peers are ready...\n";

            while (running) {
                int ready_count = 0;
                for (const NodeConfig& node : physical_nodes) {
                    if (node.id == 0)
                        continue;

                    RequestMsg ping_msg{CMD_PING, 0, 0};
                    ResponseMsg res = send_request(node.ip, ping_msg, node.port);
                    
                    if (res.status == 1)
                        ready_count++;
                }

                if (ready_count == static_cast<int>(physical_nodes.size()) - 1) {
                    std::cout << "[Coordinator] All peers online. Sending GO signal!\n";
                    break;
                }
                
                std::cout << "[Coordinator] Waiting for peers (" << ready_count << "/" << physical_nodes.size()-1 << ")...\n";
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }

            for (const auto& node : physical_nodes) {
                if (node.id == 0)
                    continue;

                RequestMsg go_msg{CMD_GO, 0, 0};
                send_request(node.ip, go_msg, node.port);
            }
            
            benchmark_ready = true;
        } else {
            std::cout << "[Worker] Waiting for Coordinator (Node 0)...\n";
            
            while (running && !benchmark_ready) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }

            std::cout << "[Worker] GO signal received! Starting...\n";
        }
    }

    bool put(int key, int value) {
        uint64_t key_hash = hash_key(key);
        NodeConfig owner = find_successor(key_hash);
        
        if (owner.id == self_config.id) {
            return put_local(key, value);
        } else {
            stats.remote_puts++;

            RequestMsg request;
            request.cmd = CMD_PUT;
            request.key = htonl(key);
            request.value = htonl(value);
            ResponseMsg response = send_request(owner.ip, request, owner.port);
            
            if (response.status == 0) {
                // Network error
                return false;
            }

            return ntohl(response.value) != 0;
        }
    }

    std::optional<int> get(int key) {
        uint64_t key_hash = hash_key(key);
        NodeConfig owner = find_successor(key_hash);

        if (owner == self_config) {
            return get_local(key);
        } else {
            stats.remote_gets++;
            RequestMsg request;
            request.cmd = CMD_GET;
            request.key = htonl(key);
            request.value = 0; // padding
            
            ResponseMsg response = send_request(owner.ip, request, owner.port);

            if (response.status == 1) {
                return ntohl(response.value);
            }

            return std::nullopt;
        }
    }

    static ResponseMsg send_request(const std::string& ip, const RequestMsg& request, int target_port = 1895) {
        sockaddr_in node_addr;
        memset(&node_addr, 0, sizeof(node_addr));
        node_addr.sin_family = AF_INET;
        node_addr.sin_port = htons(target_port);
        if (inet_pton(AF_INET, ip.c_str(), &node_addr.sin_addr) <= 0) {
            return ResponseMsg{0, 0};
        }

        // create socket
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            return ResponseMsg{0, 0};
        }

        struct timeval timeout;
        timeout.tv_sec = 2;
        timeout.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

        int opt = 1;
        if (setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0) {
            close(sock);
            ResponseMsg{0, 0};
        }

        // connect socket
        if (connect(sock, (struct sockaddr *)&node_addr, sizeof(node_addr)) < 0) {
            close(sock);
            return ResponseMsg{0, 0};
        }

        uint8_t buffer[9];
        buffer[0] = request.cmd;
        std::memcpy(&buffer[1], &request.key, 4);
        std::memcpy(&buffer[5], &request.value, 4);

        if (send(sock, buffer, 9, MSG_NOSIGNAL) != 9) {
            close(sock);
            return ResponseMsg{0, 0};
        }

        uint8_t response_buffer[5];
        if (!recv_n_bytes(sock, response_buffer, 5)) {
            close(sock);
            return ResponseMsg{0, 0};
        }

        ResponseMsg response;
        response.status = response_buffer[0];
        std::memcpy(&response.value, &response_buffer[1], 4);

        close(sock);
        return response;
    }

    void print_status() {
        // storage.print_table();
        
        uint32_t total_local_puts = stats.local_puts_success + stats.local_puts_failed;
        uint32_t total_local_gets = stats.local_gets_success + stats.local_gets_failed;

        std::cout << "\nNode " << self_config.id << " Statistics\n";
        std::cout << "LOCAL STORAGE METRICS\n";
        std::cout << "Local PUTs (Success/Insert): " << stats.local_puts_success << "\n";
        std::cout << "Local PUTs (Fail/Update):    " << stats.local_puts_failed << "\n";
        std::cout << "Total Local PUTs:            " << total_local_puts << "\n";
        std::cout << "\n";
        std::cout << "Local GETs (Success/Found):  " << stats.local_gets_success << "\n";
        std::cout << "Local GETs (Fail/NotFound):  " << stats.local_gets_failed << "\n";
        std::cout << "Total Local GETs:            " << total_local_gets << "\n";
        
        std::cout << "REMOTE TRAFFIC METRICS\n";
        std::cout << "Remote PUTs Sent:            " << stats.remote_puts << "\n";
        std::cout << "Remote GETs Sent:            " << stats.remote_gets << "\n";
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