#pragma once

#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <atomic>
#include <map>
#include <optional>
#include <chrono>
#include <mutex>
#include <unordered_map>
#include <queue>
// #include <shared_mutex>
// #include <memory>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <cstring>
#include <arpa/inet.h>

#include "concurrent_hash_table/striped_lock_concurrent_hash_table.h"
#include "common/MurmurHash3.h"
#include "common/node_properties.h"
// #include "common/threadpool.h"

/**
 * This is a Static Consistent Hashing architecture.
 * It combines the topology of Consistent Hashing (Ring) with the initialization phase
 * of a Static Cluster (Config File).
 */

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
    NodeStats stats;

    int server_fd;
    std::atomic<bool> running;
    std::atomic<bool> benchmark_ready;
    std::thread listener_thread;
    // std::unique_ptr<ThreadPool> thread_pool;

    // Map: NodeID -> Queue of open socket file descriptors
    std::mutex connection_pool_mtx;
    std::unordered_map<int, std::queue<int>> connection_pool;
    
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
        // ring is fixed so no need for a lock
        if (ring.empty())
            return self_config;

        // Find the first node with hash >= key_hash
        auto it = ring.lower_bound(key_hash);
        if (it == ring.end()) {
            return ring.begin()->second; // Wrap around
        }
        return it->second;
    }

    // ---------------------------- Client side operation -------------------------------
    int create_new_connection(const std::string& target_ip, int target_port) {
        sockaddr_in node_addr;
        memset(&node_addr, 0, sizeof(node_addr));
        node_addr.sin_family = AF_INET;
        node_addr.sin_port = htons(target_port);
        if (inet_pton(AF_INET, target_ip.c_str(), &node_addr.sin_addr) <= 0)
            return -1;

        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0)
            return -1;

        struct timeval timeout;
        timeout.tv_sec = 2; timeout.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

        int opt = 1;
        setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

        if (connect(sock, (struct sockaddr *)&node_addr, sizeof(node_addr)) < 0) {
            close(sock);
            return -1;
        }
        return sock;
    }

    // Check if the queue has an idle socket. If yes, pop it. If no, create a new one.
    int get_socket_from_pool(int target_id, const std::string& target_ip, int target_port) {
        // try to pop from pool
        {
            std::unique_lock<std::mutex> lock(connection_pool_mtx);
            // check if connection exists, if yes reuse connection and remove from pool
            if (connection_pool.count(target_id) && !connection_pool[target_id].empty()) {
                int sock = connection_pool[target_id].front();
                connection_pool[target_id].pop();
                return sock;
            }
        }

        // if pool empty, create new connection
        // establish new connection after releasing the lock
        return create_new_connection(target_ip, target_port);
    }

    // Return socket to pool for reuse or destroy if broken
    void return_socket_to_pool(int target_id, int sock, bool destroy) {
        if (destroy) {
            close(sock);
        } else {
            std::lock_guard<std::mutex> lock{connection_pool_mtx};
            connection_pool[target_id].push(sock);
        }
    }

    bool perform_rpc(int sock, const RequestMsg& req, ResponseMsg& resp) {
        uint8_t buffer[9];
        buffer[0] = req.cmd;
        std::memcpy(&buffer[1], &req.key, 4);
        std::memcpy(&buffer[5], &req.value, 4);

        if (send(sock, buffer, 9, MSG_NOSIGNAL) != 9)
            return false;

        uint8_t response_buffer[5];
        if (!recv_n_bytes(sock, response_buffer, 5))
            return false;

        resp.status = response_buffer[0];
        std::memcpy(&resp.value, &response_buffer[1], 4);
        return true;
    }

    // take active connections from pool to send request
    ResponseMsg send_request_with_pool(int target_id, const std::string& target_ip, const RequestMsg& request, int target_port) {
        // Try with a pooled socket
        // Get a active connection from the pool or create a new connection
        int sock = get_socket_from_pool(target_id, target_ip, target_port);
        if (sock < 0)
            return ResponseMsg{0, 0};

        ResponseMsg response;
        bool success = perform_rpc(sock, request, response);

        if (success) {
            return_socket_to_pool(target_id, sock, false); // Return to pool
            return response;
        }

        // Failed: The socket might have been stale (server closed it).
        // Destroy the old socket and try one more time with a fresh connection.
        return_socket_to_pool(target_id, sock, true);

        sock = create_new_connection(target_ip, target_port);
        if (sock < 0)
            return ResponseMsg{0, 0};

        success = perform_rpc(sock, request, response);
        if (success) {
            return_socket_to_pool(target_id, sock, false); // New socket is good, pool it
            return response;
        }

        return_socket_to_pool(target_id, sock, true); // Failed twice, give up
        return ResponseMsg{0, 0};
    }

    // ------------------------ Server side opeartion ---------------------------------

    bool put_local(int key, int value) {
        bool success = storage.put(key, value);
        if (success) {
            stats.local_puts_success++; // new key inserted
        } else {
            stats.local_puts_failed++; // key updated
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

    // Process client request
    ResponseMsg handle_request(RequestMsg& request) {
        request.key = ntohl(request.key);
        request.value = ntohl(request.value);

        switch (request.cmd) {
            case CMD_PUT: {
                bool success = put_local(request.key, request.value);
                return ResponseMsg{static_cast<uint8_t>(success ? 1 : 0), 0};
            }
            case CMD_GET: {
                std::optional<int> res = get_local(request.key);
                if (res.has_value()) {
                    return ResponseMsg{1, static_cast<int32_t>(htonl(res.value()))};
                } else {
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

    // Receive exactly 'n' bytes. Returns true on success, false on failure/close.
    static bool recv_n_bytes(int sock, void* buffer, size_t n) {
        size_t total_read = 0;
        char *buf_ptr = static_cast<char *>(buffer);
        while (total_read < n) {
            ssize_t received = read(sock, buf_ptr + total_read, n - total_read);
            if (received < 0) {
                if (errno == EINTR)
                    continue; // if the read call was interrupted then retry
                return false;
            }
            // recieved EOF before reading n bytes, discard the message
            if (received == 0)
                return false;
            total_read += received;
        }
        return true;
    }

    // Handle accepted client launched on a new thread
    void handle_client(int client_socket) {
        uint8_t buffer[9]; // the request from the client should be 9 bytes: CMD key value

        // Persistent loop to continue reading until the client sends requests
        // Thread handling client blocks on read, wakes up when client sends message
        // Exits the loop when client closes the connection
        while (recv_n_bytes(client_socket, buffer, 9)) {
            // Deserialize
            RequestMsg request;
            request.cmd = static_cast<CommandType>(buffer[0]);
            std::memcpy(&request.key, &buffer[1], 4);
            std::memcpy(&request.value, &buffer[5], 4);

            ResponseMsg response = handle_request(request);

            // Serialize response
            uint8_t response_buffer[5];
            response_buffer[0] = response.status;
            std::memcpy(&response_buffer[1], &response.value, 4);

            if (send(client_socket, response_buffer, 5, MSG_NOSIGNAL) != 5) {
                break; // Send failed, exit loop
            }
        }

        close(client_socket); // client closed connection, clean up socket
    }

    void listen_loop() {
        // create the socket
        server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd < 0) {
            log_error("Error making server socket: ", errno);
            return;
        }

        // set socket options
        int opt = 1;
        if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
            log_error("setsockopt(SO_REUSEADDR) failed: ", errno);
            return;
        }
        if (setsockopt(server_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0) {
            log_error("Disabling Nagle's Algorithm failed: ", errno);
            return;
        }

        // create the address
        sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
        server_addr.sin_port = htons(self_config.port);

        // bind the socket to the address
        if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            log_error("Error binding socket to local address: ", errno);
            return;
        }

        // mark the socket to listen state
        if (listen(server_fd, 128) < 0) {
            log_error("Error listening on socket: ", errno);
            return;
        }

        std::cout << "Node listening on port " << self_config.port << "..." << std::endl;

        // server listens to incoming clients on server_fd socket
        while (running) {
            // initialize client address to store client information
            sockaddr_in client_addr;
            memset(&client_addr, 0, sizeof(client_addr));
            socklen_t client_addr_len = sizeof(client_addr);

            // accept a client and create a new_socket to communicate with the client
            // server_fd socket continues listening
            int new_socket = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
            if (new_socket < 0) {
                if (!running)
                    break; // server stopped listening, do not accept any new client
                continue;  // otherwise continue listening
            }

            // disable Nagle's algorithm on this new socket
            if (setsockopt(new_socket, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0) {
                log_error("Disabling Nagle's Algorithm failed: ", errno);
                close(new_socket);
                continue;
            }

            // launch a new thread to communicate with the new client over the new_socket
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

        // Close all pooled sockets
        std::lock_guard<std::mutex> lock{connection_pool_mtx};
        for (auto& entry : connection_pool) {
            while (!entry.second.empty()) {
                close(entry.second.front());
                entry.second.pop();
            }
        }
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

            // uses connection pool to reuse open sockets with target node
            ResponseMsg response = send_request_with_pool(owner.id, owner.ip, request, owner.port);
            
            if (response.status == 1) {
                return true;
            }

            return false; // insert fail (update or network failure)
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
            
            // uses connection pool to reuse open sockets with target node
            ResponseMsg response = send_request_with_pool(owner.id, owner.ip, request, owner.port);

            if (response.status == 1) {
                return ntohl(response.value);
            }

            return std::nullopt;
        }
    }

    // distributed barrier: Node 0 acts as the coordinator
    void wait_for_barrier() {
        if (self_config.id == 0) {
            std::cout << "[Coordinator] Checking if peers are ready...\n";

            // wait for all the peers to come online
            while (running) {
                int ready_count = 0;
                for (const NodeConfig& node : physical_nodes) {
                    if (node.id == 0)
                        continue;
                    
                    // send a ping message to all the other nodes and get a response
                    RequestMsg ping_msg{CMD_PING, 0, 0};
                    ResponseMsg res = send_request_with_pool(node.id, node.ip, ping_msg, node.port);
                    
                    if (res.status == 1)
                        ready_count++;
                }

                if (ready_count == static_cast<int>(physical_nodes.size()) - 1) {
                    std::cout << "[Coordinator] All peers online. Sending GO signal!\n";
                    break;
                }
                
                std::cout << "[Coordinator] Waiting for peers (" << ready_count << "/" << physical_nodes.size()-1 << ")...\n";
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }

            // when all the nodes are ready, send the go signal
            for (const auto& node : physical_nodes) {
                if (node.id == 0)
                    continue;

                RequestMsg go_msg{CMD_GO, 0, 0};
                send_request_with_pool(node.id, node.ip, go_msg, node.port);
            }
            
            // other nodes set benchmark_ready to true after GO message
            benchmark_ready = true;
        } else {
            std::cout << "[Worker] Waiting for Coordinator (Node 0)...\n";
            
            // wait until GO command and set benchmark_ready
            while (running && !benchmark_ready) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }

            std::cout << "[Worker] GO signal received! Starting...\n";
        }
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