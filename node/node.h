#pragma once

#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <atomic>
#include <sstream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <arpa/inet.h>
#include <errno.h>

#include "../concurrent_hash_table/striped_lock_concurrent_hash_table.h"

/**
 * In a peer-to-peer (P2P) Distributed Hash Table, every node must act as both a Server and a Client simultaneously.
 * Server Thread: Listens on a specific port to accept incoming requests from other nodes.
 * Client: Connects to other nodes to forward requests or retrieve data.
 * 
 * insert: SET key value
 * search: GET key
 * delete: DEL key
 */

class DHTNode {
private:
    StripedLockConcurrentHashTable<int, int> storage;

    // networking
    int port;
    int server_fd;
    std::atomic<bool> running;
    std::thread listener_thread;

    std::vector<std::thread> client_threads;
    std::mutex thread_mutex;

    static const int peer_port = 1895;

    void log_error(const char* prefix, int err) {
        char buf[1024];
        std::cerr << "[Error] " << prefix << " " << strerror_r(err, buf, sizeof(buf)) << std::endl;
    }

    // Parse a raw string command
    std::string handle_client_request(const std::string& request) {
        std::stringstream ss(request);
        std::string command;
        int key, value;
        ss >> command;

        if (command == "SET") {
            if (ss >> key >> value) {
                storage.insert(key, value);
                return "OK\n";
            } else {
                return "ERROR: Invalid Integer Format\n";
            }
        } else if (command == "GET") {
            if (ss >> key) {
                auto result = storage.search(key);
                if (result.has_value()) {
                    return std::to_string(result.value()) + "\n";
                } else {
                    return "NOT FOUND\n";
                }
            }
        } else if (command == "DEL") {
            if (ss >> key) {
                storage.remove(key);
                return "OK\n";
            }
        }

        return "ERROR: Unknown Command or Bad Format\n";
    }

    void listen_loop() {
        sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
        server_addr.sin_port = htons(port);

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

        std::cout << "Node listening on port " << port << "..." << std::endl;

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
                        std::string response = handle_client_request(std::string(buffer));
                        send(new_socket, response.c_str(), response.length(), 0);
                    }
                    close(new_socket);
                });
            }
        }
    }

public:
    DHTNode(int p) : port(p), running(false) {}
    ~DHTNode() {
        stop();
    }

    void start() {
        running = true;
        listener_thread = std::thread(&DHTNode::listen_loop, this);
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

    static std::string send_to_1895(const std::string& ip, const std::string& message) {
        return send_request_raw(ip, PEER_PORT, message);
    }

    static std::string send_request_raw(const std::string& ip, int target_port, const std::string& message) {
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
};
