#include "network/connection_pool.h"

#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <netinet/tcp.h>
#include <unistd.h>

// Constructor and destructor
ConnectionPool::ConnectionPool() {}

ConnectionPool::~ConnectionPool() {
    std::lock_guard<std::mutex> lock(pool_mutex);
    for (auto& entry : pool) {
        while (!entry.second.empty()) {
            close(entry.second.front());
            entry.second.pop();
        }
    }
}

// Create a new connection with the target node
int ConnectionPool::create_new_connection(const std::string& target_ip, int target_port) {
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
int ConnectionPool::get_connection(int target_id, const std::string& target_ip, int target_port) {
    // try to pop from pool
    {
        std::unique_lock<std::mutex> lock(pool_mutex);
        // check if connection exists, if yes reuse connection and remove from pool
        if (pool.count(target_id) && !pool[target_id].empty()) {
            int sock = pool[target_id].front();
            pool[target_id].pop();
            return sock;
        }
    }

    // if pool empty, create new connection
    return create_new_connection(target_ip, target_port);
}

// Return socket to pool for reuse or destroy if broken
void ConnectionPool::return_connection(int target_id, int sock, const bool destroy) {
    if (destroy) {
        close(sock);
    } else {
        std::lock_guard<std::mutex> lock{pool_mutex};
        pool[target_id].push(sock);
    }
}