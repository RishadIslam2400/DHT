#pragma once

#include <mutex>
#include <unordered_map>
#include <queue>
#include <string>

class ConnectionPool {
    std::mutex pool_mutex;
    std::unordered_map<int, std::queue<int>> pool;

    int create_new_connection(const std::string &target_ip, int target_port);
public:
    ConnectionPool();
    ~ConnectionPool();

    int get_connection(int target_id, const std::string &target_ip, int target_port);
    void return_connection(int target_id, int sock, const bool destroy);
};