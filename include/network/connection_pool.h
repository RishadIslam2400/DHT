#pragma once

#include <mutex>
#include <unordered_map>
#include <queue>
#include <string>

constexpr int MAX_SOCKETS_PER_NODE = 16;

class ConnectionPool {
private:
  struct TargetPool {
    std::mutex mtx;
    std::vector<int> sockets; // LIFO stack

    TargetPool() {
      sockets.reserve(MAX_SOCKETS_PER_NODE);
    }
  };

  std::vector<TargetPool> pools;

  int create_new_connection(const std::string &target_ip, const int target_port);
public:
  explicit ConnectionPool(int num_nodes);
  ~ConnectionPool();

  int get_connection(const int target_id, const std::string &target_ip, const int target_port);
  void return_connection(const int target_id, const int sock, const bool destroy);

  // Optional: Call this before the benchmark starts to establish initial TCP handshakes
  void pre_warm(const int target_id, const std::string &target_ip, const int target_port, const int count);
};