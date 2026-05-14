#pragma once

#include <mutex>
#include <unordered_map>
#include <queue>
#include <string>
#include <vector>
#include <atomic>
#include <memory>

#include "common/spin_lock.h"

constexpr int MAX_SOCKETS_PER_NODE = 128;

class ConnectionPool {
private:
  struct TargetPool {
    AlignedSpinlock pool_mtx;
    std::vector<int> sockets; // LIFO stack

    TargetPool() {
      sockets.reserve(MAX_SOCKETS_PER_NODE);
    }
  };

  std::vector<TargetPool> pools;

  // Lock-free array to track permanently dead nodes
  std::unique_ptr<std::atomic<bool>[]> dead_nodes;

  int create_new_connection(const int target_id, const std::string &target_ip, const int target_port);
public:
  explicit ConnectionPool(int num_nodes);
  ~ConnectionPool();

  int get_connection(const int target_id, const std::string &target_ip, const int target_port);
  void return_connection(const int target_id, const int sock, const bool destroy);

  void pre_warm(const int target_id, const std::string &target_ip, const int target_port, const int count);
};