#include "network/connection_pool.h"
#include "common/utils.h"

#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <netinet/tcp.h>
#include <unistd.h>
#include <iostream>
#include <thread>

// Constructor and destructor
ConnectionPool::ConnectionPool(int num_nodes) : pools(num_nodes) {
  if (num_nodes > 0) {
    dead_nodes = std::make_unique<std::atomic<bool>[]>(num_nodes);
    
    // Explicitly initialize each atomic to false
    for (int i = 0; i < num_nodes; ++i) {
      dead_nodes[i].store(false, std::memory_order_relaxed);
    }
  }
}

ConnectionPool::~ConnectionPool() {
  for (TargetPool& target_pool : pools) {
    std::lock_guard<Spinlock> lock(target_pool.pool_mtx.mutex);
    for (int sock : target_pool.sockets) {
      close(sock);
    }
    target_pool.sockets.clear();
  }
}

// Create a new connection with the target node
int ConnectionPool::create_new_connection(const int target_id, const std::string &target_ip, const int target_port) {
  sockaddr_in node_addr;
  memset(&node_addr, 0, sizeof(node_addr));
  node_addr.sin_family = AF_INET;
  node_addr.sin_port = htons(target_port);
  if (inet_pton(AF_INET, target_ip.c_str(), &node_addr.sin_addr) <= 0) {
    log_error("Could not process ip address", errno);
    return -1;
  }

  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    log_error("Invalid Socket", errno);
    return -1;
  }

  // 500ms max block time for Read AND Send
  struct timeval timeout;
  timeout.tv_sec = 0; 
  timeout.tv_usec = 500000; 
  setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

  int opt = 1;
  setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

  // Bounded Retry Loop for connection establishment
  int max_retries = 3;
  for (int attempts = 0; attempts < max_retries; ++attempts) {
    if (connect(sock, (struct sockaddr *)&node_addr, sizeof(node_addr)) == 0) {
      return sock;
    }
    
    if (errno != ECONNREFUSED && errno != EAGAIN && errno != ETIMEDOUT) {
      break;
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(10 * (1 << attempts)));
  }

  // If we exhaust retries or get a hard failure, the node is permanently dead.
  // Mark it in the lock-free array
  #ifndef NDEBUG
    std::cerr << "[ConnectionPool] Node " << target_id << " is unresponsive. Blacklisting permanently.\n";
  #endif
  
  dead_nodes[target_id].store(true, std::memory_order_relaxed);
  
  close(sock);
  return -1;
}

// Check if the queue has an idle socket. Fast-fail if the node is dead.
int ConnectionPool::get_connection(const int target_id, const std::string &target_ip, const int target_port) {
  // If the node is marked dead, reject instantly
  if (dead_nodes[target_id].load(std::memory_order_relaxed)) [[unlikely]] {
    return -1; 
  }

  // try to pop from pool
  {
    std::lock_guard<Spinlock> lock(pools[target_id].pool_mtx.mutex);
    if (!pools[target_id].sockets.empty()) {
      int sock = pools[target_id].sockets.back(); 
      pools[target_id].sockets.pop_back();
      return sock;
    }
  }

  // Pool empty, create new connection
  return create_new_connection(target_id, target_ip, target_port);
}

// Return socket to pool for reuse or destroy if broken
void ConnectionPool::return_connection(const int target_id, const int sock, const bool destroy) {
  if (destroy) {
    close(sock);

    // If an active socket broke, assume the node crashed.
    // Instantly blacklist the node so future get_connection() calls fast-fail.
    dead_nodes[target_id].store(true, std::memory_order_relaxed);
    
    #ifndef NDEBUG
      std::cerr << "[ConnectionPool] Active socket broke. Blacklisting Node " << target_id << ".\n";
    #endif
    return;
  }
  
  std::unique_lock<Spinlock> lock(pools[target_id].pool_mtx.mutex);
  
  // Bounds checking: Prevent File Descriptor Exhaustion
  if (pools[target_id].sockets.size() >= MAX_SOCKETS_PER_NODE) {
    lock.unlock();
    close(sock);
  } else {
    pools[target_id].sockets.push_back(sock);
  }
}

void ConnectionPool::pre_warm(const int target_id, const std::string &target_ip, const int target_port, const int count) {  
  std::vector<int> temp_sockets;
  temp_sockets.reserve(count);

  bool logged_waiting = false;

  while (temp_sockets.size() < static_cast<size_t>(count)) {
    int sock = create_new_connection(target_id, target_ip, target_port); 
    
    if (sock != -1) {
      temp_sockets.push_back(sock);
    } else {
      if (!logged_waiting) {
        std::cout << "[ConnectionPool] Waiting for Node " << target_id 
                  << " (" << target_ip << ":" << target_port << ") to come online...\n";
        logged_waiting = true;
      }
      
      // If pre-warm fails, un-blacklist it so we keep trying to boot the cluster
      dead_nodes[target_id].store(false, std::memory_order_relaxed);
      
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }

  if (logged_waiting) {
    std::cout << "[ConnectionPool] Node " << target_id << " is online. Connections established.\n";
  }

  for (int sock : temp_sockets) {
    return_connection(target_id, sock, false);
  }
}