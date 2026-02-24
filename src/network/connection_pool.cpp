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
ConnectionPool::ConnectionPool(int num_nodes) : pools(num_nodes) {}

ConnectionPool::~ConnectionPool() {
  for (TargetPool& target_pool : pools) {
    std::lock_guard<std::mutex> lock(target_pool.mtx);
    for (int sock : target_pool.sockets) {
      close(sock);
    }
    target_pool.sockets.clear();
  }
}

// Create a new connection with the target node
int ConnectionPool::create_new_connection(const std::string &target_ip, const int target_port) {
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

  struct timeval timeout;
  timeout.tv_sec = 2; timeout.tv_usec = 0;
  setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

  int opt = 1;
  setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

  if (connect(sock, (struct sockaddr *)&node_addr, sizeof(node_addr)) < 0) {
    log_error("Connection failure", errno);
    close(sock);
    return -1;
  }
  return sock;
}

// Check if the queue has an idle socket. If yes, pop it. If no, create a new one.
int ConnectionPool::get_connection(const int target_id, const std::string &target_ip, const int target_port) {
  // try to pop from pool
  {
    std::unique_lock<std::mutex> lock(pools[target_id].mtx);

    if (!pools[target_id].sockets.empty()) {
      int sock = pools[target_id].sockets.back(); // Get recently used connection
      pools[target_id].sockets.pop_back();
      return sock;
    }
  }

  // Pool empty or target queue empty, create new connection
  return create_new_connection(target_ip, target_port);
}

// Return socket to pool for reuse or destroy if broken
void ConnectionPool::return_connection(const int target_id, const int sock, const bool destroy) {
  if (destroy) {
    close(sock);
    return;
  }
  
  std::unique_lock<std::mutex> lock(pools[target_id].mtx);
  
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

  // Actively poll until the exact number of connections are established
  while (temp_sockets.size() < static_cast<size_t>(count)) {
    int sock = create_new_connection(target_ip, target_port);
    
    if (sock != -1) {
      temp_sockets.push_back(sock);
    } else {
      // Only log the warning once to avoid spamming the console
      if (!logged_waiting) {
        std::cout << "[ConnectionPool] Waiting for Node " << target_id 
                  << " (" << target_ip << ":" << target_port << ") to come online...\n";
        logged_waiting = true;
      }
      // Sleep briefly to avoid blasting the network with SYN packets
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }

  if (logged_waiting) {
    std::cout << "[ConnectionPool] Node " << target_id << " is online. Connections established.\n";
  }

  // Return all successfully established hot sockets to the LIFO stack
  for (int sock : temp_sockets) {
    return_connection(target_id, sock, false);
  }
}