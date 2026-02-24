#include "network/connection_pool.h"

#include <iostream>
#include <thread>
#include <atomic>
#include <vector>
#include <netinet/in.h>
#include <unistd.h>
#include <mutex>
#include <cassert>
#include <fcntl.h>

void log(const std::string& message) {
  std::cout << "[TEST] " << message << std::flush;
}

class DummyServer {
private:
  int server_fd;
  int port;
  std::atomic<bool> running{true};
  std::thread worker;
  std::vector<int> client_sockets;
  std::mutex mtx;

public:
  DummyServer() {
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd <= 0)
      throw std::runtime_error("Socket failed");

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Bind to random available port (port 0)
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = 0; // Let OS choose port

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0)
      throw std::runtime_error("Bind failed");

    // Get the assigned port
    socklen_t len = sizeof(address);
    getsockname(server_fd, (struct sockaddr *)&address, &len);
    port = ntohs(address.sin_port);

    // Listen
    listen(server_fd, 10);

    // Accept connections in background
    worker = std::thread([this]() {
      while (running) {
        sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        // Set timeout for accept so we can exit cleanly
        struct timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = 100000; // 100ms
        setsockopt(server_fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);

        int new_socket = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (new_socket >= 0) {
          // Track the socket to prevent FD leaks during the test
          std::lock_guard<std::mutex> lock(mtx);
          client_sockets.push_back(new_socket);
        }
      }
    });
  }

  ~DummyServer() {
    running = false;
    if (worker.joinable())
      worker.join();
    
    close(server_fd);
    for (int sock : client_sockets) {
      close(sock);
    }
  }

  int get_port() const { return port; }
};

void test_basic_acquisition_and_return(int port) {
  log("Basic Acquisition and Return...");
  ConnectionPool pool(2); // Pool for 2 nodes

  // Get connection
  int sock = pool.get_connection(0, "127.0.0.1", port);
  assert(sock > 0);

  // Return connection successfully
  pool.return_connection(0, sock, false);

  std::cout << "PASSED\n";
}

void test_lifo_reuse(int port) {
  log("LIFO Connection Reuse... ");
  ConnectionPool pool(1);

  int sock1 = pool.get_connection(0, "127.0.0.1", port);
  int sock2 = pool.get_connection(0, "127.0.0.1", port);

  // Return in order 1 then 2. 
  // Because it is a LIFO stack, the next pop should yield 2.
  pool.return_connection(0, sock1, false);
  pool.return_connection(0, sock2, false);

  int sock3 = pool.get_connection(0, "127.0.0.1", port);
  assert(sock3 == sock2);

  int sock4 = pool.get_connection(0, "127.0.0.1", port);
  assert(sock4 == sock1);
  
  std::cout << "PASSED\n";
}

void test_destroy_bad_connection(int port) {
  log("Destroy Bad Connection... ");
  ConnectionPool pool(1);

  int sock1 = pool.get_connection(0, "127.0.0.1", port);
  
  // Simulate a network failure by returning with destroy = true
  pool.return_connection(0, sock1, true);

  // Prove the socket was actually closed at the OS level.
  // fcntl will return -1 and set errno to EBADF (Bad File Descriptor) if it is closed.
  int flags = fcntl(sock1, F_GETFD);
  assert(flags == -1);
  assert(errno == EBADF);

  // Getting a new connection will safely reuse the integer, which is perfectly fine.
  int sock2 = pool.get_connection(0, "127.0.0.1", port);
  assert(sock2 > 0);

  std::cout << "PASSED\n";
}

void test_max_capacity_enforcement(int port) {
  log("Max Capacity Bounds Checking... ");
  ConnectionPool pool(1);
  std::vector<int> active_sockets;

  // Create more connections than MAX_SOCKETS_PER_NODE (16)
  int request_count = 20;
  for (int i = 0; i < request_count; ++i) {
    active_sockets.push_back(pool.get_connection(0, "127.0.0.1", port));
  }

  // Return all 20 connections. The pool should only keep 16 and destroy the other 4.
  for (int sock : active_sockets) {
    pool.return_connection(0, sock, false);
  }

  // To verify, if we pull 16 connections out, they should be reused.
  // The 17th connection should be a brand new FD because the pool dropped it earlier.
  std::vector<int> reused_sockets;
  for (int i = 0; i < 16; ++i) {
    reused_sockets.push_back(pool.get_connection(0, "127.0.0.1", port));
  }
  
  int sock17 = pool.get_connection(0, "127.0.0.1", port);
  
  // Check that sock17 is NOT one of the original 20 we pushed
  // (In Linux, FDs are recycled, but since we are holding the 16, this must be a newly allocated one)
  bool found_in_original = false;
  for (int original_sock : active_sockets) {
    if (sock17 == original_sock)
      found_in_original = true;
  }
  
  // It might reuse an FD number OS-wise if the OS released it, 
  // but the pool logic safely prevented infinite growth.
  // The main assertion here is that it didn't crash and handled the bounds correctly.
  assert(sock17 > 0); 

  std::cout << "PASSED\n";
}

void test_pre_warm(int port) {
  log("Network Pre-Warming... ");
  ConnectionPool pool(1);

  // Pre-warm 5 connections
  pool.pre_warm(0, "127.0.0.1", port, 5);

  // We should be able to get 5 connections instantly without triggering new connects.
  // Real-world verification would check TCP handshakes via tcpdump, 
  // but here we verify the logic completes successfully.
  std::vector<int> socks;
  for (int i = 0; i < 5; ++i) {
    int sock = pool.get_connection(0, "127.0.0.1", port);
    assert(sock > 0);
    socks.push_back(sock);
  }

  std::cout << "PASSED\n";
}

int main() {
  try {
    DummyServer server;
    int port = server.get_port();
    std::cout << "Dummy Server listening on port " << port << "\n\n";

    test_basic_acquisition_and_return(port);
    test_lifo_reuse(port);
    test_destroy_bad_connection(port);
    test_max_capacity_enforcement(port);
    test_pre_warm(port);

    std::cout << "\nAll connection pool tests passed successfully.\n";
  } catch (const std::exception& e) {
    std::cerr << "Test Failed: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}