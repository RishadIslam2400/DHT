#include "connection_pool.h"

#include <iostream>
#include <thread>
#include <vector>
#include <cassert>
#include <chrono>
#include <future>
#include <atomic>

class DummyServer {
private:
    int server_fd;
    int port;
    std::atomic<bool> running{true};
    std::thread worker;

public:
    DummyServer() {
        // Setup socket
        server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd == 0)
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
                    // Just accept and ignore. Close when destructor runs.
                    // In a real echo server, we would read/write here.
                }
            }
        });
    }

    ~DummyServer() {
        running = false;
        close(server_fd);
        if (worker.joinable()) worker.join();
    }

    int get_port() const { return port; }
};

void test_basic_acquisition(int port) {
    std::cout << "[Test] Basic Acquisition... ";
    auto pool = std::make_shared<ConnectionPool>(2, "127.0.0.1", port);

    {
        auto conn = pool->get_connection();
        assert(conn);
        assert(conn->is_connected());
        conn->send_data("Ping");
    } // Returns to pool here

    std::cout << "PASSED\n";
}

void test_connection_reuse(int port) {
    std::cout << "[Test] Connection Reuse... ";
    auto pool = std::make_shared<ConnectionPool>(1, "127.0.0.1", port);

    Connection* ptr1 = nullptr;
    {
        auto conn1 = pool->get_connection();
        ptr1 = &(*conn1);
    } // Returns conn1

    {
        auto conn2 = pool->get_connection();
        Connection* ptr2 = &(*conn2);
        // Verify we got the exact same memory address (same object recycled)
        assert(ptr1 == ptr2);
    }
    std::cout << "PASSED\n";
}

void test_pool_exhaustion(int port) {
    std::cout << "[Test] Pool Exhaustion & Blocking... ";
    // Pool size 1
    auto pool = std::make_shared<ConnectionPool>(1, "127.0.0.1", port);

    // Take the only connection
    auto conn1 = pool->get_connection();

    // Try to take another in a separate thread
    auto start = std::chrono::steady_clock::now();
    
    std::future<void> result = std::async(std::launch::async, [&]() {
        // This should BLOCK until conn1 is destroyed
        auto conn2 = pool->get_connection(); 
        assert(conn2);
    });

    // Verify the thread is actually blocked (wait 100ms)
    std::future_status status = result.wait_for(std::chrono::milliseconds(100));
    assert(status == std::future_status::timeout); 

    // Return the first connection
    // Explicitly destroy conn1 to return it to pool
    conn1 = {nullptr, nullptr}; 

    // Now the thread should finish
    result.get(); // Will throw if assertion inside failed
    
    auto end = std::chrono::steady_clock::now();
    // Verify it took at least 100ms (proving it waited)
    assert(end - start >= std::chrono::milliseconds(100));

    std::cout << "PASSED\n";
}

void test_shutdown_unblocks_waiting(int port) {
    std::cout << "[Test] Shutdown Unblocks Waiters... ";
    auto pool = std::make_shared<ConnectionPool>(1, "127.0.0.1", port);

    // Drain the pool
    auto conn = pool->get_connection();

    // Spawn a waiter
    std::future<void> waiter = std::async(std::launch::async, [&]() {
        try {
            auto c = pool->get_connection();
            // Should NOT get here
            assert(false); 
        } catch (const std::runtime_error& e) {
            std::string msg = e.what();
            assert(msg == "Pool is shutting down");
        }
    });

    // Give the thread time to block
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Trigger shutdown
    pool->shutdown();

    // Waiter should unblock and finish immediately
    waiter.get();

    std::cout << "PASSED\n";
}

int main() {
    try {
        // Start a dummy TCP server on a random local port
        DummyServer server;
        int port = server.get_port();
        std::cout << "Dummy Server listening on port " << port << "\n\n";

        test_basic_acquisition(port);
        test_connection_reuse(port);
        test_pool_exhaustion(port);
        test_shutdown_unblocks_waiting(port);

        std::cout << "\nAll Tests Passed!\n";
    } catch (const std::exception& e) {
        std::cerr << "Test Failed: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}