#pragma once

#include <iostream>
#include <vector>
#include <mutex>
#include <memory>
#include <condition_variable>
#include <string>
#include <stdexcept>
#include <atomic>
#include <cstring> 
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cerrno>

class Connection {
public:
    virtual ~Connection() = default;
    virtual bool connect() = 0;
    virtual void disconnect() = 0;
    virtual bool is_connected() const = 0;
    virtual bool is_healthy() = 0;
    virtual void send_data(const std::string &data) = 0;
};

class TcpConnection : public Connection {
private:
    std::string m_host;
    int m_port;
    int m_sock = -1;

public:
    TcpConnection(const std::string& host, int port) : m_host(host), m_port(port) {}
    ~TcpConnection() override {
        disconnect();
    }

    bool connect() override {
        if (m_sock != -1)
            return true; // already connected

        m_sock = socket(AF_INET, SOCK_STREAM, 0);
        if (m_sock < 0)
            return false;

        sockaddr_in server_addr{};
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(m_port);
        if (inet_pton(AF_INET, m_host.c_str(), &server_addr.sin_addr) <= 0) {
             disconnect();
             return false;
        }

        if (::connect(m_sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            disconnect();
            return false;
        }

        return true;
    }

    void disconnect() override {
        if (m_sock != -1) {
            close(m_sock);
            m_sock = -1;
        }
    }

    bool is_connected() const override {
        return m_sock != -1;
    }

    bool is_healthy() override {
        if (m_sock == -1)
            return false;

        char buf;
        ssize_t result = ::recv(m_sock, &buf, 1, MSG_PEEK | MSG_DONTWAIT);

        if (result == 0)
            return false;
        
        if (result == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return true;
            return false;
        }

        return true;
    }

    void send_data(const std::string& data) override {
        if (m_sock != -1) {
            std::cout << "[TCP] Sending: " << data << " on socket " << m_sock << std::endl;
        }
    }
};

class ConnectionPool;

class ConnectionHandle {
private:
    Connection *m_connection;
    std::shared_ptr<ConnectionPool> m_pool;

public:
    ConnectionHandle(Connection* connection, std::shared_ptr<ConnectionPool> pool)
        : m_connection(connection), m_pool(std::move(pool)) {}

    // Cannot duplicate a connection handle
    ConnectionHandle(const ConnectionHandle &) = delete;
    ConnectionHandle &operator=(const ConnectionHandle &) = delete;

    // Transfer ownership of conneciton handle
    ConnectionHandle(ConnectionHandle&& other) noexcept 
        : m_connection(other.m_connection), m_pool(std::move(other.m_pool)) {
        other.m_connection = nullptr;
    }
    ConnectionHandle& operator=(ConnectionHandle&& other) noexcept;

    // Destructor automatically returns connection to the pool
    ~ConnectionHandle();

    Connection *operator->() { return m_connection; }
    Connection &operator*() { return *m_connection; }

    explicit operator bool() const { return m_connection != nullptr; }
};


class ConnectionPool : public std::enable_shared_from_this<ConnectionPool> {
private:
    // explicit storage
    std::vector<std::unique_ptr<Connection>> m_all_connections;

    // raw pointers for tracking availability
    std::vector<Connection *> m_free_connections;

    std::mutex m_mutex;
    std::condition_variable m_cv;
    bool m_shutdown = false;

public:
    ConnectionPool(size_t pool_size, const std::string& host, int port) {
        for (size_t i = 0; i < pool_size; ++i) {
            auto connection = std::make_unique<TcpConnection>(host, port);
            connection->connect();
            m_free_connections.push_back(connection.get());
            m_all_connections.push_back(std::move(connection));
        }
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lock{m_mutex};
            m_shutdown = true;
        }
        m_cv.notify_all(); // Wake up anyone waiting in get_connection
    }

    ~ConnectionPool() {
        shutdown();
    }

    ConnectionHandle get_connection() {
        std::unique_lock<std::mutex> lock{m_mutex};

        // wait until a connection is available or pool is shutting down
        m_cv.wait(lock, [this] {
            return !m_free_connections.empty() || m_shutdown;
        });

        if (m_shutdown) {
            throw std::runtime_error("Pool is shutting down");
        }

        Connection *connection = m_free_connections.back();
        m_free_connections.pop_back();

        lock.unlock();

        if (!connection->is_healthy()) {
            // reconnect
            std::cout << "[Pool] Connection dead, reconnecting...\n";
            connection->disconnect();
            if (!connection->connect()) {
                std::cerr << "[Pool] Reconnect failed.\n";
                return_connection(connection);
                throw std::runtime_error("Failed to connect to database");
            }
        }

        return ConnectionHandle(connection, shared_from_this());
    }

    void return_connection(Connection* connection) {
        std::unique_lock<std::mutex> lock{m_mutex};
        m_free_connections.push_back(connection);

        m_cv.notify_one(); // let one thread know ConnectionPool is not empty
    }
};

ConnectionHandle& ConnectionHandle::operator=(ConnectionHandle&& other) noexcept {
    if (this != &other) {
        // return any exisitng connection
        if (m_connection && m_pool) {
            m_pool->return_connection(m_connection);
        }

        // steal resources from the other handle
        m_connection = other.m_connection;
        m_pool = std::move(other.m_pool);

        // Nullify the other handle so it doesn't double-return
        other.m_connection = nullptr;
    }
    return *this;
}

ConnectionHandle::~ConnectionHandle() {
    if (m_connection && m_pool) {
        m_pool->return_connection(m_connection);
    }
}