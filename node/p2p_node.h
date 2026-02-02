#pragma once

#include <iostream>
#include <thread>
#include <vector>
#include <string>
#include <cstring>
#include <atomic>
#include <mutex>

#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>
#include <errno.h>
#include <sys/time.h>


constexpr int BUFFER_SIZE = 1024;

class P2PNode {
private:
    size_t server_port;
    int server_socket_fd;
    std::atomic<bool> running;
    std::thread listener_thread;

    std::vector<std::thread> client_threads;
    std::mutex thread_mutex;

    void log_error(const char* prefix, int err) {
        char buf[BUFFER_SIZE];
        std::cerr << "[Error] " << prefix << " " << strerror_r(err, buf, sizeof(buf)) << std::endl;
    }
    
    /**
     * Receive text over the provided socket file descriptor, and send it back to
     * the client.  When the client sends an EOF, return.
     *
     * @param client_fd   The socket file descriptor to use for the echo operation
     * @param verbose     Should stats be printed upon completion?
     */
    void echo_server(int client_fd, bool verbose) {
        // vars for tracking connection duration, bytes transmitted
        size_t xmit_bytes = 0;
        struct timeval start_time, end_time;
        if (verbose) {
            gettimeofday(&start_time, nullptr);
        }

        // read data for as long as there is data, and always send it back
        while (true) {
            char buf[16] = {0}; // Small buffer to test partial sends
            ssize_t rcd = recv(client_fd, buf, sizeof(buf), 0);
            if (rcd < 0) {
                if (errno != EINTR) {
                    log_error("Error in recv(): ", errno);
                    break;
                }
            } else if (rcd == 0) {
                // Client closed connection
                break;
            } else {
                // Immediately send back what we got
                xmit_bytes += rcd;
                char *next_byte = buf;
                std::size_t remain = rcd;
                while (remain > 0) {
                    std::size_t sent = send(client_fd, next_byte, remain, 0);
                    if (sent <= 0) {
                        if (errno != EINTR) {
                            log_error("Error in send(): ", errno);
                            return; // stop echo on send error
                        }
                    } else {
                        next_byte += sent;
                        remain -= sent;
                    }
                }
            }
        }

        if (verbose) {
            gettimeofday(&end_time, nullptr);
            long seconds = end_time.tv_sec - start_time.tv_sec;
            long micros = end_time.tv_usec - start_time.tv_usec;
            double elapsed = seconds + micros*1e-6;
            printf("Transmitted %zu bytes in %.4f seconds\n", xmit_bytes, elapsed);
        }
    }

    // Handle an individual incoming connection
    void handle_client(int client_fd, sockaddr_in client_addr) {
        char client_ip[INET_ADDRSTRLEN];
        if (inet_ntop(AF_INET, &(client_addr.sin_addr), client_ip, INET_ADDRSTRLEN)) {
            printf("Connected to %s:%d\n", client_ip, ntohs(client_addr.sin_port));
        } else {
            printf("Connected to unknown client\n");
        }

        echo_server(client_fd, true);
        close(client_fd);
    }

    /**
     * Create a server socket that we can use to listen for new incoming requests
     *
     */
    void server_loop() {
        server_socket_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (server_socket_fd < 0) {
            log_error("Error making server socket: ", errno);
            return;
        }

        // The default is that when the server crashes, the socket can't be used for a
        // few minutes.  This lets us re-use the socket immediately:
        int tmp = 1;
        if (setsockopt(server_socket_fd, SOL_SOCKET, SO_REUSEADDR, &tmp, sizeof(int)) < 0) {
            close(server_socket_fd);
            log_error("setsockopt(SO_REUSEADDR) failed: ", errno);
            return;
        }

        // bind the socket to the server's address and the provided port, and then
        // start listening for connections
        sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
        server_addr.sin_port = htons(server_port);

        if (bind(server_socket_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            log_error("Error binding socket to local address: ", errno);
            close(server_socket_fd);
            return;
        }

        if (listen(server_socket_fd, 5) < 0) {
            log_error("Error listening on socket: ", errno);
            close(server_socket_fd);
            return;
        }

        
        while (running) {
            std::cout << "Waiting for a client to connect...\n";
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);
            
            int client_fd = accept(server_socket_fd, (struct sockaddr *)&client_addr, &client_len);
            if (client_fd < 0) {
                // If the socket was closed by the destructor, stop the loop
                if (!running) break;
                log_error("Error accepting request from client: ", errno);
                continue;
            }

            {
                std::lock_guard<std::mutex> lock(thread_mutex);
                client_threads.emplace_back(&P2PNode::handle_client, this, client_fd, client_addr);
            }

            //std::thread(&P2PNode::handle_client, this, client_fd, client_addr).detach();
        }

        // Cleanup if loop exits naturally
        close(server_socket_fd);
    }

public:
    explicit P2PNode(size_t port = 1895) : server_port(port), running(true) {
        // Start the server listener in the background immediately
        listener_thread = std::thread(&P2PNode::server_loop, this);
    }

    ~P2PNode() {
        running = false;
        if (server_socket_fd > 0) {
            close(server_socket_fd);
        }

        if (listener_thread.joinable()) {
            listener_thread.join();
        }

        std::lock_guard<std::mutex> lock(thread_mutex);
        for (auto& t : client_threads) {
            if (t.joinable()) t.join();
        }
    }

    /**
     * Connect to a server so that we can have bidirectional communication on the
     * socket (represented by a file descriptor) that this function returns
     *
     * @param hostname The name of the server (ip or DNS) to connect to
     * @param port     The server's port that we should use
     */
    int connect_to_node(const std::string& hostname, size_t port) {
        // figure out the IP address that we need to use and put it in a sockaddr_in
        struct hostent *host = gethostbyname(hostname.c_str());
        if (host == nullptr) {
            log_error("DNS lookup failed", h_errno); // h_errno is specific to gethostbyname
            return -1;
        }

        sockaddr_in peer_addr{};
        memset(&peer_addr, 0, sizeof(peer_addr));
        peer_addr.sin_family = AF_INET;
        peer_addr.sin_addr.s_addr = inet_addr(inet_ntoa(*(struct in_addr *)*host->h_addr_list));
        // memcpy(&peer_addr.sin_addr, host->h_addr, host->h_length);
        peer_addr.sin_port = htons(port);

        // create the socket and try to connect to it
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            log_error("Error making client socket: ", errno);
            return -1;
        }

        if (connect(sock, (struct sockaddr *)&peer_addr, sizeof(peer_addr)) < 0) {
            log_error("Connection Failed: ", errno);
            close(sock);
            return -1;
        }

        std::cout << "Connected\n";
        return sock;
    }

    /**
     * Receive text from the keyboard (well, actually, stdin), send it to the
     * server, and then print whatever the server sends back.
     *
     * @param sd      The socket file descriptor to use for the echo operation
     * @param verbose Should stats be printed upon completion?
     */
    void echo_client(int sd, bool verbose) {
        // vars for tracking connection duration, bytes transmitted
        size_t xmit_bytes = 0;
        struct timeval start_time, end_time;
        if (verbose) {
            gettimeofday(&start_time, nullptr);
        }

        // string for holding user input that we send to the server
        std::string data;

        // read from stdin for as long as it isn't EOF (ctrl+D or "exit"), send to server, print reply
        while (true) {
            std::cout << "Client> " << std::flush;
            std::getline(std::cin, data);
            if (std::cin.eof() || data == "exit") {
                break;
            }

            // When we send, we need to be ready for the possibility that not all the
            // data will transmit at once
            const char *next_byte = data.c_str();
            size_t remain = data.length();
            while (remain > 0) {
                // NB: send() with last parameter 0 is the same as write() syscall
                ssize_t sent = send(sd, next_byte, remain, 0);
                // NB: Sending 0 bytes means the server closed the socket, and we should
                //     crash.
                //
                // NB: Sending -1 bytes means an error.  If the error is EINTR, it's OK,
                //     try again.  Otherwise crash.
                if (sent <= 0) {
                    if (errno != EINTR) {
                        log_error("Error in send(): ", errno);
                        return;
                    }
                } else {
                    next_byte += sent;
                    remain -= sent;
                }
            }
            // update the transmission count
            xmit_bytes += data.length();

            // Now it's time to receive data.
            //
            // Receiving is hard when we don't know how much data we are going to
            // receive.  Two workarounds are (1) receive until a certain token comes in
            // (such as newline), or (2) receive a fixed number of bytes.  Since we're
            // expecting back exactly what we sent, we can take strategy #2.
            //
            // NB: need an extra byte in the buffer, so we can null-terminate the string
            //     before printing it.
            std::vector<char> buf(data.length() + 1, 0);
            remain = data.length();
            char *recv_ptr = buf.data();

            while (remain > 0) {
                ssize_t rcd = recv(sd, recv_ptr, remain, 0);
                if (rcd <= 0) {
                    if (errno != EINTR) {
                         // 0 means server closed unexpectedly
                        if (rcd == 0) std::cerr << "[Error] Server closed connection.\n";
                        else log_error("Recv failed", errno);
                        return;
                    }
                } else {
                    recv_ptr += rcd;
                    remain -= rcd;
                }
            }
            // Print back the message from the server, and update the transmission count
            xmit_bytes += data.length();
            // Since vector is zero-initialized, and we filled it with data.length(),
            // the last byte is guaranteed to be \0.
            printf("Server: %s\n", buf.data());
        }

        if (verbose) {
            gettimeofday(&end_time, nullptr);
            long seconds = end_time.tv_sec - start_time.tv_sec;
            long micros = end_time.tv_usec - start_time.tv_usec;
            double elapsed = seconds + micros*1e-6;
            printf("Transmitted %zu bytes in %.4f seconds\n", xmit_bytes, elapsed);
        }
    }
};