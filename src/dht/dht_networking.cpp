#include "dht/dht_static_partitioning.h"
#include "common/xxHash64.h"
#include "common/utils.h"
#include "network/buffered_socket.h"
#include "dht/consensus_interface.h"
#include "dht/dht_transaction_manager.h"

#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <iostream>

// Listener thread creates a server socket which is listening for incoming connections
// This model only works for the assignment but not real world with millions of connections
void StaticClusterDHTNode::listen_loop() {
  server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    log_error("Error making server socket: ", errno);
    return;
  }

  // set socket options to reuse address
  int opt = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
    log_error("setsockopt(SO_REUSEADDR) failed: ", errno);
    return;
  }

  // create the address to bind with server socket
  sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  server_addr.sin_port = htons(self_config.port);

  // bind the socket to the address
  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    log_error("Error binding socket to local address: ", errno);
    return;
  }

  // mark the socket to listen state
  if (listen(server_fd, SOMAXCONN) < 0) {
    log_error("Error listening on socket: ", errno);
    return;
  }

  std::cout << "Node listening on port " << self_config.port << "..." << std::endl;

  // server_fd socket accepts incoming connections in a persistent loop until the system is running
  while (running.load(std::memory_order_relaxed)) {
    // initialize client address to store client information
    sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);

    // accept a incoming connection that returns a new_socket to communicate with the 
    // client listener thread is blocked on accept
    int new_socket = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
    if (new_socket < 0) {
      // Check if we are shutting down
      if (errno == EINVAL || errno == EBADF || !running) {
          break; 
      }

      if (errno == EMFILE || errno == ENFILE) {
          log_error("CRITICAL: Node ran out of File Descriptors!", errno);
          // Sleep to let the OS naturally close old TIME_WAIT sockets and free up FDs
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
          continue;
      }

      // Log other errors but keep listening
      log_error("Accept failed: ", errno);
      continue;
    }

    // disable Nagle's algorithm on this new socket
    if (setsockopt(new_socket, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0) {
      log_error("Disabling Nagle's Algorithm failed: ", errno);
      close(new_socket);
      continue;
    }

    #ifdef __linux__
      // Disable Delayed ACKs
      if (setsockopt(new_socket, IPPROTO_TCP, TCP_QUICKACK, &opt, sizeof(opt)) < 0) {
        log_error("Enabling TCP_QUICKACK failed: ", errno);
      }
    #endif

    // Enable Aggressive TCP Keepalives
    int keepalive = 1;
    if (setsockopt(new_socket, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(keepalive)) < 0) {
      log_error("Enabling SO_KEEPALIVE failed: ", errno);
    }

    #ifdef __linux__
      // Customize the keepalive thresholds (Default Linux is 2 hours! We want 8 seconds total)
      int idle = 5;       // Start sending probes after 5 seconds of total silence
      int interval = 1;   // Send a new probe every 1 second
      int maxpkt = 3;     // If 3 probes drop in a row, the peer is dead. Close the socket.

      setsockopt(new_socket, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
      setsockopt(new_socket, IPPROTO_TCP, TCP_KEEPINTVL, &interval, sizeof(interval));
      setsockopt(new_socket, IPPROTO_TCP, TCP_KEEPCNT, &maxpkt, sizeof(maxpkt));
    #endif

    // Expand OS Buffers to handle massive AppendEntries
    int buf_size = 1024 * 1024 * 2; // 2 Megabytes
    setsockopt(new_socket, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
    setsockopt(new_socket, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));

#ifndef NDEBUG
    char clientname[1024];
    std::cout << "Connected to "
              << inet_ntop(AF_INET, &client_addr.sin_addr, clientname, sizeof(clientname))
              << std::endl;
#endif

    try {
      // Register the socket before spawning the thread
      {
        std::lock_guard<std::mutex> lock(thread_mutex);
        active_sockets.insert(new_socket);
      }

      // Spawn the thread
      std::thread client_thread([this, new_socket]() {
        // handle_client manages its own execution and removes the 
        // socket from active_sockets upon exit.
        this->handle_client(new_socket);
      });

      pin_thread_to_control_cores(client_thread);
      client_thread.detach();
    } catch (const std::system_error& e) {
      log_error("CRITICAL: OS refused to spawn more threads! Dropping connection.", errno);
      
      // Rollback the socket registration
      {
        std::lock_guard<std::mutex> lock(thread_mutex);
        active_sockets.erase(new_socket);
      }
      close(new_socket);
    }
  }
}

// Thread-per-connection model. Persistently reads from a specific client socket until EOF
void StaticClusterDHTNode::handle_client(int client_socket) {  
  // Wrap the raw socket in User-Space buffer
  BufferedSocket buffered_sock(client_socket);

  // Pre-allocate required buffers
  std::vector<std::pair<uint32_t, uint32_t>> tx_batch;
  tx_batch.reserve(BATCH_SIZE);
  std::vector<uint8_t> response_buffer;
  response_buffer.reserve(BATCH_SIZE);

  // Instantiate a telemetry batcher for this thread
  TelemetryBatcher batcher{};
  batcher.stats = &this->stats;

  while (running.load(std::memory_order_relaxed)) {
    // Read the universal 8-byte network envelope
    uint8_t* env_ptr = buffered_sock.read_ptr(sizeof(NetworkEnvelope));
    if (!env_ptr) [[unlikely]] {
      #ifndef NDEBUG
        if (running && errno != 0 && errno != ECONNRESET) {
          log_error("Could not read NetworkEnvelope", errno);
        }
      #endif
      break; // EOF or connection dropped
    }

    NetworkEnvelope env{};
    std::memcpy(&env, env_ptr, sizeof(NetworkEnvelope));

    // Read the payload
    uint8_t* payload_ptr = nullptr;
    if (env.payload_size > 0) {
      payload_ptr = buffered_sock.read_ptr(env.payload_size);
      if (!payload_ptr) [[unlikely]] {
        log_error("Failed to read payload bytes", errno);
        goto cleanup;
      }
    }

    switch(env.protocol_type) {
      // DHT commands
      case ProtocolType::ClientDht: {
        DhtCommand cmd = static_cast<DhtCommand>(env.command_type);

        if (cmd == DhtCommand::Quit) [[unlikely]] {
          std::cout << "Client requested disconnect.\n";
          goto cleanup;
        }

        switch (cmd) {
          case DhtCommand::Put: {
            if (env.payload_size == 0 || (env.payload_size % sizeof(PutRequest) != 0)) [[unlikely]] {
                goto cleanup;
            }

            size_t batch_count = env.payload_size / sizeof(PutRequest);
            std::vector<std::pair<uint32_t, uint32_t>> client_kv_pairs;
            client_kv_pairs.reserve(batch_count);

            for (size_t i = 0; i < batch_count; ++i) {
              PutRequest req;
              std::memcpy(&req, payload_ptr + (i * sizeof(PutRequest)), sizeof(PutRequest));
              client_kv_pairs.push_back({req.key, req.value});
            }

            // Delegate to the transaction manager
            TransactionResult result = TransactionResult::Aborted;
            if (tx_manager) [[likely]] {
               result = tx_manager->execute_transaction(client_kv_pairs);
            }

            // Respond to the follower node
            uint8_t final_status = static_cast<uint8_t>(
                result == TransactionResult::Committed ? PutResult::Inserted : PutResult::Failed
            );
            
            response_buffer.assign(batch_count, final_status);
            if (send(client_socket, response_buffer.data(), batch_count, MSG_NOSIGNAL) != static_cast<ssize_t>(batch_count)) [[unlikely]] {
                goto cleanup;
            }
            break;
          }

          case DhtCommand::Get: {
            GetRequest req{};
            std::memcpy(&req, payload_ptr, sizeof(GetRequest));
            synchronize_clock(req.timestamp);

            // Read from local storage
            auto res = get_local(req.key, batcher);
            
            GetResponse response{};
            if (res.has_value()) {
              response = GetResponse::success(res.value().value, res.value().timestamp);
            } else {
              response = GetResponse::not_found(0);
            }

            if (send(client_socket, &response, sizeof(GetResponse), MSG_NOSIGNAL) != sizeof(GetResponse)) [[unlikely]]
              goto cleanup;

            break;
          }

          case DhtCommand::LogIntent: {
            uint8_t response = 0;

            // Only propose command if this node is currently the Raft Leader
            if (consensus_engine && consensus_engine->get_role() == ConsensusRole::LEADER) {
              // Propose the intent to the Raft WAL.
              // This function will naturally block until a Control Quorum (majority) is reached.
              bool success = consensus_engine->propose_command(payload_ptr, env.payload_size);
              response = success ? 1 : 0;
            }

            // Send the ACK/NACK back to the Follower who is coordinating the transaction
            if (send(client_socket, &response, 1, MSG_NOSIGNAL) != 1) [[unlikely]] {
              goto cleanup;
            }
            break;
          }

          case DhtCommand::Barrier: {
            {
              std::lock_guard<std::mutex> lock(barrier_mtx);
              barrier_checkins.insert(env.sender_id);
            }
            barrier_cv.notify_all();

            uint8_t ack = 1; 
            if (send(client_socket, &ack, 1, MSG_NOSIGNAL) != 1) [[unlikely]]
              goto cleanup;
            break;
          }

          case DhtCommand::ExitBarrier: {
            {
              std::lock_guard<Spinlock> lock(exit_mtx.mutex);
              exited_peers.insert(env.sender_id);
            }

            uint8_t ack = 1;
            if (send(client_socket, &ack, 1, MSG_NOSIGNAL) != 1) [[unlikely]]
              goto cleanup;
            break;
          }

          case DhtCommand::Ping:
          case DhtCommand::Go: {
            if (cmd == DhtCommand::Go)
              benchmark_ready.store(true, std::memory_order_release);
            
            uint8_t ack = 1; 
            if (send(client_socket, &ack, 1, MSG_NOSIGNAL) != 1) [[unlikely]]
              goto cleanup;
            break;
          }

          default: break;
        }
        break; // End ClientDht
      }

      // 2PC commands
      case ProtocolType::TwoPhaseCommit: {
        TwoPhaseCommitCommand cmd = static_cast<TwoPhaseCommitCommand>(env.command_type);

        switch (cmd) {
          case TwoPhaseCommitCommand::Prepare: {
            TxPrepareHeader header{};
            std::memcpy(&header, payload_ptr, sizeof(TxPrepareHeader));
            synchronize_clock(header.tx_timestamp);

            size_t expected_payload = sizeof(TxPrepareHeader) + (header.batch_size * 8);
            if (env.payload_size < expected_payload) [[unlikely]] {
              log_error("Corrupted PREPARE packet: Payload too small", 0);
              goto cleanup;
            }

            tx_batch.clear();
            for (size_t i = 0; i < header.batch_size; ++i) {
                uint32_t k, v;
                std::memcpy(&k, payload_ptr + sizeof(TxPrepareHeader) + (i * 8), 4);
                std::memcpy(&v, payload_ptr + sizeof(TxPrepareHeader) + (i * 8) + 4, 4);
                tx_batch.push_back({k, v});
            }

            bool prepared = local_tx_prepare(header.tx_timestamp, header.coordinator_id, tx_batch, batcher);

            TxPrepareResponse response{};
            response.remote_clock = logical_clock.load(std::memory_order_relaxed);
            response.vote = static_cast<uint8_t>(prepared ? 1 : 0);

            if (send(client_socket, &response, sizeof(TxPrepareResponse), MSG_NOSIGNAL) != sizeof(TxPrepareResponse)) [[unlikely]]
              goto cleanup;
            break;
          }

          case TwoPhaseCommitCommand::Commit:
          case TwoPhaseCommitCommand::Abort:
          case TwoPhaseCommitCommand::StatusCheck: {
            TxCommitHeader header{};
            std::memcpy(&header, payload_ptr, sizeof(TxCommitHeader));

            if (cmd == TwoPhaseCommitCommand::StatusCheck) {
              bool is_committed = false;
              if (tx_manager != nullptr) [[likely]] {
                is_committed = tx_manager->is_transaction_committed(header.tx_timestamp);
              }
              uint8_t response = is_committed ? 1 : 0;
              if (send(client_socket, &response, 1, MSG_NOSIGNAL) != 1) [[unlikely]] goto cleanup;
            } else {
              synchronize_clock(header.tx_timestamp);
              if (cmd == TwoPhaseCommitCommand::Commit) {
                local_tx_commit(header.tx_timestamp, batcher);
              } else {
                local_tx_abort(header.tx_timestamp, batcher);
              }
              uint8_t ack = 1;
              if (send(client_socket, &ack, 1, MSG_NOSIGNAL) != 1) [[unlikely]] goto cleanup;
            }
            break;
          }
        }
        break; // End TwoPhaseCommit
      }

      // Consensus Engine
      case ProtocolType::Paxos:
      case ProtocolType::Raft: {
        // Offload to consensus engine
        if (consensus_engine) {
          consensus_engine->on_network_message(env.sender_id, env.command_type, payload_ptr, env.payload_size);
        }
        break; // End Consensus
      }

      default:
        std::cerr << "[Network] Unknown Protocol Type: " << static_cast<int>(env.protocol_type) << "\n";
        break;
    }
  }

  cleanup:
    close(client_socket);

    {
      std::lock_guard<std::mutex> lock(thread_mutex);
      active_sockets.erase(client_socket);
    }
}

// Background recovery thread
void StaticClusterDHTNode::recovery_dispatcher_loop() {
  std::vector<RecoveryTask> tasks_to_process;
  tasks_to_process.reserve(32);

  while (running.load(std::memory_order_relaxed)) {
    {
      std::unique_lock<std::mutex> lock(recovery_mtx);
      recovery_cv.wait_for(lock, std::chrono::milliseconds(500), [this]() {
        return !running.load(std::memory_order_relaxed) || !recovery_queue.empty();
      });

      if (!running.load(std::memory_order_relaxed)) {
        break; 
      }

      if (recovery_queue.empty()) {
        continue; 
      }

      tasks_to_process.swap(recovery_queue);
    }

    // Use thread pool to execute transactions
    for (const auto& task : tasks_to_process) {
      this->thread_pool.submit_task([this, task]() {
        TelemetryBatcher local_batcher;
        local_batcher.stats = &this->stats;

        if (task.cmd == TwoPhaseCommitCommand::Commit) {
          this->send_tx_commit(task.target_id, task.tx_timestamp, local_batcher);
        } else {
          this->send_tx_abort(task.target_id, task.tx_timestamp, local_batcher);
        }
      });
    }

    // Clear local buffers for the next iteration
    tasks_to_process.clear();
  }
}

bool StaticClusterDHTNode::recv_n_bytes(const int sock, void* buffer, const size_t n) {
  size_t total_read = 0;
  char *buf_ptr = static_cast<char *>(buffer);

  while (total_read < n) {
    // MSG_WAITALL forces the kernel to block until exactly 'n' bytes are read.
    ssize_t received = recv(sock, buf_ptr + total_read, n - total_read, MSG_WAITALL);

    if (received < 0) {
      if (errno == EINTR)
        continue; // Retry if interrupted by system signal

      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        log_error("Socket read timed out", errno);
        return false; 
      }

      return false;
    }

    if (received == 0) {
      // EOF detected
      errno = ECONNRESET; // "Connection reset by peer"
      return false;
    }

    total_read += received;
  }
  return true;
}

bool StaticClusterDHTNode::perform_rpc_fire_and_forget(
  int sock, ProtocolType proto, uint8_t cmd, const uint8_t* payload, size_t size
) {
    NetworkEnvelope env{};
    env.protocol_type = proto;
    env.command_type = cmd;
    env.payload_size = static_cast<uint16_t>(size);
    env.sender_id = self_config.id;

    struct iovec iov[2];
    iov[0].iov_base = &env;
    iov[0].iov_len = sizeof(NetworkEnvelope);
    
    int iovcnt = 1;
    if (size > 0 && payload != nullptr) {
        iov[1].iov_base = const_cast<uint8_t*>(payload);
        iov[1].iov_len = size;
        iovcnt = 2;
    }

    struct msghdr msg{};
    msg.msg_iov = iov;
    msg.msg_iovlen = iovcnt;

    // MSG_DONTWAIT prevents the thread from blocking if TCP buffer is full
    // MSG_NOSIGNAL prevents crashing if the peer disconnected
    ssize_t sent = sendmsg(sock, &msg, MSG_NOSIGNAL | MSG_DONTWAIT);
    
    // Return true if the kernel successfully accepted the bytes
    return sent >= 0; 
}

// Scatter-Gather I/O execution
RpcResult StaticClusterDHTNode::perform_rpc_single_request(
  const int sock, ProtocolType proto, uint8_t cmd, const uint8_t *request,
  const size_t request_size, uint8_t *response, const size_t response_size
) {
  static_assert(BATCH_SIZE * sizeof(PutRequest) <= std::numeric_limits<uint16_t>::max(), 
              "BATCH_SIZE exceeds maximum TCP payload capacity");

  // Construct the 8-byte header
  NetworkEnvelope env;
  env.protocol_type = proto;
  env.command_type = cmd;
  env.payload_size = static_cast<uint16_t>(request_size);
  env.sender_id = self_config.id;

  // Writev eliminates memcpy overhead by passing two pointers directly to the kernel
  struct iovec iov[2];
  iov[0].iov_base = &env;
  iov[0].iov_len = sizeof(NetworkEnvelope);

  int iov_count = 1;
  if (request_size > 0) [[likely]] {
    iov[1].iov_base = const_cast<uint8_t *>(request);
    iov[1].iov_len = request_size;
    iov_count = 2;
  }

  const size_t total_expected = sizeof(NetworkEnvelope) + request_size;
  struct msghdr msg{}; 
  msg.msg_iov = iov;
  msg.msg_iovlen = iov_count;

  // Handle partial writes
  size_t total_sent = 0;
  while (total_sent < total_expected) {
    ssize_t sent = sendmsg(sock, &msg, MSG_NOSIGNAL);
    
    if (sent < 0) {
      if (errno == EINTR)
        continue;
      
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        log_error("Socket buffer full (EAGAIN) during sendmsg", errno);
        return RpcResult::SendFailed; // Safe to retry the request
      }

      log_error("Error sending RPC request via sendmsg", errno);
      return RpcResult::SendFailed; // Safe to retry the request
    }

    total_sent += sent;

    // Advance the iovec array pointers if we experienced a partial write
    if (total_sent < total_expected) {
      size_t advanced = sent;
      while (msg.msg_iovlen > 0 && advanced >= msg.msg_iov[0].iov_len) {
        advanced -= msg.msg_iov[0].iov_len;
        msg.msg_iov++; // Move pointer to the next iovec
        msg.msg_iovlen--; // Reduce the count of remaining iovecs
      }

      // If there is still a remainder, adjust the base of the current leading iovec
      if (msg.msg_iovlen > 0 && advanced > 0) {
        msg.msg_iov[0].iov_base = static_cast<char*>(msg.msg_iov[0].iov_base) + advanced;
        msg.msg_iov[0].iov_len -= advanced;
      }
    }
  }

  // Process the fixed-length response (if expected)
  if (response_size > 0) {
    if (!recv_n_bytes(sock, response, response_size)) {
      if (errno != ECONNRESET && errno != EPIPE && errno != ETIMEDOUT) {
        log_error("Error receiving response from single request", errno);
      }
      return RpcResult::RecvFailed; // Unsafe to retry
    }
  }

  return RpcResult::Success;
}

// Wraps the RPC in a bounded retry loop, managing socket checkout and return 
// against the Thread-Safe Connection Pool
bool StaticClusterDHTNode::send_single_request(
  const int target_id, const std::string &target_ip, const int target_port,
  ProtocolType proto, uint8_t cmd, const uint8_t *request, size_t request_size,
  uint8_t *response, size_t response_size
) {
  const int max_attempts = 2;

  for (int attempt = 0; attempt < max_attempts; ++attempt) {
    // Get an active connection from the pool or create a new connection
    int sock = connection_pool.get_connection(target_id, target_ip, target_port);
    if (sock < 0) {
      // Hardware/OS failure: Out of file descriptors
      return false;
    }

    RpcResult result = perform_rpc_single_request(sock, proto, cmd, request,
                                                   request_size, response, response_size);

    if (result == RpcResult::Success) {
      connection_pool.return_connection(target_id, sock, false);
      return true;
    }

    // Regardless of the error type, the socket is now in an undefined/broken state. 
    connection_pool.return_connection(target_id, sock, true);

    if (result == RpcResult::RecvFailed) {
      // Unsure if the remote node executed the command before dropping the response
      return false; 
    }

    // result == RpcResult::SendFailed, safe to retry
  }

  return false;
}