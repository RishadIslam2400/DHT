#include "dht/dht_static_partitioning.h"
#include "common/xxHash64.h"
#include "common/utils.h"

#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>


// -----------------------------Constructor/Destructor----------------------------------
StaticClusterDHTNode::StaticClusterDHTNode(std::vector<NodeConfig> map, NodeConfig self, int hash_table_size, int num_locks)
  : cluster_map{std::move(map)}, 
    self_config{std::move(self)},
    storage{hash_table_size, num_locks},
    server_fd{-1},
    running{false},
    connection_pool(cluster_map.size()),
    benchmark_ready{false}
{  
  std::cout << "Booted Node " << self_config.id 
            << " (" << self_config.ip << ":" << self_config.port << ")" << std::endl;
}

StaticClusterDHTNode::~StaticClusterDHTNode() {
  stop();
}

void StaticClusterDHTNode::start() {
  running = true;
  listener_thread = std::thread(&StaticClusterDHTNode::listen_loop, this);
}

void StaticClusterDHTNode::warmup_network(int connections_per_peer) {
  std::cout << "[Node " << self_config.id << "] Pre-warming " 
            << connections_per_peer << " connections to each peer..." << std::endl;
  
  for (NodeConfig& peer : cluster_map) {
    if (peer.id == self_config.id)
      continue;

    connection_pool.pre_warm(peer.id, peer.ip, peer.port, connections_per_peer);
  }

  std::cout << "[Node " << self_config.id << "] Network pre-warm complete." << std::endl;
}

void StaticClusterDHTNode::stop()
{
  if (!running)
    return;
    
  std::cout << "Stopping Node..." << std::endl;
  running = false;
  
  // Wake up listener thread (blocked in accept)
  if (server_fd > 0) {
    shutdown(server_fd, SHUT_RDWR);
    close(server_fd);
    server_fd = -1;
  }

  // Wake up all client threads (blocked in recv)
  {
    std::lock_guard<std::mutex> lock(thread_mutex);
    for (int sock : active_sockets) {
      shutdown(sock, SHUT_RDWR); 
      close(sock);
    }
    active_sockets.clear();
  }

  // Join listener thread
  if (listener_thread.joinable()) {
    listener_thread.join();
  }

  // Join all client threads
  {
    std::lock_guard<std::mutex> lock(thread_mutex);
    for (std::thread &t : client_threads) {
      if (t.joinable()) {
        t.join();
      }
    }
    client_threads.clear();
  }
  
  std::cout << "Node stopped gracefully." << std::endl;
}

void StaticClusterDHTNode::print_status() {    
  uint32_t total_local_puts = stats.local_puts_inserted + stats.local_puts_updated;
  uint32_t total_local_gets = stats.local_gets_found + stats.local_gets_not_found;

  std::cout << "\nNode " << self_config.id << " Statistics\n";
  std::cout << "Local Hash Table Metrics\n";
  std::cout << "  Total Local PUTs:    " << total_local_puts << "\n";
  std::cout << "    - Inserted:        " << stats.local_puts_inserted << "\n";
  std::cout << "    - Updated :        " << stats.local_puts_updated << "\n";
  
  std::cout << "\n";
  std::cout << "  Total Local GETs:    " << total_local_gets << "\n";
  std::cout << "    - Found:           " << stats.local_gets_found << "\n";
  std::cout << "    - Not Found:       " << stats.local_gets_not_found << "\n";
  
  std::cout << "\nRemote Operations Metrics\n";
  std::cout << "  Remote PUT Requests:\n";
  std::cout << "    - Success:         " << stats.remote_puts_success << "\n";
  std::cout << "    - Failed:          " << stats.remote_puts_failed << "\n";

  std::cout << "  Remote GET Requests:\n";
  std::cout << "    - Success:         " << stats.remote_gets_success << "\n";
  std::cout << "    - Failed:          " << stats.remote_gets_failed << "\n";
}

// -----------------------------Public API----------------------------------
// This put API is for a single insert, batched insert in the batcher class PUT
PutResult StaticClusterDHTNode::put(const uint32_t& key, const uint32_t& value) {
  const NodeConfig& target = get_target_node(key);

  // Local operation
  if (target.id == self_config.id) {
    bool result = put_local(key, value);
    return result ? PutResult::Inserted : PutResult::Updated;
  }

  // Remote operation
  uint32_t net_key = htonl(key);
  uint32_t net_value = htonl(value);
  
  uint8_t request[8];
  std::memcpy(request, &net_key, 4);
  std::memcpy(request + 4, &net_value, 4);

  uint8_t response;
  bool success = send_single_request(target.id, target.ip, target.port,
                                     CommandType::CMD_PUT, request, 8, &response, 1);

  if (!success) {
    stats.remote_puts_failed++;
    return PutResult::Failed;
  }

  PutResult remote_result = static_cast<PutResult>(response);
  if (remote_result == PutResult::Inserted || remote_result == PutResult::Updated) {
    stats.remote_puts_success++;
  } else {
    stats.remote_puts_failed++;
  }

  return remote_result;
}

// Single GET API, batcher uses the same GET
GetResponse StaticClusterDHTNode::get(const uint32_t& key) {
  const NodeConfig& target = get_target_node(key);

  // Local operation
  if (target.id == self_config.id) {
    std::optional<uint32_t> res = get_local(key);
    if (res.has_value()) {
      return GetResponse::success(res.value());
    } else {
      return GetResponse::not_found();
    }
  }

  // Remote operation
  return get_remote(key, target);
}

// distributed barrier: Node 0 acts as the coordinator
void StaticClusterDHTNode::wait_for_barrier() {
  if (self_config.id == 0) { // Coordinator Logic
    std::cout << "[Coordinator] Barrier initiated. Waiting for pings from peers...\n";
    size_t total_peers = cluster_map.size() - 1;

    // Block and wait for N-1 workers to check in
    {
      std::unique_lock<std::mutex> lock(barrier_mtx);
      barrier_cv.wait(lock, [&] {
        return barrier_checkins.size() >= total_peers || !running;
      });
    }

    if (!running)
      return;

    // Broadcast GO signal to all workers
    std::cout << "[Coordinator] All peers checked in. Fetching sockets...\n";

    // use the warm up connections from the connections from connection_pool
    std::vector<std::pair<int, int>> barrier_sockets;
    barrier_sockets.reserve(total_peers);

    for (const NodeConfig& node : cluster_map) {
      if (node.id == 0)
        continue;

      int sock = connection_pool.get_connection(node.id, node.ip, node.port);
      if (sock != -1) {
        barrier_sockets.push_back({node.id, sock});
      } else {
        std::cerr << "[Coordinator] CRITICAL: Failed to get pre-warmed socket for Node " << node.id << "\n";
      }
    }

    std::cout << "[Coordinator] Broadcasting GO signal...\n";
    uint8_t go_signal = 1;
    for (const auto& peer : barrier_sockets) {
      if (send(peer.second, &go_signal, 1, MSG_NOSIGNAL) != 1) {
        log_error("Failed to send go signal", errno);
      }
    }

    // Return sockets to the pool for the benchmark
    for (const auto& peer : barrier_sockets) {
      connection_pool.return_connection(peer.first, peer.second, false);
    }

    // other nodes set benchmark_ready to true after GO message
    benchmark_ready.store(true, std::memory_order_release);
  } else { // Worker logic
    std::cout << "[Worker] Reached barrier. Notifying Coordinator...\n";

    // Send the node id to coordinator for checkin
    const NodeConfig& coord = cluster_map[0];
    uint8_t response = 0;

    uint32_t net_id = htonl(self_config.id);
    uint8_t buf[4];
    std::memcpy(buf, &net_id, 4);

    bool success = false;
    while (running) {
      // Use the pre-warmed pool for the check-in as well
      int sock = connection_pool.get_connection(coord.id, coord.ip, coord.port);
      if (sock != -1) {
        success = send_single_request(coord.id, coord.ip, coord.port, CommandType::CMD_BARRIER, buf, 4, &response, 1);
        connection_pool.return_connection(coord.id, sock, !success);
      }
      if (success && response == 1) {
        std::cout << "[Worker] Coordinator got checkin notification. Waiting for GO...\n";
        break;
      }

      std::cout << "[Worker] Coordinator busy. Retrying...\n";
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    // wait until GO command and set benchmark_ready 
    while (running && !benchmark_ready.load(std::memory_order_acquire)) {
      #if defined(__x86_64__)
        __builtin_ia32_pause();
      #else
        std::this_thread::yield();
      #endif
    }
  }
}

// -------------------------------Utility Functions------------------------------------
// Receive exactly 'n' bytes. Returns true on success, false on failure/close.
bool StaticClusterDHTNode::recv_n_bytes(const int sock, void* buffer, const size_t n) {
  size_t total_read = 0;
  char *buf_ptr = static_cast<char *>(buffer);
  while (total_read < n) {
    ssize_t received = read(sock, buf_ptr + total_read, n - total_read);
    if (received < 0) {
      if (errno == EINTR)
        continue; // if the read call was interrupted then retry
      return false;
    }
    // recieved EOF before reading n bytes, discard the message
    if (received == 0)
      return false;
    total_read += received;
  }
  return true;
}

// Communication protocol for handling a single request
bool StaticClusterDHTNode::perform_rpc_single_request(const int sock, CommandType cmd, const uint8_t* request, size_t request_size, uint8_t* response, size_t response_size) {
  // Pack the command and request into a single buffer to avoid multiple send() syscalls
  uint8_t buffer[1024];
  buffer[0] = static_cast<uint8_t>(cmd);
  
  if (request_size > 0) {
    std::memcpy(buffer + 1, request, request_size);
  }

  size_t total_send_size = 1 + request_size;
  if (send(sock, buffer, total_send_size, MSG_NOSIGNAL) != static_cast<ssize_t>(total_send_size)) {
    log_error("Error sending single request", errno);
    return false;
  }

  // Process the response
  if (!recv_n_bytes(sock, response, 1)) {
    log_error("Error receiving response status", errno);
    return false;
  }

  // Handle variable length response for CMD_GET
  if (cmd == CommandType::CMD_GET && response[0] == static_cast<uint8_t>(GetStatus::Found)) {
    // The server found the key and is appending 4 bytes for the value.
    // We read them directly into the response buffer offset by 1.
    if (!recv_n_bytes(sock, response + 1, 4)) {
      log_error("Error recieving the value", errno);
      return false;
    }
  }

  return true;
}

// take active connections from pool to send request
bool StaticClusterDHTNode::send_single_request(const int target_id,
                                               const std::string& target_ip,
                                               const int target_port,
                                               CommandType cmd,
                                               const uint8_t* request,
                                               size_t request_size,
                                               uint8_t* response,
                                               size_t response_size)
{
  // Get a active connection from the pool or create a new connection
  int sock = connection_pool.get_connection(target_id, target_ip, target_port);
  if (sock < 0) {
    log_error("Invalid Socket", errno);
    return false;
  }

  bool success = perform_rpc_single_request(sock, cmd, request, request_size, response, response_size);

  if (success) {
    connection_pool.return_connection(target_id, sock, false); // Return to pool
    return true;
  }

  // Failed: The socket might have been stale (server closed it).
  // Destroy the old socket and try one more time with a fresh connection.
  connection_pool.return_connection(target_id, sock, true);

  sock = connection_pool.get_connection(target_id, target_ip, target_port);
  if (sock < 0) {
    log_error("Invalid Socket", errno);
    return false;
  }

  success = perform_rpc_single_request(sock, cmd, request, request_size, response, response_size);
  if (success) {
    connection_pool.return_connection(target_id, sock, false); // New socket is good, pool it
    return true;
  }

  connection_pool.return_connection(target_id, sock, true); // Failed twice, give up
  return false;
}

bool StaticClusterDHTNode::send_batch(const int target_id,
                                      const std::string& target_ip,
                                      const int target_port,
                                      const std::vector<PutRequest>& batch_requests)
{
  #ifndef NDEBUG
    if (batch_requests.empty() || batch_requests.size() > BATCH_SIZE) {
      return false;
    }
  #endif

  constexpr size_t MAX_SEND_BYTES = 3 + (BATCH_SIZE * 8);
  uint8_t send_buffer[MAX_SEND_BYTES];
  uint8_t recv_buffer[BATCH_SIZE];

  // Serialize all the requests into the send buffer
  uint16_t batch_size = static_cast<uint16_t>(batch_requests.size());
  uint16_t batch_size_net = htons(batch_size);

  send_buffer[0] = static_cast<uint8_t>(CommandType::CMD_BATCH_PUT);
  std::memcpy(&send_buffer[1], &batch_size_net, 2);
  uint8_t *buf_ptr = &send_buffer[3];

  // Copy all requests to buffer
  for (const PutRequest& request : batch_requests) {
    uint32_t net_key = htonl(request.key);
    uint32_t net_val = htonl(request.value);

    std::memcpy(buf_ptr, &net_key, 4);
    std::memcpy(buf_ptr + 4, &net_val, 4);
    buf_ptr += 8;
  }

  // Send the entire batch and recieve response
  size_t total_send_bytes = 3 + (batch_size * 8);
  size_t expected_response_bytes = batch_size;

  for (int attempt = 1; attempt <= 2; ++attempt) {
    int sock = connection_pool.get_connection(target_id, target_ip, target_port);
    if (sock < 0) {
      log_error("Invalid socket", errno);
      return false;
    }

    // Attempt send and receive
    bool success = false;
    if (send(sock, send_buffer, total_send_bytes, MSG_NOSIGNAL) == static_cast<ssize_t>(total_send_bytes)) {
      if (recv_n_bytes(sock, recv_buffer, expected_response_bytes)) {
        success = true;
      }
    }

    if (success) {
      // return healthy connection to connection pool
      connection_pool.return_connection(target_id, sock, false);

      // Note: If you need to track stats for the batch, you can loop through 
      // recv_buffer here and update stats.remote_puts_success/failed
      return true;
    }

    // Destroy the stale/broken socket and loop again
    connection_pool.return_connection(target_id, sock, true);
  }

  // Failed all attempts
  log_error("Batch request failed after retries", errno);
  return false;
}


// ------------------------------Local Operations------------------------------------
bool StaticClusterDHTNode::put_local(const uint32_t& key, const uint32_t& value) {
  bool added = storage.put(key, value);
  if (added) {
    stats.local_puts_inserted++;
  } else {
    stats.local_puts_updated++;
  }

  return added;
}

std::optional<uint32_t> StaticClusterDHTNode::get_local(const uint32_t& key) const {
  std::optional<uint32_t> res = storage.get(key);
  if (res.has_value()) {
    stats.local_gets_found++;
  } else {
    stats.local_gets_not_found++;
  }

  return res;
}

GetResponse StaticClusterDHTNode::get_remote(const uint32_t &key, const NodeConfig &target) {
  uint32_t net_key = htonl(key);
  uint8_t request[4];
  std::memcpy(request, &net_key, 4);

  uint8_t response[5];

  bool success = send_single_request(target.id, target.ip, target.port, CommandType::CMD_GET, request, 4, response, 5);

  if (!success) {
    stats.remote_gets_failed++;
    return GetResponse::error();
  }

  // Process the response
  GetStatus status = static_cast<GetStatus>(response[0]);
  switch (status) {
    case GetStatus::Found: {
      stats.remote_gets_success++;

      uint32_t net_val;
      std::memcpy(&net_val, response + 1, 4);
      return GetResponse::success(ntohl(net_val));
    }

    case GetStatus::NotFound:
      stats.remote_gets_success++;
      return GetResponse::not_found();

    case GetStatus::NetworkError:
    default:
      stats.remote_gets_failed++;
      return GetResponse::error();
  }
}

// ------------------------------Server Side Handling------------------------------------
// Handle accepted client launched on a new thread
// Thread per connection model: once a thread connects to a client it will recieve
// messages from the client until the client closes the connection or until timeout
void StaticClusterDHTNode::handle_client(int client_socket) {
  // Set a 60-second receive timeout
  struct timeval tv;
  tv.tv_sec = 60;
  tv.tv_usec = 0;
  setsockopt(client_socket, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof tv);

  uint8_t cmd_buf;

  // Persistent loop to continue reading until the node stops running
  while (running) {
    // Both the batch request and single request has a command type in the first byte
    if (!recv_n_bytes(client_socket, &cmd_buf, 1)) {
      #ifndef NDEBUG
        log_error("Could not read command byte", errno);
      #endif
      break;
    }

    CommandType cmd = static_cast<CommandType>(cmd_buf);

    // Clients wants to disconnect
    if (cmd == CommandType::CMD_QUIT) {
      std::cout << "Client requested disconnect.\n";
      break;
    }

    switch(cmd) {
      case CommandType::CMD_PUT: { // Single put request
        // Read 8 Bytes: [Key:4][Value:4]
        uint8_t buf[8];
        if (!recv_n_bytes(client_socket, buf, 8)) {
          log_error("Failed to read PUT request", errno);
          goto cleanup; // exit loop
        }
        uint32_t key, value;
        std::memcpy(&key, buf, 4);
        std::memcpy(&value, buf + 4, 4);
        PutRequest request{ntohl(key), ntohl(value)};

        // Process the request
        PutResult result = put(request.key, request.value);

        // Send 1 byte response
        if (send(client_socket, &result, 1, MSG_NOSIGNAL) != 1) {
          log_error("Failed to send the response to PUT", errno);
          goto cleanup;
        }

        break; // exit the switch
      }

      case CommandType::CMD_GET: { // Single GET request
        // Read 4 bytes: [Key:4]
        uint8_t buf[4];
        if (!recv_n_bytes(client_socket, buf, 4)) {
          log_error("Failed to read GET request", errno);
          goto cleanup; // exit loop
        }
        uint32_t key;
        std::memcpy(&key, buf, 4);
        GetRequest request{ntohl(key)};

        // Process the request
        GetResponse result = get(request.key);

        // Serialize response
        if (result.status == GetStatus::Found) {
          // Send 5 Bytes: [Status:1][Value:4]
          uint8_t buf[5];
          buf[0] = static_cast<uint8_t>(result.status);

          uint32_t net_val = htonl(result.value);
          std::memcpy(buf + 1, &net_val, 4);

          if (send(client_socket, buf, 5, MSG_NOSIGNAL) != 5) {
            log_error("Failed to send GET found response", errno);
            goto cleanup;
          }
        } else {
          // Send 1 Byte: [Status] (NotFound or Error)
          uint8_t status = static_cast<uint8_t>(result.status);
          if (send(client_socket, &status, 1, MSG_NOSIGNAL) != 1) {
            log_error("Failed to send GET status response", errno);
            goto cleanup;
          }
        }

        break;
      }

      case CommandType::CMD_BATCH_PUT: {
        uint8_t request_buffer[BATCH_SIZE * 8]; // Max size for batch puts
        uint8_t response_buffer[BATCH_SIZE];    // Max size for batch responses

        // Read Batch Count (2 bytes)
        uint16_t batch_count_net;
        if (!recv_n_bytes(client_socket, &batch_count_net, 2)) {
          log_error("Failed to read batch count", errno);
          goto cleanup;
        }

        uint16_t batch_count = ntohs(batch_count_net);
        if (batch_count > BATCH_SIZE) {
          std::cerr << "Invalid batch size: " << batch_count << "\n";
          goto cleanup;
        }

        // Read batch requests
        size_t total_req_bytes = batch_count * 8;
        if (!recv_n_bytes(client_socket, request_buffer, total_req_bytes)) {
          log_error("Failed to read batched requests", errno);
          goto cleanup;
        }

        // Process Loop
        uint8_t* req_ptr = request_buffer;
        uint8_t* resp_ptr = response_buffer;

        for (int i = 0; i < batch_count; ++i) {
          uint32_t k, v;
          std::memcpy(&k, req_ptr, 4);
          std::memcpy(&v, req_ptr + 4, 4);
          
          PutResult res = put(ntohl(k), ntohl(v));
          *resp_ptr = static_cast<uint8_t>(res);

          req_ptr += 8;
          resp_ptr++;
        }

        // Send Batch Response (N bytes)
        if (send(client_socket, response_buffer, batch_count, MSG_NOSIGNAL) != batch_count) {
          log_error("Failed to send batched response", errno);
          goto cleanup;
        }

        break;
      }

      case CommandType::CMD_BARRIER: {
        // Read the node id of the worker node
        uint8_t buf[4];
        if (!recv_n_bytes(client_socket, buf, 4)) {
          log_error("Failed to read node id for barrier", errno);
          goto cleanup;
        }

        uint32_t net_id;
        std::memcpy(&net_id, buf, 4);
        uint32_t worker_id = ntohl(net_id);

        // Add to checkins
        {
          std::lock_guard<std::mutex> lock(barrier_mtx);
          barrier_checkins.insert(worker_id);
        }

        barrier_cv.notify_all(); // wake up coordinator's wait_for_barrier()

        // Send 1 byte ACK
        uint8_t ack = 1; 
        if (send(client_socket, &ack, 1, MSG_NOSIGNAL) != 1) {
          log_error("Failed to send ack signal for barrier", errno);
          goto cleanup;
        }

        break;
      }

      case CommandType::CMD_PING: {
        // Simple Echo/ACK
        uint8_t ack = 1; 
        if (send(client_socket, &ack, 1, MSG_NOSIGNAL) != 1) {
          log_error("Failed to response to ping", errno);
          goto cleanup;
        }

        break;
      }

      case CommandType::CMD_GO: {
        // Used to signal start of benchmark
        uint8_t ack = 1;
        if (send(client_socket, &ack, 1, MSG_NOSIGNAL) != 1) {
          log_error("Failed to send bechmark signal", errno);
          goto cleanup;
        }

        benchmark_ready = true;
        break;
      }
    }
  }

  cleanup:
    close(client_socket);
}

// Listener thread creates a server socket which is listening for incoming connections
void StaticClusterDHTNode::listen_loop() {
  // create the socket
  server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    log_error("Error making server socket: ", errno);
    return;
  }

  // set socket options to use address in use
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
  if (listen(server_fd, 128) < 0) {
    log_error("Error listening on socket: ", errno);
    return;
  }

  std::cout << "Node listening on port " << self_config.port << "..." << std::endl;

  // server_fd socket accepts incoming connections in a persistent loop
  while (true) {
    // initialize client address to store client information
    sockaddr_in client_addr = {0};
    socklen_t client_addr_len = sizeof(client_addr);

    // accept a incoming connection that returns a new_socket to communicate with the client
    // listener thread is blocked on accept
    int new_socket = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
    if (new_socket < 0) {
      // Check if we are shutting down
      if (errno == EINVAL || errno == EBADF || !running) {
          break; 
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

    // debug
    char clientname[1024];
    std::cout << "Connected to "
              << inet_ntop(AF_INET, &client_addr.sin_addr, clientname, sizeof(clientname))
              << std::endl;

    std::thread client_thread(&StaticClusterDHTNode::handle_client, this, new_socket);
    
    // Safely track the socket and thread for graceful shutdown
    {
      std::lock_guard<std::mutex> lock(thread_mutex);
      active_sockets.push_back(new_socket);
      client_threads.push_back(std::move(client_thread));
    }

    // TODO: implement a threadpool instead of thread per connection
    // threadpool();
  }
}