#pragma once

#include <cstddef>
#include <string>
#include <vector>
#include <unordered_map>
#include <optional>
#include <arpa/inet.h>
#include <random>

#include "dht_common.h"
#include "node_properties.h"
#include "dht_static_partitioning.h"

constexpr int MAX_RETRIES = 2;
constexpr int BASE_DELAY_MS = 20;

// We want each thread to have their own buffer, instead of having a single buffer
// for all the threads. For a single buffer we have to synchronize access to the buffer
// from different threads. The constructor for the batcher will take the DHT class as a
// parameter. So all the threads will operate on the same DHT instead of a separate DHT
// object for each thread.

// This is client side batching. The client will batch the put requests before sending
// the requests over the tcp network. This will reduce the number of system calls 
// and network traffic. We opted out of server side batching since the in-memory
// DHT operations are significantly faster than the network I/O.

class DHTMessageBatcher {
private:
  StaticClusterDHTNode &dht_node;
  
  // Caching information for each node including requests
  struct NodeBatch {
    std::string ip;
    int port;
    int id;
    std::vector<PutRequest> requests;

    NodeBatch() : port(-1), id(-1) { requests.reserve(BATCH_SIZE); }
  };
  std::unordered_map<int, NodeBatch> buffers;

  // Flush the buffer for a node
  void flush_node(NodeBatch &node_batch);

public:
  DHTMessageBatcher(StaticClusterDHTNode& node)
    : dht_node(node) { }
  
  ~DHTMessageBatcher() {
    flush_all();
  }

  void flush_all();
  void put(const uint32_t &key, const uint32_t &value);
  GetResponse get(const uint32_t &key);
};