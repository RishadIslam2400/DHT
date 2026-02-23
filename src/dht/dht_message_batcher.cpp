#include "dht/dht_message_batcher.h"

// Flush the buffer for a node
void DHTMessageBatcher::flush_node(NodeBatch& node_batch) {
  if (node_batch.requests.empty())
    return;

  size_t count = node_batch.requests.size();
  bool success = false;
  int attempt = 0;
  static thread_local std::mt19937 generator(std::random_device{}());

  while (attempt < MAX_RETRIES && !success) {
    success = dht_node.send_batch(node_batch.id, node_batch.ip, node_batch.port, node_batch.requests);

    if (success) {
      dht_node.stats.remote_puts_success += count;
      break;
    } else {
      attempt++;
      if (attempt < MAX_RETRIES) {
        int delay_ms = BASE_DELAY_MS * (1 << (attempt - 1));
        std::uniform_int_distribution<int> distribution(delay_ms * 0.8, delay_ms * 1.2);
        int jittered_delay = distribution(generator);
        
        #ifndef NDEBUG
          std::cerr << "[Batcher] Retry " << attempt << "/" << MAX_RETRIES 
                    << " for Node " << node_batch.id 
                    << " after " << jittered_delay << "ms...\n";
        #endif

        std::this_thread::sleep_for(std::chrono::milliseconds(jittered_delay));
      }
    }
  }

  if (!success) {
    dht_node.stats.remote_puts_failed += count;
    
    #ifndef NDEBUG
      std::cerr << "[Batcher] CRITICAL: Dropping batch for Node " << node_batch.id 
                << " after " << MAX_RETRIES << " attempts.\n";
    #endif
  }
  
  // Clear the buffer. 
  node_batch.requests.clear();
}

// This PUT API is for batched insert
void DHTMessageBatcher::put(const uint32_t& key, const uint32_t& value) {
  const NodeConfig& target = dht_node.get_target_node(key);

    // if local no need to batch the reqeuest
  if (target.id == dht_node.self_config.id) {
    dht_node.put_local(key, value);
    return;
  }

  // lazy initialization
  NodeBatch &node_batch = buffers[target.id];
  if (node_batch.id == -1) {
    node_batch.id = target.id;
    node_batch.ip = target.ip;
    node_batch.port = target.port;
  }

  // put it into the buffer for target node
  node_batch.requests.emplace_back(key, value); //  endianness conversion done in send_batch()

  // flush the node if the buffer reaches limit
  if (node_batch.requests.size() >= BATCH_SIZE) {
    flush_node(node_batch);
  }
}

// Get requests are not batched, added for consistency
GetResponse DHTMessageBatcher::get(const uint32_t& key) {
  // Check local hash table
  const NodeConfig& target = dht_node.get_target_node(key);
  if (target.id == dht_node.self_config.id) {
    std::optional<uint32_t> res = dht_node.get_local(key);
    if (res.has_value()) {
      return GetResponse::success(res.value());
    }

    return GetResponse::not_found();
  }

  // Remote Read: enforce "Read-Your-Writes" consistency
  // Use buffer snooping to send the get response instantly
  auto it = buffers.find(target.id);
  if (it != buffers.end()) {
    const std::vector<PutRequest>& requests = it->second.requests;

    // Check if the key exists in the buffer from backwards
    // If exists send the value immediately
    for (auto rev_it = requests.rbegin(); rev_it != requests.rend(); ++rev_it) {
      if (rev_it->key == key) {
        return GetResponse::success(rev_it->value);
      }
    }
  }

  // Key not in the buffer, perform get remote operation
  return dht_node.get_remote(key, target);
}

// flush buffers for all the nodes
void DHTMessageBatcher::flush_all() {
    for (auto& entry : buffers) {
        flush_node(entry.second);
    }
}