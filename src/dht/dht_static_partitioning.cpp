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
#include <endian.h>
#include <bit>
#include <random>

void StaticClusterDHTNode::apply_committed_log(uint64_t commit_index, const uint8_t *data, size_t data_len) {
  if (data_len < 1) [[unlikely]]
    return;

  // Extract the log type
  uint8_t log_type = data[0];
  const uint8_t* payload = data + 1;
  size_t payload_size = data_len - 1;

  switch (log_type) {
    // Case A: Standard replicated KV storage
    case static_cast<uint8_t>(DhtCommand::Put): {
      if (payload_size == sizeof(PutRequest)) [[likely]] {
        PutRequest req;
        std::memcpy(&req, payload, sizeof(PutRequest));
        synchronize_clock(req.timestamp);
        storage.put(req.key, req.value, req.timestamp);
      }
      break;
    }

    // Case B: Replicated 2PC coordinator log
    case static_cast<uint8_t>(TwoPhaseCommitCommand::Commit):
    case static_cast<uint8_t>(TwoPhaseCommitCommand::Abort): {
      if (payload_size == sizeof(CoordinatorCommitIntent)) [[likely]] {
        CoordinatorCommitIntent intent;
        std::memcpy(&intent, payload, sizeof(CoordinatorCommitIntent));

        bool is_commit = (intent.decision == 1);
        TwoPhaseCommitCommand phase2_cmd = is_commit ? TwoPhaseCommitCommand::Commit 
                                                     : TwoPhaseCommitCommand::Abort;

        // Update the decision to transaction manager for fast status check
        if (tx_manager) [[likely]] {
            tx_manager->record_transaction_decision(intent.tx_timestamp, is_commit);
        }

        // Push to recovery queue
        {
          std::lock_guard<std::mutex> lock(recovery_mtx);
          recovery_queue.reserve(recovery_queue.size() + intent.num_shards);
          
          for (uint16_t i = 0; i < intent.num_shards; ++i) {
            recovery_queue.push_back({intent.target_shards[i], phase2_cmd, intent.tx_timestamp});
          }
        }

        recovery_cv.notify_all();
      }
      break;
    }

    default:
      #ifndef NDEBUG
        std::cerr << "[State Machine] Unknown log discriminator: " << static_cast<int>(log_type) << "\n";
      #endif
      break;
  }

  // Advance the state machine index
  stats.consensus_state_machine_applied.fetch_add(1, std::memory_order_relaxed);
}

void StaticClusterDHTNode::send_message(int target_node_id, ProtocolType proto, uint8_t command_type,
                                        const uint8_t* payload, size_t payload_size) 
{
  if (target_node_id < 0 || target_node_id >= static_cast<int>(cluster_map.size()))
    return;

  if (target_node_id == self_config.id) [[unlikely]] {
    if (consensus_engine) {
      consensus_engine->on_network_message(self_config.id, command_type, payload, payload_size);
    }
    return;
  }
  
  const NodeConfig& target = cluster_map[target_node_id];

  int sock = connection_pool.get_connection(target.id, target.ip, target.port);
  if (sock >= 0) {
      // Dispatch immediately and return
      perform_rpc_fire_and_forget(sock, proto, command_type, payload, payload_size);
      connection_pool.return_connection(target.id, sock, false);
  }
}

std::vector<int> StaticClusterDHTNode::get_peer_ids() const {
  // Returns a copy of the precomputed vector.
  return cached_peer_ids;
}