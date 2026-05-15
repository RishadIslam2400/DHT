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

void StaticClusterDHTNode::apply_committed_log(const std::vector<std::pair<uint64_t, std::vector<uint8_t>>>& committed_batch) {
  if (committed_batch.empty())
    return;

  bool needs_dispatch = false;

  // Lock the recovery queue for the entire batch
  std::unique_lock<std::mutex> lock(recovery_mtx, std::defer_lock);

  bool is_leader = (consensus_engine && consensus_engine->get_role() == ConsensusRole::LEADER);
  if (is_leader) {
    lock.lock();
    recovery_queue.reserve(recovery_queue.size() + (committed_batch.size() * 3));
  }

  for (const auto& [commit_index, data_vec] : committed_batch) {
    if (data_vec.empty())
      continue;

    // Extract the log type
    uint8_t log_type = data_vec[0];
    const uint8_t* payload = data_vec.data() + 1;
    size_t payload_size = data_vec.size() - 1;

    if (log_type == static_cast<uint8_t>(TwoPhaseCommitCommand::Commit) ||
        log_type == static_cast<uint8_t>(TwoPhaseCommitCommand::Abort)) {

      if (payload_size == sizeof(CoordinatorCommitIntent))[[likely]] {
        CoordinatorCommitIntent intent;
        std::memcpy(&intent, payload, sizeof(CoordinatorCommitIntent));

        bool is_commit = (intent.decision == 1);
        TwoPhaseCommitCommand phase2_cmd = is_commit ? TwoPhaseCommitCommand::Commit 
                                                    : TwoPhaseCommitCommand::Abort;

        // Update the decision to transaction manager for fast status check
        if (tx_manager) [[likely]] {
          tx_manager->record_transaction_decision(intent.tx_timestamp, is_commit);
        }

        // Raft leader handles the recovery loop
        if (is_leader) {          
          for (uint16_t i = 0; i < intent.num_shards; ++i) {
            recovery_queue.push_back({intent.target_shards[i], phase2_cmd, intent.tx_timestamp});
          }
          needs_dispatch = true;
        }
      }
    } else {
      #ifndef NDEBUG
        std::cerr << "[State Machine] Unknown log discriminator: " << static_cast<int>(log_type) << "\n";
      #endif
    }
  }

  // Notify the dispatcher
    if (is_leader && needs_dispatch) {
      lock.unlock();
      recovery_cv.notify_all();
    }

  stats.consensus_state_machine_applied.fetch_add(committed_batch.size(), std::memory_order_relaxed);
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
      bool success = perform_rpc_fire_and_forget(sock, proto, command_type, payload, payload_size);

      // If success is false, the pool will destroy the socket and temporarily blacklist the node
      connection_pool.return_connection(target.id, sock, !success);
  }
}

std::vector<int> StaticClusterDHTNode::get_peer_ids() const {
  // Returns a copy of the precomputed vector.
  return cached_peer_ids;
}