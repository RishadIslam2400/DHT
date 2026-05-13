#include "dht/dht_static_partitioning.h"
#include "common/xxHash64.h"
#include "common/utils.h"
#include "network/buffered_socket.h"
#include "dht/consensus_interface.h"

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
        const PutRequest* req = reinterpret_cast<const PutRequest*>(payload);

        // Advance Lamport clock
        synchronize_clock(req->timestamp);

        // Write directly to local storage
        storage.put(req->key, req->value, req->timestamp);
      }
      break;
    }

    // Case B: Replicated 2PC coordinator
    case static_cast<uint8_t>(TwoPhaseCommitCommand::Commit):
    case static_cast<uint8_t>(TwoPhaseCommitCommand::Abort): {
      if (payload_size == sizeof(CoordinatorCommitIntent)) [[likely]] {
        const CoordinatorCommitIntent* intent = reinterpret_cast<const CoordinatorCommitIntent*>(payload);

        // Offload the work to the recovery queue.
        TwoPhaseCommitCommand phase2_cmd = (intent->decision == 1) 
                                         ? TwoPhaseCommitCommand::Commit 
                                         : TwoPhaseCommitCommand::Abort;

        // Push all targets to the async queue.
        {
          std::lock_guard<Spinlock> lock(recovery_mtx.mutex);
          for (uint16_t i = 0; i < intent->num_shards; ++i) {
            recovery_queue.push_back({intent->target_shards[i], phase2_cmd, intent->tx_timestamp});
          }
        }
      }
      break;
    }

    case static_cast<uint8_t>(DhtCommand::BatchPut): {
      // Ensure the payload divides perfectly into PutRequest structs
      if (payload_size > 0 && (payload_size % sizeof(PutRequest) == 0)) [[likely]] {
        size_t batch_count = payload_size / sizeof(PutRequest);
        const PutRequest* requests = reinterpret_cast<const PutRequest*>(payload);

        // Execute the batch locally
        for (size_t i = 0; i < batch_count; ++i) {
            synchronize_clock(requests[i].timestamp);
            storage.put(requests[i].key, requests[i].value, requests[i].timestamp);
        }
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

// Background recovery thread
void StaticClusterDHTNode::recovery_dispatcher_loop() {
  TelemetryBatcher background_batcher;
  background_batcher.stats = &this->stats;

  // Allocate buffers with minimal capacity
  std::vector<RecoveryTask> tasks_to_process;
  std::vector<RecoveryTask> still_failed_tasks;
  tasks_to_process.reserve(32);
  still_failed_tasks.reserve(32);

  while (running.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Check the leader
    if (!consensus_engine || consensus_engine->get_role() != ConsensusRole::LEADER) {
      std::lock_guard<Spinlock> lock(recovery_mtx.mutex);
      if (!recovery_queue.empty()) {
        // We lost leadership. Drop volatile intents to prevent stale multi-casting 
        // if we are re-elected later.
        recovery_queue.clear();
      }
      continue;
    }

    {
      std::lock_guard<Spinlock> lock(recovery_mtx.mutex);
      if (recovery_queue.empty()) {
        continue;
      }
      tasks_to_process.swap(recovery_queue); 
    }

    std::vector<RecoveryTask> still_failed_tasks;

    // Process the orphaned Phase 2 intents
    for (const auto& task : tasks_to_process) {
      bool success = false;
      
      if (task.cmd == TwoPhaseCommitCommand::Commit) {
        success = send_tx_commit(task.target_id, task.tx_timestamp, background_batcher);
      } else {
        success = send_tx_abort(task.target_id, task.tx_timestamp, background_batcher);
      }

      // If it failed again (the target node is still physically offline), 
      // save it so we can push it back to the main queue.
      if (!success) {
        still_failed_tasks.push_back(task);
      }
    }

    // Return unresolved tasks to the master queue
    if (!still_failed_tasks.empty()) {
      std::lock_guard<Spinlock> lock(recovery_mtx.mutex);
      recovery_queue.insert(recovery_queue.end(), 
                            still_failed_tasks.begin(), 
                            still_failed_tasks.end());
      
      still_failed_tasks.clear(); 
    }

    // Clear local buffer for the next iteration
    tasks_to_process.clear();
  }
}

void StaticClusterDHTNode::send_message(int target_node_id, ProtocolType proto, uint8_t command_type,
                                        const uint8_t* payload, size_t payload_size) 
{
  if (target_node_id < 0 || target_node_id >= static_cast<int>(cluster_map.size()))
    return;
  const NodeConfig& target = cluster_map[target_node_id];

  send_single_request(target.id, target.ip, target.port, proto, command_type, 
                      payload, payload_size, nullptr, 0);
}