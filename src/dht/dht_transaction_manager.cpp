#include "dht/dht_transaction_manager.h"
#include "dht/dht_static_partitioning.h"
#include "common/xxHash64.h"

#include <iostream>
#include <algorithm>
#include <cstring>
#include <future>

DHTTransactionManager::DHTTransactionManager(StaticClusterDHTNode &node)
    : dht_node(node) {}

void DHTTransactionManager::record_transaction_decision(uint64_t tx_timestamp, bool is_commit) {
  std::lock_guard<Spinlock> lock(registry_mutex.mutex);
  transaction_registry[tx_timestamp] = is_commit;
}

bool DHTTransactionManager::is_transaction_committed(uint64_t tx_timestamp) const {
  std::lock_guard<Spinlock> lock(registry_mutex.mutex);
  auto it = transaction_registry.find(tx_timestamp);
  
  if (it != transaction_registry.end()) [[likely]] {
    return it->second;
  }
  
  // If we have no memory of this transaction, default to false (Abort).
  // This is safe: if a transaction isn't in the log, it never committed.
  return false; 
}

// Client API
TransactionResult DHTTransactionManager::execute_transaction(const std::vector<std::pair<uint32_t, uint32_t>>& kv_pairs) {
  if (kv_pairs.empty()) [[unlikely]]
    return TransactionResult::Committed;

  // Reroute to the leader if this node is a follower
  if (!dht_node.consensus_engine || dht_node.consensus_engine->get_role() != ConsensusRole::LEADER) {
    int leader_id = dht_node.consensus_engine ? dht_node.consensus_engine->get_leader_id() : -1;

    if (leader_id == -1 || leader_id == dht_node.self_config.id || leader_id >= static_cast<int>(dht_node.cluster_map.size())) {
      return TransactionResult::Aborted; // Cluster is electing a new leader
    }

    // Forward the request to the leader
    const NodeConfig &leader_node = dht_node.cluster_map[leader_id];
    int proxy_sock = dht_node.connection_pool.get_connection(leader_node.id, leader_node.ip, leader_node.port);
    if (proxy_sock < 0)
      return TransactionResult::Aborted;
    
    // Serialize the payload
    std::vector<PutRequest> req_buffer(kv_pairs.size());
    for (size_t i = 0; i < kv_pairs.size(); ++i) {
      // Leader will generate the timestamp
      req_buffer[i] = PutRequest(kv_pairs[i].first, kv_pairs[i].second, 0);
    }

    std::vector<uint8_t> proxy_response(kv_pairs.size());
    RpcResult proxy_status = dht_node.perform_rpc_single_request(
      proxy_sock, ProtocolType::ClientDht, static_cast<uint8_t>(DhtCommand::Put), 
      reinterpret_cast<const uint8_t*>(req_buffer.data()), req_buffer.size() * sizeof(PutRequest), 
      proxy_response.data(), proxy_response.size()
    );

    dht_node.connection_pool.return_connection(leader_node.id, proxy_sock, proxy_status != RpcResult::Success);

    // If the Leader successfully committed, it returns PutResult::Inserted.
    return (proxy_status == RpcResult::Success && proxy_response[0] == static_cast<uint8_t>(PutResult::Inserted)) 
           ? TransactionResult::Committed : TransactionResult::Aborted;
  }

  // This node is the current leader
  // Perform the operation with consensus protocol
  uint64_t tx_ts = dht_node.generate_hlc_timestamp();

  std::unordered_map<int, std::vector<std::pair<uint32_t, uint32_t>>> cohort_batches;
  for (const auto& kv : kv_pairs) {
    auto replicas = dht_node.get_replica_nodes(kv.first);
    for (int i = 0; i < dht_node.replication_degree; ++i) {
      cohort_batches[replicas.node_ids[i]].push_back(kv);
    }
  }

  if (cohort_batches.size() > 6) [[unlikely]] {
    return TransactionResult::Aborted; // Exceeds packed struct size
  }

  CoordinatorCommitIntent intent{};
  intent.tx_timestamp = tx_ts;
  intent.num_shards = 0;
  for (const auto& [cohort_id, _] : cohort_batches) {
    intent.target_shards[intent.num_shards++] = static_cast<uint8_t>(cohort_id);
  }

  // Quorum 2PC phase 1: PREPARE
  std::unordered_map<uint32_t, int> successful_prepares_per_key;
  AlignedSpinlock tally_mutex;
  std::vector<std::future<void>> prepare_futures;
  prepare_futures.reserve(8);

  for (const auto& [cohort_id, batch] : cohort_batches) {
    // Launch each network call asynchronously
    prepare_futures.push_back(dht_node.thread_pool.submit_task( 
      [&, cohort_id = cohort_id, batch = batch]() {
        TelemetryBatcher local_batcher;
        local_batcher.stats = &dht_node.stats;
        bool vote_yes = false;

        if (cohort_id == dht_node.self_config.id) {
          vote_yes = dht_node.local_tx_prepare(tx_ts, dht_node.self_config.id, batch, local_batcher);
        } else {
          vote_yes = dht_node.send_tx_prepare(cohort_id, tx_ts, batch, local_batcher);
        }

        // Safely tally the votes as they arrive
        if (vote_yes) {
          std::lock_guard<Spinlock> lock(tally_mutex.mutex);
          for (const auto& kv : batch) {
             successful_prepares_per_key[kv.first]++;
          }
        }
      }
    ));
  }

  // Wait for all async calls to finish or timeout
  for (auto& fut : prepare_futures) {
    fut.get(); 
  }

  // Verify quorum
  bool phase1_success = true;
  int required_quorum = (dht_node.replication_degree / 2) + 1;
  
  for (const auto& kv : kv_pairs) {
    if (successful_prepares_per_key[kv.first] < required_quorum) {
      phase1_success = false;
      break; 
    }
  }

  // Phase 2: Commit/Abort through Log Replication
  if (phase1_success && dht_node.consensus_engine && dht_node.consensus_engine->get_role() == ConsensusRole::LEADER) {
    intent.decision = 1;
    uint8_t payload_buffer[sizeof(CoordinatorCommitIntent) + 1];
    payload_buffer[0] = static_cast<uint8_t>(TwoPhaseCommitCommand::Commit);
    std::memcpy(payload_buffer + 1, &intent, sizeof(CoordinatorCommitIntent));

    bool success = dht_node.consensus_engine->propose_command(payload_buffer, sizeof(payload_buffer));
    
    if (success) {
      // The commands will be applied asynchronously
      return TransactionResult::Committed;
    }
  }

  // Phase 1 failed, or Consensus failed, or lost leadership in the during this operation
  // Bypass consensus and instruct all shards to release logical locks
  intent.decision = 0;
  record_transaction_decision(tx_ts, false);
  
  for (int i = 0; i < intent.num_shards; ++i) {
    dht_node.enqueue_for_async_recovery(intent.target_shards[i], TwoPhaseCommitCommand::Abort, tx_ts);
  }

  return TransactionResult::Aborted;
}

GetResponse DHTTransactionManager::get_sync(const uint32_t& key) {
  // Get the Primary and all Backup nodes for this key
  auto replicas = dht_node.get_replica_nodes(key);
  int required_quorum = (dht_node.replication_degree / 2) + 1;

  std::vector<std::future<GetResponse>> read_futures;
  read_futures.reserve(4);

  // Issue all reads simultaneously
  for (int i = 0; i < dht_node.replication_degree; ++i) {
    int target_id = replicas.node_ids[i];
    
    read_futures.push_back(dht_node.thread_pool.submit_task([&, target_id]() {
      // Thread-Local Batcher
      TelemetryBatcher local_batcher;
      local_batcher.stats = &dht_node.stats;

      if (target_id == dht_node.self_config.id) {
        auto res = dht_node.get_local(key, local_batcher); 
        if (res.has_value()) {
          return GetResponse::success(res.value().value, res.value().timestamp);
        }
        return GetResponse::not_found(0);
      } else {
        const NodeConfig& target = dht_node.cluster_map[target_id];
        return dht_node.get_remote(key, 0, target, local_batcher);
      }
    }));
  }

  int successful_reads = 0;
  uint64_t highest_ts = 0;
  std::optional<uint32_t> best_value = std::nullopt;

  // Process responses as futures complete
  for (auto& fut : read_futures) {
    GetResponse response = fut.get();
    
    if (response.status != GetStatus::NetworkError) [[likely]] {
      successful_reads++;
      
      // Resolve Last-Write-Wins
      if (response.timestamp >= highest_ts) {
        highest_ts = response.timestamp;
        if (response.status == GetStatus::Found) {
          best_value = response.value;
        } else {
          best_value = std::nullopt; 
        }
      }
    }
  }

  if (successful_reads >= required_quorum) {
    return best_value.has_value() ? GetResponse::success(best_value.value(), highest_ts) 
                                  : GetResponse::not_found(highest_ts);
  }

  // If the loop finishes and ALL replicas returned NetworkError, the quorum is dead.
  return GetResponse::error();
}