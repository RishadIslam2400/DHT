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

struct Phase1Quorum {
  std::mutex mtx;
  std::condition_variable cv;
  std::unordered_map<uint32_t, int> successful_prepares_per_key;
  int rpcs_completed = 0;
  bool early_quorum_achieved = false;
};

// Client API
TransactionResult DHTTransactionManager::execute_transaction(const std::vector<std::pair<uint32_t, uint32_t>>& kv_pairs) {
  auto e2e_start = std::chrono::high_resolution_clock::now();
  if (kv_pairs.empty()) [[unlikely]]
    return TransactionResult::Committed;

  // Generate the timestamp
  uint64_t tx_ts = dht_node.generate_hlc_timestamp();

  std::unordered_map<int, std::vector<std::pair<uint32_t, uint32_t>>> cohort_batches;
  std::vector<uint32_t> keys_only;
  keys_only.reserve(8);

  for (const auto& kv : kv_pairs) {
    keys_only.push_back(kv.first);
    auto replicas = dht_node.get_replica_nodes(kv.first);
    for (int i = 0; i < dht_node.replication_degree; ++i) {
      cohort_batches[replicas.node_ids[i]].push_back(kv);
    }
  }

  if (cohort_batches.size() > 6) [[unlikely]]
    return TransactionResult::Aborted;

  CoordinatorCommitIntent intent{};
  intent.tx_timestamp = tx_ts;
  intent.num_shards = 0;
  for (const auto& [cohort_id, _] : cohort_batches) {
    intent.target_shards[intent.num_shards++] = static_cast<uint8_t>(cohort_id);
  }

  // Phase 1: PREPARE (Current node is the coordinator)
  auto quorum_counter = std::make_shared<Phase1Quorum>();
  int total_rpcs = cohort_batches.size();
  int required_quorum = (dht_node.replication_degree / 2) + 1;

  for (const auto& [cohort_id, batch] : cohort_batches) {
    // Launch each network call asynchronously
    dht_node.thread_pool.submit_task(
      [&dht_node = this->dht_node, quorum_counter, cohort_id, batch, tx_ts, keys_only, required_quorum, total_rpcs]() {
        TelemetryBatcher local_batcher;
        local_batcher.stats = &dht_node.stats;
        bool vote_yes = false;

        if (cohort_id == dht_node.self_config.id) {
          vote_yes = dht_node.local_tx_prepare(tx_ts, dht_node.self_config.id, batch, local_batcher);
        } else {
          vote_yes = dht_node.send_tx_prepare(cohort_id, tx_ts, batch, local_batcher);
        }

        {
          std::lock_guard<std::mutex> lock(quorum_counter->mtx);
          if (vote_yes) {
            for (const auto& kv : batch) {
              quorum_counter->successful_prepares_per_key[kv.first]++;
            }
          }
          
          quorum_counter->rpcs_completed++;

          bool all_met = true;
          for (uint32_t key : keys_only) {
            if (quorum_counter->successful_prepares_per_key[key] < required_quorum) {
              all_met = false;
              break; 
            }
          }

          if (all_met) {
            quorum_counter->early_quorum_achieved = true;
          }
        }
        quorum_counter->cv.notify_one();
      }
    );
  }

  // Wait only until quorum is met or all RPCs finish/fail
  {
    std::unique_lock<std::mutex> lock(quorum_counter->mtx);
    quorum_counter->cv.wait(lock, [&]() {
      return quorum_counter->early_quorum_achieved || quorum_counter->rpcs_completed == total_rpcs;
    });
  }

  // Phase 1.5: Consensus
  bool consensus_achieved = false;

  if (quorum_counter->early_quorum_achieved && dht_node.consensus_engine) {
    intent.decision = 1; 
    uint8_t payload_buffer[sizeof(CoordinatorCommitIntent) + 1];
    payload_buffer[0] = static_cast<uint8_t>(TwoPhaseCommitCommand::Commit);
    std::memcpy(payload_buffer + 1, &intent, sizeof(CoordinatorCommitIntent));

    if (dht_node.consensus_engine->get_role() == ConsensusRole::LEADER) {
      consensus_achieved = dht_node.consensus_engine->propose_command(payload_buffer, sizeof(payload_buffer));
    } else {
      int leader_id = dht_node.consensus_engine->get_leader_id();
      if (leader_id != -1 && leader_id < static_cast<int>(dht_node.cluster_map.size())) {
        const NodeConfig &leader_node = dht_node.cluster_map[leader_id];
        int proxy_sock = dht_node.connection_pool.get_connection(leader_node.id, leader_node.ip, leader_node.port);
        
        if (proxy_sock >= 0) {
          uint8_t proxy_response = 0;
          RpcResult proxy_status = dht_node.perform_rpc_single_request(
            proxy_sock, ProtocolType::ClientDht, static_cast<uint8_t>(DhtCommand::LogIntent), 
            payload_buffer, sizeof(payload_buffer), 
            &proxy_response, 1
          );

          dht_node.connection_pool.return_connection(leader_node.id, proxy_sock, proxy_status != RpcResult::Success);
          consensus_achieved = (proxy_status == RpcResult::Success && proxy_response == 1);
        }
      }
    }
  }

  // Phase 2: Commit
  if (consensus_achieved) {
    for (int i = 0; i < intent.num_shards; ++i) {
      int target_id = intent.target_shards[i];
      
      // Grab the size of the payload being sent to this specific shard
      size_t keys_in_shard = cohort_batches[target_id].size();

      if (target_id != dht_node.self_config.id) {
        dht_node.stats.remote_puts_success.fetch_add(keys_in_shard, std::memory_order_relaxed);
      }

      dht_node.thread_pool.submit_task([&dht_node = this->dht_node, target_id = intent.target_shards[i], tx_ts]() {
         TelemetryBatcher local_batcher;
         local_batcher.stats = &dht_node.stats;
         dht_node.send_tx_commit(target_id, tx_ts, local_batcher);
      });
    }

    auto e2e_end = std::chrono::high_resolution_clock::now();
    dht_node.stats.coordinator_tx_total_ns.fetch_add(
      std::chrono::duration_cast<std::chrono::nanoseconds>(e2e_end - e2e_start).count(), 
      std::memory_order_relaxed
    );
    return TransactionResult::Committed;
  }

  // Phase 2: Abort (Rollback Path)
  intent.decision = 0;
  record_transaction_decision(tx_ts, false);
  
  for (int i = 0; i < intent.num_shards; ++i) {
    int target_id = intent.target_shards[i];
    size_t keys_in_shard = cohort_batches[target_id].size();

    if (target_id != dht_node.self_config.id) {
      dht_node.stats.remote_puts_failed.fetch_add(keys_in_shard, std::memory_order_relaxed);
    }

    dht_node.enqueue_for_async_recovery(target_id, TwoPhaseCommitCommand::Abort, tx_ts);
  }

  auto e2e_end = std::chrono::high_resolution_clock::now();
  dht_node.stats.coordinator_tx_total_ns.fetch_add(
    std::chrono::duration_cast<std::chrono::nanoseconds>(e2e_end - e2e_start).count(), 
    std::memory_order_relaxed
  );

  return TransactionResult::Aborted;
}

struct ReadQuorumTally {
  std::mutex mtx;
  std::condition_variable cv;
  int successful_reads = 0;
  int completed_rpcs = 0;
  uint64_t highest_ts = 0;
  std::optional<uint32_t> best_value = std::nullopt;
};

GetResponse DHTTransactionManager::get_sync(const uint32_t key) {
  // Get the Primary and all Backup nodes for this key
  auto replicas = dht_node.get_replica_nodes(key);
  int required_quorum = (dht_node.replication_degree / 2) + 1;

  auto tally = std::make_shared<ReadQuorumTally>();

  // Issue all reads simultaneously
  for (int i = 0; i < dht_node.replication_degree; ++i) {
    int target_id = replicas.node_ids[i];
    
    dht_node.thread_pool.submit_task([&dht_node = this->dht_node, tally, target_id, key]() {
      TelemetryBatcher local_batcher;
      local_batcher.stats = &dht_node.stats;
      GetResponse response{};

      // Perform the get operation
      if (target_id == dht_node.self_config.id) {
        auto res = dht_node.get_local(key, local_batcher); 
        if (res.has_value()) {
          response = GetResponse::success(res.value().value, res.value().timestamp);
        } else {
          response = GetResponse::not_found(0);
        }
      } else {
        const NodeConfig& target = dht_node.cluster_map[target_id];
        response = dht_node.get_remote(key, 0, target, local_batcher);
      }

      // Process the response and tally
      {
        std::lock_guard<std::mutex> lock(tally->mtx);
        tally->completed_rpcs++;
        
        if (response.status != GetStatus::NetworkError) {
          tally->successful_reads++;
          
          // Resolve Last-Write-Wins
          if (response.timestamp >= tally->highest_ts) {
            tally->highest_ts = response.timestamp;
            if (response.status == GetStatus::Found) {
              tally->best_value = response.value;
            } else {
              tally->best_value = std::nullopt; 
            }
          }
        }
      }

      // Ping the main thread
      tally->cv.notify_one();
    });
  }

  // Verify quorum reads
  {
    std::unique_lock<std::mutex> lock(tally->mtx);
    tally->cv.wait(lock, [&]() {
      return tally->successful_reads >= required_quorum || tally->completed_rpcs == dht_node.replication_degree;
    });
  }

  // Final evaluation
  if (tally->successful_reads >= required_quorum) {
    return tally->best_value.has_value() ? GetResponse::success(tally->best_value.value(), tally->highest_ts) 
                                         : GetResponse::not_found(tally->highest_ts);
  }

  return GetResponse::error();
}