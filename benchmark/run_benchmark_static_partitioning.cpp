#include "dht/dht_static_partitioning.h"
#include "dht/dht_transaction_manager.h"

#include <random>
#include <iomanip>
#include <chrono>
#include <iostream>
#include <vector>
#include <thread>

// Global Metrics
std::atomic<uint64_t> total_latency_ns{0};
std::atomic<uint64_t> total_ops_completed{0};
std::atomic<uint64_t> client_side_aborts{0};

// Synchronization primitives for the barrier
std::atomic<int> threads_ready_count{0};
std::atomic<bool> start_benchmark_flag{false};

// To store exact finish times per thread
std::vector<std::chrono::high_resolution_clock::time_point> thread_end_times;

enum class OpType { 
  GET, 
  PUT, 
  MULTI_PUT
};

struct OpData {
  OpType type;
  uint32_t key[3];
  uint32_t val[3];
};

void do_benchmark(StaticClusterDHTNode* node, int thread_id, int ops_count, 
                  int key_range, int node_id)
{
  std::vector<OpData> operations;
  operations.reserve(ops_count);

  std::seed_seq seq{node_id, thread_id, 42};
  std::mt19937 rng(seq);
  std::uniform_int_distribution<int> key_dist(0, key_range - 1);
  std::uniform_int_distribution<int> val_dist(1, 10000);
  std::uniform_int_distribution<int> op_dist(1, 100);

  // Generate the 60/20/20 Workload Mix
  for (int i = 0; i < ops_count; ++i) {
    OpData op;
    int op_roll = op_dist(rng);

    if (op_roll <= 60) {
      op.type = OpType::GET;
      op.key[0] = key_dist(rng);
    } 
    else if (op_roll <= 80) {
      op.type = OpType::PUT;
      op.key[0] = key_dist(rng);
      op.val[0] = val_dist(rng);
    } 
    else {
      op.type = OpType::MULTI_PUT;

      // Generate unique keys. Bound it safely in case key_range < 3.
      int num_keys = std::min(3, key_range);
      for (int k = 0; k < num_keys; ++k) {
        uint32_t candidate_key;
        bool is_duplicate;

        do {
          is_duplicate = false;
          candidate_key = key_dist(rng);

          // Check against previously generated keys in this specific batch
          for (int prev = 0; prev < k; ++prev) {
            if (op.key[prev] == candidate_key) {
              is_duplicate = true;
              break;
            }
          }
        } while (is_duplicate);

        op.key[k] = candidate_key;
        op.val[k] = val_dist(rng);
      }

      // Pad remaining slots to avoid uninitialized data if key_range < 3
      for (int k = num_keys; k < 3; ++k) {
        op.key[k] = op.key[0]; 
        op.val[k] = op.val[0];
      }
    }
    operations.push_back(op);
  }

  // Signal this thread is ready
  threads_ready_count.fetch_add(1, std::memory_order_relaxed);
  while(!start_benchmark_flag.load(std::memory_order_acquire)) {
    #if defined(__x86_64__)
      __builtin_ia32_pause();
    #else
      std::this_thread::yield();
    #endif
  }

  auto* ops_ptr = operations.data();
  size_t n_ops = operations.size();
  DHTTransactionManager batcher(*node);

  std::vector<std::pair<uint32_t, uint32_t>> tx_batch;
  tx_batch.reserve(3);

  uint64_t local_aborts = 0;

  auto start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < n_ops; ++i) {
    const OpData &op = ops_ptr[i];

    if (op.type == OpType::GET) {
      auto result = batcher.get_sync(op.key[0]);
    } else {
      tx_batch.clear(); // O(1) clear, resets size but keeps capacity
      int num_unique = (op.type == OpType::PUT) ? 1 : std::min(3, key_range);

      for (int k = 0; k < num_unique; ++k) {
        tx_batch.push_back({op.key[k], op.val[k]});
      }

      TransactionResult result = batcher.execute_transaction(tx_batch);

      if (result != TransactionResult::Committed) {
        local_aborts++;
      }
    }
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  
  thread_end_times[thread_id] = end;
  total_latency_ns.fetch_add(duration, std::memory_order_relaxed);
  total_ops_completed.fetch_add(n_ops, std::memory_order_relaxed);
  client_side_aborts.fetch_add(local_aborts, std::memory_order_relaxed);
}

int main(int argc, char** argv) {
  if (argc < 7) {
    std::cerr << "Usage: " << argv[0] << " <config_file> <node_id> <num_ops> <num_threads> <key_range> <replication_degree>\n";
    return 1;
  }

  std::string config_file = argv[1];
  int node_id = std::stoi(argv[2]);
  int total_num_ops = std::stoi(argv[3]);
  int num_threads = std::stoi(argv[4]);
  int key_range = std::stoi(argv[5]);
  int replication_degree = std::stoi(argv[6]);

  int num_ops_per_thread = total_num_ops / num_threads;

  thread_end_times.resize(num_threads);

  try {
    // Load configuration and find self
    std::vector<NodeConfig> cluster_map = load_config(config_file);
    
    bool found = false;
    NodeConfig self_config;
    for (const NodeConfig& node : cluster_map) {
      if (node.id == node_id) {
        self_config = node;
        found = true;
        break;
      }
    }

    if (!found) {
      throw std::runtime_error("Config file does not contain node ID " + std::to_string(node_id));
    }

    size_t total_peers = cluster_map.size() - 1;
    size_t num_locks = total_peers * num_threads * 4;

    // Pass nullptr for the IConsensusEngine
    StaticClusterDHTNode node(cluster_map, self_config, key_range * 2, num_locks, replication_degree);

    node.start(); 
    node.warmup_network(num_threads);
    node.wait_for_barrier();

    #ifndef NDEBUG
      if (node_id == 0) {
        std::cout << "[TestApp] Barrier passed. Preparing workload (" << total_num_ops << " ops)...\n";
      }
    #endif

    std::vector<std::thread> workers;

    // spawn threads they will wait at the barrier
    for (int i = 0; i < num_threads; ++i) {
      workers.emplace_back(do_benchmark, &node, i, num_ops_per_thread, key_range, node_id);
    }

    // Wait for all local threads to finish memory allocation and PRNG generation
    while(threads_ready_count.load() < num_threads) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    #ifndef NDEBUG
      std::cout << "[TestApp] All threads ready. Starting measurement.\n";
    #endif

    auto global_start = std::chrono::high_resolution_clock::now();
    start_benchmark_flag.store(true, std::memory_order_release);

    for (auto& t : workers) {
      t.join();
    }

    // find the latest end time among all threads
    auto latest_end = global_start;
    for (const auto& t_end : thread_end_times) {
      if (t_end > latest_end) {
        latest_end = t_end;
      }
    }

    std::cout << "\n[TestApp] Local benchmark complete. Waiting for all peers at exit barrier..." << std::endl;
    node.wait_for_exit_barrier();

    std::cout << "[TestApp] Exit barrier cleared. Stopping node..." << std::endl;
    node.stop();

    // Metrics calculation
    std::chrono::duration<double> total_wall_time = latest_end- global_start;

    // Calculate IOPS
    uint64_t total_storage_ops = node.stats.local_puts_inserted.load()
                               + node.stats.local_puts_updated.load()
                               + node.stats.local_puts_dropped.load()
                               + node.stats.local_gets_found.load()
                               + node.stats.local_gets_not_found.load();

    // Goodput Volume: Only count successful operations
    uint64_t goodput_ops = node.stats.local_puts_inserted.load()
                         + node.stats.local_puts_updated.load()
                         + node.stats.local_gets_found.load();
   
    double storage_iops = total_storage_ops / total_wall_time.count();
    double server_goodput_iops = goodput_ops / total_wall_time.count();

    // Aggregate Client Metrics
    uint64_t ops_generated = total_ops_completed.load();
    double client_tps = ops_generated / total_wall_time.count();

    double avg_latency_ns = static_cast<double>(total_latency_ns.load()) / ops_generated;
    double avg_latency_us = avg_latency_ns / 1000.0;
    
    uint64_t app_aborts = client_side_aborts.load();

    node.print_status();

    // Aggregate Network & Coordinator Metrics
    uint64_t failed_network_puts = node.stats.remote_puts_failed.load();
    uint64_t failed_network_gets = node.stats.remote_gets_failed.load();
    uint64_t total_network_failures = failed_network_puts + failed_network_gets;

    uint64_t tx_aborted = node.stats.local_tx_aborted.load();
    uint64_t tx_committed = node.stats.local_tx_committed.load();
    uint64_t total_tx = tx_aborted + tx_committed;

    double abort_rate = (total_tx > 0) ? (static_cast<double>(tx_aborted) / total_tx) * 100.0 : 0.0;

    std::cout << "Benchmark RESULTS (Node " << node_id << ")\n";
    node.print_status(); 

    std::cout << "[Benchmark Macro Performance]\n";
    std::cout << "  Total Wall Time:          " << std::fixed << std::setprecision(4) << total_wall_time.count() << " s\n";
    std::cout << "  Total Client Operations:  " << ops_generated << "\n";
    std::cout << "  Client-Side Aborts:       " << app_aborts << " (Rejected by Gateway/Elections)\n";
    std::cout << "  Client Throughput (TPS):  " << std::fixed << std::setprecision(2) << client_tps << " tx/sec\n";
    std::cout << "  Average TX Latency:       " << std::fixed << std::setprecision(2) << avg_latency_us << " us\n";
    std::cout << "  Storage Throughput:       " << std::fixed << std::setprecision(2) << storage_iops << " IOPS\n";
    std::cout << "  Server Goodput:           " << std::fixed << std::setprecision(2) << server_goodput_iops << " IOPS\n\n";

    std::cout << "[TestApp] Shutdown complete." << std::endl;

  } catch (const std::exception& e) {
      std::cerr << "[Fatal Error] " << e.what() << std::endl;
      return 1;
  }

  return 0;
}