#include "dht/dht_static_partitioning.h"
#include "dht/dht_transaction_manager.h"

#include <random>
#include <iomanip>

// Global Metrics
std::atomic<uint64_t> total_latency_ns{0};
std::atomic<uint64_t> total_ops_completed{0};

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
      // Generate exactly 3 keys for the 2PC transaction
      for (int k = 0; k < 3; ++k) {
        op.key[k] = key_dist(rng);
        op.val[k] = val_dist(rng);
      }
    }
    operations.push_back(op);
  }

  // Signal this thread is ready
  threads_ready_count++;
  while(!start_benchmark_flag.load(std::memory_order_acquire)) {
    #if defined(__x86_64__)
      __builtin_ia32_pause();
    #else
      std::this_thread::yield();
    #endif
  }

  uint64_t local_latency_ns = 0;
  auto* ops_ptr = operations.data();
  size_t n_ops = operations.size();
  DHTTransactionManager batcher(*node);

  auto start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < n_ops; ++i) {
    const OpData &op = ops_ptr[i];

    if (op.type == OpType::GET) {
      batcher.get_sync(op.key[0]);
    } 
    else if (op.type == OpType::PUT) {
      batcher.put_sync(op.key[0], op.val[0]);
    } 
    else {
      std::vector<std::pair<uint32_t, uint32_t>> batch(3);
      batch[0] = {op.key[0], op.val[0]};
      batch[1] = {op.key[1], op.val[1]};
      batch[2] = {op.key[2], op.val[2]};
      batcher.multi_put(batch);
    }
  }
  // batcher.flush_all();

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  
  thread_end_times[thread_id] = end;
  total_latency_ns.fetch_add(duration, std::memory_order_relaxed);
  total_ops_completed.fetch_add(n_ops, std::memory_order_relaxed);
}

int main(int argc, char** argv) {
  // Argument Parsing
  if (argc < 6) {
      std::cerr << "Usage: " << argv[0] << " <config_file> <node_id> <num_ops> <num_threads> <key_range>\n";
      return 1;
  }

  std::string config_file = argv[1];
  int node_id = std::stoi(argv[2]);
  int total_num_ops = std::stoi(argv[3]);
  int num_threads = std::stoi(argv[4]);
  int key_range = std::stoi(argv[5]);
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
    StaticClusterDHTNode node(cluster_map, self_config, key_range, num_locks);

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

    // wait for other threads to finish data generation
    while(threads_ready_count.load() < num_threads) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    std::cout << "[TestApp] All threads ready. Starting measurement.\n";

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

    std::chrono::duration<double> total_wall_time = latest_end- global_start;

    uint64_t storage_puts = node.stats.local_puts_inserted.load()
                            + node.stats.local_puts_updated.load();
    uint64_t storage_gets = node.stats.local_gets_found.load()
                            + node.stats.local_gets_not_found.load();
    uint64_t total_storage_ops = storage_puts + storage_gets;
   
    // Storage Goodput: Physical reads/writes handled by this node's RAM
    double storage_iops = total_storage_ops / total_wall_time.count();

    uint64_t ops_generated = total_ops_completed.load();
    
    // Client Throughput: Logical transactions pushed by this node's worker threads
    double client_tps = ops_generated / total_wall_time.count();

    uint64_t failed_network_puts = node.stats.remote_puts_failed.load();
    uint64_t failed_network_gets = node.stats.remote_gets_failed.load();
    uint64_t total_network_failures = failed_network_puts + failed_network_gets;

    double avg_latency_ns = static_cast<double>(total_latency_ns.load()) / ops_generated;
    double avg_latency_us = avg_latency_ns / 1000.0;

    std::cout << "Benchmark RESULTS (Node " << node_id << ")\n";
    node.print_status(); 

    std::cout << "\nClient Experience\n";
    std::cout << "Logical Transactions:   " << ops_generated << "\n";
    std::cout << "Client Throughput:      " << std::fixed << std::setprecision(2) << client_tps << " tx/sec\n";
    std::cout << "Average Latency:        " << std::fixed << std::setprecision(2) << avg_latency_us << " us\n";
    std::cout << "Network Drop/Failures:  " << total_network_failures << "\n";
    std::cout << "Total Wall Time:        " << total_wall_time.count() << " s\n";

    std::cout << "\nNode Workload (Storage Backend)\n";
    std::cout << "Physical KV Operations: " << total_storage_ops << "\n";
    std::cout << "Storage IOPS:           " << std::fixed << std::setprecision(2) << storage_iops << " ops/sec\n";
    std::cout << "2PC Transactions Aborted: " << node.stats.tx_aborted.load() << "\n";

    std::cout << "\n[TestApp] Benchmark complete. Entering grace period before shutdown...\n";
    std::this_thread::sleep_for(std::chrono::seconds(10));

    node.stop();

  } catch (const std::exception& e) {
      std::cerr << "[Fatal Error] " << e.what() << std::endl;
      return 1;
  }

  return 0;
}