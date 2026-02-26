#include "dht/dht_static_partitioning.h"
#include "dht/dht_message_batcher.h"

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

struct OpData {
  int key;
  int val;
  bool is_put;
};

void do_benchmark(StaticClusterDHTNode* node, int thread_id, int ops_count, 
            int key_range, int seed, int put_prob = 20)
{
  std::vector<OpData> operations;
  operations.reserve(ops_count);

  std::mt19937 rng(seed);
  std::uniform_int_distribution<int> key_dist(0, key_range - 1);
  std::uniform_int_distribution<int> val_dist(1, 10000);
  std::uniform_int_distribution<int> op_dist(1, 100);

  for (int i = 0; i < ops_count; ++i) {
    OpData op;
    op.key = key_dist(rng);
    op.is_put = (op_dist(rng) <= put_prob);
    if (op.is_put) {
        op.val = val_dist(rng);
    }
    operations.push_back(op);
  }

  // Signal this thread is ready
  threads_ready_count++;

  // Spin wait until main thread signals start
  // We use yield to be nice to the scheduler, but for strict benchmarking 
  // on dedicated cores, a tight spin is sometimes preferred.
  while(!start_benchmark_flag.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }

  uint64_t local_latency_ns = 0;

  auto* ops_ptr = operations.data();
  size_t n_ops = operations.size();

  DHTMessageBatcher batcher(*node);

  auto start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < n_ops; ++i) {
    const OpData &op = ops_ptr[i];

    if (op.is_put) {
      batcher.put(op.key, op.val);
    } else {
      GetResponse res = batcher.get(op.key);
    }
  }
  batcher.flush_all();

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  local_latency_ns += duration;

  thread_end_times[thread_id] = std::chrono::high_resolution_clock::now();
  total_latency_ns += local_latency_ns;
  total_ops_completed += n_ops;
}

int main(int argc, char** argv) {
  // 1. Argument Parsing
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
        int seed = (node_id * 10000) + (i * 100) + std::time(nullptr);
        workers.emplace_back(do_benchmark, &node, i, num_ops_per_thread, key_range, seed, 20);
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

    // Only count operations physically executed in this node's storage layer
    uint64_t storage_puts = node.stats.local_puts_inserted.load() + 
                            node.stats.local_puts_updated.load();
    
    uint64_t storage_gets = node.stats.local_gets_found.load() + 
                            node.stats.local_gets_not_found.load();

    uint64_t total_storage_ops = storage_puts + storage_gets;
    double goodput_ops_sec = total_storage_ops / total_wall_time.count();

    // Track the latency for this client
    uint64_t ops_generated = total_ops_completed.load();
    
    uint64_t failed_network_puts = node.stats.remote_puts_failed.load();
    uint64_t failed_network_gets = node.stats.remote_gets_failed.load();
    uint64_t total_network_failures = failed_network_puts + failed_network_gets;

    double avg_latency_ns = static_cast<double>(total_latency_ns.load()) / ops_generated;
    double avg_latency_us = avg_latency_ns / 1000.0;

    std::cout << "Benchmark RESULTS (Node " << node_id << ")\n";
    node.print_status(); 

    std::cout << "\nPerformance Metrics\n";
    std::cout << "\nSystem Goodput (Storage Layer)\n";
    std::cout << "Total Operations Processed: " << total_storage_ops << "\n";
    std::cout << "Node Throughput:            " << std::fixed << std::setprecision(2) << goodput_ops_sec << " ops/sec\n";

    std::cout << "\nClient Experience (Network Layer)\n";
    std::cout << "Operations Initiated:       " << ops_generated << "\n";
    std::cout << "Network Drops/Failures:     " << total_network_failures << "\n";
    std::cout << "Average Client Latency:     " << std::fixed << std::setprecision(2) << avg_latency_us << " us\n";
    std::cout << "Total Wall Time:            " << total_wall_time.count() << " s\n";

    std::cout << std::endl;

    std::cout << "\n[TestApp] Benchmark complete. Entering grace period before shutdown...\n";
    // Allow slower nodes to finish routing their final packets to this node's storage layer
    std::this_thread::sleep_for(std::chrono::seconds(10));

    node.stop();

  } catch (const std::exception& e) {
      std::cerr << "[Fatal Error] " << e.what() << std::endl;
      return 1;
  }

  return 0;
}