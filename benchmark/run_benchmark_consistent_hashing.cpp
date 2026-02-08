#include "dht_consistent_hashing.h"

#include <random>
#include <iomanip>

// Global Metrics
std::atomic<uint64_t> total_latency_us{0};
std::atomic<uint32_t> total_ops_completed{0};

void worker(ConsistentHashingDHTNode* node, int ops_count, int key_range, int seed, int put_prob = 20) {
    std::mt19937 rng(std::random_device{}() + seed);
    std::uniform_int_distribution<int> key_dist(0, key_range - 1);
    std::uniform_int_distribution<int> val_dist(1, 10000);
    std::uniform_int_distribution<int> op_dist(1, 100);

    uint64_t local_latency_us = 0;
    int local_ops_completed = 0;

    for (int i = 0; i < ops_count; ++i) {
        int key = key_dist(rng);
        bool is_put = op_dist(rng) <= put_prob;

        auto start = std::chrono::high_resolution_clock::now();

        if (is_put) {
            int val = val_dist(rng);
            node->put(key, val);
        } else {
            node->get(key);
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

        local_latency_us += duration;
        local_ops_completed++;
    }

    total_latency_us += local_latency_us;
    total_ops_completed += local_ops_completed;
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

    try {
        ConsistentHashingDHTNode node(config_file, node_id);
        node.start();
        node.wait_for_barrier();

        if (node_id == 0) {
            std::cout << "[TestApp] Barrier passed. Starting workload with Key Range: " << key_range << "\n";
        }

        std::vector<std::thread> workers;
        auto global_start = std::chrono::high_resolution_clock::now();

        for (int i = 0; i < num_threads; ++i) {
            int seed = (node_id * 10000) + (i * 100) + std::time(nullptr);
            workers.emplace_back(worker, &node, num_ops_per_thread, key_range, seed, 20);
        }

        for (auto& t : workers) {
            t.join();
        }

        auto global_end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> total_wall_time = global_end - global_start;

        double total_ops = total_ops_completed.load();
        double throughput = total_ops / total_wall_time.count();
        double avg_latency_us = (double)total_latency_us.load() / total_ops;

        std::cout << "Benchmark RESULTS (Node " << node_id << ")\n";
        node.print_status(); 

        std::cout << "\nPerformance Metrics\n";
        std::cout << "Key Range:       " << key_range << "\n";
        std::cout << "Total Ops:       " << total_ops << "\n";
        std::cout << "Time Elapsed:    " << total_wall_time.count() << " s\n";
        std::cout << "Throughput:      " << std::fixed << std::setprecision(2) << throughput << " ops/sec\n";
        std::cout << "Average Latency: " << std::fixed << std::setprecision(2) << avg_latency_us << " us\n";

        node.stop();

    } catch (const std::exception& e) {
        std::cerr << "[Fatal Error] " << e.what() << std::endl;
        return 1;
    }

    return 0;
}