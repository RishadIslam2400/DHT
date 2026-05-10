#include <benchmark/benchmark.h>
#include <random>
#include <thread>

#include "hash_table/timestamped_striped_lock_concurrent_hash_table.h"

// Helper to generate random keys
std::vector<uint32_t> generate_keys(size_t num_keys) {
  std::vector<uint32_t> keys(num_keys);
  std::mt19937 gen(42);
  std::uniform_int_distribution<uint32_t> dist(1, 10000000);
  for (size_t i = 0; i < num_keys; ++i) keys[i] = dist(gen);
  return keys;
}

// Concurrent writes (High Contention)
static void BM_ConcurrentPuts(benchmark::State& state) {
  // Setup a shared table for all threads
  static TimestampedStripedLockConcurrentHashTable<uint32_t, uint32_t> table(100000, 65536);
  static auto keys = generate_keys(1000000);
  
  uint64_t ts = 1;
  size_t i = 0;

  for (auto _ : state) {
    // Threads loop through the keys and pound the table with writes
    table.put(keys[i % keys.size()], 42, ts++);
    i++;
  }
}

// Run with 1, 4, 8, and 16 threads
BENCHMARK(BM_ConcurrentPuts)->Threads(1)->Threads(4)->Threads(8)->Threads(16);

// Benchmark: Mixed Read/Write (80% Reads, 20% Writes - Typical DHT workload)
static void BM_MixedReadWrite(benchmark::State& state) {
  static TimestampedStripedLockConcurrentHashTable<uint32_t, uint32_t> table(100000, 65536);
  static auto keys = generate_keys(1000000);
  
  // Pre-populate the table
  if (state.thread_index() == 0) {
    for (int i = 0; i < 50000; ++i) table.put(keys[i], 42, 1);
  }

  std::mt19937 gen(state.thread_index());
  std::uniform_int_distribution<int> op_dist(1, 100);
  size_t i = 0;

  for (auto _ : state) {
    uint32_t key = keys[i % keys.size()];
    if (op_dist(gen) <= 80) {
      benchmark::DoNotOptimize(table.get(key));
    } else {
      table.put(key, 99, 2);
    }
    i++;
  }
}
BENCHMARK(BM_MixedReadWrite)->Threads(8);

BENCHMARK_MAIN();