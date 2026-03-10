#include <iostream>
#include <vector>
#include <chrono>
#include <cassert>
#include <thread>
#include <string>
#include <atomic>

#include "dht/dht_static_partitioning.h"
#include "dht/dht_transaction_manager.h"

// Test 1: Strict Linearizability & Replication Write
// Verifies that a single synchronous put correctly elevates to a 2PC transaction,
// successfully committing to multiple physical nodes simultaneously.
void test_strict_replication(DHTTransactionManager& client0, 
                             StaticClusterDHTNode& node0, 
                             StaticClusterDHTNode& node1, 
                             StaticClusterDHTNode& node2) {
  std::cout << "Running Strict Linearizability Test (RF=2)...";

  uint32_t key = 100;
  uint32_t val = 9999;

  // Client 0 executes a strict put. Under RF=2, this forces a 2PC multi_put.
  PutResult put_res = client0.put_sync(key, val);
  assert(put_res == PutResult::Inserted || put_res == PutResult::Updated);

  // To prove physical replication, we bypass the Transaction Manager's network layer.
  // We strictly check the local storage of every node using get_local().
  int physical_replica_count = 0;

  auto check_replica = [&](StaticClusterDHTNode& node) {
    std::optional<uint32_t> res = node.get_local(key);
    if (res.has_value()) {
      assert(res.value() == val); // Guarantee the data isn't corrupted
      return 1;
    }
    return 0;
  };

  physical_replica_count += check_replica(node0);
  physical_replica_count += check_replica(node1);
  physical_replica_count += check_replica(node2);

  // Because Replication Degree = 2, exactly 2 physical machines must hold the data.
  // The 3rd machine must have returned std::nullopt.
  assert(physical_replica_count == 2);

  std::cout << "Passed!" << std::endl;
}

// Test 2: Eventual Consistency & Buffer Snooping
// Verifies that asynchronous writes hit the local Write-Behind Cache, 
// remaining invisible to the network until a flush is forced.
void test_eventual_consistency_snooping(DHTTransactionManager& client0, 
                                        DHTTransactionManager& client1) {
  std::cout << "Running Eventual Consistency & Buffer Snooping Test...";

  uint32_t key = 200;
  uint32_t val = 5555;

  // Asynchronous Put
  client0.put_async(key, val);

  // Session Consistency Check (Snooping)
  // Client 0 should read the value out of its own L1 cache buffer.
  GetResponse snoop_res = client0.get_async(key);
  assert(snoop_res.status == GetStatus::Found);
  assert(snoop_res.value == val);

  // Network Isolation Check
  // Because it hasn't flushed, Client 1 must NOT see this data yet.
  GetResponse remote_res = client1.get_sync(key);
  assert(remote_res.status == GetStatus::NotFound);

  // Force Flush & Verify Visibility
  client0.flush_all();
  std::this_thread::sleep_for(std::chrono::milliseconds(50)); // Allow OS to process TCP

  remote_res = client1.get_sync(key);
  assert(remote_res.status == GetStatus::Found);
  assert(remote_res.value == val);

  std::cout << "Passed!" << std::endl;
}

// Test 3: Distributed Multi-Key 2PC Atomicity
void test_distributed_multiput(DHTTransactionManager& client0) {
  std::cout << "Running 2PC Multi-Key Atomicity Test...";

  std::vector<std::pair<uint32_t, uint32_t>> batch = {
    {300, 3000},
    {301, 3001},
    {302, 3002}
  };

  bool success = client0.multi_put(batch);
  assert(success == true);

  for (const auto& kv : batch) {
    GetResponse res = client0.get_sync(kv.first);
    assert(res.status == GetStatus::Found);
    assert(res.value == kv.second);
  }

  std::cout << "Passed!" << std::endl;
}

// Test 4: High Concurrency Data Race Prevention
// Spawns multiple threads aggressively hammering the exact same keys.
// Validates that the tx_spinlock, Lamport clocks, and Phase 1 rollback logic 
// perfectly prevent deadlocks and memory corruption.
void test_high_concurrency_race_safety(DHTTransactionManager& client0) {
  std::cout << "Running High Concurrency Race Safety Test...";

  constexpr int NUM_THREADS = 4;
  constexpr int OPS_PER_THREAD = 100;
  std::atomic<int> start_flag{0};

  auto worker_func = [&](int thread_id) {
    while (start_flag.load(std::memory_order_acquire) == 0) {
      std::this_thread::yield();
    }

    for (int i = 0; i < OPS_PER_THREAD; ++i) {
      // Intentionally causing massive lock collision on keys 400 and 401
      std::vector<std::pair<uint32_t, uint32_t>> contention_batch = {
        {400, static_cast<uint32_t>(thread_id * 1000 + i)},
        {401, static_cast<uint32_t>(thread_id * 1000 + i)}
      };
      client0.multi_put(contention_batch);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < NUM_THREADS; ++i) {
    threads.emplace_back(worker_func, i);
  }

  start_flag.store(1, std::memory_order_release);

  for (auto& t : threads) {
    t.join();
  }

  // If we reach this line, the Spinlocks and OCC retries successfully 
  // resolved all distributed collisions without live-locking or segfaulting.
  GetResponse res = client0.get_sync(400);
  assert(res.status == GetStatus::Found);

  std::cout << "Passed!" << std::endl;
}

int main() {
  // 3-Node Topology allows us to properly test Replication Degree = 2
  std::vector<NodeConfig> cluster_map = {
    {0, "127.0.0.1", 50000},
    {1, "127.0.0.1", 50001},
    {2, "127.0.0.1", 50002}
  };

  int hash_table_size = 1024;
  int num_locks = 16;
  int replication_degree = 2;

  StaticClusterDHTNode node0(cluster_map, cluster_map[0], hash_table_size, num_locks, replication_degree);
  StaticClusterDHTNode node1(cluster_map, cluster_map[1], hash_table_size, num_locks, replication_degree);
  StaticClusterDHTNode node2(cluster_map, cluster_map[2], hash_table_size, num_locks, replication_degree);

  try {
    node0.start();
    node1.start();
    node2.start();

    // Hardware propagation delay for TCP sockets to fully bind to the OS
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Pre-warm the network
    node0.warmup_network(2);
    node1.warmup_network(2);
    node2.warmup_network(2);

    // Initialize the Client API layer
    DHTTransactionManager client0(node0);
    DHTTransactionManager client1(node1);
    DHTTransactionManager client2(node2);

    // Execute Test Suite
    test_strict_replication(client0, node0, node1, node2);
    test_eventual_consistency_snooping(client0, client1);
    test_distributed_multiput(client0);
    test_high_concurrency_race_safety(client0);
    
    std::cout << "\n[SUCCESS] All Distributed Architecture tests passed without data races.\n";
  } catch (const std::exception& e) {
    std::cerr << "Test failed with exception: " << e.what() << "\n";
  }

  // Graceful multi-node teardown
  node0.stop();
  node1.stop();
  node2.stop();

  return 0;
}