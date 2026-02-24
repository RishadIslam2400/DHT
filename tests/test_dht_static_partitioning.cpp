#include <iostream>
#include <vector>
#include <chrono>
#include <cassert>
#include <thread>
#include <string>

#include "dht/dht_static_partitioning.h"

void log(const std::string& msg) {
  std::cout << "[TEST] " << msg << std::endl;
}

// Helper to find a key that mathematically maps to a specific target node
uint32_t find_key_for_node(StaticClusterDHTNode& node, int target_id) {
  for (uint32_t i = 0; i < 100000; ++i) {
    if (node.get_target_node(i).id == target_id) {
      return i;
    }
  }
  throw std::runtime_error("Could not find key for target node");
}

void test_local_routing(StaticClusterDHTNode& node0) {
  log("Running Local Routing Test...");
  uint32_t local_key = find_key_for_node(node0, 0);
  
  // Test Insert
  PutResult put_res = node0.put(local_key, 999);
  assert(put_res == PutResult::Inserted);
  
  // Test Update
  put_res = node0.put(local_key, 1000);
  assert(put_res == PutResult::Updated);
  
  // Test Retrieve
  GetResponse get_res = node0.get(local_key);
  assert(get_res.status == GetStatus::Found);
  assert(get_res.value == 1000);
  
  // Test Missing
  uint32_t missing_local_key = find_key_for_node(node0, 0);
  while (missing_local_key == local_key) {
    missing_local_key++; // Ensure different key
  }
  if (node0.get_target_node(missing_local_key).id == 0) {
    GetResponse miss_res = node0.get(missing_local_key);
    assert(miss_res.status == GetStatus::NotFound);
  }
  
  log("Local Routing Test Passed!");
}

void test_remote_single_rpc(StaticClusterDHTNode& node0, StaticClusterDHTNode& node1) {
  log("Running Remote Single RPC Test...");
  // Find a key that belongs to Node 1
  uint32_t remote_key = find_key_for_node(node0, 1);
  
  // Node 0 issues a PUT. It should route over TCP to Node 1.
  PutResult put_res = node0.put(remote_key, 5555);
  assert(put_res == PutResult::Inserted);
  
  // Node 0 issues a GET. It should fetch over TCP from Node 1.
  GetResponse get_res = node0.get(remote_key);
  assert(get_res.status == GetStatus::Found);
  assert(get_res.value == 5555);
  
  // Verify Node 1 actually holds the data locally
  get_res = node1.get(remote_key);
  assert(get_res.status == GetStatus::Found);
  assert(get_res.value == 5555);
  
  log("Remote Single RPC Test Passed!");
}

void test_remote_batch_rpc(StaticClusterDHTNode& node0, StaticClusterDHTNode& node1) {
  log("Running Remote Batch RPC Test...");
  
  std::vector<PutRequest> batch;
  // Generate 10 distinct keys that belong to Node 1
  uint32_t k = 0;
  while (batch.size() < 10) {
    if (node0.get_target_node(k).id == 1) {
      batch.push_back({k, k * 2});
    }
    k++;
  }
  
  // Send the batch directly using Node 0's internal network API
  bool success = node0.send_batch(1, "127.0.0.1", 50001, batch);
  assert(success == true);
  
  // Give the receiver thread on Node 1 a few milliseconds to process the batch
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  
  // Verify Node 1 successfully unpacked and stored all 10 keys
  for (const auto& req : batch) {
    GetResponse res = node1.get(req.key);
    assert(res.status == GetStatus::Found);
    assert(res.value == req.value);
  }
  
  log("Remote Batch RPC Test Passed!");
}

void test_distributed_barrier(StaticClusterDHTNode& node0, StaticClusterDHTNode& node1) {
  log("Running Distributed Barrier Test...");
  
  // Run the barriers on background threads so they don't block the test suite
  std::thread t0([&]() { node0.wait_for_barrier(); });
  std::thread t1([&]() { node1.wait_for_barrier(); });
  
  t0.join();
  t1.join();
  
  // Both nodes should now have the atomic benchmark_ready flag set to true
  assert(node0.benchmark_ready.load() == true);
  assert(node1.benchmark_ready.load() == true);
  
  log("Distributed Barrier Test Passed!");
}

int main() {
  std::vector<NodeConfig> cluster_map = {
    {0, "127.0.0.1", 50000},
    {1, "127.0.0.1", 50001}
  };

  StaticClusterDHTNode node0(cluster_map, cluster_map[0], 1024, 16);
  StaticClusterDHTNode node1(cluster_map, cluster_map[1], 1024, 16);

  try {
    node0.start();
    node1.start();

    // Wait for sockets to fully bind
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Pre-warm the network (using 2 connections per peer for the test)
    node0.warmup_network(2);
    node1.warmup_network(2);

    // Execute Test Suite
    test_local_routing(node0);
    test_remote_single_rpc(node0, node1);
    test_remote_batch_rpc(node0, node1);
    test_distributed_barrier(node0, node1);
    
    std::cout << "\nAll DHT integration tests passed successfully.\n";
  } catch (const std::exception& e) {
    std::cerr << "Test failed with exception: " << e.what() << "\n";
  }

  node0.stop();
  node0.stop();
}