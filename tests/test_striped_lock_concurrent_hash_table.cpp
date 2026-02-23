#include <cassert>
#include <thread>
#include <random>

#include "hash_table/striped_lock_concurrent_hash_table.h"

void log(const std::string& message) {
  std::cout << "[TEST] " << message << std::endl;
}

void test_basic_operations() {
  log("Running Basic Opearting Test...");
  // Initialize with size 10, 4 locks
  StripedLockConcurrentHashTable<std::string, int> ht(10, 4);

  // insert
  ht.put("key1", 100);
  ht.put("key2", 200);

  // search
  auto val1 = ht.get("key1");
  auto val2 = ht.get("key2");
  auto val3 = ht.get("key_missing");

  assert(val1.has_value() && val1.value() == 100);
  assert(val2.has_value() && val2.value() == 200);
  assert(!val3.has_value());

  // update
  ht.put("key1", 101);
  val1 = ht.get("key1");
  assert(val1.value() == 101);

  log("Basic Operations Test Passed!");
}

void insert_range(StripedLockConcurrentHashTable<int, int>& ht, int start, int end) {
  for (int i = start; i < end; ++i) {
    ht.put(i, i * 10);
  }
}

void test_concurrent_inserts() {
  log("Running Concurrent Inserts Test...");
  StripedLockConcurrentHashTable<int, int> ht(100, 16);
  int num_threads = 4;
  int items_per_thread = 10000;

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(insert_range, std::ref(ht), i * items_per_thread, (i + 1) * items_per_thread);
  }

  for (auto& t : threads) {
    t.join();
  }

  int expected = num_threads * items_per_thread;
  assert(ht.get_count() == expected);

  auto res = ht.get(500);
  assert(res.has_value() && res.value() == 5000);

  log("Concurrent Inserts Test Passed!");
}

void test_concurrent_read_writes() {
  log("Running Concurrent Reads/Writes Test...");
  StripedLockConcurrentHashTable<std::string, int> ht(1000, 32);
  std::atomic<bool> done = false;

  std::thread writer([&]() {
    for (int i = 0; i < 1000; ++i) {
      ht.put("Key" + std::to_string(i), i);
      std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
    done = true;
  });

  std::thread reader([&]() {
    while (!done) {
      auto res = ht.get("Key50");
      if (res.has_value()) {
        assert(res.value() == 50);
      }
    }
  });

  writer.join();
  reader.join();

  assert(ht.get_count() == 1000);
  log("Concurrent Reads/Writes Test Passed!");
}

void test_atomic_multiput() {
  log("Running Atomic Multi Put Test...");
  StripedLockConcurrentHashTable<int, int> ht(100, 8);

  std::vector<std::pair<int, int>> batch1 = {
    {1, 10}, {2, 20}, {3, 30}
  };
  
  int added = ht.multi_put(batch1);
  assert(added == 3);
  assert(ht.get_count() == 3);

  // Test updates mixed with inserts in a batch
  std::vector<std::pair<int, int>> batch2 = {
    {2, 25}, // Update
    {4, 40}  // New
  };
  
  added = ht.multi_put(batch2);
  assert(added == 1); // Only 1 new item added
  assert(ht.get_count() == 4);
  
  auto val = ht.get(2);
  assert(val.has_value() && val.value() == 25);

  log("Batch Operations Test Passed!");
}

void test_atomic_multiput_deadlock_prevention() {
  log("Running Concurrent Batch Deadlock Prevention Test...");
  StripedLockConcurrentHashTable<int, int> ht(1000, 16);

  // Create two batches with overlapping keys but in reverse order.
  // Without strict lock sorting, Thread A grabs Lock 1 waiting for Lock 2, 
  // while Thread B grabs Lock 2 waiting for Lock 1.
  std::vector<std::pair<int, int>> batch_forward;
  std::vector<std::pair<int, int>> batch_reverse;
  
  for (int i = 0; i < 500; ++i) {
    batch_forward.push_back({i, i});
    batch_reverse.push_back({499 - i, 499 - i});
  }

  std::thread t1([&]() { ht.multi_put(batch_forward); });
  std::thread t2([&]() { ht.multi_put(batch_reverse); });

  t1.join();
  t2.join();

  // Both threads attempt to insert the same 500 keys. 
  // Only 500 total should exist.
  assert(ht.get_count() == 500);

  log("Concurrent Batch Deadlock Prevention Test Passed!");
}

void test_severe_collision_chaining() {
  log("Running Severe Collision Chaining Test...");
  // Force all items into the same 2 buckets to test deep linked-list traversal
  StripedLockConcurrentHashTable<int, int> ht(2, 2); 
  int total_items = 10000;
  int num_threads = 4;
  int chunk = total_items / num_threads;

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; ++i) {
      threads.emplace_back(insert_range, std::ref(ht), i * chunk, (i + 1) * chunk);
  }

  for (auto& t : threads) {
      t.join();
  }

  assert(ht.get_count() == total_items);
  log("Severe Collision Chaining Test Passed!");
}

void test_concurrent_workload() {
  log("Running Concurrent Workload Test (Put, Multi-Put, Get)...");
  // High capacity, heavily striped to maximize interleaving opportunities
  StripedLockConcurrentHashTable<int, int> ht(2000, 64);

  std::atomic<bool> start_flag{false};
  std::atomic<int> ready_threads{0};
  std::atomic<bool> writers_done{false};

  int num_put_threads = 4;
  int num_multiput_threads = 4;
  int num_get_threads = 4;
  int items_per_thread = 2000;
  std::vector<std::thread> threads;

  // Single Put Threads (Keys: 0 to 7999)
  for (int t = 0; t < num_put_threads; ++t) {
    threads.emplace_back([&, t]() {
      ready_threads++;
      while (!start_flag.load(std::memory_order_acquire)) { 
        std::this_thread::yield();
      }
      
      int start_key = t * items_per_thread;
      for (int i = 0; i < items_per_thread; ++i) {
        ht.put(start_key + i, (start_key + i) * 10);
      }
    });
  }

  // Multi-Put Threads (Keys: 8000 to 15999)
  for (int t = 0; t < num_multiput_threads; ++t) {
    threads.emplace_back([&, t]() {
      ready_threads++;
      while (!start_flag.load(std::memory_order_acquire)) { 
        std::this_thread::yield(); 
      }
      
      int start_key = 8000 + (t * items_per_thread);
      std::vector<std::pair<int, int>> batch;
      batch.reserve(50);
      
      for (int i = 0; i < items_per_thread; ++i) {
        batch.push_back({start_key + i, (start_key + i) * 10});
        
        // Flush in chunks of 50 to heavily interleave with readers
        if (batch.size() == 50) { 
          ht.multi_put(batch);
          batch.clear();
        }
      }

      if (!batch.empty())
        ht.multi_put(batch);
    });
  }

  // Get Threads (Randomly querying 0 to 15999)
  for (int t = 0; t < num_get_threads; ++t) {
    threads.emplace_back([&]() {
      ready_threads++;
      while (!start_flag.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      
      std::mt19937 rng(std::random_device{}());
      std::uniform_int_distribution<int> dist(0, 15999);
      
      // Continuously read while writers are actively mutating the table
      while (!writers_done.load(std::memory_order_relaxed)) {
        int target_key = dist(rng);
        auto res = ht.get(target_key);
        
        // Correctness Check: If a read succeeds during active writes, 
        // the data must not be corrupted or partially written.
        if (res.has_value()) {
          assert(res.value() == target_key * 10);
        }
      }
    });
  }

  // Synchronize all 12 threads at the starting line
  int total_threads = num_put_threads + num_multiput_threads + num_get_threads;
  while (ready_threads.load() < total_threads) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }

  // Release the barrier
  start_flag.store(true, std::memory_order_release);

  // Join writers first
  for (int i = 0; i < num_put_threads + num_multiput_threads; ++i) {
    threads[i].join();
  }

  // Signal readers to stop polling and join
  writers_done.store(true, std::memory_order_relaxed);
  for (int i = num_put_threads + num_multiput_threads; i < total_threads; ++i) {
    threads[i].join();
  }

  // Final Data Validation
  int expected_total = (num_put_threads + num_multiput_threads) * items_per_thread;
  assert(ht.get_count() == expected_total);

  // Ensure every single key from 0 to 15999 made it into the storage layer
  for (int i = 0; i < expected_total; ++i) {
    auto res = ht.get(i);
    assert(res.has_value() && res.value() == i * 10);
  }

  log("Concurrent Workload Test Passed!");
}

int main() {
  test_basic_operations();
  test_concurrent_inserts();
  test_concurrent_read_writes();
  test_atomic_multiput();
  test_atomic_multiput_deadlock_prevention();
  test_severe_collision_chaining();
  test_concurrent_workload();

  return 0;
}