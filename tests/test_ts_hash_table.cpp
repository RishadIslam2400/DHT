#include <cassert>
#include <thread>
#include <random>

#include "hash_table/timestamped_striped_lock_concurrent_hash_table.h"

void test_initialization_alignment() {
  std::cout << "Initialization Alignment Test...";

  TimestampedStripedLockConcurrentHashTable<uint32_t, uint32_t> ht(100, 3);
  
  assert(ht.get_capacity() >= 100);
  assert(ht.get_capacity() % 4 == 0); 
  assert(ht.get_count() == 0);
  std::cout << "PASS" << std::endl;
}

void test_basic_operations() {
  std::cout << "Basic Opearting Test...";
  TimestampedStripedLockConcurrentHashTable<std::string, int> ht(10, 4);

  // insert
  ht.put("key1", 100, 1);
  ht.put("key2", 200, 2);

  // search
  auto val1 = ht.get("key1");
  auto val2 = ht.get("key2");
  auto val3 = ht.get("key_missing");

  assert(val1.has_value() && val1.value() == 100);
  assert(val2.has_value() && val2.value() == 200);
  assert(!val3.has_value());

  // update
  ht.put("key1", 101, 3);
  val1 = ht.get("key1");
  assert(val1.value() == 101);

  std::cout << "PASS" << std::endl;
}

void test_lww_timestamp_resolution() {
  std::cout << "LWW Timestamp Resolution Test...";
  TimestampedStripedLockConcurrentHashTable<uint32_t, uint32_t> ht(1024, 16);

  // Initial Insert
  assert(ht.put(1, 100, 10) == PutResult::Inserted);
  assert(ht.get(1) == 100);
  assert(ht.get_timestamp(1) == 10);

  // Obsolete Write (Timestamp is older)
  assert(ht.put(1, 999, 5) == PutResult::Dropped);
  assert(ht.get(1) == 100);

  // Exact Same Timestamp (Duplicate packet)
  assert(ht.put(1, 888, 10) == PutResult::Dropped);
  assert(ht.get(1) == 100);

  // Newer Write
  assert(ht.put(1, 200, 15) == PutResult::Updated);
  assert(ht.get(1) == 200);
  assert(ht.get_timestamp(1) == 15);

  std::cout << "PASS" << std::endl;
}

void test_multiput_lock_deduplication() {
  std::cout << "Multiput Lock Deduplication...";
  TimestampedStripedLockConcurrentHashTable<uint32_t, uint32_t> ht(16, 2);
  // With 2 locks, even/odd hashes will likely fall into the same stripes.
  // By pushing 4 items, we guarantee multiple keys will require the exact same lock.
  
  std::vector<std::pair<uint32_t, uint32_t>> batch = {
      {10, 100}, 
      {20, 200}, 
      {30, 300}, 
      {40, 400}
  };

  // If deduplication fails, this call will hang forever.
  int added = ht.multi_put(batch, 50);
  
  assert(added == 4);
  assert(ht.get(10) == 100);
  assert(ht.get_timestamp(30) == 50);
  std::cout << "PASS" << std::endl;
}

void test_multiput_partial_obsolescence() {
  std::cout << "Multiput Partial Update Test...";
  TimestampedStripedLockConcurrentHashTable<uint32_t, uint32_t> ht(1024, 16);

  // Key 1 exists at TS=100
  ht.put(1, 555, 100);

  std::vector<std::pair<uint32_t, uint32_t>> batch = {
      {1, 999}, // Should be rejected (Batch TS 50 < Existing TS 100)
      {2, 222}  // Should be inserted
  };

  ht.multi_put(batch, 50);

  // Verify partial success
  assert(ht.get(1) == 555); // Protected by LWW
  assert(ht.get(2) == 222); // Successfully inserted
  std::cout << "PASS" << std::endl;
}

#include <thread>

void test_concurrent_stress() {
  std::cout << "Concurrent Operations Test...";
  TimestampedStripedLockConcurrentHashTable<uint32_t, uint32_t> ht(1024, 16);
  std::atomic<int> start_flag{0};

  auto writer_func = [&](int start_key, uint64_t ts) {
      while (start_flag.load() == 0) { std::this_thread::yield(); }
      for (int i = 0; i < 1000; ++i) {
          ht.put(start_key + i, i, ts);
      }
  };

  auto reader_func = [&](int start_key) {
      while (start_flag.load() == 0) { std::this_thread::yield(); }
      for (int i = 0; i < 1000; ++i) {
          ht.get(start_key + i); // Just verifying it doesn't segfault
      }
  };

  std::vector<std::thread> threads;
  // Spawn 4 writers and 4 readers hammering overlapping key ranges
  for (int i = 0; i < 4; ++i) {
      threads.emplace_back(writer_func, 0, i + 1);
      threads.emplace_back(reader_func, 0);
  }

  start_flag.store(1); // Release the hounds
  for (auto& t : threads) { t.join(); }

  assert(ht.get_count() == 1000);
  std::cout << "PASS" << std::endl;
}

int main() {
  test_initialization_alignment();
  test_basic_operations();
  test_lww_timestamp_resolution();
  test_multiput_lock_deduplication();
  test_multiput_partial_obsolescence();
  test_concurrent_stress();

  return 0;
}