#pragma once

#include <vector>
#include <memory>
#include <optional>
#include <iostream>
#include <algorithm>
#include <ranges>
#include <bit>
#include <numeric>

#include "common/xxHash64.h"
#include "common/dht_common.h"
#include "common/spin_lock.h"

template <typename K, typename V>
struct Ht_item {
  K key;
  V value;
  uint64_t timestamp; // Logical clock commit timestamp
};

struct alignas(64) AlignedAtomicInt {
  std::atomic<int> val{0};

  AlignedAtomicInt& operator=(int v) { 
    val.store(v, std::memory_order_relaxed);
    return *this;
  }

  int operator++(int) { return val.fetch_add(1, std::memory_order_relaxed); }
  int operator--(int) { return val.fetch_sub(1, std::memory_order_relaxed); }
  int load(std::memory_order order = std::memory_order_seq_cst) const { return val.load(order); }
};

template <typename K, typename V>
class TimestampedStripedLockConcurrentHashTable {
private:
  // Storage member variables
  std::vector<std::vector<Ht_item<K, V>>> table;
  std::unique_ptr<AlignedAtomicInt[]> striped_counts;
  int capacity;
  
  // Concurrency member variables
  mutable std::unique_ptr<AlignedSpinlock[]> table_mutexes;
  int num_locks;

  inline uint64_t get_raw_hash(const K& key) const {
    if constexpr (std::is_same_v<K, std::string>) {
      return XXHash64::hash(key.data(), key.size(), 0);
    } else {
      return XXHash64::hash(&key, sizeof(K), 0);
    }
  }

  // Uses bitwise AND instead of modulo. 
  inline int get_lock_index(size_t bucket_index) const {
    return bucket_index & (num_locks - 1);
  }

  inline size_t get_bucket_index(uint64_t raw_hash) const {
  return raw_hash & (capacity - 1);
}

public:
  explicit TimestampedStripedLockConcurrentHashTable(int requested_capacity, int requested_locks) {
    // Force capacity and locks to be powers of two
    capacity = std::bit_ceil(static_cast<size_t>(std::max(1, requested_capacity)));
    num_locks = std::bit_ceil(static_cast<size_t>(std::max(1, requested_locks)));

    table.resize(capacity);
    table_mutexes = std::make_unique<AlignedSpinlock[]>(num_locks);
    striped_counts = std::make_unique<AlignedAtomicInt[]>(num_locks);

    for (int i = 0; i < num_locks; ++i) {
      striped_counts[i].val.store(0, std::memory_order_relaxed);
    }

    for (int i = 0; i < capacity; ++i) {
      table[i].reserve(4); 
    }
  }

  // Returns the total size
  int size() const {
    int total = 0;
    for (int i = 0; i < num_locks; ++i) {
      total += striped_counts[i].load(std::memory_order_relaxed);
    }
    return total;
  }

  // Single put operation using Last-Write-Wins
  PutResult put(const K& key, const V& value, const uint64_t incoming_ts) {
    uint64_t raw_hash = get_raw_hash(key);
    size_t bucket_index = get_bucket_index(raw_hash);
    int lock_idx = get_lock_index(bucket_index);

    std::lock_guard<Spinlock> lock(table_mutexes[lock_idx].mutex);

    std::vector<Ht_item<K, V>>& bucket = table[bucket_index];
    for (Ht_item<K, V>& item : bucket) {
      if (item.key == key) {
        if (incoming_ts > item.timestamp) [[likely]] {
          item.value = value;
          item.timestamp = incoming_ts;
          return PutResult::Updated;
        }
        return PutResult::Dropped; 
      }
    }

    // Key not found, append new entry
    bucket.push_back({key, value, incoming_ts});
    striped_counts[lock_idx].val.fetch_add(1, std::memory_order_relaxed);
    return PutResult::Inserted;
  }

  // Multi-Put Phase 2 Commit
  std::pair<int, int> multi_put(const std::pair<K, V>* items, size_t batch_size, const uint64_t tx_timestamp) {
    if (batch_size == 0) return {0, 0};

    int total_inserted = 0;
    int total_updated = 0;

    // Stack allocation for lock deduplication
    constexpr size_t MAX_BATCH = 64;
    size_t actual_batch = std::min(batch_size, MAX_BATCH);

    int lock_indices[MAX_BATCH];
    size_t bucket_indices[MAX_BATCH];

    // Compute hashes and map to locks
    for (size_t i = 0; i < actual_batch; ++i) {
      uint64_t raw_hash = get_raw_hash(items[i].first);
      bucket_indices[i] = get_bucket_index(raw_hash);
      lock_indices[i] = get_lock_index(bucket_indices[i]);
    }

    // Sort locks in ascending global order to prevent deadlock
    int sorted_locks[MAX_BATCH];
    std::copy(lock_indices, lock_indices + actual_batch, sorted_locks);
    std::sort(sorted_locks, sorted_locks + actual_batch);

    // Deduplicate locks
    size_t num_unique_locks = 0;
    for (size_t i = 0; i < actual_batch; ++i) {
      if (i == 0 || sorted_locks[i] != sorted_locks[num_unique_locks - 1]) {
        sorted_locks[num_unique_locks++] = sorted_locks[i];
      }
    }

    // Acquire all unique locks sequentially
    for (size_t i = 0; i < num_unique_locks; ++i) {
      table_mutexes[sorted_locks[i]].mutex.lock();
    }

    // Track inserts locally
    int inserts_per_lock[MAX_BATCH] = {0};

    // Apply writes
    for (size_t i = 0; i < actual_batch; ++i) {
      const auto& item_pair = items[i];
      size_t bucket_index = bucket_indices[i];
      int lock_idx = lock_indices[i];

      auto& bucket = table[bucket_index];
      bool found = false;

      for (auto& item : bucket) {
        if (item.key == item_pair.first) {
          if (tx_timestamp > item.timestamp) [[likely]] {
            item.value = item_pair.second;
            item.timestamp = tx_timestamp;
            total_updated++;
          }
          found = true;
          break;
        }
      }

      if (!found) {
        bucket.push_back({item_pair.first, item_pair.second, tx_timestamp});
        total_inserted++;

        // Find which unique lock this belonged to and tally it
        for (size_t j = 0; j < num_unique_locks; ++j) {
          if (sorted_locks[j] == lock_idx) {
            inserts_per_lock[j]++;
            break;
          }
        }
      }
    }

    // Update striped counters & release locks
    for (int i = static_cast<int>(num_unique_locks) - 1; i >= 0; --i) {
      if (inserts_per_lock[i] > 0) {
        striped_counts[sorted_locks[i]].val.fetch_add(inserts_per_lock[i], std::memory_order_relaxed);
      }
      table_mutexes[sorted_locks[i]].mutex.unlock();
    }

    return {total_inserted, total_updated};
  }

  std::optional<LocalValue> get(const K& key) const {
    uint64_t raw_hash = get_raw_hash(key);
    size_t bucket_index = get_bucket_index(raw_hash);
    int lock_index = get_lock_index(bucket_index);

    std::lock_guard<Spinlock> lock(table_mutexes[lock_index].mutex);    
    const std::vector<Ht_item<K, V>> &bucket = table[bucket_index];

    for (const auto& item : bucket) {
      if (item.key == key) [[likely]] {
        return LocalValue{
          item.value, 
          item.timestamp
        };
      }
    }

    return std::nullopt;
  }

  // For the 2PC Coordinator's PREPARE phase.
  // Allows the node to verify if a newer transaction has already modified this key.
  uint64_t get_timestamp(const K& key) const {
    uint64_t raw_hash = get_raw_hash(key);
    size_t bucket_index = get_bucket_index(raw_hash);
    int lock_index = get_lock_index(bucket_index);

    std::lock_guard<Spinlock> lock(table_mutexes[lock_index].mutex);
    const std::vector<Ht_item<K, V>> &bucket = table[bucket_index];

    for (const auto& item : bucket) {
      if (item.key == key) [[likely]] {
        return item.timestamp;
      }
    }

    return 0; // 0 indicates the key does not exist yet
  }

  int get_capacity() const { return capacity; }
};