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

template <typename K, typename V>
class TimestampedStripedLockConcurrentHashTable {
private:
  // Storage member variables
  std::vector<std::vector<Ht_item<K, V>>> table;
  std::unique_ptr<alignas(64) std::atomic<int>[]> striped_counts;
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
    striped_counts = std::make_unique<std::atomic<int>[]>(num_locks);

    for (int i = 0; i < num_locks; ++i) {
      striped_counts[i].store(0, std::memory_order_relaxed);
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
    striped_counts[lock_idx].fetch_add(1, std::memory_order_relaxed);
    return PutResult::Inserted;
  }

  // Multi-Put Phase 2 Commit
  void multi_put(const std::vector<std::pair<K, V>>& kv_pairs, const uint64_t tx_timestamp) {
    size_t batch_size = kv_pairs.size();
    if (batch_size == 0) return;

    // Stack allocation for lock deduplication
    constexpr size_t MAX_BATCH = 64;
    int lock_indices[MAX_BATCH];
    size_t actual_batch = std::min(batch_size, MAX_BATCH);

    for (size_t i = 0; i < actual_batch; ++i) {
      uint64_t raw_hash = get_raw_hash(kv_pairs[i].first);
      lock_indices[i] = get_lock_index(get_bucket_index(raw_hash));
    }

    // Sort locks in ascending global order to prevent deadlock
    std::sort(lock_indices, lock_indices + actual_batch);

    // Deduplicate locks
    size_t num_unique_locks = 0;
    for (size_t i = 0; i < actual_batch; ++i) {
      if (i == 0 || lock_indices[i] != lock_indices[num_unique_locks - 1]) {
        lock_indices[num_unique_locks++] = lock_indices[i];
      }
    }

    // Acquire all unique locks sequentially
    for (size_t i = 0; i < num_unique_locks; ++i) {
      table_mutexes[lock_indices[i]].mutex.lock();
    }

    // Track inserts locally
    int inserts_per_lock[MAX_BATCH] = {0};

    // Apply writes
    for (size_t i = 0; i < actual_batch; ++i) {
      const auto& kv = kv_pairs[i];
      uint64_t raw_hash = get_raw_hash(kv.first);
      size_t bucket_index = get_bucket_index(raw_hash);
      int lock_idx = get_lock_index(bucket_index);

      auto& bucket = table[bucket_index];
      bool found = false;

      for (auto& item : bucket) {
        if (item.key == kv.first) {
          if (tx_timestamp > item.timestamp) [[likely]] {
            item.value = kv.second;
            item.timestamp = tx_timestamp;
          }
          found = true;
          break;
        }
      }

      if (!found) {
        bucket.push_back({kv.first, kv.second, tx_timestamp});
        
        // Find which unique lock this belonged to and tally it
        for (size_t j = 0; j < num_unique_locks; ++j) {
          if (lock_indices[j] == lock_idx) {
            inserts_per_lock[j]++;
            break;
          }
        }
      }
    }

    // Update striped counters & release locks
    for (int i = num_unique_locks - 1; i >= 0; --i) {
      if (inserts_per_lock[i] > 0) {
        striped_counts[lock_indices[i]].fetch_add(inserts_per_lock[i], std::memory_order_relaxed);
      }
      table_mutexes[lock_indices[i]].mutex.unlock();
    }
  }

  std::optional<V> get(const K& key) const {
    uint64_t raw_hash = get_raw_hash(key);
    size_t bucket_index = get_bucket_index(raw_hash);
    int lock_index = get_lock_index(bucket_index);

    std::lock_guard<Spinlock> lock(table_mutexes[lock_index].mutex);    
    const std::vector<Ht_item<K, V>> &bucket = table[bucket_index];

    for (const auto& item : bucket) {
      if (item.key == key) [[likely]] {
        return item.value;
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