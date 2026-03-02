#pragma once

#include <shared_mutex>
#include <vector>
#include <atomic>
#include <memory>
#include <optional>
#include <mutex>
#include <iostream>
#include <algorithm>
#include <ranges>

#include "common/xxHash64.h"
#include "common/dht_common.h"

template <typename K, typename V>
struct Ht_item {
  K key;
  V value;
  uint64_t timestamp; // Logical clock commit timestamp
};

// alignas(64) ensures each lock sits on its own hardware CPU cache line.
// This prevents "false sharing," where threads modifying different locks 
// accidentally invalidate each other's L1 cache.
struct alignas(64) AlignedLock {
  std::shared_mutex mutex;
};

template <typename K, typename V>
class TimestampedStripedLockConcurrentHashTable {
private:
  // Storage member variables
  std::vector<std::vector<Ht_item<K, V>>> table;
  std::atomic<int> count;
  int capacity;
  
  // Concurrency member variables
  mutable std::unique_ptr<AlignedLock[]> table_mutexes;
  int num_locks;

  inline uint64_t get_raw_hash(const K& key) const {
    if constexpr (std::is_same_v<K, std::string>) {
      return XXHash64::hash(key.data(), key.size(), 0);
    } else {
      return XXHash64::hash(&key, sizeof(K), 0);
    }
  }

  // Uses bitwise AND instead of modulo. 
  // This mathematically requires num_locks to be a strict power of 2.
  inline int get_lock_index(size_t bucket_index) const {
    return bucket_index & (num_locks - 1);
  }

  inline size_t get_bucket_index(uint64_t raw_hash) const {
    return raw_hash % capacity;
  }

public:
  explicit TimestampedStripedLockConcurrentHashTable(int size, int locks) : count(0) {
    // Force num_locks to the next highest power of 2 for the bitwise AND optimization
    num_locks = 1;
    num_locks = 1;
    while (num_locks < locks) {
      num_locks <<= 1;
    }

    // Ensure capacity is a multiple of locks so lock mapping remains uniform
    if (size % num_locks != 0) {
      size += (num_locks - (size % num_locks));
    }

    capacity = size;
    table.resize(capacity);
    table_mutexes = std::make_unique<AlignedLock[]>(num_locks);
  }

  // Single PUT operation utilizing Last-Write-Wins (LWW)
  PutResult put(const K& key, const V& value, const uint64_t incoming_ts) {
    uint64_t raw_hash = get_raw_hash(key);
    size_t bucket_index = get_bucket_index(raw_hash);
    int lock_index = get_lock_index(bucket_index);

    std::unique_lock<std::shared_mutex> lock(table_mutexes[lock_index].mutex);
    std::vector<Ht_item<K, V>>& bucket = table[bucket_index];

    auto it = std::ranges::find_if(bucket, [&key](const Ht_item<K, V> &item) {
      return item.key == key;
    });

    if (it != bucket.end()) {
      // Reject the write if the physical timestamp is newer (larger is newer).
      if (incoming_ts > it->timestamp) {
        it->value = value;
        it->timestamp = incoming_ts;
        return PutResult::Updated;
      }

      return PutResult::Dropped; // obsolete write dropped (lost updates)
    }

    // Key not found, append new entry
    bucket.push_back({key, value, incoming_ts});
    count.fetch_add(1, std::memory_order_relaxed);
    return PutResult::Inserted;
  }

  std::optional<V> get(const K& key) const {
    uint64_t raw_hash = get_raw_hash(key);
    size_t bucket_index = get_bucket_index(raw_hash);
    int lock_index = get_lock_index(bucket_index);

    // Shared lock allows concurrent readers
    std::shared_lock<std::shared_mutex> lock(table_mutexes[lock_index].mutex);    
    const std::vector<Ht_item<K, V>> &bucket = table[bucket_index];

    auto it = std::ranges::find_if(bucket, [&key](const Ht_item<K, V> &item) {
      return item.key == key;
    });
    if (it != bucket.end()) {
      return it->value;
    }

    return std::nullopt;
  }

  // For the 2PC Coordinator's PREPARE phase.
  // Allows the node to verify if a newer transaction has already modified this key.
  uint64_t get_timestamp(const K& key) const {
    uint64_t raw_hash = get_raw_hash(key);
    size_t bucket_index = get_bucket_index(raw_hash);
    int lock_index = get_lock_index(bucket_index);

    std::shared_lock<std::shared_mutex> lock(table_mutexes[lock_index].mutex);
    const std::vector<Ht_item<K, V>> &bucket = table[bucket_index];

    auto it = std::ranges::find_if(bucket, [&key](const Ht_item<K, V>& item) {
      return item.key == key;
    });

    if (it != bucket.end()) {
      return it->timestamp;
    }

    return 0; // 0 indicates the key does not exist yet
  }

  // COMMIT phase of the 2PC protocol.
  // Applies a single coordinator timestamp atomically across multiple buckets.
  int multi_put(const std::vector<std::pair<K, V>>& kv_pairs, const uint64_t tx_timestamp) {
    constexpr size_t MAX_MULTI_PUT_SIZE = 32;
    int lock_indices[MAX_MULTI_PUT_SIZE];
    size_t num_unique_locks = 0;

    size_t batch_size = std::min(kv_pairs.size(), MAX_MULTI_PUT_SIZE);
    for (size_t i = 0; i < batch_size; ++i) {
      uint64_t raw_hash = get_raw_hash(kv_pairs[i].first);
      lock_indices[i] = get_lock_index(get_bucket_index(raw_hash));
    }

    // Sort locks in ascending global order to prevent circular wait deadlock.
    std::sort(lock_indices, lock_indices + batch_size);

    // Deduplicate locks to prevent the same thread from attempting to acquire same lock multiple times.
    for (size_t i = 0; i < batch_size; ++i) {
      if (i == 0 || lock_indices[i] != lock_indices[num_unique_locks - 1]) {
        lock_indices[num_unique_locks++] = lock_indices[i];
      }
    }

    // Acquire all unique locks sequentially
    for (size_t i = 0; i < num_unique_locks; ++i) {
      table_mutexes[lock_indices[i]].mutex.lock();
    }

    int added = 0;
    for (size_t i = 0; i < batch_size; ++i) {
      const std::pair<K, V>& kv = kv_pairs[i];
      uint64_t raw_hash = get_raw_hash(kv.first);
      size_t bucket_index = get_bucket_index(raw_hash);
      std::vector<Ht_item<K, V>> &bucket = table[bucket_index];

      auto it = std::ranges::find_if(bucket, [&kv](const Ht_item<K, V>& item) {
        return item.key == kv.first;
      });

      if (it != bucket.end()) {
        if (tx_timestamp > it->timestamp) {
          it->value = kv.second;
          it->timestamp = tx_timestamp;
        }
      } else {
        bucket.push_back({kv.first, kv.second, tx_timestamp});
        added++;
      }
    }

    if (added > 0) {
        count.fetch_add(added, std::memory_order_relaxed);
    }

    // Release locks in reverse order
    for (int i = num_unique_locks - 1; i >= 0; --i) {
      table_mutexes[lock_indices[i]].mutex.unlock();
    }

    return added;
  }

  int get_capacity() const { return capacity; }
  int get_count() const { return count.load(); }
};