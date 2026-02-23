#pragma once

#include <shared_mutex>
#include <vector>
#include <atomic>
#include <memory>
#include <optional>
#include <mutex>
#include <iostream>
#include <algorithm>

#include "common/xxHash64.h"

template <typename K, typename V>
struct Ht_item {
  K key;
  V value;
};

struct alignas(64) AlignedLock {
  std::shared_mutex mutex;
};

// Removing resize for simplicity
// We know the key range so the number of unique keys can not be more than the key range
template <typename K, typename V>
class StripedLockConcurrentHashTable {
private:
  // Hash table variables
  std::vector<std::vector<Ht_item<K, V>>> table;
  std::atomic<int> count;
  int capacity;
  // constexpr static float load_factor = 0.75f;
  
  // Concurrency variables
  mutable std::unique_ptr<AlignedLock[]> table_mutexes;
  int num_locks;
  // std::mutex resize_mtx;

  inline uint64_t get_raw_hash(const K& key) const {
    if constexpr (std::is_same_v<K, std::string>) {
      return XXHash64::hash(key.data(), key.size(), 0);
    } else {
      return XXHash64::hash(&key, sizeof(K), 0);
    }
  }

  inline int get_lock_index(size_t bucket_index) const {
    return bucket_index & (num_locks - 1); // remove modulo operation with bit operation
  }

  inline size_t get_bucket_index(uint64_t raw_hash) const {
    return raw_hash % capacity;
  }

  /* void resize() {
    // Use a mutex to ensure only one thread manages the resize process
    std::unique_lock<std::mutex> resize_lock(resize_mtx, std::try_to_lock);

    if (!resize_lock.owns_lock()) {
      return; // Another thread is already resizing
    }

    // Check the load again if some other thread already resized
    if ((float) count / capacity <= load_factor) {
      return;
    }

    // Acquire exclusive access to all locks
    for (int i = 0; i < num_locks; ++i) {
      table_mutexes[i].mutex.lock();
    }

    // resize operation
    int new_capacity = 2 * capacity;
    std::vector<std::vector<Ht_item<K, V>>> temp_table(new_capacity);

    for (std::vector<Ht_item<K, V>>& bucket : table) {
      for (Ht_item<K, V>& item : bucket) {
        uint64_t raw_hash = get_raw_hash(item.key);
        size_t new_index = get_bucket_index(raw_hash, new_capacity);
        temp_table[new_index].push_back(std::move(item));
      }
    }

    table = std::move(temp_table);
    capacity = new_capacity;

    // Release exclusive access to all the locks
    for (int i = 0; i < num_locks; ++i) {
      table_mutexes[i].mutex.unlock();
    }
  } */

public:
  explicit StripedLockConcurrentHashTable(int size, int locks) : count(0) {
    // ensure lock is a multiple of 2
    num_locks = 1;
    while (num_locks < locks) {
      num_locks <<= 1; // Bitwise shift left (multiply by 2)
    }

    // Ensure capacity is a multiple of locks.
    // This guarantees that when we double capacity, the lock mapping stays stable.
    if (size % locks != 0) {
      size += (locks - (size % locks));
    }

    capacity = size;
    table.resize(capacity);
    table_mutexes = std::make_unique<AlignedLock[]>(num_locks);
  }

  bool put(const K& key, const V& value) {
    /* if ((float)(count.load()) / capacity.load() > load_factor) {
        resize();
    } */

    uint64_t raw_hash = get_raw_hash(key);
    // int current_cap = capacity.load();
    size_t bucket_index = get_bucket_index(raw_hash);
    int lock_index = get_lock_index(bucket_index);

    std::unique_lock<std::shared_mutex> lock(table_mutexes[lock_index].mutex);
    
    // recalculate bucket index, can change because of resize
    // current_cap = capacity.load();
    // bucket_index = get_bucket_index(raw_hash, current_cap);
    std::vector<Ht_item<K, V>>& bucket = table[bucket_index];

    // Check if key exists to update
    for (Ht_item<K, V>& item : bucket) {
      if (item.key == key) {
        item.value = value;
        return false;
      }
    }

    // If not found, insert at the end of the bucket (handle collision)
    bucket.push_back({key, value});
    count++;
    return true;
  }

  std::optional<V> get(const K& key) const {
      uint64_t raw_hash = get_raw_hash(key);
      // int current_cap = capacity.load();
      size_t bucket_index = get_bucket_index(raw_hash);
      int lock_index = get_lock_index(bucket_index);

      std::shared_lock<std::shared_mutex> lock(table_mutexes[lock_index].mutex);
      
      // Recalculate bucket in case resize happened while waiting
      // current_cap = capacity.load();
      // bucket_index = get_bucket_index(raw_hash, current_cap);
      const std::vector<Ht_item<K, V>> &bucket = table[bucket_index];

      for (const Ht_item<K, V>& item : bucket) {
          if (item.key == key) {
              return item.value;
          }
      }

      return std::nullopt;
  }

  // Atomic insert of multiple keys into the hash table (2 or more)
  // returns number of new elements inserted in the table
  int multi_put(const std::vector<std::pair<K, V>>& kv_pairs) {
      /* if ((float)(count.load() + kv_pairs.size()) / capacity.load() > load_factor) {
          resize();
      } */

      // Store required lock striped for this operation
      std::vector<int> lock_indices;
      lock_indices.reserve(kv_pairs.size());
      // int current_cap = capacity.load();

      for (const std::pair<K, V>& kv : kv_pairs) {
          uint64_t raw_hash = get_raw_hash(kv.first);
          lock_indices.push_back(get_lock_index(get_bucket_index(raw_hash)));
      }

      // Acquire locks in a consistent global order (increasing index).
      std::sort(lock_indices.begin(), lock_indices.end());
      lock_indices.erase(std::unique(lock_indices.begin(), lock_indices.end()), lock_indices.end());

      std::vector<std::unique_lock<std::shared_mutex>> locks;
      locks.reserve(lock_indices.size());

      for (int lock_idx : lock_indices) {
          locks.emplace_back(table_mutexes[lock_idx].mutex);
      }

      // Batched put
      int added = 0;
      // current_cap = capacity.load();

      for (const std::pair<K, V>& kv : kv_pairs) {
          uint64_t raw_hash = get_raw_hash(kv.first);
          size_t bucket_index = get_bucket_index(raw_hash);
          std::vector<Ht_item<K, V>>& bucket = table[bucket_index];

          bool found = false;
          for (Ht_item<K, V>& item : bucket) {
              if (item.key == kv.first) {
                  item.value = kv.second;
                  found = true;
                  break;
              }
          }

          if (!found) {
              bucket.push_back({kv.first, kv.second});
              added++;
          }
      }

      if (added > 0) {
          count.fetch_add(added);
      }

      return added;
  }

  /* bool remove(const K& key) {
      uint64_t raw_hash = get_raw_hash(key);
      int current_cap = capacity.load();
      size_t bucket_index = get_bucket_index(raw_hash, current_cap);
      int lock_index = get_lock_index(bucket_index);

      std::unique_lock<std::shared_mutex> lock(table_mutexes[lock_index].mutex);
      
      // recalculate bucket index, can change because of resize
      current_cap = capacity.load();
      bucket_index = get_bucket_index(raw_hash, current_cap);
      std::vector<Ht_item<K, V>>& bucket = table[bucket_index];

      for (auto it = bucket.begin(); it != bucket.end(); ++it) {
          if (it->key == key) {
              bucket.erase(it); // Automatically shifts the values to the left
              count--;
              return true;
          }
      }

      return false; // item does not exist
  } */

  int get_capacity() const { return capacity; }
  int get_count() const { return count.load(); }

  void print_table() const {
      // Acquire all shared lock for printing
      for (int i = 0; i < num_locks; ++i)
          table_mutexes[i].mutex.lock_shared();

      std::cout << "\nHash Table (Size: " << capacity << ")\n";
      for (int i = 0; i < capacity; ++i) {
          if (!table[i].empty()) {
              std::cout << "Index " << i << ": ";
              for (const auto& item : table[i]) {
                  std::cout << "[" << item.key << "] "; 
              }
              std::cout << "\n";
          }
      }
      std::cout << "\n";

      for (int i = 0; i < num_locks; ++i)
          table_mutexes[i].mutex.unlock_shared();
  }
};