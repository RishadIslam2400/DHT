#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <functional>
#include <type_traits>
#include <shared_mutex>
#include <mutex>
#include <optional>
#include <atomic>
#include <memory>
#include "../common/MurmurHash3.h"

template <typename K, typename V>
struct Ht_item {
    K key;
    V value;
};

template <typename K, typename V>
class StripedLockConcurrentHashTable {
private:
    std::vector<std::vector<Ht_item<K, V>>> table;
    std::atomic<int> capacity;
    std::atomic<int> count;
    constexpr static float load_factor = 0.75f;
    
    mutable std::vector<std::unique_ptr<std::shared_mutex>> table_mutexes;
    int num_locks;

    // Encapsulated hash function to get the raw hash value
    uint64_t get_raw_hash(const K& key) const {
        std::hash<K> hasher;
        return hasher(key);
    }

    uint64_t get_raw_murmur_hash(const K& key, int cap) const {
        uint32_t seed = 2400;
        uint64_t hash_output[2] = {0}; // 128 bit output buffer
        const void *data_ptr = nullptr;
        size_t len = 0;

        if constexpr (std::is_same_v<K, std::string>) {
            // hash the characters not the object
            data_ptr = key.data();
            len = key.length();
        } else {
            data_ptr = &key;
            len = sizeof(K);
        }

        MurmurHash3_x86_128(data_ptr, (int)len, seed, hash_output);

        return hash_output[0];
    }

    int get_lock_index(uint64_t raw_hash) const {
        return raw_hash % num_locks;
    }

    size_t get_bucket_index(uint64_t raw_hash, int cap) const {
        return raw_hash % cap;
    }

    // resize is complicated so done exclusively by freezing the table
    void resize() {
        // acquire exclusive access to all the locks
        std::vector<std::unique_lock<std::shared_mutex>> locks;
        for (int i = 0; i < num_locks; ++i) {
            locks.emplace_back(*table_mutexes[i]);
        }

        // Check the load again if some other thread already resized
        if ((float) count / capacity <= load_factor) {
            return;
        }

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
    }

public:
    StripedLockConcurrentHashTable(int size = 1000, int locks = 16) : capacity(size), count(0), num_locks(locks) {
        table.resize(capacity);
        for (int i = 0; i < num_locks; ++i) {
            table_mutexes.push_back(std::make_unique<std::shared_mutex>());
        }
    }

    bool put(const K& key, const V& value) {
        if ((float)(count) / capacity > load_factor) {
            resize();
        }

        // Acquire exclusive access to the specific lock
        uint64_t raw_hash = get_raw_hash(key);
        int lock_index = get_lock_index(raw_hash);
        std::unique_lock<std::shared_mutex> lock(*table_mutexes[lock_index]);

        // Insert into the specific bucket
        size_t bucket_index = get_bucket_index(raw_hash, capacity);
        std::vector<Ht_item<K, V>>& bucket = table[bucket_index];

        // Check if key exists to update
        for (Ht_item<K, V>& item : bucket) {
            if (item.key == key) {
                item.value = value;
                return false; // updated existing
            }
        }

        // If not found, insert at the end of the bucket (handle collision)
        bucket.push_back({key, value});
        count++;
        return true;
    }

    // Reader lock here, allows concurrent reading by multiple threads
    std::optional<V> get(const K& key) {
        uint64_t raw_hash = get_raw_hash(key);
        int lock_index = get_lock_index(raw_hash);
        std::shared_lock<std::shared_mutex> lock(*table_mutexes[lock_index]);

        size_t bucket_index = get_bucket_index(raw_hash, capacity);
        std::vector<Ht_item<K, V>> &bucket = table[bucket_index];

        for (Ht_item<K, V>& item : bucket) {
            if (item.key == key) {
                return item.value;
            }
        }

        return std::nullopt;
    }

    // removes requires exclusive access
    void remove(const K& key) {
        uint64_t raw_hash = get_raw_hash(key);
        int lock_index = get_lock_index(raw_hash);
        std::unique_lock<std::shared_mutex> lock(*table_mutexes[lock_index]);

        size_t bucket_index = get_bucket_index(raw_hash, capacity);
        std::vector<Ht_item<K, V>>& bucket = table[bucket_index];

        for (auto it = bucket.begin(); it != bucket.end(); ++it) {
            if (it->key == key) {
                bucket.erase(it); // Automatically shifts the values to the left
                count--;
                return;
            }
        }
    }

    int get_capacity() const {
        return capacity.load();
    }

    int get_count() const {
        return count.load();
    }

    void print_table() const {
        std::vector<std::shared_lock<std::shared_mutex>> locks;
        for (int i = 0; i < num_locks; ++i) {
            locks.emplace_back(*table_mutexes[i]);
        }

        std::cout << "\nHash Table (Size: " << capacity << ")\n-------------------\n";
        for (int i = 0; i < capacity; ++i) {
            if (!table[i].empty()) {
                std::cout << "Index " << i << ": ";
                for (const auto& item : table[i]) {
                    std::cout << "[" << item.key << "] "; 
                }
                std::cout << "\n";
            }
        }
        std::cout << "-------------------\n\n";
    }
};