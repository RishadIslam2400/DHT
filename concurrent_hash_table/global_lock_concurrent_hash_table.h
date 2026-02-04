#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <functional>
#include <type_traits>
#include <shared_mutex>
#include <mutex>
#include <optional>
#include "../common/MurmurHash3.h"

template <typename K, typename V>
struct Ht_item {
    K key;
    V value;
};

template <typename K, typename V>
class GlobalLockConcurrentHashTable {
private:
    std::vector<std::vector<Ht_item<K, V>>> table;
    int capacity;
    int count;
    const float load_factor = 0.75f;
    mutable std::shared_mutex table_mutex;

    // Encapsulated hash function
    unsigned long hash_function(const K& key, int cap) const {
        std::hash<K> hasher;

        return hasher(key) % cap;
    }

    int murmur_hash(const K& key, int cap) const {
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

        return hash_output[0] % cap;
    }


    // no locks here, the thread already holds the lock
    void resize() {
        int new_capacity = 2 * capacity;
        std::vector<std::vector<Ht_item<K, V>>> temp_table(new_capacity);

        for (const std::vector<Ht_item<K, V>>& bucket : table) {
            for (const Ht_item<K, V>& item : bucket) {
                unsigned long new_index = hash_function(item.key, new_capacity);
                temp_table[new_index].push_back(item);
            }
        }

        table = std::move(temp_table);
        capacity = new_capacity;
    }

public:
    GlobalLockConcurrentHashTable(int size = 100) : capacity(size), count(0) {
        table.resize(capacity);
    }

    // insert requires exclusive access
    void insert(const K& key, const V& value) {
        std::unique_lock<std::shared_mutex> lock(table_mutex);

        if ((float)(count + 1) / capacity > load_factor) {
            resize();
        }

        unsigned long index = hash_function(key, capacity);
        std::vector<Ht_item<K, V>>& bucket = table[index];

        // Check if key exists to update
        for (Ht_item<K, V>& item : bucket) {
            if (item.key == key) {
                item.value = value;
                return;
            }
        }

        // If not found, insert at the end of the bucket (handle collision)
        bucket.push_back({key, value});
        count++;
    }

    // Reader lock here, allows concurrent reading by multiple threads
    std::optional<V> search(const K& key) {
        std::shared_lock<std::shared_mutex> lock(table_mutex);

        unsigned long index = hash_function(key, capacity);
        std::vector<Ht_item<K, V>> &bucket = table[index];

        for (Ht_item<K, V>& item : bucket) {
            if (item.key == key) {
                return item.value;
            }
        }

        return std::nullopt;
    }

    // removes requires exclusive access
    void remove(const K& key) {
        std::unique_lock<std::shared_mutex> lock(table_mutex);

        unsigned long index = hash_function(key, capacity);
        std::vector<Ht_item<K, V>>& bucket = table[index];

        for (auto it = bucket.begin(); it != bucket.end(); ++it) {
            if (it->key == key) {
                bucket.erase(it);
                count--;
                return;
            }
        }
    }

    int get_capacity() const {
        std::shared_lock<std::shared_mutex> lock(table_mutex);
        return capacity;
    }

    int get_count() const {
        std::shared_lock<std::shared_mutex> lock(table_mutex);
        return count;
    }

    void print_table() const {
        std::shared_lock<std::shared_mutex> lock(table_mutex);
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