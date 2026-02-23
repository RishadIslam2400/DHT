#pragma once

#include <shared_mutex>
#include <optional>
#include <mutex>

//#include "common/MurmurHash3.h"

template <typename K, typename V>
struct Ht_item {
    K key;
    V value;
};

template <typename K, typename V>
class GlobalLockConcurrentHashTable {
private:
    // Hash table variables
    std::vector<std::vector<Ht_item<K, V>>> table;
    size_t capacity;
    size_t count;
    constexpr static float load_factor = 0.75f;

    // Concurrency variables
    mutable std::shared_mutex table_mutex;

    // Encapsulated hash function
    uint64_t get_raw_hash(const K& key) const {
        std::hash<K> hasher;
        return hasher(key);
    }

    /* uint64_t get_raw_murmur_hash(const K& key, int cap) const {
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
    } */

    size_t get_bucket_index(uint64_t raw_hash, size_t cap) const {
        return raw_hash % cap;
    }

    // put() already hold the unique lock for resizing
    void resize() {
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
    GlobalLockConcurrentHashTable(int size = 1024) : capacity(size), count(0) {
        table.resize(capacity);
    }

    // insert requires exclusive access
    bool put(const K& key, const V& value) {
        std::unique_lock<std::shared_mutex> lock(table_mutex);

        if ((float)count / capacity > load_factor) {
            resize();
        }

        uint64_t raw_hash = get_raw_hash(key);
        size_t bucket_index = get_bucket_index(raw_hash, capacity);
        std::vector<Ht_item<K, V>>& bucket = table[bucket_index];

        // Check if key exists to update
        for (Ht_item<K, V>& item : bucket) {
            if (item.key == key) {
                item.value = value;
                return false;
            }
        }

        // If not found, insert at the end of the bucket
        bucket.push_back({key, value});
        count++;
        return true;
    }

    // Reader lock here, allows concurrent reading by multiple threads
    std::optional<V> get(const K& key) const {
        std::shared_lock<std::shared_mutex> lock(table_mutex);

        uint64_t raw_hash = get_raw_hash(key);
        size_t bucket_index = get_bucket_index(raw_hash, capacity);
        const std::vector<Ht_item<K, V>> &bucket = table[bucket_index];

        for (const Ht_item<K, V>& item : bucket) {
            if (item.key == key) {
                return item.value;
            }
        }

        return std::nullopt;
    }

    // removes requires exclusive access
    void remove(const K& key) {
        std::unique_lock<std::shared_mutex> lock(table_mutex);

        uint64_t raw_hash = get_raw_hash(key);
        size_t bucket_index = get_bucket_index(raw_hash, capacity);
        std::vector<Ht_item<K, V>>& bucket = table[bucket_index];

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
    }
};