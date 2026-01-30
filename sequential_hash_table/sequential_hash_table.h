#pragma once

#include <string>
#include <iostream>
#include <vector>
#include <functional>
#include <MurmurHash3.h>
#include <type_traits>
#include <optional>

template <typename K, typename V>
struct Ht_item {
    K key;
    V value;
};


template <typename K, typename V>
class SequentialHashTable {
private:
    std::vector<std::vector<Ht_item<K, V>>> table;
    int capacity;
    int count;

    const float load_factor = 0.75f;

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
    SequentialHashTable(int size = 100) : capacity(size), count(0) {
        table.resize(capacity);
    }

    void insert(const K& key, const V& value) {
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

    std::optional<V> search(const K& key) {
        unsigned long index = hash_function(key, capacity);
        std::vector<Ht_item<K, V>> &bucket = table[index];

        for (Ht_item<K, V>& item : bucket) {
            if (item.key == key) {
                return item.value;
            }
        }

        return std::nullopt;
    }

    void remove(const K& key) {
        unsigned long index = hash_function(key, capacity);
        std::vector<Ht_item<K, V>> &bucket = table[index];

        for (auto it = bucket.begin(); it != bucket.end(); ++it) {
            if (it->key == key) {
                // vector::erase shifts subsequent elements left (O(N) of bucket size)
                // but for small buckets, this is very fast.
                bucket.erase(it);
                count--;
                return;
            }
        }
    }

    int get_capacity() const { return capacity; }
    int get_count() const { return count; }

    void print_table() {
        std::cout << "\nHash Table (Vector Buckets)\n-------------------\n";
        for (int i = 0; i < capacity; ++i) {
            if (!table[i].empty()) {
                std::cout << "Index " << i << ": ";
                for (const Ht_item<K, V>& item : table[i]) {
                    std::cout << "[" << item.key << ":" << item.value << "] ";
                }
                std::cout << "\n";
            }
        }
        std::cout << "-------------------\n\n";
    }
};