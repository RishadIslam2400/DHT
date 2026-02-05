#include <iostream>
#include <thread>
#include <vector>
#include <string>
#include <cassert>
#include <atomic>
#include <chrono>

#include "global_lock_concurrent_hash_table.h"

void log(const std::string& message) {
    std::cout << "[TEST] " << message << std::endl;
}

void test_basic_operations() {
    log("Running Basic Opearting Test...");
    GlobalLockConcurrentHashTable<std::string, int> ht(10);

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

    // remove
    ht.remove("key1");
    val1 = ht.get("key1");
    assert(!val1.has_value());
    assert(ht.get_count() == 1);

    log("Basic Operations Test Passed!");
}

void insert_range(GlobalLockConcurrentHashTable<int, int>& ht, int start, int end) {
    for (int i = start; i < end; ++i) {
        ht.put(i, i * 10);
    }
}

void test_concurrent_inserts() {
    log("Running Concurrent Inserts Test...");
    GlobalLockConcurrentHashTable<int, int> ht(100);
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

    res = ht.get(15000);
    assert(res.has_value() && res.value() == 150000);

    log("Concurrent Inserts Test Passed");
}

void test_concurrent_read_writes() {
    log("Running Concurrent Reads/Writes Test...");
    GlobalLockConcurrentHashTable<std::string, int> ht(1000);
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

        auto res = ht.get("Key999");
        if (res.has_value()) {
            assert(res.value() == 999);
        }
    });

    writer.join();
    reader.join();

    assert(ht.get_count() == 1000);
    log("Concurrent Reads/Writes Test Passed!");
}

void test_massive_resize() {
    log("Running Massive Resize Stress Test...");
    GlobalLockConcurrentHashTable<int, int> ht(2);
    int total_items = 50000;
    int num_threads = 8;
    int chunk = total_items / num_threads;

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(insert_range, std::ref(ht), i * chunk, (i + 1) * chunk);
    }

    for (auto& t : threads) {
        t.join();
    }

    assert(ht.get_count() == num_threads * chunk);
    log("Massive Resize Stress Test Passed!");
}

int main() {
    test_basic_operations();
    test_concurrent_inserts();
    test_concurrent_read_writes();
    test_massive_resize();

    return 0;
}