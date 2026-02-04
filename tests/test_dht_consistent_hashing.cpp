#include <iostream>
#include <vector>
#include <chrono>
#include <cassert>
#include <thread>
#include <string>

#include "dht_consistent_hashing_node.h"

void run_correctness_test(ConsistentHashingDHTNode& node, int my_id) {
    std::cout << "\n>>> [Test] Starting 3-Node Verification on Node " << my_id << "...\n";

    // Test 1: Node 0 writes (1, 100)
    if (my_id == 0) {
        std::cout << "[Test] Node 0 performing PUT(1, 100)...\n";
        std::string res = node.put(1, 100);
        std::cout << "[Test] Node 0 PUT result: " << res << "\n";
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Everyone verifies Key 1
    std::string val1 = node.get(1);
    if (val1 == "100") {
        std::cout << "[PASS] Node " << my_id << " read Key 1 correctly (Got: 100)\n";
    } else {
        std::cout << "[FAIL] Node " << my_id << " failed Key 1. Expected: 100, Got: " << val1 << "\n";
    }

    // Test 2: Node 1 writes (2, 200)
    if (my_id == 1) {
        std::cout << "[Test] Node 1 performing PUT(2, 200)...\n";
        std::string res = node.put(2, 200);
        std::cout << "[Test] Node 1 PUT result: " << res << "\n";
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Everyone verifies Key 2
    std::string val2 = node.get(2);
    if (val2 == "200") {
        std::cout << "[PASS] Node " << my_id << " read Key 2 correctly (Got: 200)\n";
    } else {
        std::cout << "[FAIL] Node " << my_id << " failed Key 2. Expected: 200, Got: " << val2 << "\n";
    }

    // Test 3: Node 2 writes (3, 300)
    if (my_id == 2) {
        std::cout << "[Test] Node 2 performing PUT(3, 300)...\n";
        std::string res = node.put(3, 300);
        std::cout << "[Test] Node 2 PUT result: " << res << "\n";
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Everyone verifies Key 3
    std::string val3 = node.get(3);
    if (val3 == "300") {
        std::cout << "[PASS] Node " << my_id << " read Key 3 correctly (Got: 300)\n";
    } else {
        std::cout << "[FAIL] Node " << my_id << " failed Key 3. Expected: 300, Got: " << val3 << "\n";
    }

    std::cout << ">>> [Test] Verification Complete for Node " << my_id << ".\n\n";
}

int main(int argc, char** argv) {
    // Arguments from cl.sh: <config_file> <node_id> <ops> <threads> <range>
    if (argc < 5) {
        std::cerr << "Usage: " << argv[0] << " <config_file> <node_id> ...\n";
        return 1;
    }

    std::string config_file = argv[1];
    int node_id = std::stoi(argv[2]);

    try {
        ConsistentHashingDHTNode node(config_file, node_id);

        node.start();
        node.wait_for_barrier();
        run_correctness_test(node, node_id);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        node.stop();

    } catch (const std::exception& e) {
        std::cerr << "[Fatal Error] " << e.what() << std::endl;
        return 1;
    }

    return 0;
}