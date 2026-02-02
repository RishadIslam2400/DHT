#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include "node.h"

int main(int argc, char** argv) {
    std::string target_ip;
    if (argc > 1) {
        target_ip = argv[1];
    }

    std::cout << "Starting Node on Port 1895..." << std::endl;
    std::cout << "Targeting Peer at IP: " << target_ip << std::endl;

    DHTNode node(1895);
    node.start();

    // Give the server a moment to start
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Test Communication
    std::cout << "Sending SET 100 500 to " << target_ip << "..." << std::endl;
    std::string response = DHTNode::send_to_1895(target_ip, "SET 100 500");
    std::cout << "Response: " << response << std::endl;

    std::cout << "Sending GET 100 to " << target_ip << "..." << std::endl;
    response = DHTNode::send_to_1895(target_ip, "GET 100");
    std::cout << "Response: " << response << std::endl;

    std::cout << "\nNode is running. Press [Enter] to stop.\n";
    std::cin.get(); 

    node.stop();
    return 0;
}