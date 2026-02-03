#include <iostream>
#include <string>

#include "dht_ring_node.h"

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Usage: ./dht_node <MY_IP_ADDRESS>\n";
        return 1;
    }

    std::string my_ip = argv[1];
    ConsistentHashingDHTNode node(my_ip);
    node.start();

    std::string cmd;
    std::cout << "Interactive DHT Shell (" << my_ip << ")\n";
    std::cout << "Commands: PUT k v | GET k | JOIN <peer_ip> | RING | EXIT\n";

    while (true) {
        std::cout << "> ";
        std::cin >> cmd;

        if (cmd == "EXIT") break;
        if (cmd == "RING") node.print_ring();
        else if (cmd == "JOIN") {
            std::string peer_ip;
            std::cin >> peer_ip;
            node.send_join_request(peer_ip);
        } 
        else if (cmd == "PUT") {
            int k, v;
            std::cin >> k >> v;
            std::cout << node.put(k, v);
        } 
        else if (cmd == "GET") {
            int k;
            std::cin >> k;
            std::cout << node.get(k);
        }
    }
    return 0;
}