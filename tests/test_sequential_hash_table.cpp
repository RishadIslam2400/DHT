#include <iostream>
#include <string>

#include "sequential_hash_table.h"


int main() {
    SequentialHashTable<std::string, int> ht(5); 

    std::cout << "Initial Capacity: " << ht.get_capacity() << "\n";

    // Insert enough items to trigger resize ( > 5 * 0.75 = 3.75 items)
    ht.insert("One", 1);
    ht.insert("Two", 2);
    ht.insert("Three", 3);
    ht.insert("Four", 4); // This should trigger resize
    ht.insert("Five", 5);

    std::cout << "New Capacity: " << ht.get_capacity() << "\n"; // Should be 10

    ht.print_table();
    return 0;
}