#include <iostream>
#include <string>

#include "sequential_hash_table.h"


int main() {
    SequentialHashTable<std::string, int> ht(5); 

    std::cout << "Initial Capacity: " << ht.get_capacity() << "\n";

    // Insert enough items to trigger resize ( > 5 * 0.75 = 3.75 items)
    ht.put("One", 1);
    ht.put("Two", 2);
    ht.put("Three", 3);
    ht.put("Four", 4); // This should trigger resize
    ht.put("Five", 5);

    std::cout << "New Capacity: " << ht.get_capacity() << "\n"; // Should be 10

    ht.print_table();
    return 0;
}