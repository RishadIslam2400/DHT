// CSE 375/475 Assignment #1
// Spring 2024
//
// Description:  This file implements the config_t::dump() function

#include <iostream>
#include "config_t.h"

// Print the values of the iters, and name fields as comma separated
// values, with a header row
void config_t::dump() {
    // std::cout << "# key_max, iters, limit, threads" << std::endl;
    std::cout << key_max << ", "
              << iters << ", "
              << limit << ", "
              << threads << std::endl;
}
