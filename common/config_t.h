// CSE 375/475 Assignment #1
// Spring 2024
//
// Description: This file declares a struct for storing per-execution configuration information.
#pragma once

#include <iostream>
#include <string>

// store all of our command-line configuration parameters

struct config_t {

  // The maximum key value
    int key_max;

    // The number of iterations for which a test should run
    int iters;

    // The number of threads to use
    int threads;

    // The numbers of swaps allowed
    int limit;

    // simple constructor
    config_t() : key_max(10000), iters(1000000), limit(5), threads(4) { }

    // Print the values of the iters, and name fields
    void dump();
};
