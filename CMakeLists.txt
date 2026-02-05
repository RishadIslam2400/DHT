cmake_minimum_required(VERSION 3.13)
project(DistributedHashTable LANGUAGES CXX)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
endif()
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "RelWithDebInfo")

message(STATUS "Build Type: ${CMAKE_BUILD_TYPE}")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(MSVC)
    add_compile_options(/W4 /permissive-)
else()
    add_compile_options(-Wall -Wno-unused-parameter)
    add_link_options(-static-libstdc++ -static-libgcc)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -march=native -DNDEBUG")
endif()

find_package(Threads REQUIRED)

add_library(common_utils STATIC
    common/MurmurHash3.h
    common/MurmurHash3.cpp
    common/xxHash.h
    common/config_t.h
    common/config_t.cc
    common/load_config.h
    common/threadpool.h
)
target_include_directories(common_utils PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}) 
target_link_libraries(common_utils PUBLIC Threads::Threads)

add_library(distributed_node_lib INTERFACE)
target_sources(distributed_node_lib INTERFACE
    node/dht_consistent_hashing_node.h
    node/dht_static_partitioning_node.h
)
target_include_directories(distributed_node_lib INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(distributed_node_lib INTERFACE common_utils)

# Sequential Hash Table Test
add_executable(seq_hash_table tests/test_sequential_hash_table.cpp)
target_link_libraries(seq_hash_table PRIVATE common_utils)
target_include_directories(seq_hash_table PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/sequential_hash_table)

# Global Lock Test
add_executable(global_lock_hash_table tests/test_global_lock_concurrent_hash_table.cpp)
target_link_libraries(global_lock_hash_table PRIVATE common_utils)
target_include_directories(global_lock_hash_table PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/concurrent_hash_table)

# Striped Lock Test
add_executable(striped_lock_hash_table tests/test_striped_lock_concurrent_hash_table.cpp)
target_link_libraries(striped_lock_hash_table PRIVATE common_utils)
target_include_directories(striped_lock_hash_table PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/concurrent_hash_table)

# DHT Static Partitioning Node Test
add_executable(test_dht_static_partitioning tests/test_dht_static_partitioning.cpp)
target_include_directories(test_dht_static_partitioning PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/node)
target_link_libraries(test_dht_static_partitioning PRIVATE distributed_node_lib)

# DHT Consistent Hashing Node Test
add_executable(test_dht_consistent_hashing tests/test_dht_consistent_hashing.cpp)
target_include_directories(test_dht_consistent_hashing PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/node)
target_link_libraries(test_dht_consistent_hashing PRIVATE distributed_node_lib)

# DHT Static Partitioning Benchmark
add_executable(run_benchmark_static_partitioning benchmark/run_benchmark_static_partitioning.cpp)
target_include_directories(run_benchmark_static_partitioning PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/node)
target_link_libraries(run_benchmark_static_partitioning PRIVATE distributed_node_lib)

# DHT Consistent Hashing Benchmark
add_executable(run_benchmark_consistent_hashing benchmark/run_benchmark_consistent_hashing.cpp)
target_include_directories(run_benchmark_consistent_hashing PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/node)
target_link_libraries(run_benchmark_consistent_hashing PRIVATE distributed_node_lib)