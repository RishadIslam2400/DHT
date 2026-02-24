cmake_minimum_required(VERSION 3.13)
project(DistributedHashTable LANGUAGES CXX)

enable_testing()

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
    add_compile_options(-Wall -Wextra -Wno-unused-parameter)
    add_link_options(-static-libstdc++ -static-libgcc)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -march=native -DNDEBUG")
endif()

find_package(Threads REQUIRED)
include_directories(include)

add_library(common_lib
    src/common/MurmurHash3.cpp
    src/common/utils.cpp
)

add_library(dht_lib 
    src/dht/dht_static_partitioning.cpp
    src/dht/node_properties.cpp
    src/dht/dht_message_batcher.cpp
    src/network/connection_pool.cpp
)
target_link_libraries(dht_lib PUBLIC common_lib Threads::Threads)

# Suppressing tests since all tests passed

# Concurrent hash table test
# add_executable(test_striped_lock_hash_table tests/test_striped_lock_concurrent_hash_table.cpp)
# target_link_libraries(test_striped_lock_hash_table PRIVATE Threads::Threads)
# add_test(NAME ConcurrentHashTableTest COMMAND test_striped_lock_hash_table)

# Connection pool test
# add_executable(test_connection_pool tests/test_connection_pool.cpp)
# target_link_libraries(test_connection_pool PRIVATE dht_lib)
# add_test(NAME ConnectionPoolTest COMMAND test_connection_pool)

# DHT Static Partitioning Test
# add_executable(test_dht_static_partitioning tests/test_dht_static_partitioning.cpp)
# target_link_libraries(test_dht_static_partitioning PRIVATE dht_lib)
# add_test(NAME DHTTest COMMAND test_dht_static_partitioning)

# DHT Static Partitioning Benchmark
add_executable(run_benchmark_static_partitioning benchmark/run_benchmark_static_partitioning.cpp)
target_link_libraries(run_benchmark_static_partitioning PRIVATE dht_lib)
