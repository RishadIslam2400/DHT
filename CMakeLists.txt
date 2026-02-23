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
    add_compile_options(-Wall -Wextra -Wno-unused-parameter)
    add_link_options(-static-libstdc++ -static-libgcc)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -march=native -DNDEBUG")
endif()

find_package(Threads REQUIRED)
include_directories(include)

add_library(common_lib
    src/common/MurmurHash3.cpp
)

add_library(dht_lib 
    src/dht/dht_static_partitioning.cpp
    src/dht/node_properties.cpp
    src/dht/dht_message_batcher.cpp
    src/network/connection_pool.cpp
)
target_link_libraries(dht_lib PUBLIC common_lib Threads::Threads)

add_executable(test_striped_lock_hash_table tests/test_striped_lock_concurrent_hash_table.cpp)

# # DHT Static Partitioning Test
# add_executable(test_dht_static_partitioning tests/test_dht_static_partitioning.cpp)
# target_include_directories(test_dht_static_partitioning PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/dht)
# target_link_libraries(test_dht_static_partitioning PRIVATE dht_lib)

# # DHT Consistent Hashing Test
# # add_executable(test_dht_consistent_hashing tests/test_dht_consistent_hashing.cpp)
# # target_include_directories(test_dht_consistent_hashing PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/dht)
# # target_link_libraries(test_dht_consistent_hashing PRIVATE dht_lib)

# # Connection Pool Test
# # add_executable(test_connection_pool tests/test_connection_pool.cpp)
# # target_include_directories(test_connection_pool PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/common)
# # target_link_libraries(test_connection_pool PRIVATE dht_lib)

# # DHT Static Partitioning Benchmark
# add_executable(run_benchmark_static_partitioning benchmark/run_benchmark_static_partitioning.cpp)
# target_include_directories(run_benchmark_static_partitioning PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/dht)
# target_link_libraries(run_benchmark_static_partitioning PRIVATE dht_lib)

# # DHT Consistent Hashing Benchmark
# # add_executable(run_benchmark_consistent_hashing benchmark/run_benchmark_consistent_hashing.cpp)
# # target_include_directories(run_benchmark_consistent_hashing PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/dht)
# # target_link_libraries(run_benchmark_consistent_hashing PRIVATE dht_lib)