cmake_minimum_required(VERSION 3.10)
project(DistributedHashTable)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

find_package(Threads REQUIRED)

add_library(common_utils STATIC
    common/MurmurHash3.h
    common/MurmurHash3.cpp
    common/xxHash.h
    common/config_t.h
    common/config_t.cc
)
target_include_directories(common_utils PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/common)

add_executable(seq_hash_table tests/test_sequential_hash_table.cpp)
target_link_libraries(seq_hash_table PRIVATE common_utils)
target_include_directories(seq_hash_table PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/sequential_hash_table)

add_executable(global_lock_hash_table tests/test_global_lock_concurrent_hash_table.cpp)
target_link_libraries(global_lock_hash_table PRIVATE common_utils Threads::Threads)
target_include_directories(global_lock_hash_table PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/concurrent_hash_table)

add_executable(striped_lock_hash_table tests/test_striped_lock_concurrent_hash_table.cpp)
target_link_libraries(striped_lock_hash_table PRIVATE common_utils Threads::Threads)
target_include_directories(striped_lock_hash_table PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/concurrent_hash_table)

add_library(distributed_node_lib INTERFACE 
    node/p2p_node.h
    node/node.h
    node/ring_topology_node.h
)
target_include_directories(distributed_node_lib INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/node)
target_link_libraries(distributed_node_lib INTERFACE Threads::Threads common_utils)

add_executable(text_server tests/text_server.cc)
target_link_libraries(text_server distributed_node_lib)

add_executable(text_client tests/text_client.cc)
target_link_libraries(text_client distributed_node_lib)

add_executable(test_dht_node tests/test_dht_node.cpp)
target_link_libraries(test_dht_node PRIVATE Threads::Threads distributed_node_lib)

add_executable(test_dht_ring_node tests/test_dht_ring_node.cpp)
target_link_libraries(test_dht_ring_node PRIVATE Threads::Threads distributed_node_lib)