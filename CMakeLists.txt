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

add_library(p2p_lib INTERFACE node/p2p_node.h)
target_include_directories(p2p_lib INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/node)
target_link_libraries(p2p_lib INTERFACE Threads::Threads)

add_executable(text_server node/text_server.cc)
target_link_libraries(text_server p2p_lib)

add_executable(text_client node/text_client.cc)
target_link_libraries(text_client p2p_lib)