cmake_minimum_required(VERSION 3.13)
project(DistributedHashTable LANGUAGES CXX)

enable_testing()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
endif()
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "RelWithDebInfo")

message(STATUS "Build Type: ${CMAKE_BUILD_TYPE}")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# if(CMAKE_BUILD_TYPE STREQUAL "Release")
#   include(CheckIPOSupported)
#   check_ipo_supported(RESULT ipo_supported OUTPUT error)
#   if(ipo_supported)
#     set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
#     message(STATUS "IPO / LTO Enabled: Max compiler optimizations active")
#   else()
#     message(WARNING "IPO / LTO is not supported by your compiler: ${error}")
#   endif()
# endif()

if(MSVC)
  add_compile_options(/W4 /permissive-)
else()
  add_compile_options(-Wall -Wextra -Wno-unused-parameter -Werror)
  add_link_options(-static-libstdc++ -static-libgcc)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -g -march=native -DNDEBUG")
endif()

find_package(Threads REQUIRED)

add_library(common_lib
    src/common/utils.cpp
)

target_include_directories(common_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
add_library(dht_lib 
    src/dht/dht_static_partitioning.cpp
    src/dht/dht_networking.cpp
    src/dht/dht_execution.cpp
    src/dht/dht_public_interface.cpp
    src/dht/node_properties.cpp
    src/dht/raft_engine.cpp
    src/dht/dht_transaction_manager.cpp
    src/network/connection_pool.cpp
)
target_link_libraries(dht_lib PUBLIC common_lib Threads::Threads)

# No test files exist currently

# DHT Static Partitioning Benchmark
add_executable(run_benchmark_static_partitioning benchmark/run_benchmark_static_partitioning.cpp)
target_link_libraries(run_benchmark_static_partitioning PRIVATE dht_lib)
