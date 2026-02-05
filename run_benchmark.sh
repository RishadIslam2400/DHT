#!/bin/bash
# run_benchmark.sh

KEY_RANGES=(10 100 1000 10000)
RUNS=5

for RANGE in "${KEY_RANGES[@]}"; do
    echo "Benchmarking Key Range: $RANGE"
    for ((i=1; i<=RUNS; i++)); do
        echo "  Run $i..."
        # Launch nodes in background
        ./cl.sh run run_benchmark_static_partitioning 100000 3 $RANGE &
        
        wait # Wait for all to finish
        
        # Parse logs to extract throughput/latency and append to a master CSV
    done
done