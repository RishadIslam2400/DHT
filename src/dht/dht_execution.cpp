#include "dht/dht_static_partitioning.h"
#include "common/xxHash64.h"
#include "common/utils.h"

#include <cstring>
#include <random>
#include <algorithm>
#include <iostream>

// Evaluates incoming network timestamps and fast-forwards the local Lamport clock
void StaticClusterDHTNode::synchronize_clock(const uint64_t incoming_ts) {
  uint64_t current_clock = logical_clock.load(std::memory_order_relaxed);

  // If local clock == incoming_ts, we increase it to incoming_ts + 1 
  while (current_clock <= incoming_ts) {
    // If logical_clock still equals current_clock, it updates to incoming_ts.
    // If it fails (another thread updated it), current_clock is automatically refreshed.
    if (logical_clock.compare_exchange_weak(current_clock, incoming_ts + 1, std::memory_order_relaxed)) {
      break;
    }
  }
}

void StaticClusterDHTNode::stale_lock_sweeper_loop() {
  const auto TIMEOUT_THRESHOLD = std::chrono::seconds(5);

  // Allocate memory
  std::vector<std::pair<uint64_t, int>> stale_transactions;
  stale_transactions.reserve(32);

  // Initialize telemetry
  TelemetryBatcher background_batcher;
  background_batcher.stats = &this->stats;

  while (running.load(std::memory_order_relaxed)) {
    // Low frequency sleep to avoid stealing CPU cycles
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    auto now = std::chrono::steady_clock::now();

    // Identify stale locks quickly without holding spinlocks for long
    for (size_t i = 0; i < num_logical_stripes; ++i) {
      std::lock_guard<Spinlock> lock(staging_locks[i].mutex);
      for (const auto& staged_tx : staging_stripes[i]) {
        if (now - staged_tx.staged_at > TIMEOUT_THRESHOLD) {
          stale_transactions.push_back({staged_tx.tx_timestamp, staged_tx.coordinator_id});
        }
      }
    }

    if (stale_transactions.empty()) continue;

    // Interrogate the coordinators
    for (const auto& [ts, coord_id] : stale_transactions) {
      if (coord_id < 0 || coord_id >= static_cast<int>(cluster_map.size())) [[unlikely]]
        continue;

      const NodeConfig& target = cluster_map[coord_id];
      TxCommitHeader header = { .tx_timestamp = ts };
      uint8_t response = 0; // 0 = Aborted/Unknown, 1 = Committed

      // Send a Status RPC Check
      bool success = send_single_request(target.id, target.ip, target.port,
                                         ProtocolType::TwoPhaseCommit,
                                         static_cast<uint8_t>(TwoPhaseCommitCommand::StatusCheck),
                                         reinterpret_cast<const uint8_t*>(&header),
                                         sizeof(TxCommitHeader), &response, 1);
      
      // Resolve the lock based on Coordinator's response
      if (success) {
        if (response == 1) {
          #ifndef NDEBUG
            std::cout << "[Sweeper] Recovered lost COMMIT for TX " << ts << "\n";
          #endif
          local_tx_commit(ts, background_batcher);
        } else {
          #ifndef NDEBUG
            std::cout << "[Sweeper] Coordinator forgot TX " << ts << ". Forcing ABORT.\n";
          #endif
          local_tx_abort(ts, background_batcher);
        }
      }
    }

    // Clear the vector for the next iteration
    stale_transactions.clear();
  }
}

PutResult StaticClusterDHTNode::put_local(const uint32_t key, const uint32_t value,
                                          const uint64_t timestamp, TelemetryBatcher& batcher) {
  // Optional: Track storage latency
  // auto start_time = std::chrono::high_resolution_clock::now();

  constexpr int MAX_RETRIES = 1000;
  uint16_t attempt = 0;

  size_t stripe = hash_key(key) & logical_stripe_mask;
  PutResult result;

  // Check logically locked keys
  while (true) {
    {
      // Acquire the logical stripe Spinlock
      std::lock_guard<Spinlock> lock(stripe_locks[stripe].mutex);
      std::vector<uint32_t>& locked_keys = logically_locked_stripes[stripe];

      // Check for 2PC logical locks
      if (std::find(locked_keys.begin(), locked_keys.end(), key) == locked_keys.end()) [[likely]] {
        // Insert while holding logical locks stripes so no other 2PC transactions can change it
        result = storage.put(key, value, timestamp);
        break;
      }
    } // Logical lock released

    // Backoff strategy if the key is logically locked by an active 2PC
    attempt++;
    if (attempt >= MAX_RETRIES) {
      return PutResult::Failed;
    }

    if (attempt < 100) {
      #if defined(__x86_64__) || defined(_M_X64)
        __builtin_ia32_pause(); 
      #elif defined(__aarch64__) || defined(__arm__)
        // Hardware-level pipeline yield for ARM (does not context switch)
        asm volatile("yield" ::: "memory"); 
      #else
        // If an unknown architecture, spin quietly
      #endif
    } else {
      std::this_thread::yield();
    }
  }

  if (result == PutResult::Inserted) [[likely]] batcher.local_inserted++;
  else if (result == PutResult::Updated) [[likely]] batcher.local_updated++;
  else [[unlikely]] batcher.local_dropped++;

  return result;
}

std::optional<uint32_t> StaticClusterDHTNode::get_local(const uint32_t key, TelemetryBatcher& batcher) const {
  // Reads bypass the logical locks completely (Read-Committed isolation)
  std::optional<uint32_t> val = storage.get(key);

  if (val.has_value()) [[likely]] {
    batcher.local_found++;
  } else [[unlikely]] {
    batcher.local_not_found++;
  }

  return val;
}

PutResult StaticClusterDHTNode::put_remote(const uint32_t key, const uint32_t value,
                                           const uint64_t timestamp, const NodeConfig &target,
                                           TelemetryBatcher& batcher)
{
  PutRequest req{key, value, timestamp};
  uint8_t response_byte = 0;

  auto start_time = std::chrono::high_resolution_clock::now();
  bool success = send_single_request(target.id, target.ip, target.port,
                                     ProtocolType::ClientDht,
                                     static_cast<uint8_t>(DhtCommand::Put),
                                     reinterpret_cast<const uint8_t*>(&req),
                                     sizeof(PutRequest),
                                     &response_byte, 1);

  auto end_time = std::chrono::high_resolution_clock::now();
  uint64_t duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

  batcher.remote_put_ns += duration_ns;

  PutResult result = PutResult::Failed;

  if (success) [[likely]] {
    result = static_cast<PutResult>(response_byte); 

    if (result == PutResult::Inserted || result == PutResult::Updated) [[likely]] {
      batcher.remote_put_success++;
    } else {
      batcher.remote_put_failed++;
    }
  } else [[unlikely]] {
    batcher.remote_put_failed++;
  }

  return result;
}

GetResponse StaticClusterDHTNode::get_remote(const uint32_t key, const uint64_t read_ts,
                                             const NodeConfig &target, TelemetryBatcher& batcher)
{
  GetRequest req{key, read_ts};
  GetResponse response{};

  auto start_time = std::chrono::high_resolution_clock::now();
  bool success = send_single_request(target.id, target.ip, target.port,
                                     ProtocolType::ClientDht,
                                     static_cast<uint8_t>(DhtCommand::Get),
                                     reinterpret_cast<const uint8_t*>(&req),
                                     sizeof(GetRequest),
                                     reinterpret_cast<uint8_t*>(&response),
                                     sizeof(GetResponse));

  auto end_time = std::chrono::high_resolution_clock::now();
  uint64_t duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  
  batcher.remote_get_ns += duration_ns;

  if (success) [[likely]] {
    if (response.status == GetStatus::Found || response.status == GetStatus::NotFound) [[likely]] {
      batcher.remote_get_success++;
      return response;
    } else {
      batcher.remote_get_failed++;
    }
  } else [[unlikely]] {
    batcher.remote_get_failed++;
  }

  return GetResponse::error();
}

// 2PC Phase 1: Optimistic Concurrency Control (OCC) Validation
bool StaticClusterDHTNode::local_tx_prepare(const uint64_t tx_timestamp, const int coordinator_id, 
                                            const std::vector<std::pair<uint32_t, uint32_t>> &batch, 
                                            TelemetryBatcher& batcher)
{
  auto start_time = std::chrono::high_resolution_clock::now();

  // Stack allocation for required stripes
  constexpr size_t MAX_BATCH_SIZE = 32; 
  size_t required_stripes[MAX_BATCH_SIZE];
  size_t batch_size = std::min(batch.size(), MAX_BATCH_SIZE);
  size_t num_unique_stripes = 0;

  // Map keys to their required lock stripes
  for (size_t i = 0; i < batch_size; ++i) {
    required_stripes[i] = hash_key(batch[i].first) & logical_stripe_mask;
  }

  // Global order for the stripes
  std::sort(required_stripes, required_stripes + batch_size);

  // Deduplicate stripes
  for (size_t i = 0; i < batch_size; ++i) {
    if (i == 0 || required_stripes[i] != required_stripes[num_unique_stripes - 1]) {
      required_stripes[num_unique_stripes++] = required_stripes[i];
    }
  }

  // Acquire stripe locks in strict ascending order
  for (size_t i = 0; i < num_unique_stripes; ++i) {
    stripe_locks[required_stripes[i]].mutex.lock();
  }

  // Validate the keys
  bool validation_failed = false;

  for (size_t i = 0; i < batch_size; ++i) {
    const uint32_t key = batch[i].first;
    size_t stripe = hash_key(key) & logical_stripe_mask;

    // Lock collision check
    const auto& locked_keys = logically_locked_stripes[stripe];
    if (std::find(locked_keys.begin(), locked_keys.end(), key) != locked_keys.end()) [[unlikely]] {
      batcher.tx_prepare_locked++; 
      validation_failed = true;
      break;
    }

    // LWW Timestamp check
    if (tx_timestamp <= storage.get_timestamp(key)) [[unlikely]] {
      batcher.tx_prepare_obsolete++;
      validation_failed = true;
      break;
    }
  }

  // Rollback immediately if any validation failed
  if (validation_failed) [[unlikely]] {
    for (int i = static_cast<int>(num_unique_stripes) - 1; i >= 0; --i) {
      stripe_locks[required_stripes[i]].mutex.unlock();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    batcher.tx_prepare_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    return false;
  }

  // Optimistically claim the logical locks
  for (size_t i = 0; i < batch_size; ++i) {
    size_t stripe = hash_key(batch[i].first) & logical_stripe_mask;
    logically_locked_stripes[stripe].push_back(batch[i].first);
  }

  // Release striped locks
  for (int i = static_cast<int>(num_unique_stripes) - 1; i >= 0; --i) {
    stripe_locks[required_stripes[i]].mutex.unlock();
  }

  // Push to the staging area sharded by timestamp
  size_t staging_stripe = tx_timestamp & logical_stripe_mask;
  {
    std::lock_guard<Spinlock> stage_lock(staging_locks[staging_stripe].mutex);
    staging_stripes[staging_stripe].push_back({
        tx_timestamp, 
        coordinator_id,                             
        std::chrono::steady_clock::now(),
        batch 
    });
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  batcher.tx_prepare_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

  return true;
}

// Phase 2 (Success): Extract from staging area and apply to physical storage
void StaticClusterDHTNode::local_tx_commit(const uint64_t tx_timestamp, TelemetryBatcher& batcher) {
  auto start_time = std::chrono::high_resolution_clock::now();

  // Route to the correct staging shard
  size_t staging_stripe = tx_timestamp & logical_stripe_mask;
  std::vector<std::pair<uint32_t, uint32_t>> batch_to_commit;

  // Extract the transaction from the staging area
  {
    std::lock_guard<Spinlock> stage_lock(staging_locks[staging_stripe].mutex);
    auto &stripe_vector = staging_stripes[staging_stripe];

    for (auto it = stripe_vector.begin(); it != stripe_vector.end(); ++it) {
      if (it->tx_timestamp == tx_timestamp) {
        batch_to_commit = std::move(it->batch);

        if (it != stripe_vector.end() - 1) {
          *it = std::move(stripe_vector.back());
        }

        stripe_vector.pop_back();
        break;
      }
    }
  } // Release stage lock

  if (batch_to_commit.empty()) [[unlikely]] {
    auto end_time = std::chrono::high_resolution_clock::now();
    batcher.tx_commit_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    return;
  }

  // Insert into local storage
  if (batch_to_commit.size() == 1) {
    storage.put(batch_to_commit[0].first, batch_to_commit[0].second, tx_timestamp);
  } else {
    storage.multi_put(batch_to_commit, tx_timestamp);
  }

  std::sort(batch_to_commit.begin(), batch_to_commit.end(),
            [this](const auto& a, const auto& b) {
              return (hash_key(a.first) & logical_stripe_mask) <
                     (hash_key(b.first) & logical_stripe_mask);
            });
  
  size_t current_stripe = std::numeric_limits<size_t>::max();

  // Release the logical locks to allow new transactions
  for (const auto& kv : batch_to_commit) {
    size_t lock_stripe = hash_key(kv.first) & logical_stripe_mask;
    
    // If we've moved to a new stripe, swap our locks
    if (lock_stripe != current_stripe) {
      if (current_stripe != std::numeric_limits<size_t>::max()) {
        stripe_locks[current_stripe].mutex.unlock();
      }
      stripe_locks[lock_stripe].mutex.lock();
      current_stripe = lock_stripe;
    }
    
    // Erase the key while holding the lock
    std::vector<uint32_t>& locked_keys = logically_locked_stripes[current_stripe];
    auto it = std::find(locked_keys.begin(), locked_keys.end(), kv.first);

    if (it != locked_keys.end()) [[likely]] {
      if (it != locked_keys.end() - 1) {
        *it = locked_keys.back();
      }
      locked_keys.pop_back();
    }
  }

  // Unlock the final stripe
  if (current_stripe != std::numeric_limits<size_t>::max()) {
    stripe_locks[current_stripe].mutex.unlock();
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  batcher.tx_commit_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  batcher.local_tx_committed++;
}

// Phase 2 (Failure): Coordinator ordered rollback
void StaticClusterDHTNode::local_tx_abort(const uint64_t tx_timestamp, TelemetryBatcher& batcher) {
  auto start_time = std::chrono::high_resolution_clock::now();

  size_t staging_stripe = tx_timestamp & logical_stripe_mask;
  std::vector<std::pair<uint32_t, uint32_t>> batch_to_abort;

  // Extract and remove the transaction from the staging area
  {
    std::lock_guard<Spinlock> stage_lock(staging_locks[staging_stripe].mutex);
    auto& stripe_vector = staging_stripes[staging_stripe];
    
    for (auto it = stripe_vector.begin(); it != stripe_vector.end(); ++it) {
      if (it->tx_timestamp == tx_timestamp) {
        batch_to_abort = std::move(it->batch);
        
        if (it != stripe_vector.end() - 1) {
          *it = std::move(stripe_vector.back());
        }
        stripe_vector.pop_back();
        break;
      }
    }
  } // release stage lock

  if (batch_to_abort.empty()) [[unlikely]] {
    auto end_time = std::chrono::high_resolution_clock::now();
    batcher.tx_abort_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    return;
  }

  std::sort(batch_to_abort.begin(), batch_to_abort.end(),
            [this](const auto& a, const auto& b) {
              return (hash_key(a.first) & logical_stripe_mask) <
                     (hash_key(b.first) & logical_stripe_mask);
            });

  // Release logical locks
  size_t current_stripe = std::numeric_limits<size_t>::max();
  for (const auto& kv : batch_to_abort) {
    size_t lock_stripe = hash_key(kv.first) & logical_stripe_mask;
    
    // Swap locks if we crossed into a new stripe
    if (lock_stripe != current_stripe) {
      if (current_stripe != std::numeric_limits<size_t>::max()) {
        stripe_locks[current_stripe].mutex.unlock();
      }
      stripe_locks[lock_stripe].mutex.lock();
      current_stripe = lock_stripe;
    }
    
    // Erase the key while holding the lock
    std::vector<uint32_t>& locked_keys = logically_locked_stripes[current_stripe];
    auto it = std::find(locked_keys.begin(), locked_keys.end(), kv.first);
    if (it != locked_keys.end()) [[likely]] {
      if (it != locked_keys.end() - 1) {
        *it = locked_keys.back();
      }
      locked_keys.pop_back();
    }
  }

  // Unlock the final stripe
  if (current_stripe != std::numeric_limits<size_t>::max()) {
    stripe_locks[current_stripe].mutex.unlock();
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  batcher.tx_abort_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  batcher.local_tx_aborted++;
  // discarded_batch vector goes out of scope
}

// Sends PREPARE message and awaits Cohort's vote
bool StaticClusterDHTNode::send_tx_prepare(const int target_id,
                                           const uint64_t tx_timestamp,
                                           const std::vector<std::pair<uint32_t, uint32_t>> &batch,
                                           TelemetryBatcher& batcher)
{
  auto start_time = std::chrono::high_resolution_clock::now();
  if (batch.empty()) [[unlikely]] {
    return true; 
  }

  // Locate target node details
  if (target_id < 0 || target_id >= static_cast<int>(cluster_map.size())) [[unlikely]] {
    batcher.coord_tx_failed++;
    return false;
  }
  const NodeConfig &target = cluster_map[target_id];

  size_t batch_size = batch.size();
  #ifndef NDEBUG
    if (batch_size > BATCH_SIZE)
      return false;
  #endif

  TxPrepareHeader header = {
    .tx_timestamp = tx_timestamp,
    .batch_size = static_cast<uint16_t>(batch_size),
    .coordinator_id = static_cast<int16_t>(self_config.id)
  };

  // Allocate buffer and copy
  size_t payload_size = sizeof(TxPrepareHeader) + (batch_size * 8);
  alignas(16) uint8_t payload_buffer[sizeof(TxPrepareHeader) + (BATCH_SIZE * 8)];
  std::memcpy(payload_buffer, &header, sizeof(TxPrepareHeader));
  std::memcpy(payload_buffer + sizeof(TxPrepareHeader), batch.data(), batch_size * 8);

  // Prepare the response
  TxPrepareResponse response{};

  bool success = send_single_request(target.id, target.ip, target.port,
                                     ProtocolType::TwoPhaseCommit,
                                     static_cast<uint8_t>(TwoPhaseCommitCommand::Prepare),
                                     payload_buffer, payload_size,
                                     reinterpret_cast<uint8_t*>(&response),
                                     sizeof(TxPrepareResponse));

  auto end_time = std::chrono::high_resolution_clock::now();
  batcher.coord_prepare_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

  if (success) [[likely]] {
    // Extract the remote server's clock and instantly catch up
    synchronize_clock(response.remote_clock);
    if (response.vote == 1) {
      batcher.coord_prepare_success++;
      return true;
    } else {
      // The remote node successfully replied, but voted no
      batcher.coord_tx_failed++;
      return false;
    }
  }

  batcher.coord_tx_failed++;
  return false;
}

// Send Phase 2 COMMIT message
bool StaticClusterDHTNode::send_tx_commit(const int target_id, const uint64_t tx_timestamp, TelemetryBatcher& batcher) {
  auto start_time = std::chrono::high_resolution_clock::now();

  if (target_id < 0 || target_id >= static_cast<int>(cluster_map.size())) [[unlikely]] {
    return false;
  }
  const NodeConfig &target = cluster_map[target_id];

  // Instantiate the header
  TxCommitHeader header = {
    .tx_timestamp = tx_timestamp
  };

  // 1 byte ack
  uint8_t response = 0;

  // Captures transient failures
  constexpr int MAX_INLINE_RETRIES = 3;
  const int BASE_BACKOFF_US = 50;

  for (int attempt = 0; attempt <= MAX_INLINE_RETRIES; ++attempt) {
    bool success = send_single_request(target.id, target.ip, target.port,
                                       ProtocolType::TwoPhaseCommit,
                                       static_cast<uint8_t>(TwoPhaseCommitCommand::Commit),
                                       reinterpret_cast<const uint8_t *>(&header),
                                       sizeof(TxCommitHeader), &response, 1);
    
    if (success && response == 1) [[likely]] { 
      auto end_time = std::chrono::high_resolution_clock::now();
      batcher.coord_phase2_commit_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
      batcher.coord_tx_committed++;
      return true; 
    }

    batcher.coord_phase2_retries++;

    if (attempt < MAX_INLINE_RETRIES) {
      // Exponential backoff
      thread_local std::mt19937 rng(std::random_device{}());
      int max_sleep = BASE_BACKOFF_US * (1 << attempt);
      std::uniform_int_distribution<int> dist(BASE_BACKOFF_US, max_sleep);
      std::this_thread::sleep_for(std::chrono::microseconds(dist(rng)));
    }
  }

  // Asynchronous recovery
  #ifndef NDEBUG
    std::cerr << "[Coordinator] Target " << target_id 
              << " unreachable. Offloading TX_COMMIT (" << tx_timestamp 
              << ") to async recovery queue.\n";
  #endif

  enqueue_for_async_recovery(target_id, TwoPhaseCommitCommand::Commit, tx_timestamp);

  auto end_time = std::chrono::high_resolution_clock::now();
  batcher.coord_phase2_commit_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

  return true;
}

// Send Phase 2 ABORT message
bool StaticClusterDHTNode::send_tx_abort(const int target_id, const uint64_t tx_timestamp, TelemetryBatcher& batcher) {
  auto start_time = std::chrono::high_resolution_clock::now();

  if (target_id < 0 || target_id >= static_cast<int>(cluster_map.size())) [[unlikely]] {
    return false;
  }
  const NodeConfig &target = cluster_map[target_id];

  TxCommitHeader header = {
    .tx_timestamp = tx_timestamp
  };
  uint8_t response = 0;

  // Capture transient failures
  constexpr int MAX_INLINE_RETRIES = 3;
  const int BASE_BACKOFF_US = 50;

  for (int attempt = 0; attempt <= MAX_INLINE_RETRIES; ++attempt) {
    bool success = send_single_request(target.id, target.ip, target.port,
                                       ProtocolType::TwoPhaseCommit,
                                       static_cast<uint8_t>(TwoPhaseCommitCommand::Abort),
                                       reinterpret_cast<const uint8_t *>(&header),
                                       sizeof(TxCommitHeader), &response, 1);
    
    if (success && response == 1) [[likely]] { 
      auto end_time = std::chrono::high_resolution_clock::now();
      batcher.coord_phase2_commit_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
      return true; 
    }

    batcher.coord_phase2_retries++;

    if (attempt < MAX_INLINE_RETRIES) {
      // Exponential backoff
      thread_local std::mt19937 rng(std::random_device{}());
      int max_sleep = BASE_BACKOFF_US * (1 << attempt);
      std::uniform_int_distribution<int> dist(BASE_BACKOFF_US, max_sleep);
      std::this_thread::sleep_for(std::chrono::microseconds(dist(rng)));
    }
  }

  // Asynchronous Handoff
  // The target is completely unreachable. We yield the thread back to the client.
  #ifndef NDEBUG
    std::cerr << "[Coordinator] Target " << target_id 
              << " unreachable. Offloading TX_ABORT (" << tx_timestamp 
              << ") to async recovery queue.\n";
  #endif

  enqueue_for_async_recovery(target_id, TwoPhaseCommitCommand::Abort, tx_timestamp);

  auto end_time = std::chrono::high_resolution_clock::now();
  batcher.coord_phase2_commit_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

  return true;
}