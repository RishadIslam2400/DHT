#pragma once

#include <atomic>
#include <thread>

#if defined(__x86_64__) || defined(__i386__)
  #include <immintrin.h>
#endif

class Spinlock {
private:
  std::atomic<bool> lock_flag{false};

public:
  void lock() {
    uint16_t spin_count = 0;
    
    while (true) {
      // test loop
      while (lock_flag.load(std::memory_order_relaxed)) {
        spin_count++;
        
        if (spin_count < 1000) {
          #if defined(__x86_64__) || defined(__i386__)
            _mm_pause(); // Optimizes CPU pipeline, reduces power consumption
          #elif defined(__aarch64__)
            __builtin_arm_yield(); 
          #endif
        } else {
          // Fallback: The lock holder was likely preempted by the OS.
          // Give up our time-slice so the holder can finish its work.
          std::this_thread::yield(); 
        }
      }

      // test and set
      bool expected = false;
      if (lock_flag.compare_exchange_weak(expected, true, 
                                          std::memory_order_acquire,
                                          std::memory_order_relaxed)) {
        return; // Lock successfully acquired
      }
      
      // If we failed, someone else grabbed it. Loop around and spin on load() again.
    }
  }

  void unlock() {
    // Release the lock, invalidating the cache line for the spinning threads
    lock_flag.store(false, std::memory_order_release);
  }
};

struct alignas(64) AlignedSpinlock {
  Spinlock mutex;
};

class RWSpinlock {
private:
    // 0 = Unlocked
    // 0xFFFFFFFF = Write-locked
    // > 0 = Number of active readers
    std::atomic<uint32_t> state{0};

    static constexpr uint32_t WRITE_LOCKED = 0xFFFFFFFF;

public:
    // Writer Methods (Exclusive)
    void lock() {
      uint32_t expected;
      while (true) {
        expected = state.load(std::memory_order_relaxed);
        if (expected == 0 && state.compare_exchange_weak(expected, WRITE_LOCKED, 
                                                          std::memory_order_acquire, 
                                                          std::memory_order_relaxed)) {
          break;
        }
        // Hardware pause to prevent pipeline flushing while spinning
        #if defined(__x86_64__)
          __builtin_ia32_pause();
        #else
          std::this_thread::yield();
        #endif
      }
    }

    void unlock() {
      state.store(0, std::memory_order_release);
    }

    // Reader Methods (Shared)
    void lock_shared() {
      uint32_t expected;
      while (true) {
        expected = state.load(std::memory_order_relaxed);
        // If there is no writer, try to atomically increment the reader count
        if (expected != WRITE_LOCKED && 
            state.compare_exchange_weak(expected, expected + 1, 
                                        std::memory_order_acquire, 
                                        std::memory_order_relaxed)) {
          break;
        }
        #if defined(__x86_64__)
          __builtin_ia32_pause();
        #else
          std::this_thread::yield();
        #endif
      }
    }

    void unlock_shared() {
      state.fetch_sub(1, std::memory_order_release);
    }
};