#pragma once

#include <atomic>

class Spinlock {
private:
  std::atomic_flag flag = ATOMIC_FLAG_INIT;

public:
  void lock() {
    // Spin in user-space until we successfully set the flag
    uint16_t spin_count = 0;
    while (flag.test_and_set(std::memory_order_acquire))
    {
      spin_count++;

      // Fast Path: For the first 1,000 attempts, stay in user-space
      if (spin_count < 1000) {
        #if defined(__x86_64__)
          __builtin_ia32_pause();
        #elif defined(__aarch64__)
          __builtin_arm_yield(); // ARM's equivalent to x86 PAUSE
        #endif
      } else {
        // Slow Path: Stop burning CPU cycles and give the core back to the OS.
        std::this_thread::yield();
      }
    }
  }

  void unlock() {
    // Release the lock for the next thread
    flag.clear(std::memory_order_release);
  }
};