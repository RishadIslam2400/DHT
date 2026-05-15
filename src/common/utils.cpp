#include <iostream>
#include <cstring>

#include "common/utils.h"

inline void log_error(const char *prefix, int err) {
  char buf[1024];
  std::cerr << "[Error] " << prefix << " " 
            << strerror_r(err, buf, sizeof(buf))
            << std::endl;
}

inline void pin_thread_to_control_cores(std::thread& target_thread) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  
  // Allow the thread to run on Core 0 OR Core 8
  CPU_SET(0, &cpuset);
  CPU_SET(8, &cpuset);

  pthread_t native_handle = target_thread.native_handle();
  int rc = pthread_setaffinity_np(native_handle, sizeof(cpu_set_t), &cpuset);
  
  if (rc != 0) {
    std::cerr << "Failed to set control core affinity\n";
  }
}