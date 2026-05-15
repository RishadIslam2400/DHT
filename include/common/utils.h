#pragma once

#include <thread>

inline void log_error(const char *prefix, int err);
inline void pin_thread_to_control_cores(std::thread &target_thread);