#pragma once

#include <thread>

void log_error(const char *prefix, int err);
void pin_thread_to_control_cores(std::thread &target_thread);