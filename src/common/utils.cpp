#include <iostream>
#include <cstring>

#include "common/utils.h"

void log_error(const char *prefix, int err) {
  char buf[1024];
  std::cerr << "[Error] " << prefix << " " 
            << strerror_r(err, buf, sizeof(buf))
            << std::endl;
}