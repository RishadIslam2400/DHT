#pragma once

#include <vector>
#include <cstring>
#include <sys/socket.h>
#include <algorithm>

class BufferedSocket {
private:
  int sock;
  std::vector<uint8_t> buffer;
  size_t head = 0;
  size_t tail = 0;

  bool ensure_bytes(size_t n) {
    // Loop until we have at least 'n' unparsed bytes in local RAM
    while (tail - head < n) {
      // If we are out of space at the end of the vector, compact it by 
      // sliding the unparsed partial data down to index 0.
      if (head > 0) {
        size_t remaining = tail - head;
        if (remaining > 0) {
          std::memmove(buffer.data(), buffer.data() + head, remaining);
        }
        head = 0;
        tail = remaining;
      }

      // If the requested message 'n' is larger than our entire buffer 
      // (e.g., a massive CMD_BATCH_PUT), dynamically expand the buffer.
      if (buffer.size() - tail < n - (tail - head)) {
        buffer.resize(std::max(buffer.size() * 2, tail + n));
      }

      // Issue a single system call to fill the remaining capacity
      ssize_t bytes_read = recv(sock, buffer.data() + tail, buffer.size() - tail, 0);
      
      if (bytes_read <= 0) {
        if (bytes_read == 0) errno = ECONNRESET; // Clean EOF
        if (bytes_read < 0 && errno == EINTR) continue; // Interrupted, try again
        return false; // Fatal network error or disconnect
      }
      tail += bytes_read;
    }

    return true;
  }

public:
  // Initialize with an 8KB buffer (standard OS page size multiple)
  BufferedSocket(int socket_fd, size_t capacity = 8192) : sock(socket_fd) {
    buffer.resize(capacity);
  }

  // Zero-copy read: Returns a pointer directly into the internal buffer
  uint8_t* read_ptr(size_t n) {
    if (!ensure_bytes(n)) {
      return nullptr;
    }
    uint8_t* ptr = buffer.data() + head;
    head += n;
    return ptr;
  }
};