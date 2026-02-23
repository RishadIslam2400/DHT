#pragma once

#include <vector>
#include <thread>
#include <queue>
#include <functional>
#include <mutex>
#include <condition_variable>

class ThreadPool {
public:
  explicit ThreadPool(int num_threads) : stop(false) {
    for(int i = 0; i < num_threads; ++i) {
      threads.emplace_back(std::thread(&ThreadPool::thread_func, this));
    }
  }

  ~ThreadPool() {
    // acquire lock before changing condition
    {
      std::lock_guard<std::mutex> lock(m);
      stop = true;
    }

    cv.notify_all();
    for(std::thread &thread: threads) {
      thread.join();
    }
  }

  // Add a new work item to the pool
  template<typename F>
  void submit_task(F&& func) {
    // acquire lock before changing the condition
    {
      std::lock_guard<std::mutex> lock(m);
      tasks.emplace(std::forward<F>(func));
    }
    // call notify_one after release the lock; this will prevent the waiting thread
    // from immediate block after wake up
    cv.notify_one();
  }

private:
  void thread_func() {
    while (true) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(m);
        // wait for the condition to be true; condition is required. If no
        // condition, waiting thread can not decide it's spurius wake-up or not
        cv.wait(lock, [this]() {
          return stop || !tasks.empty();
        });

        if (stop && tasks.empty()) {
          return;
        }

        // protected under the mutex, since these steps will change the condition
        task = std::move(tasks.front());
        tasks.pop();
      }

      // do not influence the condition, do it without holding the mutex
      task();
    }
  }

  std::queue<std::function<void()>> tasks;
  // This mutex is used for three purpose and these three purposes must be protected under this same mutex:
  // 1. Protect the tasks queue
  // 2. Protect the stop condition
  // 3. Protect the condition variable(which is a futex)
  // 1 and 2 both influence the result of the condition; 3 is required by the futex implementation in syscall
  std::mutex m;
  std::condition_variable cv;
  bool stop;
  std::vector<std::thread> threads;
};