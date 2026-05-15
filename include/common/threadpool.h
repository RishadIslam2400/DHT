#pragma once

#include <vector>
#include <thread>
#include <queue>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <future>
#include <memory>
#include <stdexcept>
#include <pthread.h>

class ThreadPool {
public:
  explicit ThreadPool(const std::vector<int>& target_cores) : stop(false) {
    int num_threads = target_cores.size();

    for(int i = 0; i < num_threads; ++i) {
      int core_id = target_cores[i];

      threads.emplace_back([this, core_id]() {
        this->pin_thread_to_core(core_id);
        this->thread_func(); 
      });
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
  template<class F, class... Args>
  auto submit_task(F&& f, Args&&... args) -> std::future<std::invoke_result_t<F, Args...>> {
    using return_type = std::invoke_result_t<F, Args...>;

    // Wrap the function in a packaged_task so it can generate a future
    auto task = std::make_shared<std::packaged_task<return_type()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...)
    );

    std::future<return_type> res = task->get_future();
    {
        std::lock_guard<std::mutex> lock(m);
        if (stop) {
            throw std::runtime_error("submit_task on stopped ThreadPool");
        }
        
        // Push a void lambda into the queue that executes the packaged task
        tasks.emplace([task]() { (*task)(); });
    }
    
    cv.notify_one();
    return res;
  }

private:
  void pin_thread_to_core(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    pthread_t current_thread = pthread_self();
    pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
  }

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