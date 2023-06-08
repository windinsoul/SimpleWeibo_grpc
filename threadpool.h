//
// Created by windinsoul on 23-6-1.
//

#ifndef SIMPLEWEIBO_GRPC_THREADPOOL_H
#define SIMPLEWEIBO_GRPC_THREADPOOL_H
#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <stdexcept>

class ThreadPool {
public:
    ThreadPool(size_t);
    template<class F, class... Args>
    auto enqueue(F&& f, Args&&... args)
    -> std::future<typename std::result_of<F(Args...)>::type>;
    ~ThreadPool();
private:
    // need to keep track of threads so we can join them 跟踪执行的线程以便调用.join()
    std::vector< std::thread > workers;
    // the task queue 要获取线程的任务队列
    std::queue< std::function<void()> > tasks;

    // synchronization 同步
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
};

// the constructor just launches some amount of workers 创建一些线程开始执行
inline ThreadPool::ThreadPool(size_t threads)
        :   stop(false)
{
    for(size_t i = 0;i<threads;++i)//创建threads个线程，每个线程都一直访问任务队列,没得访问就阻塞!
        workers.emplace_back(
                [this]
                {
                    for(;;)
                    {
                        std::function<void()> task;

                        {
                            std::unique_lock<std::mutex> lock(this->queue_mutex);
                            this->condition.wait(lock,
                                                 [this]{ return this->stop || !this->tasks.empty(); });
                            if(this->stop && this->tasks.empty())
                                return;
                            task = std::move(this->tasks.front());
                            this->tasks.pop();
                        }

                        task();
                    }
                }
        );
}

// add new work item to the pool 添加新任务
template<class F, class... Args>
auto ThreadPool::enqueue(F&& f, Args&&... args)
-> std::future<typename std::result_of<F(Args...)>::type>
{
    using return_type = typename std::result_of<F(Args...)>::type;

    auto task = std::make_shared< std::packaged_task<return_type()> >(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
    );

    std::future<return_type> res = task->get_future();
    {
        std::unique_lock<std::mutex> lock(queue_mutex);

        // don't allow enqueueing after stopping the pool
        if(stop)
            throw std::runtime_error("enqueue on stopped ThreadPool");

        tasks.emplace([task](){ (*task)(); });
    }
    condition.notify_one();//唤醒一个来执行任务啊喂
    return res;
}

// the destructor joins all threads
inline ThreadPool::~ThreadPool()
{
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        stop = true;
    }
    condition.notify_all();//唤醒线程们
    for(std::thread &worker: workers)
        worker.join();//线程们执行完剩下的就收工跑路
}

#endif //SIMPLEWEIBO_GRPC_THREADPOOL_H
