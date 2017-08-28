#include "concurrentqueue/blockingconcurrentqueue.h"

/* Mutli threaded producer-consumer queue */

#if 0
volatile int queue_first, queue_last;
//mutex queue_pop_mutex;

const int CIRCULAR_SIZE = 20000;
tuple<int, char*, int> pending_writes[CIRCULAR_SIZE];

void imm_write_call(int fd, char* buffer, int size) {
    pending_writes[queue_last] = make_tuple(fd, buffer, size);
    int new_value = (queue_last + 1) % CIRCULAR_SIZE;
    if (new_value == queue_first) {
        printf("Circular buffer overrun!\n");
    }
    queue_last = new_value;
}

class SpinLock {
    std::atomic_flag locked = ATOMIC_FLAG_INIT ;
public:
    void lock() {
        while (locked.test_and_set(std::memory_order_acquire)) { ; }
    }
    void unlock() {
        locked.clear(std::memory_order_release);
    }
};

SpinLock queue_pop_lock;

void consumer_thread(int thread_index) {
    global_thread_index = thread_index;
    
    printf("Started consumer thread index %d\n", thread_index); fflush(stdout);
    
    while (true) {
        if (queue_first == queue_last) continue;
        
        {
            //unique_lock<mutex> lock(queue_pop_mutex);
            queue_pop_lock.lock();
            
            if (queue_first == queue_last) {
                queue_pop_lock.unlock();
                continue;
            }
            
            auto query = pending_writes[queue_first];
            int new_value = (queue_first + 1) % CIRCULAR_SIZE;
            queue_first = new_value;
            queue_pop_lock.unlock();
            //lock.unlock();
            
            //printf("%d writes\n", thread_index); fflush(stdout);
        
            write(get<0>(query), get<1>(query), get<2>(query));
        }
    }
}
#endif

using pending_write = tuple<int, char*, int>;

moodycamel::BlockingConcurrentQueue<pending_write> write_queue(4000);
moodycamel::ProducerToken* producer_token;

void imm_write_call(int fd, char* buffer, int size) {
    if (!producer_token) {
        producer_token = new moodycamel::ProducerToken(write_queue);
    }
    
    pending_write p = make_tuple(fd, buffer, size);
    write_queue.enqueue(*producer_token, p);
}

void consumer_thread(int thread_index, int affinity_mask) {
    global_thread_index = thread_index;
    set_thread_affinity(affinity_mask, false);
    
    moodycamel::ConsumerToken consumer_token(write_queue);
    
    while (true) {
        pending_write query;
        write_queue.wait_dequeue(consumer_token, query);
        
        write(get<0>(query), get<1>(query), get<2>(query));
    }
}
