#include "concurrentqueue/blockingconcurrentqueue.h"

poller_local li global_t_ready_write;

/* Mutli threaded producer-consumer queue */

struct pending_write {
    int fd;
    char* buffer;
    int length;
    //li timestamp;
};

#ifndef DISABLE_SPINLOCKS
volatile int queue_first, queue_last;
//mutex queue_pop_mutex;

const int CIRCULAR_SIZE = 4096;
pending_write pending_writes[CIRCULAR_SIZE];

int server_socket_descriptor;
volatile bool need_accept_connections;

void handle_new_connection(int infd);

void imm_write_call(int fd, const char* buffer, int size) {
    pending_writes[queue_last] = pending_write { fd, (char*)buffer, size /*, get_ns_timestamp()*/ };
    int new_value = queue_last >= CIRCULAR_SIZE - 1 ? 0 : queue_last + 1;
    
    if (new_value == queue_first) {
        printf("Circular buffer overrun!\n"); fflush(stdout);
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

void consumer_thread(int thread_index, int affinity_mask) {
    global_thread_index = thread_index;
    set_thread_affinity(affinity_mask, false);
    
    while (true) {
        //if (queue_first == queue_last && !need_accept_connections) continue;
        
        if (queue_first != queue_last)
        {
            queue_pop_lock.lock();
            
            if (queue_first == queue_last) {
                queue_pop_lock.unlock();
                continue;
            }
            
            pending_write query = pending_writes[queue_first];
            int new_value = queue_first >= CIRCULAR_SIZE - 1 ? 0 : queue_first + 1;
            queue_first = new_value;
            queue_pop_lock.unlock();
        
            //li t_took = get_ns_timestamp();
            write(query.fd, query.buffer, query.length);
            //li t_written = get_ns_timestamp();
            //printf("process lag %.3f mks, write lag %.3f\n", (t_took - query.timestamp) / 1e3, (t_written - t_took) / 1e3);
            continue;
        }
        
        if (thread_index != 0 && need_accept_connections) {
            //printf("Need accept connections flag spotted!\n");
            
            struct sockaddr in_addr;
            socklen_t in_len = sizeof in_addr;
            //li t0 = get_ns_timestamp();
            int infd = accept4(server_socket_descriptor, &in_addr, &in_len, SOCK_NONBLOCK);
            //printf("ret %d\n", server_socket_descriptor);
            //profile_delimiter(ACCEPT_TO_ACCEPT);
            
            if (infd == -1) {
                if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
                    continue;
                }
                else {
                    perror("accept");
                    continue;
                }
            }

            handle_new_connection(infd);
            //printf("Worker thread accepted connection %d\n", infd);
        }
    }
}
#else
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
#endif
