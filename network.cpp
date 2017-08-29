/* Networking & main loop */

// networking template copyright: https://banu.com/blog/2/how-to-use-epoll-a-complete-example-in-c/

using input_request_handler = http_input_request_handler;
//using input_request_handler = instant_answer_request_handler;

// 1 thread, 1 instance -> 45k RPS
// 4 threads, 1 instance -> 45k RPS
// 4 threads, 4 instance -> 117k RPS

const int NUM_CONSUMER_THREADS = 3;

#if 1
const int NUM_POLLER_THREADS = 1;
const int MAX_POLL_EVENTS = 2048; // max events = num instances
#elif 0
const int NUM_THREADS = 4;
const int MAX_POLL_EVENTS = 1;

/*
interval_real    : avg 69.1, std dev 525.08 [18390 95 79 71 40]
connect_time     : avg 20.7, std dev 446.57 [18346 42 36 33 0]
send_time        : avg 12.5, std dev 1.73 [43 19 14 14 12]
latency          : avg 28.8, std dev 275.72 [12328 45 37 32 18]
receive_time     : avg 7.2, std dev 1.16 [31 11 9 8 7]
interval_event   : avg 27.7, std dev 275.75 [12327 44 36 32 17]
*/

#else
const int NUM_THREADS = 1;
const int MAX_POLL_EVENTS = 16;
#endif

const int MAX_FDS = 10000;
const int READ_BUFFER_SIZE = 4096 * 2;
constexpr bool ACTIVE_WAIT = true;

static int create_and_bind(int port) {
    int sfd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, IPPROTO_IP);
    verify(sfd != -1);
    
    int reuse = 1;
    if (setsockopt(sfd, SOL_SOCKET, SO_BROADCAST, (const char*)&reuse, sizeof(reuse)) < 0)
        perror("setsockopt(SO_BROADCAST) failed");
    
    if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (const char*)&reuse, sizeof(reuse)) < 0)
        perror("setsockopt(SO_REUSEADDR) failed");
    
    //
    
    sockaddr_in addr = {};
    bzero((char *) &addr, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("0.0.0.0");
    addr.sin_port = htons(port);
           
    verify(bind(sfd, (sockaddr*)&addr, sizeof(addr)) == 0);
    
    if (setsockopt(sfd, SOL_TCP, TCP_QUICKACK, (const char*)&reuse, sizeof(reuse)) < 0)
        perror("setsockopt(TCP_QUICKACK) failed");
    
    //int ms_poll = 1e9;
    //if (setsockopt(sfd, SOL_SOCKET, SO_BUSY_POLL, (const char*)&ms_poll, sizeof(ms_poll)) < 0)
    //    perror("setsockopt(SO_BUSY_POLL) failed");
    
    return sfd;
}

struct EpollServer {
    int sfd;
    int efd;
};

EpollServer epoll_server;

void poller_thread(int thread_index, int affinity_mask);

/* Memory fiddling */

void prove_thread_stack_use_is_safe(int stacksize) {
   	volatile char buffer[stacksize];
   	
   	for (int i = 0; i < stacksize; i += sysconf(_SC_PAGESIZE)) {
   		buffer[i] = i;
   	}
   
   	//show_new_pagefault_count(string("Caused by preallocating " + memory_human_readable(stacksize) + " thread stack").c_str());
    
   	stacksize += buffer[10] + buffer[20];
}

void configure_malloc_behavior()
{
   	//if (mlockall(MCL_CURRENT | MCL_FUTURE))
   	//	perror("mlockall failed:");
   
   	verify(mallopt(M_TRIM_THRESHOLD, -1) == 1);
   	verify(mallopt(M_MMAP_MAX, 0) == 1);
}

void lock_memory(void* ptr, int size, const char* name) {
    if (mlock(ptr, size) != 0) {
        printf("failed to lock %s (%s): ", name, memory_human_readable(size).c_str()); fflush(stdout);
        perror("mlock"); fflush(stdout);
    }
    else {
        //printf("succesfully locked %s (%s)\n", name, memory_human_readable(size).c_str());
    }
}

/* Start server */

void tune_realtime_params() {
    configure_malloc_behavior();
}

void add_socket_to_epoll_queue(int fd, bool mod) {
    if (mod) return;
    
    epoll_event event = {};
    event.data.fd = fd;
    event.events = EPOLLIN | EPOLLRDHUP;// | EPOLLONESHOT;// | EPOLLEXCLUSIVE;
#ifndef DISABLE_EPOLLET
    event.events |= EPOLLET;
#endif
    int ec = epoll_ctl(epoll_server.efd, mod ? EPOLL_CTL_MOD : EPOLL_CTL_ADD, fd, &event);
    if (ec == -1)
        perror("epoll_ctl");
}

void start_epoll_server() {
    epoll_server.sfd = create_and_bind(80);
    verify(epoll_server.sfd != -1);

    verify(listen(epoll_server.sfd, 4000) != -1);

    epoll_server.efd = epoll_create1(0);
    verify(epoll_server.efd != -1);

    add_socket_to_epoll_queue(epoll_server.sfd, false);
    
    vector<int> affinity_mask(NUM_POLLER_THREADS + NUM_CONSUMER_THREADS);
    
#ifndef DISABLE_AFFINITY
    printf("Proceeding to thread setup, thread id %d\n", (int)gettid()); fflush(stdout);
    
    int mask = get_affinity_mask();
    
    string avail_cpus = "";
    vector<int> avail_indices;
    for (int i = 0; i < 32; i++)
        if (mask & (1 << i)) {
            if (!avail_cpus.empty()) avail_cpus += ", ";
            avail_cpus += to_string(i);
            avail_indices.push_back(i);
        }
    
    // with affinity:
    // interval_real    : avg 15.8, std dev 11.86 [3520 32 24 21 14]
    
    // without affinity:
    // interval_real    : avg 15.5, std dev 4.27 [303 32 24 21 14]
    // interval_real    : avg 15.9, std dev 13.22 [3931 33 24 23 14]
    
    // 4 threads still worse than 1
    
    // current full response latency in phase 3: 15.4-15.8
    // 200 OK response latency: 14.2-14.4, or even 13.9
    // direct read poll is strange: it consistently gives 13.2
    // so it is better by 0.7, but a lot of disadvantages (really delayed normal poll)
    //
    // epoll is 306 ns
    // accept is 1090 ns
    // read is 187 ns
    // so no, it's not worth it
    // but maybe mixed epoll / read mode :) to ditch out that extra 0.5 seconds
    
    printf("available cpus: %s\n", avail_cpus.c_str()); fflush(stdout);
    //printf("available indices: "); fflush(stdout); for (int x: avail_indices) { printf("%d ", x); fflush(stdout); } printf("\n"); fflush(stdout);
    
    printf("thread mapping (thread -> cpu): "); fflush(stdout);
    for (int i = 0; i < (int)affinity_mask.size(); i++) {
        int cpu = avail_indices[i % avail_indices.size()];
        if (i) printf(", ");
        printf("%d -> %d", i, cpu); fflush(stdout);
        //assert(cpu >= 0 && cpu < 32);
        affinity_mask[i] = 1 << cpu;
    }
    printf("\n"); fflush(stdout);
#endif
    
    vector<thread> threads(NUM_POLLER_THREADS + NUM_CONSUMER_THREADS - 1);
    
    for (int i = 0; i < (int)threads.size(); i++)
        if (i < NUM_CONSUMER_THREADS)
            threads[i] = thread(consumer_thread, i, affinity_mask[i]);
        else
            threads[i] = thread(poller_thread, i, affinity_mask[i]);
        
    poller_thread(NUM_POLLER_THREADS + NUM_CONSUMER_THREADS - 1, affinity_mask[NUM_POLLER_THREADS + NUM_CONSUMER_THREADS - 1]);
            
    for (int i = 0; i < (int)threads.size(); i++)
        threads[i].join();
}

#if 0
const int NEED_READS = 5;
thread_local int succesful_reads[NEED_READS];
thread_local int succesful_reads_pos = 0;
thread_local bool all_reads_equal = false;
#endif
    
//vector<int> read_fds;

#ifndef DISABLE_EPOLLET
const int EAGAIN_BUFFER_SIZE = 128;
poller_local char eagain_buf[EAGAIN_BUFFER_SIZE];
#endif

poller_local char read_buf[READ_BUFFER_SIZE];

#if 0
const int TRY_READS = 5;
thread_local int antiswap_iteration = 0;
thread_local int ANTISWAP_PERIOD = 1;
int expected_read_fd = -1;
char fd_used[MAX_FDS];

thread_local int n_predict = 0, n_total = 0, n_old_total = 0;
int n_total_first;
#endif


void close_fd_fast(int fd) {
    epoll_ctl(epoll_server.efd, EPOLL_CTL_DEL, fd, 0);
    //if (ec != -1)
    //    perror("epoll_ctl del");
    close(fd);
}

void try_read_from(int fd, bool predict = false) {
    //printf("read ready for socket %d\n", fd);
    
    profile_begin(READ_CALL);
    ssize_t count = read(fd, read_buf, READ_BUFFER_SIZE);
    
    if (count < 0 && errno != EWOULDBLOCK) {
        perror("read error -> close");
        close_fd_fast(fd);
        profile_end(READ_CALL);
        return;
    }
    
#ifndef DISABLE_EPOLLET
    int n_reads = 0, sum_reads = 0;
    while (true) {
        int can = read(fd, eagain_buf, EAGAIN_BUFFER_SIZE);
        if (can == 0) {
            close(fd);
            profile_end(READ_CALL);
            return;
        }
        
        if (can < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                perror("read");
                fflush(stdout);
            }
            break;
        }
        printf("omg read\n");
        sum_reads += can;
        n_reads++;
    }
#endif
    profile_end(READ_CALL);
    
#ifndef DISABLE_EPOLLET
    add_socket_to_epoll_queue(fd, true);
    
    if (n_reads > 0) {
        printf("Strange, expected EAGAIN but got %d reads for %d bytes total\n", n_reads, sum_reads);
    }
#endif
    
    if (count <= 0) return;
    read_buf[count] = 0;
    
#if 0
    if (predict)
        n_predict++;
    n_total++;
    if (global_thread_index == 0)
        n_total_first++;
#endif
    
    input_request_handler handler;
    handler.request_content = read_buf;
    handler.request_length = count;
    
    global_t_ready = get_ns_timestamp();
    
    int pret = handler.parse(fd);
    if (pret <= 0)
        // just drop it :)
        return;
    
#if 0
    if (fd < MAX_FDS)
        fd_used[fd] = true;
    
    if (fd + 1 < MAX_FDS && fd_used[fd + 1])
        expected_read_fd = fd + 1;
    else
        expected_read_fd = -1;
    
    //read_fds.push_back(fd);
    antiswap_iteration = 0;
    ANTISWAP_PERIOD = 10;
#endif
    
#if 0
    succesful_reads[succesful_reads_pos++] = fd;
    if (succesful_reads_pos >= NEED_READS) succesful_reads_pos -= NEED_READS;
    
    all_reads_equal = true;
    for (int i = 0; i < NEED_READS; i++)
        if (succesful_reads[i] != fd)
            all_reads_equal = false;
#endif
}

void poller_thread(int thread_index, int affinity_mask) {
    global_thread_index = thread_index;
    set_thread_affinity(affinity_mask, true);
    
    epoll_event* events = nullptr;
    events = (epoll_event*)calloc(MAX_POLL_EVENTS, sizeof(epoll_event));
    
    //vector<input_request_handler> fd_to_queue(MAX_FDS);
    
    
    prove_thread_stack_use_is_safe(1024 * 1024 * 2);
    lock_memory(read_buf, READ_BUFFER_SIZE / 2, "read buffer");
    lock_memory(response_buffer, MAX_RESPONSE_SIZE / 2, "response buffer");
    //char stack_variable[1];
    //lock_memory(stack_variable, 1024, "stack");
    
    bool last_request_is_get = false;
    int max_fd = 0;
    
    //li last_print = get_ns_timestamp();
    //int n_epolls = 0;
    //li t_start = get_ns_timestamp();
    
    //const int MAX_MESSAGES = 40;
    //int n_messages = 0;
    
    //vector<int> phase_messages = { 18090, 12000, 20000 };
    vector<int> phase_messages = { 150150, 40000, 630000 };
    
    if (!is_rated_run)
        phase_messages = { 9030, 3000, 19500 };
    
    //int phase = 0;
    
    //bool last_reads = false;
    
    //li longlife_hash = 0;
    //fd_used.reserve(10000);
    
    //int predict_interval = 100;
    
    vector<int> close_fds;
    close_fds.reserve(MAX_POLL_EVENTS);
    bool need_accept = false;
    
    // 6200 mks with simple read write
    
    bool first_phase_ended = false, second_phase_ended = false, third_phase_ended = false;
    
    //li last_print = get_ns_timestamp();
    while (true) {
#if 0
        if (get_ns_timestamp() > last_print + 1e9) {
            printf("max fd + 1: %d\n", max_fd); fflush(stdout);
            print_memory_stats();
            last_print = get_ns_timestamp();
        }
#endif
        
#ifndef DISABLE_PROFILING
        bool forced_flush = false;
        profiler.maybe_flushreset((li)1e9, &minute_accumulator_profiler, forced_flush);
        minute_accumulator_profiler.maybe_flushreset((li)1e9 * 60, nullptr, forced_flush);
#endif
        
        // FIXME: detect phases & more smart heating
        
        // FIXME: more smart algorithm, detect whether tank reuses last or goes sequentially
        
        // FIXME: in fact old is better
        
#if 0
        if (expected_read_fd != -1) {
            for (int i = 0; i < TRY_READS; i++)
                try_read_from(expected_read_fd, true);
        }
#endif
        
#if 0
        if (thread_index == 0) {
            antiswap_iteration++;
            if (antiswap_iteration >= ANTISWAP_PERIOD) {
                // not fast operation, ~100 ns
                //li t0 = get_ns_timestamp();
                for (int t = 0; t < 2; t++) {
                    volatile int& x = user_by_id[rand() % user_by_id.size()].id;
                    volatile int& y = visit_by_id[rand() % visit_by_id.size()].id;
                    volatile int& z = location_by_id[rand() % location_by_id.size()].id;
                    longlife_hash ^= user_by_id[rand() % user_by_id.size()].id;
                    longlife_hash ^= visit_by_id[rand() % visit_by_id.size()].id;
                    longlife_hash ^= location_by_id[rand() % location_by_id.size()].id;
                    longlife_hash ^= read_buf[rand() % READ_BUFFER_SIZE];
                    longlife_hash ^= response_buffer[rand() % MAX_RESPONSE_SIZE];
                    
                    x = x;
                    y = y;
                    z = z;
                }
                antiswap_iteration = 0;
                //li t1 = get_ns_timestamp();
                //printf("in %lld ns\n", t1 - t0);
            }
        }
        
        if (thread_index == 1) {
            if (phase < (int)phase_messages.size() && n_total >= n_old_total + predict_interval) {
                printf("thread %d, reads phase %d: ", thread_index, phase);
                //for (int x: read_fds)
                //    printf("%d ", x);
                printf("%d predicted of %d\n", n_predict, n_total + n_total_first);
                if (n_total >= predict_interval * 10 && predict_interval <= 10000)
                    predict_interval *= 10;
                //n_total = 0;
                n_old_total = n_total;
                fflush(stdout);
                //read_fds.clear();
                phase++;
                if (phase == (int)phase_messages.size()) phase = phase_messages.size() - 1;
            }

            for (int i = 0; i < TRY_READS; i++)
                try_read_from(expected_read_fd, true);
            
            continue;
        }
#endif
        
        int n = epoll_wait(epoll_server.efd, events, MAX_POLL_EVENTS, ACTIVE_WAIT ? 0 : -1);
        
#if 0
        n_epolls++;
        if (n_epolls % 100000 == 0)
            printf("%d epolls, %.3f ns / epoll average\n", n_epolls, (get_ns_timestamp() - t_start) / (double)n_epolls);
#endif
        
        close_fds.clear();
        need_accept = false;
        
        for (int i = 0; i < n; i++) {
            bool error = (events[i].events & EPOLLERR) || (events[i].events & EPOLLHUP) || (events[i].events & EPOLLRDHUP) || (!(events[i].events & EPOLLIN));
            
            if (!error && events[i].data.fd != epoll_server.sfd) {
                global_t_polled = get_ns_timestamp();
                
                max_fd = max(max_fd, events[i].data.fd + 1);
                try_read_from(events[i].data.fd);
            }
            else if (error) {
                close_fds.push_back(events[i].data.fd);
#if 0
                printf("deleting %d, mask: %d %d %d %d\n",
                       events[i].data.fd,
                       (events[i].events & EPOLLERR) ? 1 : 0,
                       (events[i].events & EPOLLHUP) ? 1 : 0,
                       (events[i].events & EPOLLRDHUP) ? 1 : 0,
                       (!(events[i].events & EPOLLIN)) ? 1 : 0);
#endif
            }
            else {
                need_accept = true;
            }
        }
        
        if (need_accept) {
            while (true) {
                scope_profile(CONNECTION_ACCEPT);
                        
                struct sockaddr in_addr;
                socklen_t in_len;
                int infd;

                in_len = sizeof in_addr;
                infd = accept4(epoll_server.sfd, &in_addr, &in_len, SOCK_NONBLOCK);
                profile_delimiter(ACCEPT_TO_ACCEPT);
                
                if (infd == -1) {
                    if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
                        break;
                    }
                    else {
                        perror("accept");
                        break;
                    }
                }

                add_socket_to_epoll_queue(epoll_server.sfd, true);
                add_socket_to_epoll_queue(infd, false);
                
                int one = 1;
                verify(setsockopt(infd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) == 0);
                verify(setsockopt(infd, SOL_TCP, TCP_QUICKACK, &one, sizeof(one)) == 0);
            }
        }
        
        for (int fd: close_fds) {
            close_fd_fast(fd);
        }
            
        if (false) {
            printf("polled %d, %.3f mks read, %.3f mks logic, %.3f mks write, %d allocs\n", n, total_reads / 1e3, total_logic / 1e3, total_writes / 1e3, n_allocs);
            total_reads = total_logic = total_writes = 0;
            n_allocs = 0;
            fflush(stdout);
        }
        
        if (global_last_request_is_get != last_request_is_get) {
            printf("[%d] Request type switch: '%s' -> '%s', max fd + 1: %d\n", global_thread_index, (last_request_is_get ? "GET" : "POST"), (global_last_request_is_get ? "GET" : "POST"), max_fd);
            last_request_is_get = global_last_request_is_get;
            fflush(stdout);
        }
        
        if (global_last_request_is_get && total_requests == phase_messages[0] && !first_phase_ended) {
            first_phase_ended = true;
            printf("Detected end of first GET phase at %lld\n", (li)time(0)); fflush(stdout);
        }
        
        if (global_last_request_is_get && total_requests == phase_messages[2] && third_phase_ended) {
            third_phase_ended = true;
            printf("Detected end of second GET phase at %lld\n", (li)time(0)); fflush(stdout);
            print_memory_stats();
        }
        
        if (!global_last_request_is_get && total_requests == phase_messages[1] && !second_phase_ended) {
            second_phase_ended = true;
            printf("Detected end of POST phase at %lld\n", (li)time(0)); fflush(stdout);
            fix_database_caches();
        }
    }
}
