/* Networking & main loop */

// networking template copyright: https://banu.com/blog/2/how-to-use-epoll-a-complete-example-in-c/

using input_request_handler = http_input_request_handler;

// 1 thread, 1 instance -> 45k RPS
// 4 threads, 1 instance -> 45k RPS
// 4 threads, 4 instance -> 117k RPS

const int NUM_THREADS = 1;
    
const int MAX_POLL_EVENTS = 128;
const int MAX_FDS = 10000;
const int READ_BUFFER_SIZE = 4096 * 2;
constexpr bool ACTIVE_WAIT = true;

union CpuSet {
    cpu_set_t as_set;
    int as_int[sizeof(cpu_set_t) / sizeof(int)];
    unsigned char as_char[sizeof(cpu_set_t)];
};

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

#ifndef DISABLE_AFFINITY
int get_affinity_mask() {
    CpuSet cpu_mask = {};
    
    verify(sched_getaffinity(0, sizeof(cpu_mask.as_set), &cpu_mask.as_set) == 0);
    
    return cpu_mask.as_int[0];
}
#endif

void start_epoll_server() {
    epoll_server.sfd = create_and_bind(80);
    verify(epoll_server.sfd != -1);

    verify(listen(epoll_server.sfd, SOMAXCONN) != -1);

    epoll_server.efd = epoll_create1(0);
    verify(epoll_server.efd != -1);

    epoll_event event = {};
    event.data.fd = epoll_server.sfd;
    event.events = EPOLLIN | EPOLLET;
    verify(epoll_ctl(epoll_server.efd, EPOLL_CTL_ADD, epoll_server.sfd, &event) != -1);
    
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
    vector<int> affinity_mask(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; i++) {
        int cpu = avail_indices[i % avail_indices.size()];
        if (i) printf(", ");
        printf("%d -> %d", i, cpu); fflush(stdout);
        //assert(cpu >= 0 && cpu < 32);
        affinity_mask[i] = 1 << cpu;
    }
    printf("\n"); fflush(stdout);
#endif
    
    if (NUM_THREADS > 1) {
        vector<thread> threads(NUM_THREADS);
        for (int i = 0; i < (int)threads.size(); i++)
            threads[i] = thread(poller_thread, i, affinity_mask[i]);
        
        for (int i = 0; i < (int)threads.size(); i++)
            threads[i].join();
    }
    else {
        poller_thread(0, affinity_mask[0]);
    }
}

const int NEED_READS = 5;
thread_local int succesful_reads[NEED_READS];
thread_local int succesful_reads_pos = 0;
thread_local bool all_reads_equal = false;
    
vector<int> read_fds;

const int TRY_READS = 5;

int antiswap_iteration = 0;

static thread_local char read_buf[READ_BUFFER_SIZE];
    
int ANTISWAP_PERIOD = 1;

int expected_read_fd = -1;

vector<char> fd_used;

int n_predict = 0, n_total = 0;

void try_read_from(int fd, bool predict = false) {
    //printf("read ready for socket %d\n", events[i].data.fd);
    
    profile_begin(READ_CALL);
    ssize_t count = read(fd, read_buf, READ_BUFFER_SIZE);
    profile_end(READ_CALL);
    
    if (count <= 0) return;
    read_buf[count] = 0;
    
    if (predict)
        n_predict++;
    n_total++;
    
    input_request_handler handler;
    handler.request_content = read_buf;
    handler.request_length = count;
    
    global_t_ready = get_ns_timestamp();
    
    int pret = handler.parse(fd);
    if (pret <= 0)
        // just drop it :)
        return;
    
    maybe_resize(fd_used, fd);
    fd_used[fd] = true;
    
    if (fd + 1 < (int)fd_used.size() && fd_used[fd + 1])
        expected_read_fd = fd + 1;
    else
        expected_read_fd = -1;
    
    read_fds.push_back(fd);
    antiswap_iteration = 0;
    ANTISWAP_PERIOD = 10;
    
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
    
#ifndef DISABLE_AFFINITY
    CpuSet mask_set = {};
    mask_set.as_int[0] = affinity_mask;
    
    verify(sched_setaffinity(0, sizeof(cpu_set_t), &mask_set.as_set) == 0);
#endif
    
    epoll_event* events = nullptr;
    events = (epoll_event*)calloc(MAX_POLL_EVENTS, sizeof(epoll_event));
    
    //vector<input_request_handler> fd_to_queue(MAX_FDS);
    
#ifndef DISABLE_AFFINITY
    printf("Started event loop #%d (epoll, affinity mask %d)\n", thread_index + 1, get_affinity_mask()); fflush(stdout);
#else
    printf("Started event loop #%d (epoll, affinity disabled)\n", thread_index + 1); fflush(stdout);
#endif
    
    bool last_request_is_get = false;
    int max_fd = 0;
    
    //li last_print = get_ns_timestamp();
    //int n_epolls = 0;
    //li t_start = get_ns_timestamp();
    
    const int MAX_MESSAGES = 40;
    int n_messages = 0;
    
    //vector<int> phase_messages = { 18090, 12000, 20000 };
    vector<int> phase_messages = { 18900, 12000, 10000 };
    
    if (!is_rated_run)
        phase_messages = { 1515, 1000, 6800 };
    
    int phase = 0;
    
    bool last_reads = false;
    
    li longlife_hash = 0;
    fd_used.reserve(10000);
    
    while (true) {
#if 0
        if (get_ns_timestamp() > last_print + 1e9) {
            printf("max fd + 1: %d\n", max_fd); fflush(stdout);
            last_print = get_ns_timestamp();
        }
#endif
        
#ifndef DISABLE_PROFILING
        bool forced_flush = false;
        profiler.maybe_flushreset((li)1e9 * 5, &minute_accumulator_profiler, forced_flush);
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
        
        /*if (all_reads_equal) {
            for (int i = 0; i < TRY_READS; i++)
                try_read_from(succesful_reads[0]);
        }*/
        
        antiswap_iteration++;
        if (antiswap_iteration >= ANTISWAP_PERIOD) {
            // not fast operation, ~100 ns
            //li t0 = get_ns_timestamp();
            longlife_hash ^= user_by_id[rand() % user_by_id.size()].id;
            longlife_hash ^= visit_by_id[rand() % visit_by_id.size()].id;
            longlife_hash ^= location_by_id[rand() % location_by_id.size()].id;
            longlife_hash ^= read_buf[rand() % READ_BUFFER_SIZE];
            longlife_hash ^= response_buffer[rand() % MAX_RESPONSE_SIZE];
            antiswap_iteration = 0;
            //li t1 = get_ns_timestamp();
        }
        
        if (phase < (int)phase_messages.size() && (int)read_fds.size() == phase_messages[phase]) {
            printf("reads phase %d: ", phase);
            for (int x: read_fds)
                printf("%d ", x);
            printf(", %d predicted of %d\n", n_predict, n_total);
            n_predict = 0;
            n_total = 0;
            printf("\n");
            fflush(stdout);
            read_fds.clear();
            phase++;
            if (phase == (int)phase_messages.size()) phase = phase_messages.size() - 1;
        }
        
        if (all_reads_equal != last_reads) {
            last_reads = all_reads_equal;
            n_messages++;
            
            if (n_messages <= MAX_MESSAGES) {
                printf("%d: switched to %s mode, max fd %d\n", (int)time(0), all_reads_equal ? "mixed" : "epoll", max_fd);
                if (n_messages == MAX_MESSAGES)
                    printf("this was the last message due to limit %d\n", MAX_MESSAGES);
                fflush(stdout);
            }
        }
        
        int n = epoll_wait(epoll_server.efd, events, MAX_POLL_EVENTS, ACTIVE_WAIT ? 0 : -1);
        
#if 0
        n_epolls++;
        if (n_epolls % 100000 == 0)
            printf("%d epolls, %.3f ns / epoll average\n", n_epolls, (get_ns_timestamp() - t_start) / (double)n_epolls);
#endif
        
        for (int i = 0; i < n; i++) {
            if ((events[i].events & EPOLLERR) ||
                (events[i].events & EPOLLHUP) ||
                (!(events[i].events & EPOLLIN))) {
                fprintf(stderr, "epoll error\n");
                close(events[i].data.fd);
                continue;
            }
            else if (events[i].data.fd == epoll_server.sfd) {
                while (1) {
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

                    epoll_event event = {};
                    event.data.fd = infd;
                    event.events = EPOLLIN | EPOLLET;
                    verify(epoll_ctl(epoll_server.efd, EPOLL_CTL_ADD, infd, &event) != -1);
                    
                    int one = 1;
                    verify(setsockopt(infd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) == 0);
                    verify(setsockopt(infd, SOL_TCP, TCP_QUICKACK, &one, sizeof(one)) == 0);
                    
#if 0
                    socklen_t one_length = 4;
                    verify(getsockopt(infd, SOL_TCP, TCP_NODELAY, &one, &one_length) >= 0);
                    printf("length %d value %d\n", one_length, one);
                    verify(one != 0);
                    verify(getsockopt(infd, SOL_TCP, TCP_QUICKACK, &one, &one_length) >= 0);
                    printf("length %d value %d\n", one_length, one);
                    verify(one != 0);
#endif
                }
                continue;
            }
            else {
                global_t_polled = get_ns_timestamp();
                
                max_fd = max(max_fd, events[i].data.fd + 1);
                try_read_from(events[i].data.fd);
            }
            
            if (i == n - 1 && global_last_request_is_get != last_request_is_get) {
                printf("[%d] Request type switch: '%s' -> '%s', max fd + 1: %d\n", global_thread_index, (last_request_is_get ? "GET" : "POST"), (global_last_request_is_get ? "GET" : "POST"), max_fd);
                last_request_is_get = global_last_request_is_get;
            }
        }
    }
    
    /*
    breakAll:;
    printf("Entering direct read poll\n"); fflush(stdout);
    //input_request_handler handler;
    //handler.request_content = buf;
    
    t_start = get_ns_timestamp();
    n_epolls = 0;
    
    while (true) {
        ssize_t count;
        static thread_local char buf[READ_BUFFER_SIZE];
        count = read(read_fd, buf, READ_BUFFER_SIZE);
        
        n_epolls++;
        if (n_epolls % 100000 == 0)
            printf("%d reads, %.3f ns / read average\n", n_epolls, (get_ns_timestamp() - t_start) / (double)n_epolls);
        
        if (count > 0) {
            n_epolls = 0;
            t_start = get_ns_timestamp();
            //buf[count] = 0;
            //handler.request_length = count;
            write_only_header_answer(read_fd, 200);
        }
    }

    free(events);*/
}
