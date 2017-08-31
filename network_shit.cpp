/* Networking & main loop */

using input_request_handler = http_input_request_handler;

const int NUM_CONSUMER_THREADS = 3;

const int NUM_POLLER_THREADS = 1;
const int MAX_EPOLL_EVENTS = 2048; // max events = num instances

const int MAX_FDS = 10000;
const int READ_BUFFER_SIZE = 4096 * 2;
constexpr bool ACTIVE_WAIT = true;

sockaddr_in local_addr = {};

static int create_and_bind(int port) {
    int sfd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, IPPROTO_IP);
    verify(sfd != -1);
    
    int reuse = 1;
    if (setsockopt(sfd, SOL_SOCKET, SO_BROADCAST, (const char*)&reuse, sizeof(reuse)) < 0)
        perror("setsockopt(SO_BROADCAST) failed");
    
    if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (const char*)&reuse, sizeof(reuse)) < 0)
        perror("setsockopt(SO_REUSEADDR) failed");
    
    bzero((char *) &local_addr, sizeof(local_addr));
    local_addr.sin_family = AF_INET;
    local_addr.sin_addr.s_addr = inet_addr("0.0.0.0");
    local_addr.sin_port = htons(port);
           
    verify(bind(sfd, (sockaddr*)&local_addr, sizeof(local_addr)) == 0);
    
    if (setsockopt(sfd, SOL_TCP, TCP_QUICKACK, (const char*)&reuse, sizeof(reuse)) < 0)
        perror("setsockopt(TCP_QUICKACK) failed");
    
    if (setsockopt(sfd, SOL_TCP, TCP_NODELAY, (const char*)&reuse, sizeof(reuse)) < 0)
        perror("setsockopt(TCP_NODELAY) failed");
    
    return sfd;
}

struct EpollServer {
    int sfd;
    int efd;
};

EpollServer epoll_server;

void poller_thread(int thread_index, int affinity_mask);

/* Memory fiddling */

void preallocate_stack(int stacksize) {
   	volatile char buffer[stacksize];
   	
   	for (int i = 0; i < stacksize; i += sysconf(_SC_PAGESIZE)) {
   		buffer[i] = i;
   	}
    
   	stacksize += buffer[10] + buffer[20];
}

void configure_malloc_behavior() {
   	verify(mallopt(M_TRIM_THRESHOLD, -1) == 1);
   	verify(mallopt(M_MMAP_MAX, 0) == 1);
}

/* Start server */

void tune_realtime_params() {
    configure_malloc_behavior();
}

void add_socket_to_epoll_queue(int fd, bool mod) {
    if (mod) return;
    
    epoll_event event = {};
    event.data.fd = fd;
    event.events = EPOLLIN | EPOLLRDHUP;
#ifndef DISABLE_EPOLLET
    event.events |= EPOLLET;
#endif
    int ec = epoll_ctl(epoll_server.efd, mod ? EPOLL_CTL_MOD : EPOLL_CTL_ADD, fd, &event);
    if (ec == -1)
        perror("epoll_ctl");
}

void start_epoll_server() {
    epoll_server.sfd = create_and_bind(80);
    server_socket_descriptor = epoll_server.sfd;
    verify(epoll_server.sfd != -1);

    verify(listen(epoll_server.sfd, 20000) != -1);

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
        
    printf("available cpus: %s\n", avail_cpus.c_str()); fflush(stdout);
    
    printf("thread mapping (thread -> cpu): "); fflush(stdout);
    for (int i = 0; i < (int)affinity_mask.size(); i++) {
        int cpu = avail_indices[i % avail_indices.size()];
        if (i) printf(", ");
        printf("%d -> %d", i, cpu); fflush(stdout);
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

#ifndef DISABLE_EPOLLET
const int EAGAIN_BUFFER_SIZE = 128;
poller_local char eagain_buf[EAGAIN_BUFFER_SIZE];
#endif

poller_local char read_buf[READ_BUFFER_SIZE];

void close_fd_fast(int fd) {
    epoll_ctl(epoll_server.efd, EPOLL_CTL_DEL, fd, 0);
    close(fd);
}

void try_read_from(int fd) {
    ssize_t count = read(fd, read_buf, READ_BUFFER_SIZE);
    
    if (count < 0 && errno != EWOULDBLOCK) {
        close_fd_fast(fd);
        return;
    }
    
#if 0
#ifndef DISABLE_EPOLLET
    int n_reads = 0, sum_reads = 0;
    while (true) {
        int can = read(fd, eagain_buf, EAGAIN_BUFFER_SIZE);
        if (can == 0) {
            close(fd);
            return;
        }
        
        if (can < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                perror("read");
                fflush(stdout);
            }
            break;
        }
        sum_reads += can;
        n_reads++;
    }
#endif
    
#ifndef DISABLE_EPOLLET
    add_socket_to_epoll_queue(fd, true);
    
    if (n_reads > 0) {
        printf("Strange, expected EAGAIN but got %d reads for %d bytes total\n", n_reads, sum_reads);
    }
#endif
#endif
    
    if (count <= 0) return;
    read_buf[count] = 0;
    
    input_request_handler handler;
    handler.request_content = read_buf;
    handler.request_length = count;
    
    //global_t_ready = get_ns_timestamp();
    
    handler.parse(fd);
}

void handle_new_connection(int infd) {
    add_socket_to_epoll_queue(infd, false);
    
    int one = 1;
    if (setsockopt(infd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) != 0) {
        perror("setsockopt"); fflush(stdout); return;
    }
    
    if (setsockopt(infd, SOL_TCP, TCP_QUICKACK, &one, sizeof(one)) != 0) {
        perror("setsockopt"); fflush(stdout); return;
    }
}

// by unknown reasons first connections are very slow
// we will do the shittiest thing ever here

void heatup_thread() {
    int ec = system("./smart_spammer.elf 5 1000");
    if (ec != 0) {
        printf("Heatup finished with ec %d!\n", ec); fflush(stdout);
    }
    else {
        printf("Heatup succesful\n"); fflush(stdout);
    }
}

void spawn_heatup_detached() {
    printf("Spawning heatup detached\n"); fflush(stdout);
    thread t(heatup_thread);
    t.detach();
}

void poller_thread(int thread_index, int affinity_mask) {
    global_thread_index = thread_index;
    set_thread_affinity(affinity_mask, true);
    
    epoll_event events[MAX_EPOLL_EVENTS];
    
    preallocate_stack(1024 * 1024 * 2);
    
    bool last_request_is_get = false;
    
    int prepare_phase_seconds = 60 * 10;
    array<int, 3> phase_messages = { 150150, 40000, 630000 };
    
    if (!is_rated_run) {
        phase_messages = { 9030, 3000, 19500 };
        prepare_phase_seconds = 60;
    }
    
    vector<int> close_fds;
    close_fds.reserve(MAX_EPOLL_EVENTS);
    
    bool first_phase_ended = false, second_phase_ended = false, third_phase_ended = false;
    bool enable_heatup = false;
    
    printf("Prepare phase: %d seconds\n", prepare_phase_seconds); fflush(stdout);
    
    if (!is_rated_run) {
        enable_heatup = true;
    }
    
    int heatup_times = 1;
    
    li last_print = get_ns_timestamp();
    while (true) {
        // 2 heat ups, ~10 s each + 10 s to wait
        if (!enable_heatup) {
            double wait_remain = prepare_phase_seconds - (get_ns_timestamp() - process_startup_timestamp) / 1e9;
            if (wait_remain <= 200 && wait_remain >= 150) {
                printf("Enabling heat up (%lld), approx %.3f seconds should be available\n", (li)time(0), wait_remain); fflush(stdout);
                enable_heatup = true;
            }
        }
        
        if (enable_heatup && heatup_times > 0) {
            heatup_times = 0;
            spawn_heatup_detached();
        }
        
#if 0
        if (get_ns_timestamp() > last_print + 1e9) {
            printf("max fd + 1: %d\n", max_fd); fflush(stdout);
            print_memory_stats();
            last_print = get_ns_timestamp();
        }
#endif
        
        int n = epoll_wait(epoll_server.efd, events, MAX_EPOLL_EVENTS, ACTIVE_WAIT ? 0 : -1);
        
        bool need_accept = false;
        for (int i = 0; i < n; i++) {
            bool error = (events[i].events & EPOLLERR) || (events[i].events & EPOLLHUP) || (events[i].events & EPOLLRDHUP) || (!(events[i].events & EPOLLIN));
            
            if (!error && events[i].data.fd != epoll_server.sfd) {
                //global_t_polled = get_ns_timestamp();
                try_read_from(events[i].data.fd);
            }
            else if (error) {
                close_fds.push_back(events[i].data.fd);
            }
            else {
                need_accept = true;
            }
        }
        
        need_accept_connections = need_accept;
        
        if (n <= 0) {
            for (int fd: close_fds) {
                close_fd_fast(fd);
            }
            close_fds.clear();
            
#if 0
            if (false) {
                printf("polled %d, %.3f mks read, %.3f mks logic, %.3f mks write\n", n, total_reads / 1e3, total_logic / 1e3, total_writes / 1e3);
                total_reads = total_logic = total_writes = 0;
                fflush(stdout);
            }
#endif
            
            if (global_last_request_is_get != last_request_is_get) {
                printf("[%d] Request type switch: '%s' -> '%s'\n", global_thread_index, (last_request_is_get ? "GET" : "POST"), (global_last_request_is_get ? "GET" : "POST"));
                last_request_is_get = global_last_request_is_get;
                fflush(stdout);
            }
            
            if (global_last_request_is_get && total_requests == phase_messages[0] && !first_phase_ended) {
                first_phase_ended = true;
                printf("Detected end of first GET phase at %lld\n", (li)time(0));
                fflush(stdout);
            }
            
            if (global_last_request_is_get && total_requests == phase_messages[2] && !third_phase_ended) {
                third_phase_ended = true;
                printf("Detected end of second GET phase at %lld\n", (li)time(0));
                print_memory_stats();
                fflush(stdout);
            }
            
            if (!global_last_request_is_get && total_requests == phase_messages[1] && !second_phase_ended) {
                second_phase_ended = true;
                printf("Detected end of POST phase at %lld\n", (li)time(0));
                fix_database_caches();
                print_memory_stats();
                fflush(stdout);
            }
        }
    }
}
