/* Networking & main loop */

// networking template copyright: https://banu.com/blog/2/how-to-use-epoll-a-complete-example-in-c/

using input_request_handler = http_input_request_handler;

// 1 thread, 1 instance -> 45k RPS
// 4 threads, 1 instance -> 45k RPS
// 4 threads, 4 instance -> 117k RPS

const int NUM_THREADS = 4;
    
const int MAX_POLL_EVENTS = 128;
const int MAX_FDS = 10000;
const int READ_BUFFER_SIZE = 4096 * 2;

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

void poller_thread(int thread_index);

void start_epoll_server() {

    epoll_server.sfd = create_and_bind(80);
    verify(epoll_server.sfd != -1);

    verify(listen(epoll_server.sfd, SOMAXCONN) != -1);

    epoll_server.efd = epoll_create1(0);
    verify(epoll_server.efd != -1);

    epoll_event event;
    event.data.fd = epoll_server.sfd;
    event.events = EPOLLIN | EPOLLET;
    verify(epoll_ctl(epoll_server.efd, EPOLL_CTL_ADD, epoll_server.sfd, &event) != -1);
    
    vector<thread> threads(NUM_THREADS);
    for (int i = 0; i < (int)threads.size(); i++)
        threads[i] = thread(poller_thread, i);
    
    for (int i = 0; i < (int)threads.size(); i++)
        threads[i].join();
}

void poller_thread(int thread_index) {
    global_thread_index = thread_index;
    epoll_event* events = nullptr;
    events = (epoll_event*)calloc(MAX_POLL_EVENTS, sizeof(epoll_event));
    
    vector<input_request_handler> fd_to_queue(MAX_FDS);
    
    printf("Started event loop #%d (epoll)\n", thread_index + 1); fflush(stdout);
    
    bool last_request_is_get = false;
    int max_fd = 0;
    
    while (1) {
        bool forced_flush = false;
        if (global_last_request_is_get != last_request_is_get) {
            printf("[%d] Request type switch: '%s' -> '%s', max fd + 1: %d\n", global_thread_index, (last_request_is_get ? "GET" : "POST"), (global_last_request_is_get ? "GET" : "POST"), max_fd);
            last_request_is_get = global_last_request_is_get;
            forced_flush = true;
        }
        
#ifndef DISABLE_PROFILING
        profiler.maybe_flushreset((li)1e9 * 5, &minute_accumulator_profiler, forced_flush);
        minute_accumulator_profiler.maybe_flushreset((li)1e9 * 60, nullptr, forced_flush);
#endif
        
        // FIXME: test active wait
        
        constexpr bool active_wait = true;
        int n = epoll_wait(epoll_server.efd, events, MAX_POLL_EVENTS, active_wait ? 0 : -1);
        
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
                    infd = accept4(epoll_server.sfd, &in_addr, &in_len, SOCK_CLOEXEC | SOCK_NONBLOCK);
                    profile_delimiter(ACCEPT_TO_ACCEPT);
                    //printf("accept socket %d\n", infd);
                    if (infd == -1) {
                        if ((errno == EAGAIN) ||  (errno == EWOULDBLOCK)) {
                            break;
                        }
                        else {
                            perror("accept");
                            break;
                        }
                    }

                    epoll_event event;
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
                int done = 0;
                
                global_t_polled = get_ns_timestamp();
                
                max_fd = max(max_fd, events[i].data.fd + 1);
                verify(events[i].data.fd >= 0 && events[i].data.fd < max_fd);
                if ((int)fd_to_queue.size() < max_fd)
                    fd_to_queue.resize(max_fd);
                
                input_request_handler& q = fd_to_queue[events[i].data.fd];
                //printf("read ready for socket %d\n", events[i].data.fd);

                while (1) {
                    ssize_t count;
                    static thread_local char buf[READ_BUFFER_SIZE];

                    profile_begin(READ_CALL);
                    count = read(events[i].data.fd, buf, READ_BUFFER_SIZE);
                    profile_end(READ_CALL);
                    
                    if (count == -1) {
                        if (errno != EAGAIN) {
                            perror("read");
                            done = 1;
                        }
                        break;
                    }
                    else
                    {
                        buf[count] = 0;
                        if (count == 0) {
                            done = 1;
                            break;
                        }
                    }
                    
                    q.request_content += buf;
                }
                
                if (done) {
                    // maybe close? no :)
                    continue;
                }
                
                global_t_ready = get_ns_timestamp();
                int pret = q.parse(events[i].data.fd);
                verify(pret != -1);

                if (pret > 0) {
                    //li t_end = get_ns_timestamp();
                    //printf("processed in %.3f mks, without read %.3f mks\n", (t_end - t_start) / 1000.0, (t_end - t_ready) / 1000.0);
                    
                    q.request_content = "";
                }
            }
        }
    }

    free(events);
}
