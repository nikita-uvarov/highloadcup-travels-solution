#include <bits/stdc++.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <errno.h>

#define all(v) (v).begin(), (v).end()

using namespace std;

const int MAX_POLL_EVENTS = 128;

//#undef SUBMISSION_MODE

#define DISABLE_VALIDATE

#define verify(cond) if (!(cond)) { fprintf(stderr, "Verification failed: '" #cond "' on line %d\n", __LINE__); perror("perror"); fflush(stderr); abort(); }

#ifndef SUBMISSION_MODE
#define check(cond) if (!(cond)) { fprintf(stderr, "Verification failed: '" #cond "' on line %d\n", __LINE__); perror("perror"); fflush(stderr); abort(); }
#else
#define check(cond) {}
#endif

/* Profiling */

using li = long long;

li timespec_to_li(const timespec& ts) {
    return ts.tv_sec * (li)1e9 + ts.tv_nsec;
}

li get_ns_timestamp() {
    timespec ts;
    verify(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
    return timespec_to_li(ts);
}

struct IntervalProfiler {
    li sum_time_ns = 0;
    li open = 0;
    int n_intervals = 0;
    bool interval_open = false;
    const char* name = "unnamed";
    
    void begin_interval() {
        //printf("begin %s\n", name);
        verify(!interval_open);
        interval_open = true;
        
        timespec ts;
        verify(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
        open = -timespec_to_li(ts);
    }
    
    void end_interval() {
        //printf("end %s\n", name);
        verify(interval_open);
        interval_open = false;
        n_intervals++;
        
        timespec ts;
        verify(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
        open += timespec_to_li(ts);
        sum_time_ns += open;
    }
    
    void delimiter_event() {
        if (interval_open) {
            end_interval();
            begin_interval();
        }
        else {
            begin_interval();
        }
    }
    
    double average_ns() const {
        return n_intervals ? (sum_time_ns / (double)n_intervals) : 0.0;
    }
};

enum ProfilerIntervals {
    ACCEPT_TO_ACCEPT,
    CONNECTION_ACCEPT,
    READ_CALL,
    PARSE_HEADERS,
    PARSE_PATH,
    PARSE_JSON_RAPIDJSON,
    PARSE_JSON_CUSTOM,
    BUILD_JSON_RESPONSE,
    WRITE_RESPONSE,
    
    NUM_INTERVALS
};

li max_request_ns = 0;

struct Profiler {
    const char* profiler_prefix;
    IntervalProfiler intervals[NUM_INTERVALS];
    
    Profiler(const char* profiler_prefix = "[?] "): profiler_prefix(profiler_prefix) {
        intervals[ACCEPT_TO_ACCEPT].name = "A";
        intervals[CONNECTION_ACCEPT].name = "C";
        intervals[READ_CALL].name = "R";
        intervals[PARSE_HEADERS].name = "H";
        intervals[PARSE_PATH].name = "P";
        intervals[PARSE_JSON_RAPIDJSON].name = "JR";
        intervals[PARSE_JSON_CUSTOM].name = "JC";
        intervals[BUILD_JSON_RESPONSE].name = "B";
        intervals[WRITE_RESPONSE].name = "W";
    }
    
    void begin(int id) {
        intervals[id].begin_interval();
    }
    
    void end(int id) {
        intervals[id].end_interval();
    }
    
    void delimiter(int id) {
        intervals[id].delimiter_event();
    }
    
    void write_status() {
        printf("%ssec %d avg (micro): ", profiler_prefix, (int)time(0));
        for (int i = 0; i < NUM_INTERVALS; i++) {
            if (i) printf(", ");
            printf("%s %.2f (%d)", intervals[i].name, intervals[i].average_ns() / 1000.0, intervals[i].n_intervals);
        }
        if (strlen(profiler_prefix) == 0) {
            printf(" max request %.3f mks", max_request_ns / 1000.0);
            max_request_ns = 0;
        }
        printf("\n"); fflush(stdout);
    }
    
    void submit_to_other(Profiler& accumulator) {
        for (int i = 0; i < NUM_INTERVALS; i++) {
            accumulator.intervals[i].n_intervals += intervals[i].n_intervals;
            accumulator.intervals[i].sum_time_ns += intervals[i].sum_time_ns;
        }
    }
    
    void reset() {
        for (int i = 0; i < NUM_INTERVALS; i++) {
            intervals[i].sum_time_ns = 0;
            intervals[i].n_intervals = 0;
        }
    }
    
    li last_flushreset = -1;
    
    void maybe_flushreset(li ns_interval, Profiler* accumulator, bool forced = false) {
        timespec now_ts;
        clock_gettime(CLOCK_MONOTONIC, &now_ts);
        li now = timespec_to_li(now_ts);
        
        if (last_flushreset == -1) {
            last_flushreset = now;
            return;
        }
        
        if (now - last_flushreset >= ns_interval || forced) {
            last_flushreset = now;
            write_status();
            if (accumulator)
                submit_to_other(*accumulator);
            reset();
        }
    }
};

Profiler profiler("");
Profiler minute_accumulator_profiler("[minute] ");

struct VisibilityProfiler {
    int id = -1;
    
    VisibilityProfiler(int id = -1): id(id) { profiler.begin(id); }
    void end() { if (id != -1) profiler.end(id); id = -1; }
    ~VisibilityProfiler() { if (id != -1) profiler.end(id); }
};

/* Networking & main loop */

// networking template copyright: https://banu.com/blog/2/how-to-use-epoll-a-complete-example-in-c/

static int make_socket_non_blocking(int sfd) {
    int flags = fcntl(sfd, F_GETFL, 0);
    verify(flags != -1);

    flags |= O_NONBLOCK;
    verify(fcntl(sfd, F_SETFL, flags) != -1);
    
#if 0
    int one = 1;
    verify(setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) == 0);
#endif

    return 0;
}

#include <arpa/inet.h>

static int create_and_bind(int port) {
    int sfd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, IPPROTO_IP);
    verify(sfd != -1);
    
    int reuse = 1;
    if (setsockopt(sfd, SOL_SOCKET, SO_BROADCAST, (const char*)&reuse, sizeof(reuse)) < 0)
        perror("setsockopt(SO_BROADCAST) failed");
    
    if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (const char*)&reuse, sizeof(reuse)) < 0)
        perror("setsockopt(SO_REUSEADDR) failed");
    

#if 0
    #ifdef SO_REUSEPORT
    if (setsockopt(sfd, SOL_SOCKET, SO_REUSEPORT, (const char*)&reuse, sizeof(reuse)) < 0) 
        perror("setsockopt(SO_REUSEPORT) failed");
    #else
    printf("SO_REUSEPORT is not supported\n");
    #endif
#endif
    
    sockaddr_in addr = {};
    bzero((char *) &addr, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("0.0.0.0");
    addr.sin_port = htons(port);
           
    verify(bind(sfd, (sockaddr*)&addr, sizeof(addr)) == 0);
    
    return sfd;
        
    /*struct addrinfo hints;
    struct addrinfo *result, *rp;
    int s, sfd;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    s = getaddrinfo(NULL, to_string(port).c_str(), &hints, &result);
    if (s != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(s));
        abort();
    }

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        sfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sfd == -1)
            continue;
        
        int reuse = 1;
        if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (const char*)&reuse, sizeof(reuse)) < 0)
            perror("setsockopt(SO_REUSEADDR) failed");

        #ifdef SO_REUSEPORT
        if (setsockopt(sfd, SOL_SOCKET, SO_REUSEPORT, (const char*)&reuse, sizeof(reuse)) < 0) 
            perror("setsockopt(SO_REUSEPORT) failed");
        #else
        printf("SO_REUSEPORT is not supported\n");
        #endif

        s = bind(sfd, rp->ai_addr, rp->ai_addrlen);
        if (s == 0) {
            break;
        }

        close(sfd);
    }
    
    freeaddrinfo(result);

    if (rp == NULL) {
        fprintf(stderr, "Could not bind to port %d\n", port); fflush(stderr);
        abort();
    }
    else {
        return sfd;
    }*/
}

#include "picohttpparser/picohttpparser.h"

bool was_really_closed = false;

void write_only_header_answer(int fd, int code) {
    check(code == 200 || code == 400 || code == 404);
    //printf("response %d\n\n", code);
    
    VisibilityProfiler write_response_prof(WRITE_RESPONSE);
    
#define header_200 "HTTP/1.1 200 OK\r\n"
#define header_400 "HTTP/1.1 400 Bad Request\r\n"
#define header_404 "HTTP/1.1 404 Not Found\r\n"
    
#define header_content_length_zero "Content-Length: 0\r\n"
//#define header_connection_close "Connection: keep-alive\r\n"
#define header_connection_close ""
#define header_server "Server: 1\r\n"
//#define header_host "Host: travels.com\r\n"
#define header_host
#define header_rn "\r\n"
    
// if doesn't work, easy to fix
#define header_connection_close_real "Connection: keep-alive\r\n"
    
    if (code == 200) {
#define response_200 header_200 header_content_length_zero header_connection_close header_server header_host header_rn
        write(fd, response_200, sizeof response_200 - 1);
#undef response_200
    }
    
    if (code == 400) {
#define response_400 header_400 header_content_length_zero header_connection_close_real header_server header_host header_rn
        write(fd, response_400, sizeof response_400 - 1);
        //close(fd);
        //was_really_closed = true;
#undef response_400
    }
    
    if (code == 404) {
#define response_404 header_404 header_content_length_zero header_connection_close_real header_server header_host header_rn
        write(fd, response_404, sizeof response_404 - 1);
        //close(fd);
        //was_really_closed = true;
#undef response_404
    }
    
}

int process_get_request(int fd, const char* path, int path_length);
int process_post_request(int fd, const char* path, int path_length, const char* body);

struct travels_input_request_handler {
    string request_content;
    
    int parse(int fd) {
        profiler.begin(PARSE_HEADERS);
        size_t prevbuflen = 0, method_len, path_len, num_headers;
        
        // FIXME: 10 here is enough
        struct phr_header headers[15];
        num_headers = sizeof(headers) / sizeof(headers[0]);
        const char* path;
        const char* method;
        int minor_version;
        int pret = phr_parse_request(request_content.data(), request_content.length(), &method, &method_len, &path, &path_len,
                                &minor_version, headers, &num_headers, prevbuflen);
        
#if 0
        printf("request '%s'\n", request_content.c_str());
        printf("request is %d bytes long\n", pret);
        printf("method is %.*s\n", (int)method_len, method);
        printf("path is %.*s\n", (int)path_len, path);
        printf("HTTP version is 1.%d\n", minor_version);
        printf("headers:\n");
        for (size_t i = 0; i != num_headers; ++i) {
            printf("%.*s: %.*s\n", (int)headers[i].name_len, headers[i].name,
                (int)headers[i].value_len, headers[i].value);
        }
#endif

        if (pret < 0) {
            profiler.end(PARSE_HEADERS);
            if (pret == -1) {
                printf("Request failed to parse headers:\n'%s'\n", request_content.c_str());
                fflush(stdout);
            }
            
            return pret;
        }
        
        if (method_len == 3) {
            // GET request, can already answer
            profiler.end(PARSE_HEADERS);
            
            int code = process_get_request(fd, path, path_len);
            if (code != 200)
                write_only_header_answer(fd, code);
        }
        else {
            // POST request
            
            // look for Content-Length = 14 symbols    
            int content_length = -1;
            for (size_t i = 0; i != num_headers; ++i) {
                if (headers[i].name_len == 14) {
                    content_length = atoi(headers[i].value);
                    break;
                }
            }
            
            profiler.end(PARSE_HEADERS);
            
            if (content_length < 0) {
                //printf("Request with no content length:\n'%s'\n", request_content.c_str());
                write_only_header_answer(fd, 400);
                return pret;
            }
                
            check(content_length >= 0);
            
            int have_length = request_content.length() - pret;
            if (have_length < content_length) {
                printf("warning -- large request (%d have, %d need)\n", have_length, content_length); fflush(stdout);
                return -2;
            }
            
            /*if (have_length > content_length) {
                printf("warning -- more data received then promised (%d received, %d content length)\n", have_length, content_length);
            }*/
            
            //write_only_header_answer(fd, 400);
            //return pret;
            
            int code = process_post_request(fd, path, path_len, request_content.c_str() + pret);
            if (code != 200)
                write_only_header_answer(fd, code);
        }
        
        //string response = "HTTP/1.1 200 OK\r\n\r\n";
        //write(fd, response.c_str(), response.length());
        
        return pret;
    }
};

int current_timestamp;
int is_rated_run;

void load_json_dump_from_file(string file_name);
void reindex_database();

void initialize_validator();

void load_options_from_file(string file_name, bool must_exist) {
    ifstream is(file_name);
    if (must_exist) {
        verify(is);
    }
    else if (!is) {
        printf("Tried to load options from '%s' unsuccessfully\n", file_name.c_str()); fflush(stdout);
        return;
    }
    
    verify(is >> current_timestamp >> is_rated_run);
    printf("Options loaded from '%s': timestamp %d, is rated %d\n", file_name.c_str(), current_timestamp, is_rated_run); fflush(stdout);
}

bool string_ends_with(string a, string b) {
    return a.length() >= b.length() && a.substr(a.length() - b.length()) == b;
}

void load_initial_data() {
    current_timestamp = time(0);
    
    IntervalProfiler initial_data_prof;
    initial_data_prof.begin_interval();
    
#ifndef DISABLE_VALIDATE
    initialize_validator();
#endif
    
#if 0
    
    load_json_dump_from_file("../data/data/locations_1.json");
    load_json_dump_from_file("../data/data/users_1.json");
    load_json_dump_from_file("../data/data/visits_1.json");
#else
    printf("Running in submission mode, proceeding to unzip\n"); fflush(stdout);
    {
        load_options_from_file("/tmp/data/options.txt", false);
        
        string unzip_cmd = "unzip -o -j /tmp/data/data.zip '*' -d data >/dev/null";
        int ec = system(unzip_cmd.c_str());
        verify(ec == 0);
        
        string find_cmd = "find data -name '*' >db_files.txt";
        ec = system(find_cmd.c_str());
        verify(ec == 0);
        
        ifstream db_files("db_files.txt");
        verify(db_files);
        string db_file_name = "";
        int n_files = 0;
        
        string concat;
        while (getline(db_files, db_file_name) && db_file_name.length() > 0) {
            if (!concat.empty()) concat += " ";
            concat += db_file_name;
            
            n_files++;
            if (string_ends_with(db_file_name, ".json")) {
                load_json_dump_from_file(db_file_name.c_str());
            }
            else if (string_ends_with(db_file_name, ".txt")) {
                load_options_from_file(db_file_name, true);
            }
        }
        printf("files %s\n", concat.c_str()); fflush(stdout);
        
        verify(n_files > 0);
    }
#endif
    
    reindex_database();
    initial_data_prof.end_interval();
    printf("Initial data loaded in %.3f seconds\n", initial_data_prof.average_ns() / (double)1e9); fflush(stdout);
}

void inspect_server_parameters() {
    verify(sizeof(int) == 4);
    verify(sizeof(time_t) == 8);
    
    {
        IntervalProfiler ts_profiler;
        ts_profiler.begin_interval();
        
        const int N_TEST_GETTIMES = 1e6;
        li hash = 1;
        for (int i = 0; i < N_TEST_GETTIMES; i++) {
            timespec ts;
            clock_gettime(CLOCK_MONOTONIC, &ts);
            hash += timespec_to_li(ts);
        }
        
        ts_profiler.end_interval();
        
        printf("clock_gettime cost (ns): %.5f, hash %lld\n", ts_profiler.average_ns() / (double)N_TEST_GETTIMES, hash); fflush(stdout);
    }
}

struct instant_answer_request_handler {
    string request_content;
    
    int parse(int fd) {
        write_only_header_answer(fd, 200);
        return 1;
    }
};

using input_request_handler = travels_input_request_handler;
//using input_request_handler = instant_answer_request_handler;

#if 0
void start_bullshit_server() {
    int sfd = create_and_bind(80);
    verify(sfd != -1);
    
    verify(make_socket_non_blocking(sfd) != -1);
    verify(listen(sfd, SOMAXCONN) != -1);
    
    while (true) {
        //printf("bullshit ready :)\n");
        
        struct sockaddr in_addr;
        socklen_t in_len;
        in_len = sizeof in_addr;
        int socket = accept(sfd, &in_addr, &in_len);
        verify(socket != -1);
        
        //printf("accepted :)\n");
        
        const int READ_BUFFER_SIZE = 4096;
        static char buf[READ_BUFFER_SIZE];
        int len = recv(socket, buf, READ_BUFFER_SIZE, 0);
        
        if (len <= 0) {
            printf("had to close\n");
            close(socket);
            continue;
        }
        
        buf[len] = 0;
        //printf("%.*s\n", len, buf);
        
        input_request_handler handler;
        handler.request_content = buf;
        handler.parse(socket);
    }
}
#endif

#if 0
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <signal.h>
#include <poll.h>

void start_poll_server() {
    int server_sockfd, client_sockfd;
    int server_len, client_len;
    struct sockaddr_un server_address;
    struct sockaddr_un client_address;
    int result;
     
    vector<pollfd> poll_set;
    int numfds = 0;
    int max_fd = 0;
    
    int sfd = create_and_bind(80);
    verify(sfd != -1);
    
    server_sockfd = sfd;
    
    verify(listen(sfd, SOMAXCONN) != -1);
    
    poll_set.resize(1);
    poll_set[0].fd = sfd;
    poll_set[0].events = POLLIN;
    //signal(SIGPIPE, SIG_IGN);
    //update_maxfd(server_sockfd, &max_fd);
 
 
    while (1) {
        char ch;
        int fd_index;
        int nread;
        
        //for (int i = 0; i < poll_set.size(); i++)
            poll_set[0].events = POLLIN;
         
        //printf("Waiting for client (%d total)...\n", poll_set.size());
        //result = select( (max_fd + 1), &testfds, (fd_set *)0, (fd_set *)0, (struct timeval *) 0);
        poll(poll_set.data(), poll_set.size(), 10000);
        
        //vector<pollfd> new_poll_set(1);
        //new_poll_set[0] = poll_set[0];
        
        for(fd_index = 0; fd_index < poll_set.size(); fd_index++)
        {
            //printf("index %d\n", fd_index);
            if( poll_set[fd_index].revents & POLLIN ) {
                if(poll_set[fd_index].fd == server_sockfd) {
                    client_len = sizeof(client_address);
                    client_sockfd = accept(server_sockfd, (struct sockaddr *)&client_address, (socklen_t *)&client_len);
                    close(client_sockfd);
                    
                    write_only_header_answer(client_sockfd, 200);
                     
                    /*
                    pollfd pfd;
                    pfd.fd = client_sockfd;
                    pfd.events = POLLIN;
                    new_poll_set.push_back(pfd);
                    
                    printf("%d: Adding client on fd %d\n", fd_index, client_sockfd);*/
                }
                else {
                    ioctl(poll_set[fd_index].fd, FIONREAD, &nread);
                     
                    if( nread == 0 )
                    {
                        printf("close that shit\n");
                        close(poll_set[fd_index].fd);
                        continue;
                    }
                    
                    printf("Reading from client on %d\n", poll_set[fd_index].fd);
                    char buffer[512 + 1];
                    int len = read(poll_set[fd_index].fd, buffer, 512);
                    if (len <= 0) {
                        close(poll_set[fd_index].fd);
                    }
                    else {
                        buffer[len] = 0;
                        //printf("'%s'\n", buffer);
                        
                        input_request_handler handler;
                        handler.request_content = buffer;
                        int pret = handler.parse(poll_set[fd_index].fd);
                        verify(pret >= 0);
                    }
                    
                    /*
                    ioctl(poll_set[fd_index].fd, FIONREAD, &nread);
                     
                    if( nread == 0 )
                    {
                        close(poll_set[fd_index].fd);
                         
                        numfds--;
                         
                         
                        poll_set[fd_index].events = 0;
                        printf("Removing client on fd %d\n", poll_set[fd_index].fd);
                        poll_set[fd_index].fd = -1;
                    }
                 
                    else {
                        read(poll_set[fd_index].fd, &ch, 1);
                        //printf("Serving client on fd %d -> '%c'\n", poll_set[fd_index].fd, ch);
                        ch++;
                        write(poll_set[fd_index].fd, &ch, 1);
                    }*/
                }
            }
        }
        //poll_set = new_poll_set;
    }
}
#endif

li global_t_ready;
bool global_last_request_is_get = false;

vector<input_request_handler> fd_to_queue;

void start_epoll_server() {
    epoll_event event;
    epoll_event* events = nullptr;

    int sfd = create_and_bind(80);
    verify(sfd != -1);

    //verify(make_socket_non_blocking(sfd) != -1);
    verify(listen(sfd, SOMAXCONN) != -1);

    int efd = epoll_create1(0);
    verify(efd != -1);

    event.data.fd = sfd;
    event.events = EPOLLIN | EPOLLET;
    verify(epoll_ctl(efd, EPOLL_CTL_ADD, sfd, &event) != -1);

    events = (epoll_event*)calloc(MAX_POLL_EVENTS, sizeof event);
    
    //unordered_map<int, input_request_handler*> fd_to_queue;
    //fd_to_queue.reserve(1000);
    fd_to_queue.resize(10000);
    
    printf("Started event loop (epoll)\n"); fflush(stdout);
    
    bool last_request_is_get = false;
    int max_fd = 0;
    
    while (1) {
        bool forced_flush = false;
        if (global_last_request_is_get != last_request_is_get) {
            printf("\nRequest type switch: '%s' -> '%s', max fd + 1: %d\n", (last_request_is_get ? "GET" : "POST"), (global_last_request_is_get ? "GET" : "POST"), max_fd);
            last_request_is_get = global_last_request_is_get;
            forced_flush = true;
        }
        
        profiler.maybe_flushreset((li)1e9 * 5, &minute_accumulator_profiler, forced_flush);
        minute_accumulator_profiler.maybe_flushreset((li)1e9 * 60, nullptr, forced_flush);
        
        int n = epoll_wait(efd, events, MAX_POLL_EVENTS, -1);
        for (int i = 0; i < n; i++) {
            if ((events[i].events & EPOLLERR) ||
                (events[i].events & EPOLLHUP) ||
                (!(events[i].events & EPOLLIN))) {
                fprintf(stderr, "epoll error\n");
                close(events[i].data.fd);
                continue;
            }

            else if (sfd == events[i].data.fd) {
                while (1) {
                    VisibilityProfiler connection_accept_prof(CONNECTION_ACCEPT);
                    
                    struct sockaddr in_addr;
                    socklen_t in_len;
                    int infd;

                    in_len = sizeof in_addr;
                    infd = accept4(sfd, &in_addr, &in_len, SOCK_CLOEXEC | SOCK_NONBLOCK);
                    profiler.delimiter(ACCEPT_TO_ACCEPT);
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
                    
                    //verify(make_socket_non_blocking(infd) != -1);

                    event.data.fd = infd;
                    event.events = EPOLLIN | EPOLLET;
                    verify(epoll_ctl(efd, EPOLL_CTL_ADD, infd, &event) != -1);
                    
                    int one = 1;
                    verify(setsockopt(infd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) == 0);
                }
                continue;
            }
            else {
                li t_start = get_ns_timestamp();
                int done = 0;
                
                max_fd = max(max_fd, events[i].data.fd + 1);
                verify(events[i].data.fd >= 0 && events[i].data.fd < max_fd);
                if (fd_to_queue.size() < max_fd)
                    fd_to_queue.resize(max_fd);
                
                input_request_handler& q = fd_to_queue[events[i].data.fd];
                //printf("read ready for socket %d\n", events[i].data.fd);

                while (1) {
                    ssize_t count;
                    const int READ_BUFFER_SIZE = 4096 * 2;
                    static char buf[READ_BUFFER_SIZE];

                    profiler.begin(READ_CALL);
                    count = read(events[i].data.fd, buf, READ_BUFFER_SIZE);
                    profiler.end(READ_CALL);
                    
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
                    
                    /*if (q == 0) {
                        q = new input_request_handler;
                    }*/
                    q.request_content += buf;
                }
                
                if (done) {
#if 0
                    close(events[i].data.fd);
                    
                    if (q)
                        delete q;
                    fd_to_queue.erase(events[i].data.fd);
                    continue;
#endif
                    continue;
                }
                
                //li t_ready = get_ns_timestamp();
                global_t_ready = get_ns_timestamp();
                int pret = q.parse(events[i].data.fd);
                verify(pret != -1);

                if (pret > 0) {
                    //li t_end = get_ns_timestamp();
                    //printf("processed in %.3f mks, without read %.3f mks\n", (t_end - t_start) / 1000.0, (t_end - t_ready) / 1000.0);
                    
                    q.request_content = "";
                    
                    if (was_really_closed) {
                        /*
                        delete q;
                        fd_to_queue.erase(events[i].data.fd);
                        was_really_closed = false;*/
                    }
                    
#if 0
                    delete q;
                    fd_to_queue.erase(events[i].data.fd);
#endif
                }
            }
        }
    }

    free(events);
    close(sfd);
}

void do_benchmark();

int main(int argc, char *argv[]) {
    inspect_server_parameters();
    load_initial_data();
    
    do_benchmark();
    
    start_epoll_server();
    //start_bullshit_server();
    //start_poll_server();
    
    return 0;
}

/* Answer validation */

#include "rapidjson/include/rapidjson/rapidjson.h"
#include "rapidjson/include/rapidjson/writer.h"
#include "rapidjson/include/rapidjson/stringbuffer.h"
#include "rapidjson/include/rapidjson/document.h"
#include "rapidjson/include/rapidjson/error/en.h"

using namespace rapidjson;

string json_escape_string(const string& str) {
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.String(str.data(), str.length());
    string result = s.GetString();
    return result;
}

#ifndef DISABLE_VALIDATE
struct AnswerValidator {
    map<pair<string, string>, pair<int, string>> expected_answer;
    
    AnswerValidator() {}
    AnswerValidator(string from) {
        load(from);
    }
    
    void load(string answ_file_name) {
        ifstream file(answ_file_name);
        verify(file);
        printf("Loading validation from '%s'\n", answ_file_name.c_str());
        verify(file);
        
        string line;
        while (getline(file, line)) {
            istringstream line_stream(line);
            
            string method, path;
            int code;
            verify(line_stream >> method >> path >> code);
            
            string line_rest;
            getline(line_stream, line_rest, '\0');
            
            int pos = 0;
            while (pos < (int)line_rest.size() && isspace(line_rest[pos])) pos++;
            line_rest = line_rest.substr(pos);
            
            expected_answer[make_pair(method, path)] = make_pair(code, line_rest);
        }
        printf("Loaded %d validation entries\n", (int)expected_answer.size());
    }
    
    const char* next_answer = 0;
    int next_answer_length = 0;
    
    void supply_data(const char* answer, int answer_length) {
        verify(!next_answer);
        
        next_answer = answer;
        next_answer_length = answer_length;
    }
    
    void check_answer(bool is_get, const char* path, int path_length, int code, const char* answer, int answer_length) {
        if (next_answer) {
            verify(!answer && !answer_length);
            answer = next_answer;
            answer_length = next_answer_length;
            
            next_answer = 0;
            next_answer_length = 0;
        }
        
        static bool post_loaded = false;
        if (!is_get && !post_loaded) {
            expected_answer.clear();
            post_loaded = true;
            load("/tmp/answers/phase_2_post.answ");
            load("/tmp/answers/phase_3_get.answ");
            printf("Validator switched to phases 2-3\n");
        }
        
        string method = (is_get ? "GET" : "POST");
        auto it = expected_answer.find(make_pair(method, string(path, path + path_length)));
        
        string request = method + " " + string(path, path + path_length);
        
        if (request == "GET /locations/1081/avg?toDate=813110400&fromAge=17") {
            printf("Known WA, skipping\n");
            return;
        }
        
        printf("answer '%.*s'\n", answer_length, answer);
        if (it == expected_answer.end()) {
            printf("Failed to check answer for '%s'\n", request.c_str());
            abort();
            return;
        }
        
        if (code != it->second.first) {
            printf("Wrong answer code for request '%s': %d, %d expected\n", request.c_str(), code, it->second.first);
            
            if (!(it->second.first == 200 && code == 404))
                abort();
            return;
        }
        
        if (answer == 0) {
            bool nonempty = false;
            for (char c: it->second.second)
                if (!isspace(c)) {
                    nonempty = true;
                    break;
                }
                
            if (nonempty) {
                printf("Wrong answer for request '%s': empty, but nonempty '%s' expected\n", request.c_str(), it->second.second.c_str());
                //abort();
                return;
            }
        }
        else {
            printf("Answers for '%s':\n'%.*s',\n'%s' expected\n", request.c_str(), answer_length, answer, it->second.second.c_str());
            
            Document expected;
            expected.Parse(it->second.second.c_str());
            verify(!expected.HasParseError());
            
            Document answer_doc;
            answer_doc.Parse(string(answer, answer + answer_length).c_str());
            if (answer_doc.HasParseError()) {
                printf("Answer has parse error: %s (%d)\n", GetParseError_En(answer_doc.GetParseError()), (int)answer_doc.GetErrorOffset());
                abort();
            }
            
            verify(expected == answer_doc);
            printf("Correct answer!\n\n");
        }
    }
};

AnswerValidator validator;

void initialize_validator() {
    //validator.load("../data/answers/phase_2_post.answ");
    //validator.load("../data/answers/phase_3_get.answ");
    
    validator.load("/tmp/answers/phase_1_get.answ");
}
#endif

/* Request handling */

// naive implementation

using timestamp = int;

// http://www.unixtimestamp.com/index.php
const timestamp MIN_ALLOWED_BIRTH_DATE = -1262304000;
const timestamp MAX_ALLOWED_BIRTH_DATE = 915235199;

const timestamp MIN_ALLOWED_VISIT_DATE = 946684800;
const timestamp MAX_ALLOWED_VISIT_DATE = 1420156799;

struct DatedVisit {
    int id;
    timestamp visited_at;
    
    bool operator<(const DatedVisit& rhs) const {
        if (visited_at != rhs.visited_at)
            return visited_at < rhs.visited_at;
        else
            return id < rhs.id;
    }
    
    bool operator==(const DatedVisit& rhs) const {
        return id == rhs.id && visited_at == rhs.visited_at;
    }
};

struct User {
    int id = -1;
    string email;
    string first_name, last_name; // json-escaped
    char gender;
    timestamp birth_date;
    
    string json_cache;
    
    void update_cache();
    
    vector<DatedVisit> visits;
};

struct Location {
    int id = -1;
    string place; // json-escaped
    string country; // json-escaped
    string country_unescaped; // original
    string city; // json-escaped
    int distance;
    
    vector<DatedVisit> visits;
};

struct Visit {
    int id = -1;
    int location_id;
    int user_id;
    timestamp visited_at;
    char mark;
};

vector<User> user_by_id;
vector<Location> location_by_id;
vector<Visit> visit_by_id;
unordered_set<string> all_user_emails;

template<class T> void maybe_resize(vector<T>& by_id, int id) {
    if (id >= (int)by_id.size())
        by_id.resize(id + 1);
}

template<class T>
bool id_exists(vector<T>& by_id, int id) {
    if (id < 0 || id >= (int)by_id.size()) return false;
    
    return by_id[id].id == id;
}

enum class Entity : char {
    USERS,
    LOCATIONS,
    VISITS,
    
    INVALID_PATH
};

const char* entity_to_string(Entity e) {
    if (e == Entity::USERS) return "users";
    if (e == Entity::LOCATIONS) return "locations";
    if (e == Entity::VISITS) return "visits";
    if (e == Entity::INVALID_PATH) return "invalid path";
    return "unknown";
}

void reindex_database() {
    int n_users = 0;
    for (int id = 0; id < (int)user_by_id.size(); id++)
        if (user_by_id[id].id == id) {
            user_by_id[id].visits.clear();
            n_users++;
        }
    
    int n_locations = 0;
    for (int id = 0; id < (int)location_by_id.size(); id++)
        if (location_by_id[id].id == id) {
            location_by_id[id].visits.clear();
            n_locations++;
        }
    
    int n_visits = 0;
    for (int id = 0; id < (int)visit_by_id.size(); id++) {
        if (visit_by_id[id].id != id)
            continue;
        
        n_visits++;
        
        Visit& visit = visit_by_id[id];
        
        DatedVisit dv = { visit.id, visit.visited_at };
        user_by_id[visit.user_id].visits.push_back(dv);
        location_by_id[visit.location_id].visits.push_back(dv);
    }
    
    for (int id = 0; id < (int)user_by_id.size(); id++)
        if (user_by_id[id].id == id)
            sort(all(user_by_id[id].visits));
    
    for (int id = 0; id < (int)location_by_id.size(); id++)
        if (location_by_id[id].id == id)
            sort(all(location_by_id[id].visits));

    const int RESERVE = 4000;
    visit_by_id.reserve(visit_by_id.size() + RESERVE);
    location_by_id.reserve(location_by_id.size() + RESERVE);
    user_by_id.reserve(user_by_id.size() + RESERVE);
    
    printf("Database is ready (%d users, %d locations, %d visits)\n", n_users, n_locations, n_visits);
    fflush(stdout);
}

void load_json_dump(char* mutable_buffer) {
    Document document;
    document.ParseInsitu(mutable_buffer);
    
    verify(document.IsObject());
    
    Entity e = Entity::INVALID_PATH;
    for (Value::ConstMemberIterator itr = document.MemberBegin(); itr != document.MemberEnd(); ++itr) {
        string name = itr->name.GetString();
        verify(e == Entity::INVALID_PATH);
        if (name == "users") e = Entity::USERS;
        if (name == "visits") e = Entity::VISITS;
        if (name == "locations") e = Entity::LOCATIONS;
        verify(e != Entity::INVALID_PATH);
    }

    const Value& root = document[entity_to_string(e)];
    verify(root.IsArray());
    
    //printf("loading '%s', %d entries\n", entity_to_string(e), root.Size());
    
    // FIXME: add hints knowing real data size and/or estimations by file size
    if (e == Entity::USERS) {
        user_by_id.reserve(user_by_id.size() + root.Size());
        all_user_emails.max_load_factor(0.25);
        all_user_emails.reserve(all_user_emails.size() + root.Size());
    }
    if (e == Entity::LOCATIONS) location_by_id.reserve(location_by_id.size() + root.Size());
    if (e == Entity::VISITS) visit_by_id.reserve(visit_by_id.size() + root.Size());
    
    for (SizeType i = 0; i < root.Size(); i++) {
        const auto& o = root[i].GetObject();
        int id = o["id"].GetInt();
        
        if (e == Entity::USERS) {
            maybe_resize(user_by_id, id);
            User& new_user = user_by_id[id];
            
            new_user.id = id;
            new_user.email = o["email"].GetString();
            new_user.first_name = json_escape_string(o["first_name"].GetString());
            new_user.last_name = json_escape_string(o["last_name"].GetString());
            new_user.gender = o["gender"].GetString()[0];
            new_user.birth_date = o["birth_date"].GetInt();
            new_user.update_cache();
            all_user_emails.emplace(new_user.email);
            
            verify(new_user.gender == 'm' || new_user.gender == 'f');
        }
        else if (e == Entity::LOCATIONS) {
            maybe_resize(location_by_id, id);
            Location& new_location = location_by_id[id];
            
            new_location.id = id;
            new_location.place = json_escape_string(o["place"].GetString());
            new_location.country_unescaped = o["country"].GetString();
            new_location.country = json_escape_string(new_location.country_unescaped);
            new_location.city = json_escape_string(o["city"].GetString());
            new_location.distance = o["distance"].GetInt();
            
            verify(new_location.distance >= 0);
        }
        else {
            maybe_resize(visit_by_id, id);
            Visit& new_visit = visit_by_id[id];
            
            new_visit.id = id;
            new_visit.location_id = o["location"].GetInt();
            new_visit.user_id = o["user"].GetInt();
            new_visit.visited_at = o["visited_at"].GetInt();
            new_visit.mark = o["mark"].GetInt();
            
            verify(new_visit.mark >= 0 && new_visit.mark <= 5);
        }
    }
}

void load_json_dump_from_file(string file_name) {
    FILE* file = fopen(file_name.c_str(), "r");
    verify(file);
    
    fseek(file, 0, SEEK_END);
    unsigned long long file_size = ftell(file);
    fseek(file, 0, SEEK_SET);
    
    char* buffer = new char[file_size + 1] ();
    verify(fread(buffer, 1, file_size, file) == file_size);
    
    load_json_dump(buffer);
    
    delete[] buffer;
    
    //printf("Data loaded from file '%s'\n", file_name.c_str());
}

Entity get_entity(const char*& path, int path_length) {
    // 1 + 5 + 1 = /users/ = 7
    if (path_length < 7) return Entity::INVALID_PATH;
    
    if (path[1] == 'u') {
        if (memcmp(path, "/users/", 7))
            return Entity::INVALID_PATH;
        
        path += 7;
        return Entity::USERS;
    }
    
    if (path[1] == 'l' && path_length >= 11) {
        if (memcmp(path, "/locations/", 11))
            return Entity::INVALID_PATH;
        
        path += 11;
        return Entity::LOCATIONS;
    }
    
    if (path_length < 8 || memcmp(path, "/visits/", 8))
        return Entity::INVALID_PATH;
    
    path += 8;
    return Entity::VISITS;
}

inline int fast_atoi(const char*& str, bool& correct) {
    int val = 0;
    bool minus = false;
    if (*str == '-') {
        minus = true;
        str++;
    }
    while (*str >= '0' && *str <= '9') {
        val = val*10 + (*str++ - '0');
        correct = true;
    }
    return (minus ? -val : val);
}

#define fast_is(begin, end, str) ((end - begin == (sizeof str - 1)) && !memcmp(begin, str, sizeof str - 1))

const timestamp MAGIC_TIMESTAMP = std::numeric_limits<int>::min() + 1234;
const int MAGIC_INTEGER = std::numeric_limits<int>::min() + 1234;

bool parse_timestamp(const char* str, const char* to, timestamp& value) {
    bool correct = false;
    value = fast_atoi(str, correct);
    return correct && str == to;
}

const int MAX_RESPONSE_SIZE = 4096 * 2;
char shared_buffer[MAX_RESPONSE_SIZE];

const int MAX_INTEGER_SIZE = 30;
char itoa_buffer[MAX_INTEGER_SIZE];
int itoa_length;

void itoa_unsafe(int x) {
    itoa_length = 0;
    if (x < 0) {
        check(x != numeric_limits<int>::min());
        x = -x; itoa_buffer[itoa_length++] = '-';
    }
    
    int begin = itoa_length;
    while (x) {
        itoa_buffer[itoa_length++] = (x % 10) + '0';
        x /= 10;
    }
    
    if (itoa_length == begin)
        itoa_buffer[itoa_length++] = '0';
    
    char* a = itoa_buffer + begin;
    char* b = itoa_buffer + itoa_length - 1;
    
    while (a < b) {
        swap(*a++, *b--);
    }
}

struct ResponseBuilder {
    char* buffer_begin;
    char* buffer_pos;
    char* buffer_end;
    
    ResponseBuilder() {
        buffer_begin = buffer_pos = shared_buffer;
        buffer_end = shared_buffer + MAX_RESPONSE_SIZE;
    }
    
    void discard_old() {
        if (buffer_begin != shared_buffer)
            delete[] buffer_begin;
    }
    
    void realloc_if_needed(int length) {
        while (buffer_pos + length >= buffer_end) {
            int double_length = (buffer_end - buffer_begin) * 2;
            
            char* new_buffer = new char[double_length];
            memcpy(new_buffer, buffer_begin, buffer_end - buffer_begin);
            buffer_pos = new_buffer + (buffer_pos - buffer_begin);
            discard_old();
            
            buffer_begin = new_buffer;
            buffer_end = buffer_begin + double_length;
        }
    }
    
    void append(const char* data, int length) {
        realloc_if_needed(length);
        memcpy(buffer_pos, data, length);
        buffer_pos += length;
    }
    
    void append_int(int x) {
        itoa_unsafe(x);
        append(itoa_buffer, itoa_length);
    }
    
    void embed_content_length(int length_offset, int content_offset) {
        char* content_begin = buffer_begin + content_offset;
        int content_length = buffer_pos - content_begin;
        itoa_unsafe(content_length);
        memcpy(buffer_begin + length_offset, itoa_buffer, itoa_length);
    }
    
    void write(int fd) {
        profiler.begin(WRITE_RESPONSE);
        //li t0 = get_ns_timestamp();
        ::write(fd, buffer_begin, buffer_pos - buffer_begin);
        //li t1 = get_ns_timestamp();
        //printf("write call taken %.3f mks\n", (t1 - t0) / 1000.0);
        //close(fd);
        profiler.end(WRITE_RESPONSE);
    }
};

const char* percent_decode(const char* pos, const char* end) {
    char* rewrite_pos = (char*)pos;
    
    while (pos < end) {
        if (*pos == '%') {
            if (pos + 3 > end)
                return 0;
            
            char c = 0;
            for (int t = 0; t < 2; t++) {
                pos++;
                c <<= 4;
                
                char now = *pos;
                if (now >= '0' && now <= '9') c += now - '0';
                else if (now >= 'a' && now <= 'f') c += now - 'a' + 10;
                else if (now >= 'A' && now <= 'F') c += now - 'A' + 10;
                else {
                    return 0;
                }
            }
            
            *rewrite_pos++ = c;
            pos++;
        }
        else {
            char c = *pos++;
            *rewrite_pos++ = (c == '+' ? ' ' : c);
        }
    }
    
    return rewrite_pos;
}

int utf8_string_length(const char* s, int character_length) {
    int len = 0;
    while (character_length--) len += (*s++ & 0xc0) != 0x80;
    return len;
}

#define append_str(builder, str) builder.append(str, sizeof str - 1)

void User::update_cache() {
    json_cache = "";
    
    ResponseBuilder json;
    append_str(json, "{\"id\":");
    json.append_int(id);
    append_str(json, ",\"email\":\"");
    json.append(email.data(), email.length());
    append_str(json, "\",\"first_name\":");
    json.append(first_name.data(), first_name.length());
    append_str(json, ",\"last_name\":");
    json.append(last_name.data(), last_name.length());
    append_str(json, ",\"gender\":\"");
    json.append(&gender, 1);
    append_str(json, "\",\"birth_date\":");
    json.append_int(birth_date);
    append_str(json, "}");
    
    json_cache = string(json.buffer_begin, json.buffer_pos);
}

struct RequestHandler {
    bool is_get;
    Entity entity;
    int id;
    
    bool get_visits = false, get_avg = false;
    bool is_new = false;
    
    timestamp from_date = MAGIC_TIMESTAMP, to_date = MAGIC_TIMESTAMP;
    int from_age = MAGIC_INTEGER, to_age = MAGIC_INTEGER;
    char gender = '*';
    const char* country_ptr_begin = 0, *country_ptr_end = 0;
    int to_distance = MAGIC_INTEGER;
    
    int process_option(const char* opt_key_begin, const char* opt_key_end, const char* opt_value_begin, const char* opt_value_end) {
        //printf("%.*s = %.*s\n", (int)(opt_key_end - opt_key_begin), opt_key_begin, (int)(opt_value_end - opt_value_begin), opt_value_begin);
        //printf("%d %d %d\n", is_get, get_visits, get_avg);
        
        if (fast_is(opt_key_begin, opt_key_end, "query_id")) return 200;
        if (!is_get || !(get_visits || get_avg)) return 400;
        
#define timestamp_field(field_str, field_variable) \
        if (fast_is(opt_key_begin, opt_key_end, field_str)) { \
            if (field_variable != MAGIC_TIMESTAMP) return 400; \
            if (!parse_timestamp(opt_value_begin, opt_value_end, field_variable)) \
                return 400; \
            return 200; \
        }
        
        timestamp_field("fromDate", from_date);
        timestamp_field("toDate", to_date);
        
#undef timestamp_field
        
#define integer_field(field_str, field_variable) \
        if (fast_is(opt_key_begin, opt_key_end, field_str)) { \
            if (field_variable != MAGIC_INTEGER) return 400; \
            bool correct = false; \
            field_variable = fast_atoi(opt_value_begin, correct); \
            if (!correct || opt_value_begin != opt_value_end) \
                return 400; \
            return 200; \
        }
        
        if (get_visits) {
            integer_field("toDistance", to_distance);
            
            if (fast_is(opt_key_begin, opt_key_end, "country")) {
                if (country_ptr_begin) return 400;
                country_ptr_begin = opt_value_begin;
                country_ptr_end = opt_value_end;
                return 200;
            }
        }
        else {
            check(get_avg);
            
            integer_field("fromAge", from_age);
            integer_field("toAge", to_age);
            
            if (fast_is(opt_key_begin, opt_key_end, "gender")) {
                if (opt_value_end != opt_value_begin + 1) return 400;
                if (gender != '*') return 400;
                
                gender = *opt_value_begin;
                if (gender != 'm' && gender != 'f') return 400;
                
                return 200;
            }
        }
        
#undef integer_field
        
        return 400;
    }
    
#define header_content_length_tbd "Content-Length: 0      \r\n"
#define zero_offset_string header_200 header_connection_close header_host header_server "Content-Length:"
#define HTTP_OK_PREFIX header_200 header_connection_close header_host header_server header_content_length_tbd header_rn

#ifndef DISABLE_VALIDATE
#define validate_json() \
        char* data_ptr = json.buffer_begin + sizeof HTTP_OK_PREFIX - 1; \
        validator.supply_data(data_ptr, json.buffer_pos - data_ptr)
#else
#define validate_json()
#endif

#define begin_response() \
    ResponseBuilder json; \
    VisibilityProfiler build_json_response_prof(BUILD_JSON_RESPONSE);

#define send_response() \
    json.embed_content_length(sizeof zero_offset_string, sizeof HTTP_OK_PREFIX - 1); \
    build_json_response_prof.end(); \
    json.write(fd); \
    validate_json()
    
    int handle_get(int fd) {
        if (entity == Entity::USERS) {
            if (!id_exists(user_by_id, id)) return 404;
            User& user = user_by_id[id];
            
            if (!get_visits) {
                // simple
                
                begin_response();
                
#if 0
                append_str(json, HTTP_OK_PREFIX "{\"id\":");
                json.append_int(user.id);
                append_str(json, ",\"email\":\"");
                json.append(user.email.data(), user.email.length());
                append_str(json, "\",\"first_name\":");
                json.append(user.first_name.data(), user.first_name.length());
                append_str(json, ",\"last_name\":");
                json.append(user.last_name.data(), user.last_name.length());
                append_str(json, ",\"gender\":\"");
                json.append(&user.gender, 1);
                append_str(json, "\",\"birth_date\":");
                json.append_int(user.birth_date);
                append_str(json, "}");
#else
                append_str(json, HTTP_OK_PREFIX);
                json.append(user.json_cache.data(), user.json_cache.length());
#endif
                
                send_response();
                return 200;
            }
            else {
                // visits query
                
                begin_response();
                
                append_str(json, HTTP_OK_PREFIX "{\"visits\":[");
                int n_visits = 0;
                
                auto it = user.visits.begin();
                if (from_date != MAGIC_TIMESTAMP)
                    it = lower_bound(all(user.visits), DatedVisit { -1, from_date + 1 });
                
                auto it_end = user.visits.end();
                if (to_date != MAGIC_TIMESTAMP)
                    it_end = lower_bound(all(user.visits), DatedVisit{ -1, to_date });
                
                if (country_ptr_begin) {
                    // decode percent-encoded in-place
                    country_ptr_end = percent_decode(country_ptr_begin, country_ptr_end);
                    //printf("decoded country as '%.*s'\n", (int)(country_ptr_end - country_ptr_begin), country_ptr_begin);
                    if (!country_ptr_end)
                        return 400;
                }
                
                //li startb = get_ns_timestamp() - global_t_ready;
                while (it < it_end) {
                    Visit& visit = visit_by_id[it->id];
                    it++;
                    Location& location = location_by_id[visit.location_id];
                    
                    if (country_ptr_begin) {
                        if (country_ptr_end - country_ptr_begin != (long)location.country_unescaped.length() || memcmp(country_ptr_begin, location.country_unescaped.data(), location.country_unescaped.length()))
                            continue;
                    }
                    
                    if (to_distance != MAGIC_INTEGER) {
                        if (location.distance >= to_distance)
                            continue;
                    }
                    
                    if (n_visits) {
                        append_str(json, ",{\"mark\":");
                    }
                    else {
                        append_str(json, "{\"mark\":");
                    }
                    n_visits++;
                    
                    json.append_int(visit.mark);
                    append_str(json, ",\"visited_at\":");
                    json.append_int(visit.visited_at);
                    append_str(json, ",\"place\":");
                    json.append(location.place.data(), location.place.length());
                    append_str(json, "}");
                }
                
                append_str(json, "]}");
                
                //li endb = get_ns_timestamp() - global_t_ready;
                //printf("timings %.3f %.3f\n", startb / 1000.0, endb / 1000.0);
                send_response();
                return 200;
            }
        }
        else if (entity == Entity::LOCATIONS) {
            if (!id_exists(location_by_id, id)) return 404;
            Location& location = location_by_id[id];
            
            if (!get_avg) {
                // simple
                
                begin_response();
                
                append_str(json, HTTP_OK_PREFIX "{\"id\":");
                json.append_int(location.id);
                append_str(json, ",\"place\":");
                json.append(location.place.data(), location.place.length());
                append_str(json, ",\"country\":");
                json.append(location.country.data(), location.country.length());
                append_str(json, ",\"city\":");
                json.append(location.city.data(), location.city.length());
                append_str(json, ",\"distance\":");
                json.append_int(location.distance);
                append_str(json, "}");
                
                send_response();
                return 200;
            }
            else {
                // average query
                
                begin_response();
                
                append_str(json, HTTP_OK_PREFIX "{\"avg\":");
                
                auto it = location.visits.begin();
                if (from_date != MAGIC_TIMESTAMP)
                    it = lower_bound(all(location.visits), DatedVisit { -1, from_date + 1 });
                
                auto it_end = location.visits.end();
                if (to_date != MAGIC_TIMESTAMP)
                    it_end = lower_bound(all(location.visits), DatedVisit{ -1, to_date });
                
                // timestamp jerk
                
                time_t now = {};
                tm time_struct = {};
                
                bool filter_age = false;
                if (from_age != MAGIC_INTEGER || to_age != MAGIC_INTEGER) {
                    //now = time(0);
                    now = current_timestamp;
                    gmtime_r(&now, &time_struct);
                    filter_age = true;
                }
                
                timestamp from_filter = numeric_limits<int>::min(), to_filter = numeric_limits<int>::max();
                
                if (from_age != MAGIC_INTEGER) {
                    int orig_year = time_struct.tm_year;
                    time_struct.tm_year -= from_age;
                    time_struct.tm_year = max(time_struct.tm_year, 2);
                    //printf("need %d\n", time_struct.tm_year);
                    to_filter = min((time_t)to_filter, mktime(&time_struct) - 1);
                    time_struct.tm_year = orig_year;
                }
                
                if (to_age != MAGIC_INTEGER) {
                    time_struct.tm_year -= to_age;
                    time_struct.tm_year = max(time_struct.tm_year, 2);
                    from_filter = max((time_t)from_filter, mktime(&time_struct) + 1);
                }
                
#if 0
                time_t now_li = now, from_li = from_filter, to_li = to_filter;
                printf("filters %d %d\n", from_filter, to_filter);
                gmtime_r(&now_li, &time_struct);
                printf("now year %d\n", time_struct.tm_year);
                gmtime_r(&from_li, &time_struct);
                printf("from year %d\n", time_struct.tm_year);
                gmtime_r(&to_li, &time_struct);
                printf("to year %d\n", time_struct.tm_year);
#endif
                
                int n_marks = 0, mark_sum = 0;
                while (it < it_end) {
                    Visit& visit = visit_by_id[it->id];
                    it++;
                    
                    if (filter_age) {
                        User& user = user_by_id[visit.user_id];
                        if (!(user.birth_date >= from_filter && user.birth_date <= to_filter)) {
                            continue;
                        }
                        
                        if (gender != '*' && gender != user.gender) continue;
                    }
                    else if (gender != '*') {
                        User& user = user_by_id[visit.user_id];
                        if (user.gender != gender) continue;
                    }
                    
                    n_marks++;
                    mark_sum += visit.mark;
                }
                
                if (mark_sum == 0) {
                    append_str(json, "0}");
                }
                else {
                    static char mark_avg[50];
                    sprintf(mark_avg, "%.5f}", mark_sum / (double)n_marks + 1e-12);
                    json.append(mark_avg, strlen(mark_avg));
                }
                
                send_response();
                return 200;
            }
        }
        else {
            if (!id_exists(visit_by_id, id)) return 404;
            Visit& visit = visit_by_id[id];
            
            begin_response();
            append_str(json, HTTP_OK_PREFIX "{\"id\":");
            json.append_int(visit.id);
            append_str(json, ",\"location\":");
            json.append_int(visit.location_id);
            append_str(json, ",\"user\":");
            json.append_int(visit.user_id);
            append_str(json, ",\"visited_at\":");
            json.append_int(visit.visited_at);
            append_str(json, ",\"mark\":");
            json.append_int(visit.mark);
            append_str(json, "}");
            
            send_response();
            return 200;
        }
        
        printf("VERY STRANGE -- failed to answer GET\n"); fflush(stdout);
        return 400;
    }
    
    void performance_test(int mode) {
        const int N_TRIES = 10;
        li hash = 0;
        
        vector<int> ids;
        for (int id = 0; id < (int)user_by_id.size(); id++) {
            if (user_by_id[id].id != id) continue;
            
            ids.push_back(id);
            if (ids.size() > 10000) break;
        }
        
        //for (int id: ids)
        //    if (id != 1)
        //        user_by_id[id].visits = user_by_id[1].visits;
        
        li t0 = get_ns_timestamp();
        srand(0);
        for (int tries = 0; tries < N_TRIES; tries++) {
            ResponseBuilder json;
            
            User& user = user_by_id[1];
            
            append_str(json, HTTP_OK_PREFIX "{\"visits\":[");
            
            auto it = user.visits.begin();
            auto it_end = user.visits.end();
            
            // mode 0: 2410.195 mks/query, hash 0
            // mode 1: 4687.358 mks/query, hash 63
            // mode 2: 2835.726 mks/query, hash 71
            
            if (mode == 0) {
#if 1
                int n = it_end - it;
                if (n < 0) n = 0;
                
                for (int i = 0; i < n; i++) {
                    static char str[] = "{\"mark\":3,\"visited_at\":1403153768,\"place\":\"averageaverage\"},";
                    for (int t = 0; t < sizeof(str); t += 4)
                        json.append("xxxx", 4);
                    //json.append(str, sizeof(str) - 1);
                }
#endif
            }
            else if (mode == 1) {
                int n_visits = 0;
                while (it < it_end) {
                    Visit& visit = visit_by_id[it->id];
                    it++;
                    Location& location = location_by_id[visit.location_id];
                    
                    if (country_ptr_begin) {
                        if (country_ptr_end - country_ptr_begin != (long)location.country.length() || memcmp(country_ptr_begin, location.country.data(), location.country.length()))
                            continue;
                    }
                    
                    if (to_distance != MAGIC_INTEGER) {
                        if (location.distance >= to_distance)
                            continue;
                    }
                    
                    if (n_visits) {
                        append_str(json, ",{\"mark\":");
                    }
                    else {
                        append_str(json, "{\"mark\":");
                    }
                    n_visits++;
                    
                    json.append_int(visit.mark);
                    append_str(json,",\"visited_at\":");
                    json.append_int(visit.visited_at);
                    append_str(json,",\"place\":\"");
                    json.append(location.place.data(), location.place.length());
                    append_str(json,"\"}");
                }
            }
            else if (mode == 2) {
                int n_visits = 0;
                while (it < it_end) {
                    //Visit& visit = visit_by_id[it->id];
                    it++;
                    //Location& location = location_by_id[visit.location_id];
                    
                    if (country_ptr_begin) {
                        //if (country_ptr_end - country_ptr_begin != (long)location.country.length() || memcmp(country_ptr_begin, location.country.data(), location.country.length()))
                        //    continue;
                    }
                    
                    if (to_distance != MAGIC_INTEGER) {
                        //if (location.distance >= to_distance)
                        //    continue;
                    }
                    
                    if (n_visits) {
                        append_str(json, ",{\"mark\":");
                    }
                    else {
                        append_str(json, "{\"mark\":");
                    }
                    n_visits++;
                    
                    json.append_int(100500);
                    append_str(json,",\"visited_at\":");
                    json.append_int(100600);
                    append_str(json,",\"place\":\"");
                    json.append("averageaverage", 14);
                    append_str(json,"\"}");
                }
            }
            append_str(json, "]}");
            
            for (auto x = json.buffer_begin; x < json.buffer_pos; x++)
                hash ^= *x;
        }
        li t1 = get_ns_timestamp();
        
        printf("mode %d: %.3f mks/query, hash %lld\n", mode, (t1 - t0) / (double)N_TRIES / 1000.0, hash);
    }
    
    
    int handle_post(int fd, const char* body) {
#define header_content_length_four "Content-Length: 2\r\n"
//#define header_content_type "Content-Type: application/json\r\n"
#define header_content_type
#define HTTP_OK_WITH_EMPTY_JSON_RESPONSE header_200 header_connection_close header_server header_host header_content_type header_content_length_four header_rn "{}"
//printf("ok json '%s'\n", HTTP_OK_WITH_EMPTY_JSON_RESPONSE);
//#define HTTP_OK_WITH_EMPTY_JSON_RESPONSE "HTTP/1.1 200 OK\r\n\r\n{}"
#define successful_update() \
        profiler.begin(WRITE_RESPONSE); \
        write(fd, HTTP_OK_WITH_EMPTY_JSON_RESPONSE, sizeof(HTTP_OK_WITH_EMPTY_JSON_RESPONSE) - 1); \
        profiler.end(WRITE_RESPONSE)
        //close(fd);
        
        //successful_update();
        //return 200;
        
#define declare_doc() \
        profiler.begin(PARSE_JSON_RAPIDJSON); \
        Document json; \
        json.ParseInsitu((char*)body); \
        profiler.end(PARSE_JSON_RAPIDJSON); \
        if (json.HasParseError()) return 400;
        
#define begin_parsing_update(EntityType, entity) \
            EntityType* entity = nullptr; \
            if (!is_new) { \
                if (!id_exists((entity ## _by_id), id)) return 404; \
                entity = &((entity ## _by_id)[id]); \
            } \
            else { \
                id = MAGIC_INTEGER; \
            } \
            declare_doc();
            
#define iterate_over_fields \
    VisibilityProfiler parse_json_custom_prof(PARSE_JSON_CUSTOM); \
    for (Value::ConstMemberIterator it = json.MemberBegin(); it != json.MemberEnd(); ++it) { \
                    const char* name = it->name.GetString(); \
                    const char* name_end = name + it->name.GetStringLength();

#define end_fields_iteration \
    return 400; } \
    parse_json_custom_prof.end();
                
#define string_field(field_name, min_length, max_length, save_to, code) \
                {if (fast_is(name, name_end, field_name)) { \
                    if (!it->value.IsString()) return 400; \
                    if (save_to) return 400; \
                    save_to = it->value.GetString(); \
                    int real_length = utf8_string_length(save_to, it->value.GetStringLength()); \
                    if (!(real_length >= min_length && real_length <= max_length)) return 400; \
                    code \
                    continue; \
                }}
                
                
#define integer_field(field_name, save_to, code) \
                {if (fast_is(name, name_end, field_name)) { \
                    if (!it->value.IsInt()) return 400; \
                    if (save_to != MAGIC_INTEGER) return 400; \
                    save_to = it->value.GetInt(); \
                    code \
                    continue; \
                }}

        if (entity == Entity::USERS) {
            begin_parsing_update(User, user);
            
            const char* email = 0;
            const char* first_name = 0;
            const char* last_name = 0;
            const char* gender = 0;
            timestamp birth_date = MAGIC_INTEGER;
                
            iterate_over_fields {
                if (is_new)
                    integer_field("id", id, { if (id_exists(user_by_id, id)) return 400; });
                
                string_field("first_name", 1, 50, first_name, {});
                string_field("last_name", 1, 50, last_name, {});
                string_field("email", 1, 100, email, {});
                string_field("gender", 1, 1, gender, { if (*gender != 'm' && *gender != 'f') return 400; });
            
                integer_field("birth_date", birth_date, { if (!(birth_date >= MIN_ALLOWED_BIRTH_DATE && birth_date <= MAX_ALLOWED_BIRTH_DATE)) return 400; });
            } end_fields_iteration
                
            if (is_new) {
                if (!(email && first_name && last_name && gender && birth_date != MAGIC_INTEGER && id != MAGIC_INTEGER))
                    return 400;
                
                if (all_user_emails.find(email) != all_user_emails.end())
                    return 400;
                
                successful_update();
                
                maybe_resize(user_by_id, id);
                User& new_user = user_by_id[id];
                new_user.id = id;
                new_user.email = email;
                new_user.first_name = json_escape_string(first_name);
                new_user.last_name = json_escape_string(last_name);
                new_user.gender = *gender;
                new_user.birth_date = birth_date;
                new_user.update_cache();
                all_user_emails.emplace(email);
                
                return 200;
            }
            else {
                if (email) {
                    if (all_user_emails.find(email) != all_user_emails.end() && user->email != email)
                        return 400;
                }
                
                successful_update();
                
                if (email) {
                    all_user_emails.erase(user->email);
                    user->email = email;
                    all_user_emails.emplace(user->email);
                }
                
                if (first_name) user->first_name = json_escape_string(first_name);
                if (last_name) user->last_name = json_escape_string(last_name);
                if (gender) user->gender = *gender;
                if (birth_date != MAGIC_INTEGER) user->birth_date = birth_date;
                
                user->update_cache();
                
                return 200;
            }
        }
        else if (entity == Entity::LOCATIONS) {
            begin_parsing_update(Location, location);
            
            const char* place = 0;
            const char* country = 0;
            const char* city = 0;
            int distance = MAGIC_INTEGER;
            
            iterate_over_fields {
                if (is_new)
                    integer_field("id", id, { if (id_exists(location_by_id, id)) return 400; });
                
                string_field("place", 0, (int)1e9, place, {});
                string_field("country", 1, 50, country, {});
                string_field("city", 1, 50, city, {});
                integer_field("distance", distance, { if (distance < 0) return 400; });
            } end_fields_iteration
            
            if (is_new) {
                if (!(place && country && city && distance != MAGIC_INTEGER && id != MAGIC_INTEGER))
                    return 400;
                
                successful_update();
                
                maybe_resize(location_by_id, id);
                Location& new_location = location_by_id[id];
                new_location.id = id;
                new_location.place = json_escape_string(place);
                new_location.country_unescaped = country;
                new_location.country = json_escape_string(new_location.country_unescaped);
                new_location.city = json_escape_string(city);
                new_location.distance = distance;
                
                return 200;
            }
            else {
                successful_update();
                
                if (place) {
                    location->place = json_escape_string(place);
                }
                
                if (country) {
                    location->country_unescaped = country;
                    location->country = json_escape_string(location->country_unescaped);
                }
                
                if (city) {
                    location->city = json_escape_string(city);
                }
                
                if (distance != MAGIC_INTEGER) location->distance = distance;
                
                return 200;
            }
        }
        else {
            begin_parsing_update(Visit, visit);
            
            Location* location = nullptr;
            User* user = nullptr;
            
            int location_id = MAGIC_INTEGER;
            int user_id = MAGIC_INTEGER;
            timestamp visited_at = MAGIC_INTEGER;
            int mark = MAGIC_INTEGER;
            
            iterate_over_fields {
                if (is_new)
                    integer_field("id", id, { if (id_exists(visit_by_id, id)) return 400; });
                
                integer_field("location", location_id, {
                    if (!id_exists(location_by_id, location_id))
                        return 404;
                    
                    location = &location_by_id[location_id];
                });
                
                integer_field("user", user_id, {
                    if (!id_exists(user_by_id, user_id))
                        return 404;
                    
                    user = &user_by_id[user_id];
                });
                
                integer_field("visited_at", visited_at, { if (!(visited_at >= MIN_ALLOWED_VISIT_DATE && visited_at <= MAX_ALLOWED_VISIT_DATE)) return 400; });
                integer_field("mark", mark, { if (!(mark >= 0 && mark <= 5)) return 400; });
            } end_fields_iteration
            
            if (is_new) {
                if (!(location && user && visited_at != MAGIC_INTEGER && mark != MAGIC_INTEGER && id != MAGIC_INTEGER))
                    return 400;
                
                successful_update();
                
                maybe_resize(visit_by_id, id);
                Visit& new_visit = visit_by_id[id];
                new_visit.id = id;
                new_visit.visited_at = visited_at;
                new_visit.mark = mark;
                new_visit.user_id = user_id;
                new_visit.location_id = location_id;
                
                DatedVisit dv = { new_visit.id, new_visit.visited_at };
                
                user->visits.insert(upper_bound(all(user->visits), dv), dv);
                location->visits.insert(upper_bound(all(location->visits), dv), dv);
                
                return 200;
            }
            else {
                successful_update();
                
                if (mark != MAGIC_INTEGER) visit->mark = mark;
                
                // hard updates
                
                if (user_id == visit->user_id) { user_id = MAGIC_INTEGER; user = nullptr; }
                if (location_id == visit->location_id) { location_id = MAGIC_INTEGER; location = nullptr; }
                
                timestamp new_visited_at = visit->visited_at;
                if (visited_at != MAGIC_INTEGER) new_visited_at = visited_at;
                bool visit_time_changed = new_visited_at != visit->visited_at;
                
                if (user || visit_time_changed) {
                    User* old_user = &user_by_id[visit->user_id];
                    
                    auto it = lower_bound(all(old_user->visits), DatedVisit { id, visit->visited_at });
                    check(it != old_user->visits.end());
                    old_user->visits.erase(it);
                    
                    User* new_user = user;
                    if (!new_user)
                        new_user = old_user;
                    
                    DatedVisit dv = { id, new_visited_at };
                    new_user->visits.insert(upper_bound(all(new_user->visits), dv), dv);
                }
                
                if (location || visit_time_changed) {
                    Location* old_location = &location_by_id[visit->location_id];
                    
                    auto it = lower_bound(all(old_location->visits), DatedVisit { id, visit->visited_at });
                    check(it != old_location->visits.end());
                    old_location->visits.erase(it);
                    
                    Location* new_location = location;
                    if (!new_location)
                        new_location = old_location;
                    
                    DatedVisit dv = { id, new_visited_at };
                    new_location->visits.insert(upper_bound(all(new_location->visits), dv), dv);
                }
                
                visit->visited_at = new_visited_at;
                if (location) visit->location_id = location_id;
                if (user) visit->user_id = user_id;
                
                return 200;
            }
        }
        
        printf("VERY STRANGE -- failed to answer POST\n"); fflush(stdout);
        return 400;
    }
    
#undef HTTP_OK_PREFIX
#undef validate_json
};

void do_benchmark() {
#if 0
    RequestHandler handler;
    for (int mode = 0; mode < 3; mode++)
        handler.performance_test(mode);
#endif
}

int process_request_options(RequestHandler& handler, const char* path, const char* path_end) {
    // optional parameters
    if (path < path_end && *path == '?') {
        path++;
        
        // string of params
        const char* last = path;
        const char* equ_pos = 0;
        for (const char* x = path; ; x++) {
            if (*x == '&' || x == path_end) {
                // process range [last, x)
                if (!equ_pos || equ_pos == last || equ_pos + 1 == x)
                    return 404;
                
                int code = handler.process_option(last, equ_pos, equ_pos + 1, x);
                if (code != 200)
                    return code;
                
                equ_pos = 0;
                last = x + 1;
                if (x == path_end) break;
            }
            else if (*x == '=') {
                if (equ_pos)
                    return 404;
                
                equ_pos = x;
            }
        }
    }
    else if (path != path_end)
        return 404;
    
    return 200;
}

// omg boilerplate shit
int process_get_request_raw(int fd, const char* path, int path_length) {
    //printf("GET %.*s\n", path_length, path);
    VisibilityProfiler parse_path_prof(PARSE_PATH);
    
    RequestHandler handler;
    handler.is_get = true;
    
    const char* path_end = path + path_length;
    handler.entity = get_entity(path, path_length);
    if (handler.entity == Entity::INVALID_PATH) return 404;
    
    bool correct = false;
    handler.id = fast_atoi(path, correct);
    if (path > path_end || !correct) return 404;
    
    if (*path == '/') {
        path++;
        
        if (*path == 'v') {
            if (handler.entity != Entity::USERS || path + 6 > path_end || memcmp(path, "visits", 6)) return 404;
            path += 6;
            
            handler.get_visits = true;
        }
        else if (*path == 'a') {
            if (handler.entity != Entity::LOCATIONS || path + 3 > path_end || memcmp(path, "avg", 3)) return 404;
            path += 3;
            
            handler.get_avg = true;
        }
        else
            return 404;
    }
    
    //printf("entity %s id %d\n", entity_to_string(handler.entity), handler.id);
    
    int opts_code = process_request_options(handler, path, path_end);
    if (opts_code != 200)
        return opts_code;
    
    parse_path_prof.end();
    int code = handler.handle_get(fd);
    return code;
}

#if 0
const li LONG_REQUEST_NS = 250 * (li)1000; // 500 mks is long
#else
const li LONG_REQUEST_NS = 400 * (li)1000;
#endif

int process_get_request(int fd, const char* path, int path_length) {
    global_last_request_is_get = true;
    
#ifndef DISABLE_VALIDATE
    string path_backup(path, path + path_length);
#endif
    int code = process_get_request_raw(fd, path, path_length);
    li t_answered = get_ns_timestamp();
    if (t_answered - global_t_ready > LONG_REQUEST_NS) {
        printf("long GET %.*s (%d): %.3f mks\n", path_length, path, code, (t_answered - global_t_ready) / 1000.0);
    }
    max_request_ns = max(max_request_ns, t_answered - global_t_ready);
#ifndef DISABLE_VALIDATE
    validator.check_answer(true, path_backup.c_str(), path_length, code, 0, 0);
#endif
    return code;
}

int process_post_request_raw(int fd, const char* path, int path_length, const char* body) {
    //printf("POST %.*s\n'%s'\n", path_length, path, body);
    //printf("POST %.*s '%s'\n", path_length, path, body);
    //fflush(stdout);
    VisibilityProfiler parse_path_prof(PARSE_PATH);
    
    RequestHandler handler;
    handler.is_get = false;
    
    const char* path_end = path + path_length;
    handler.entity = get_entity(path, path_length);
    if (handler.entity == Entity::INVALID_PATH) return 404;
    
    if (path + 3 <= path_end && *path == 'n' && !memcmp(path, "new", 3)) {
        handler.is_new = true;
        handler.id = -1;
        path += 3;
    }
    else {
        bool correct = false;
        handler.id = fast_atoi(path, correct);
        if (path > path_end || !correct) return 404;
    }
    
    int opts_code = process_request_options(handler, path, path_end);
    if (opts_code != 200)
        return opts_code;
    
    parse_path_prof.end();
    int code = handler.handle_post(fd, body);
    return code;
}

int process_post_request(int fd, const char* path, int path_length, const char* body) {
    global_last_request_is_get = false;
    
    int code = process_post_request_raw(fd, path, path_length, body);
    li t_answered = get_ns_timestamp();
    if (t_answered - global_t_ready > LONG_REQUEST_NS) {
        printf("long POST %.*s '%s' (%d): %.3f mks\n", path_length, path, body, code, (t_answered - global_t_ready) / 1000.0);
    }
    max_request_ns = max(max_request_ns, t_answered - global_t_ready);
    
#ifndef DISABLE_VALIDATE
    validator.check_answer(false, path, path_length, code, 0, 0);
#endif
    return code;
}

