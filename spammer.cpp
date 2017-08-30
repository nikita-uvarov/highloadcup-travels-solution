#include <bits/stdc++.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/mman.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sched.h>
#include <sys/syscall.h>
#include <malloc.h>

using namespace std;

using li = long long;

int N_THREADS = 2000;

sockaddr_in addr = {};
vector<int> sockets;

li timespec_to_li(const timespec& ts) {
    return ts.tv_sec * (li)1e9 + ts.tv_nsec;
}

li get_ns_timestamp() {
    timespec ts = {};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return timespec_to_li(ts);
}

mutex total_mutex;
li total = 0;

void connector_thread(int thread_index) {
    usleep(100000);
    li t0 = get_ns_timestamp();
    int ec = connect(sockets[thread_index], (sockaddr*)&addr, sizeof addr);
    if (ec != 0) {
        perror("connect");
    }
    assert(ec == 0);
    li t1 = get_ns_timestamp();
    
    unique_lock<mutex> lock(total_mutex);
    total += (t1 - t0);
    lock.unlock();
    
    usleep(100000);
    close(sockets[thread_index]);
    
    //printf("connect took %.3f mks\n", (t1 - t0) / 1e3);
}


int main(int argc, char** argv) {
    assert(argc == 2);
    assert(sscanf(argv[1], "%d", &N_THREADS) == 1);
    
    int port = 80;
    bzero((char *) &addr, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("0.0.0.0");
    addr.sin_port = htons(port);
    
    sockets.resize(N_THREADS);
    
    for (int i = 0; i < N_THREADS; i++) {
        sockets[i] = socket(AF_INET, SOCK_STREAM, 0);
        if (sockets[i] == -1)
            perror("socket");
        assert(sockets[i] >= 0);
    }
    
    vector<thread> threads(N_THREADS);
    for (int i = 0; i < N_THREADS; i++)
        threads[i] = thread(connector_thread, i);
    
    for (int i = 0; i < N_THREADS; i++)
        threads[i].join();
    
    printf("%.3f mks / connect avg, took %.3f s\n", total / 1e3 / N_THREADS, total / 1e9); fflush(stdout);
    fprintf(stderr, "%.6f\n", total / 1e3 / N_THREADS); fflush(stderr);
    
    return 0;
}
