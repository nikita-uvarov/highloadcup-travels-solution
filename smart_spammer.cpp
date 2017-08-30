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

int main(int argc, char** argv) {
    assert(argc == 3);
    int max_tries = 0, n_threads = 0;
    assert(sscanf(argv[1], "%d", &max_tries) == 1);
    assert(sscanf(argv[2], "%d", &n_threads) == 1);
    printf("Spammer started, maximum %d times in %d threads\n", max_tries, n_threads); fflush(stdout);
    
    for (int i = 0; i < max_tries; i++) {
        int ec = system((string("./spammer.elf ") + to_string(n_threads) + string(" 2>spammer_out.txt")).c_str());
        if (ec != 0) {
            printf("Spammer error code: %d\n", ec); fflush(stdout);
            continue;
        }
        ifstream ef("spammer_out.txt");
        double avg_mks;
        if (!(ef >> avg_mks)) {
            printf("Could not open spammer output file!\n"); fflush(stdout);
            continue;
        }
        
        printf("Avg mks: %.3f\n", avg_mks); fflush(stdout);
    }
    
    return 0;
}
