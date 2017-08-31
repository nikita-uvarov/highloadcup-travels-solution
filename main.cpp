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

#define gettid() syscall(SYS_gettid)

#include "picohttpparser/picohttpparser.h"

#include "rapidjson/include/rapidjson/rapidjson.h"
#include "rapidjson/include/rapidjson/writer.h"
#include "rapidjson/include/rapidjson/stringbuffer.h"
#include "rapidjson/include/rapidjson/document.h"
#include "rapidjson/include/rapidjson/error/en.h"

using namespace rapidjson;

long long process_startup_timestamp;

#define all(v) (v).begin(), (v).end()

using namespace std;

#define DISABLE_VALIDATE
#define DISABLE_PROFILING
//#define DISABLE_AFFINITY
#define DISABLE_DATABASE_WRITE_LOCKS
#define DISABLE_EPOLLET
//#define DISABLE_SPINLOCKS
//#define MULTIPLE_POLLERS

#include "utils.cpp"
#include "profiler.cpp"
#include "validator.cpp"
#include "mtqueue.cpp"
#include "nofree.cpp"
#include "response_builder.cpp"
#include "database.cpp"
#include "http_support.cpp"
#include "network.cpp"

int main() {
    process_startup_timestamp = get_ns_timestamp();
    
    LONG_REQUEST_NS = 0;
    LONG_REQUEST_NS = 2e6;
    
    tune_realtime_params();
    
    inspect_server_parameters();
#ifndef DISABLE_VALIDATE
    initialize_validator();
#endif
    load_initial_data();
    initialize_age_cache();
    
    start_epoll_server();
    
    return 0;
}
