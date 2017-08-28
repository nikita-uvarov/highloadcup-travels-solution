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

#define all(v) (v).begin(), (v).end()

using namespace std;

//#undef SUBMISSION_MODE

#define DISABLE_VALIDATE
#define DISABLE_PROFILING
#define DISABLE_AFFINITY

#include "utils.cpp"
#include "profiler.cpp"
#include "validator.cpp"
#include "response_builder.cpp"
#include "database.cpp"
#include "http_support.cpp"
#include "network.cpp"

int main() {
    LONG_REQUEST_NS = 0;
    LONG_REQUEST_NS = 2e6;
    
    tune_realtime_params();
    
    inspect_server_parameters();
#ifndef DISABLE_VALIDATE
    initialize_validator();
#endif
    load_initial_data();
    initialize_age_cache();
    
    do_benchmark();
    
    start_epoll_server();
    
    return 0;
}
