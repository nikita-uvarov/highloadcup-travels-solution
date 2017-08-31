/* Common */

#define verify(cond) if (!(cond)) { fprintf(stderr, "Verification failed: '" #cond "' on line %d\n", __LINE__); perror("perror"); fflush(stderr); fflush(stdout); sleep(2); abort(); }

#ifndef SUBMISSION_MODE
#define check(cond) if (!(cond)) { fprintf(stderr, "Verification failed: '" #cond "' on line %d\n", __LINE__); perror("perror"); fflush(stderr); abort(); }
#else
#define check(cond) {}
#endif

using li = long long;

#ifdef MULTIPLE_POLLERS
#define poller_local thread_local
#else
#define poller_local
#endif

/* Decoding different things */

string json_escape_string(const string& str) {
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.String(str.data(), str.length());
    string result = s.GetString();
    return result;
}

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

/* Parsing utilities */

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

using timestamp = int;

const timestamp MAGIC_TIMESTAMP = std::numeric_limits<int>::min() + 1234;
const int MAGIC_INTEGER = std::numeric_limits<int>::min() + 1234;

bool parse_timestamp(const char* str, const char* to, timestamp& value) {
    bool correct = false;
    value = fast_atoi(str, correct);
    return correct && str == to;
}

/* Utils */

thread_local int global_thread_index;

li timespec_to_li(const timespec& ts) {
    return ts.tv_sec * (li)1e9 + ts.tv_nsec;
}

li get_ns_timestamp() {
    timespec ts = {};
    verify(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
    return timespec_to_li(ts);
}

bool string_ends_with(string a, string b) {
    return a.length() >= b.length() && a.substr(a.length() - b.length()) == b;
}

/* Server initialization */

void load_json_dump_from_file(string file_name);
void reindex_database();

timestamp current_timestamp;
bool is_rated_run;
bool is_local_run;

void load_options_from_file(string file_name, bool must_exist) {
    ifstream is(file_name);
    if (must_exist) {
        verify(is);
    }
    else if (!is) {
        printf("Tried to load options from '%s' unsuccessfully\n", file_name.c_str()); fflush(stdout);
        return;
    }
    
    int is_rated_int;
    verify(is >> current_timestamp >> is_rated_int);
    verify(is_rated_int == 0 || is_rated_int == 1);
    is_rated_run = (is_rated_int == 1);
    
    int is_local_int = 0;
    if (is >> is_local_int)
        is_local_run = (is_local_int == 1);
    else
        is_local_run = false;
    
    printf("Options loaded from '%s': timestamp %d, is rated: %s, is local: %s\n", file_name.c_str(), current_timestamp,
           is_rated_run ? "true" : "false", is_local_run ? "true" : "false"); fflush(stdout);
}

string memory_human_readable(unsigned int bytes) {
    if (bytes < 1024) return to_string(bytes) + " bytes";
    
    char buf[256];
    if (bytes < 1024 * 1024)
        sprintf(buf, "%.1f KiB", bytes / (double)1024);
    else if (bytes < 1024 * 1024 * 1024)
        sprintf(buf, "%.1f MiB", bytes / (double)(1024 * 1024));
    else
        sprintf(buf, "%.2f GiB", bytes / (1024 * (double)1024 * 1024));
        
    return string(buf);
}

void print_nofree_stats();

void print_memory_stats() {
    struct mallinfo info = mallinfo();
    printf("Memory stats: occupied %s, arena %s, preallocated free %s, keepcost %s; ",
        memory_human_readable(info.uordblks).c_str(),
        memory_human_readable(info.arena).c_str(),
        memory_human_readable(info.fordblks).c_str(),
        memory_human_readable(info.keepcost).c_str()
    );
    print_nofree_stats();
}

void load_initial_data() {
    current_timestamp = time(0);
    li t_begin = get_ns_timestamp();
    
    printf("Running in submission mode, proceeding to unzip\n"); fflush(stdout);
    {
        load_options_from_file("/tmp/data/options.txt", false);
        
        string unzip_cmd = "unzip -o -j /tmp/data/data.zip '*' -d data >/dev/null";
        int ec = system(unzip_cmd.c_str());
        verify(ec == 0);
        
        li t_unzip = get_ns_timestamp();
        printf("Unzip done in %.3f seconds\n", (t_unzip - t_begin) / (double)1e9);
        
        string find_cmd = "find data -name '*' >db_files.txt";
        ec = system(find_cmd.c_str());
        verify(ec == 0);
        
        int n_files = 0;
        {
            ifstream db_files("db_files.txt");
            verify(db_files);
            string db_file_name = "";
            
            while (getline(db_files, db_file_name) && db_file_name.length() > 0) {
                n_files++;
                if (string_ends_with(db_file_name, ".json")) {
                    load_json_dump_from_file(db_file_name.c_str());
                }
                else if (string_ends_with(db_file_name, ".txt")) {
                    load_options_from_file(db_file_name, true);
                }
            }
        }
        
        li t_read = get_ns_timestamp();
        printf("%d files, read in %.3f seconds\n", n_files, (t_read - t_unzip) / (double)1e9); fflush(stdout);
        
        ec = system("rm -r data db_files.txt");
        verify(ec == 0);
        
        printf("erased intermediate directory succesfully\n");
        
        verify(n_files > 0);
    }
    
    reindex_database();
    li t_end = get_ns_timestamp();
    printf("Initial data loaded in %.3f seconds\n", (t_end - t_begin) / (double)1e9); fflush(stdout);
    
    print_memory_stats();
}

char slow_avg_buffer[16];

void slow_get_avg(int n_marks, int mark_sum) {
    sprintf(slow_avg_buffer, "%.5f}", mark_sum / (double)n_marks + 1e-12);
}

char fast_avg_buffer[16] = "0.00000}";
constexpr int fast_avg_expression_length = 8;

// 0.02 s. win & maybe crash? ok I optimize

void fast_get_avg(int n_marks, int mark_sum) {
    int base = (mark_sum * (li)1000000 / n_marks);
    int add = base % 10 >= 5; base /= 10;
    
    for (int t = 0; t < 6; t++) {
        fast_avg_buffer[t == 5 ? 0 : 6 - t] = '0' + base % 10;
        base /= 10;
    }
    
    int pos = 6;
    while (add) {
        add += fast_avg_buffer[pos] - '0';
        fast_avg_buffer[pos] = '0' + add % 10;
        add /= 10;
        pos--;
        if (pos == 1) pos = 0;
    }
}

double get_double_rep(string s) {
    istringstream is(s);
    double x;
    is >> x;
    return x;
}

void inspect_server_parameters() {
    verify(sizeof(int) == 4);
    verify(sizeof(time_t) == 8);
    
    {
        li t_begin = get_ns_timestamp();
        
        const int N_TEST_GETTIMES = 1e6;
        li hash = 1;
        for (int i = 0; i < N_TEST_GETTIMES; i++) {
            timespec ts;
            clock_gettime(CLOCK_MONOTONIC, &ts);
            hash += timespec_to_li(ts);
        }
        
        li t_end = get_ns_timestamp();
        printf("clock_gettime cost (ns): %.5f, hash %lld\n", (t_end - t_begin) / (double)N_TEST_GETTIMES, hash); fflush(stdout);
    }
    
    {
        const int N_TEST_SPRINTFS = 1e6;
        li t_begin = get_ns_timestamp();
        
        li hash = 0;
        for (int i = 0; i < N_TEST_SPRINTFS; i++) {
            int nm = rand() % 1000 + 1, sum = rand() % (5 * nm);
            slow_get_avg(nm, sum);
            hash += slow_avg_buffer[2] - '0';
        }
            
        li t_end = get_ns_timestamp();
        printf("get_avg(slow) cost (ns): %.5f, hash %lld\n", (t_end - t_begin) / (double)N_TEST_SPRINTFS, hash); fflush(stdout);
        
        t_begin = get_ns_timestamp();
        
        hash = 0;
        for (int i = 0; i < N_TEST_SPRINTFS; i++) {
            int nm = rand() % 1000 + 1, sum = rand() % (5 * nm);
            fast_get_avg(nm, sum);
            hash += fast_avg_buffer[2] - '0';
        }
            
        t_end = get_ns_timestamp();
        printf("get_avg(fast) cost (ns): %.5f, hash %lld\n", (t_end - t_begin) / (double)N_TEST_SPRINTFS, hash); fflush(stdout);
        
#if 0
        int n_errors = 0;
        for (int n = 1; n < 10000; n++) {
            for (int s = 0; s <= 5 * n; s++) {
                slow_get_avg(n, s);
                string slow_res = slow_avg_buffer;
                fast_get_avg(n, s);
                string fast_res = fast_avg_buffer;
                
                //if (abs(get_double_rep(slow_res) - get_double_rep(fast_res)) > 1e-13) {
                if (fast_res != slow_res) {
                    printf("%d / %d: answer: '%s', expected: '%s'\n", s, n, fast_res.c_str(), slow_res.c_str());
                    n_errors++;
                    if (n_errors > 100) exit(1);
                }
            }
            if (n % 1000 == 0) printf("%d\n", n);
        }
#endif
    }
}

/* Affinity */

union CpuSet {
    cpu_set_t as_set;
    int as_int[sizeof(cpu_set_t) / sizeof(int)];
    unsigned char as_char[sizeof(cpu_set_t)];
};

int get_affinity_mask() {
    CpuSet cpu_mask = {};
    
    verify(sched_getaffinity(0, sizeof(cpu_mask.as_set), &cpu_mask.as_set) == 0);
    
    return cpu_mask.as_int[0];
}

void set_thread_affinity(int affinity_mask, bool is_poller) {
    #ifndef DISABLE_AFFINITY
        CpuSet mask_set = {};
        mask_set.as_int[0] = affinity_mask;
        
        verify(sched_setaffinity(0, sizeof(cpu_set_t), &mask_set.as_set) == 0);
        printf("Started %s thread #%d (affinity mask %d)\n", is_poller ? "epoll" : "consumer", global_thread_index + 1, get_affinity_mask()); fflush(stdout);
    #else
        printf("Started %s thread #%d (affinity disabled)\n", is_poller ? "epoll" : "consumer", global_thread_index + 1); fflush(stdout);
    #endif
}
