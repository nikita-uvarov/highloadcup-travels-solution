/* Common */

#define verify(cond) if (!(cond)) { fprintf(stderr, "Verification failed: '" #cond "' on line %d\n", __LINE__); perror("perror"); fflush(stderr); fflush(stdout); sleep(2); abort(); }

#ifndef SUBMISSION_MODE
#define check(cond) if (!(cond)) { fprintf(stderr, "Verification failed: '" #cond "' on line %d\n", __LINE__); perror("perror"); fflush(stderr); abort(); }
#else
#define check(cond) {}
#endif

using li = long long;

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
    printf("Options loaded from '%s': timestamp %d, is rated: %s\n", file_name.c_str(), current_timestamp, is_rated_run ? "true" : "false"); fflush(stdout);
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

void print_memory_stats() {
    struct mallinfo info = mallinfo();
    printf("Memory stats: occupied %s, arena %s, preallocated free %s, keepcost %s\n",
        memory_human_readable(info.uordblks).c_str(),
        memory_human_readable(info.arena).c_str(),
        memory_human_readable(info.fordblks).c_str(),
        memory_human_readable(info.keepcost).c_str()
    );
}

void load_initial_data() {
    current_timestamp = time(0);
    li t_begin = get_ns_timestamp();
    
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
#endif
    
    reindex_database();
    li t_end = get_ns_timestamp();
    printf("Initial data loaded in %.3f seconds\n", (t_end - t_begin) / (double)1e9); fflush(stdout);
    
    print_memory_stats();
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
