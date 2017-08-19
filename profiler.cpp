/* Profiling */

// thread unaware

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

#ifndef DISABLE_PROFILING
Profiler profiler("");
Profiler minute_accumulator_profiler("[minute] ");

struct VisibilityProfiler {
    int id = -1;
    
    VisibilityProfiler(int id = -1): id(id) { profiler.begin(id); }
    void end() { if (id != -1) profiler.end(id); id = -1; }
    ~VisibilityProfiler() { if (id != -1) profiler.end(id); }
};

#define scope_profile(id) \
    VisibilityProfiler prof(id);
    
#define scope_profile_end() \
    prof.end()
    
#define profile_begin(id) \
    profiler.begin(id)
    
#define profile_end(id) \
    profiler.end(id)
    
#define profile_delimiter(event) \
    profiler.delimiter(event)

#else

#define scope_profile(id)
#define scope_profile_end()
#define profile_begin(id)
#define profile_end(id)
#define profile_delimiter(event)

#endif
