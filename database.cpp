/* Request handling */

// naive implementation

#if 0
// http://www.unixtimestamp.com/index.php
const timestamp MIN_ALLOWED_BIRTH_DATE = -1262304000;
const timestamp MAX_ALLOWED_BIRTH_DATE = 915235199;

const timestamp MIN_ALLOWED_VISIT_DATE = 946684800;
const timestamp MAX_ALLOWED_VISIT_DATE = 1420156799;
#else
const timestamp MIN_ALLOWED_BIRTH_DATE = numeric_limits<int>::min();
const timestamp MAX_ALLOWED_BIRTH_DATE = numeric_limits<int>::max();

const timestamp MIN_ALLOWED_VISIT_DATE = numeric_limits<int>::min();
const timestamp MAX_ALLOWED_VISIT_DATE = numeric_limits<int>::max();
#endif

// string - 8, User - 72, Location - 72, Visit - 20

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
    
    fast_string http_cache;
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
    
    fast_string http_cache;
    void update_cache();
    
    vector<DatedVisit> visits;
    
    // total sum, male only sum, male count
    vector<tuple<short, short, short>> mark_sum;
    
    void invalidate_mark_sums();
    bool maybe_update_mark_sums();
    void update_mark_sums();
};

struct Visit {
    int id = -1;
    int location_id;
    int user_id;
    timestamp visited_at;
    char mark;
    
    fast_string http_cache;
    void update_cache();
    
    fast_string json_cache;
    void update_dependent_cache();
};

vector<User> user_by_id;
vector<Location> location_by_id;
vector<Visit> visit_by_id;

#ifndef DISABLE_DATABASE_WRITE_LOCKS
std::mutex write_mutex;
#endif

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

const int MAX_ALLOWED_AGE = 300;
time_t age_filter_cache[MAX_ALLOWED_AGE + 1];

void initialize_age_cache() {
    time_t now = {};
    tm time_struct = {};
    now = current_timestamp;
    gmtime_r(&now, &time_struct);
    
    for (int age = 0; age <= MAX_ALLOWED_AGE; age++) {
        int orig_year = time_struct.tm_year;
        
        time_struct.tm_year -= age;
        time_struct.tm_year = max(time_struct.tm_year, 2);
        age_filter_cache[age] = mktime(&time_struct);
        
        time_struct.tm_year = orig_year;
    }
}

void show_new_pagefault_count(const char* logtext) {
    static int last_majflt = 0, last_minflt = 0;
    struct rusage usage;

    getrusage(RUSAGE_SELF, &usage);

    printf("%s: Pagefaults, Major:%ld, " \
            "Minor:%ld\n", logtext,
            usage.ru_majflt - last_majflt,
            usage.ru_minflt - last_minflt);

    last_majflt = usage.ru_majflt; 
    last_minflt = usage.ru_minflt;
}

void reserve_process_memory(size_t size) {
   	char* buffer = (char*)malloc(size);
   
   	for (size_t i = 0; i < size; i += sysconf(_SC_PAGESIZE)) {
   		buffer[i] = 0;
   	}
   	free(buffer);
    
   	show_new_pagefault_count(("Caused by reserving " + memory_human_readable(size) + " through malloc").c_str());
}

void setup_nf() {
    show_new_pagefault_count("Initial count");
    reserve_process_memory(200 * 1024 * (size_t)1024 + NEED_NF_MEMORY);
    initialize_nf();
}

void lock_all_memory() {
    printf("Locking all memory...\n");
    
    if (mlockall(MCL_CURRENT | MCL_FUTURE) != 0) {
        perror("mlockall");
    }
    else {
        printf("Successfully locked!\n");
        
    }
    fflush(stdout);
}

void reindex_database() {
    li reindex_start = get_ns_timestamp();
    
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
        
    const int RESERVE = 12000;
    visit_by_id.reserve(visit_by_id.size() + RESERVE);
    location_by_id.reserve(location_by_id.size() + RESERVE);
    user_by_id.reserve(user_by_id.size() + RESERVE);
    
    printf("Reindexing took %.3f s\n", (get_ns_timestamp() - reindex_start) / (double)1e9);
    reindex_start = get_ns_timestamp();
    
    printf("Beginning to setup NF memory\n"); fflush(stdout);
    setup_nf();
    
    for (int id = 0; id < (int)user_by_id.size(); id++) {
        if (user_by_id[id].id != id)
            continue;
        user_by_id[id].update_cache();
    }
    
    for (int id = 0; id < (int)visit_by_id.size(); id++) {
        if (visit_by_id[id].id != id)
            continue;
        visit_by_id[id].update_cache();
        visit_by_id[id].update_dependent_cache();
    }
    
    for (int id = 0; id < (int)location_by_id.size(); id++) {
        if (location_by_id[id].id != id)
            continue;
        location_by_id[id].update_cache();
        location_by_id[id].update_mark_sums();
    }
    
    printf("Dependent cache update took %.3f s\n", (get_ns_timestamp() - reindex_start) / (double)1e9);
    
    printf("Database is ready (%d users, %d locations, %d visits)\n", n_users, n_locations, n_visits);
    fflush(stdout);
    
    lock_all_memory();
}

void fix_database_caches() {
    int work = 0;
    li fix_start = get_ns_timestamp();
    
    for (int id = 0; id < (int)location_by_id.size(); id++) {
        if (location_by_id[id].id != id)
            continue;
        
        if (location_by_id[id].maybe_update_mark_sums())
            work += location_by_id[id].visits.size();
    }
    printf("Database caches fixed in %.3f s, total work %d\n", (get_ns_timestamp() - fix_start) / (double)1e9, work);
    print_memory_stats();
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
    
#define header_200 "HTTP/1.1 200 OK\r\n"
#define header_400 "HTTP/1.1 400 Bad Request\r\n"
#define header_404 "HTTP/1.1 404 Not Found\r\n"
    
#define header_content_length_zero "Content-Length: 0\r\n"
#define header_two_headers "X: 1\r\nY: 1\r\n"
#define header_rn "\r\n"

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
#define zero_offset_string header_200 "Content-Length:"
#define HTTP_OK_PREFIX header_200 header_content_length_tbd header_rn

#ifndef DISABLE_VALIDATE
#define validate_json() \
        char* data_ptr = json.buffer_begin + sizeof HTTP_OK_PREFIX - 1; \
        validator.supply_data(data_ptr, json.buffer_pos - data_ptr)
#else
#define validate_json()
#endif

#ifndef DISABLE_VALIDATE
#define validate_raw(fast_str) \
        char* data_ptr = nf_memory + fast_str.offset + sizeof HTTP_OK_PREFIX - 1; \
        validator.supply_data(data_ptr, fast_str.length - (sizeof HTTP_OK_PREFIX - 1))
#else
#define validate_raw(fast_str)
#endif

#define begin_response() \
    ResponseBuilder json; \
    scope_profile(BUILD_JSON_RESPONSE);

#define send_response() \
    json.embed_content_length(sizeof zero_offset_string, sizeof HTTP_OK_PREFIX - 1); \
    scope_profile_end(); \
    json.write(fd); \
    validate_json()
    
    int handle_get(int fd) {
        if (entity == Entity::USERS) {
            if (!id_exists(user_by_id, id)) return 404;
            
            if (!get_visits) {
                validate_raw(user_by_id[id].http_cache);
                user_by_id[id].http_cache.imm_write(fd);
                return 200;
            }
            
            User& user = user_by_id[id];
            
            if (!get_visits) {
                // simple
                
                begin_response();
                
#if 1
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
                    
                    if (to_distance != MAGIC_INTEGER) {
                        if (location.distance >= to_distance)
                            continue;
                    }
                    
                    if (country_ptr_begin) {
                        if (country_ptr_end - country_ptr_begin != (long)location.country_unescaped.length() || memcmp(country_ptr_begin, location.country_unescaped.data(), location.country_unescaped.length()))
                            continue;
                    }
                    
                    json.append(nf_memory + visit.json_cache.offset, visit.json_cache.length);
                    n_visits++;
                }
                
                // remove last comma
                if (n_visits)
                    json.buffer_pos--;
                
                append_str(json, "]}");
                
                send_response();
                return 200;
            }
        }
        else if (entity == Entity::LOCATIONS) {
            if (!id_exists(location_by_id, id)) return 404;
            
            if (!get_avg) {
                validate_raw(location_by_id[id].http_cache);
                location_by_id[id].http_cache.imm_write(fd);
                return 200;
            }
            
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
                
                bool filter_age = false;
                if (from_age != MAGIC_INTEGER || to_age != MAGIC_INTEGER) {
                    filter_age = true;
                }
                
                timestamp from_filter = numeric_limits<int>::min(), to_filter = numeric_limits<int>::max();
                
                if (from_age != MAGIC_INTEGER) {
                    from_age = max(from_age, 0);
                    from_age = min(from_age, MAX_ALLOWED_AGE);
                    
                    to_filter = min((time_t)to_filter, age_filter_cache[from_age] - 1);
                }
                
                if (to_age != MAGIC_INTEGER) {
                    to_age = max(to_age, 0);
                    to_age = min(to_age, MAX_ALLOWED_AGE);
                    
                    from_filter = max((time_t)from_filter, age_filter_cache[to_age] + 1);
                }
                
                int n_marks = 0, mark_sum = 0;
                
                if ((to_age == MAGIC_INTEGER && from_age == MAGIC_INTEGER) && !location.mark_sum.empty()) {
                    n_marks = it_end - it;
                    
                    if (n_marks > 0) {
                        int l = it - location.visits.begin();
                        int r = it_end - location.visits.begin() - 1;
                        
                        mark_sum = get<0>(location.mark_sum[r]) - (l ? get<0>(location.mark_sum[l - 1]) : 0);
                        if (gender != '*') {
                            int male_sum = get<1>(location.mark_sum[r]) - (l ? get<1>(location.mark_sum[l - 1]) : 0);
                            int male_cnt = get<2>(location.mark_sum[r]) - (l ? get<2>(location.mark_sum[l - 1]) : 0);
                            
                            if (gender == 'm')
                                n_marks = male_cnt, mark_sum = male_sum;
                            else
                                n_marks = n_marks - male_cnt, mark_sum = mark_sum - male_sum;
                        }
                    }
                    else {
                        n_marks = 0;
                    }
                }
                else {
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
                }
                
                if (mark_sum == 0) {
                    append_str(json, "0}");
                }
                else {
                    fast_get_avg(n_marks, mark_sum);
                    json.append(fast_avg_buffer, fast_avg_expression_length);
                }
                
                send_response();
                return 200;
            }
        }
        else {
            if (!id_exists(visit_by_id, id)) return 404;
            
            {   
                validate_raw(visit_by_id[id].http_cache);
                visit_by_id[id].http_cache.imm_write(fd);
                return 200;
            }
            
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
        
        //printf("VERY STRANGE -- failed to answer GET\n"); fflush(stdout);
        return 400;
    }
    
    
    int handle_post(int fd, const char* body) {
#define header_content_length_four "Content-Length: 2\r\n"
#define HTTP_OK_WITH_EMPTY_JSON_RESPONSE header_200 header_content_length_four header_rn "{}"
        
#define successful_update() \
        profile_begin(WRITE_RESPONSE); \
        imm_write_call(fd, HTTP_OK_WITH_EMPTY_JSON_RESPONSE, sizeof(HTTP_OK_WITH_EMPTY_JSON_RESPONSE) - 1); \
        profile_end(WRITE_RESPONSE)
        //close(fd);
        
        //successful_update();
        //return 200;
        
#define declare_doc() \
        profile_begin(PARSE_JSON_RAPIDJSON); \
        Document json; \
        json.ParseInsitu((char*)body); \
        profile_end(PARSE_JSON_RAPIDJSON); \
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
    scope_profile(PARSE_JSON_CUSTOM); \
    for (Value::ConstMemberIterator it = json.MemberBegin(); it != json.MemberEnd(); ++it) { \
                    const char* name = it->name.GetString(); \
                    const char* name_end = name + it->name.GetStringLength();

#define end_fields_iteration \
    return 400; } \
    scope_profile_end();
                
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

#ifndef DISABLE_DATABASE_WRITE_LOCKS
#define lock_write_access() \
    unique_lock<std::mutex> write_lock(write_mutex)
#else
#define lock_write_access()
#endif

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
                
                //if (all_user_emails.find(email) != all_user_emails.end())
                //    return 400;
                
                successful_update();
                lock_write_access();
                
                maybe_resize(user_by_id, id);
                User& new_user = user_by_id[id];
                new_user.id = id;
                new_user.email = email;
                new_user.first_name = json_escape_string(first_name);
                new_user.last_name = json_escape_string(last_name);
                new_user.gender = *gender;
                new_user.birth_date = birth_date;
                new_user.update_cache();
                //all_user_emails.emplace(email);
                
                return 200;
            }
            else {
                successful_update();
                lock_write_access();
                
                if (email) {
                    user->email = email;
                }
                
                if (first_name) user->first_name = json_escape_string(first_name);
                if (last_name) user->last_name = json_escape_string(last_name);
                
                if (gender && user->gender != *gender) {
                    user->gender = *gender;
                    
                    for (DatedVisit& dv: user->visits) {
                        location_by_id[visit_by_id[dv.id].location_id].invalidate_mark_sums();
                    }
                }
                
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
                lock_write_access();
                
                maybe_resize(location_by_id, id);
                Location& new_location = location_by_id[id];
                new_location.id = id;
                new_location.place = json_escape_string(place);
                new_location.country_unescaped = country;
                new_location.country = json_escape_string(new_location.country_unescaped);
                new_location.city = json_escape_string(city);
                new_location.distance = distance;
                new_location.update_cache();
                
                return 200;
            }
            else {
                successful_update();
                lock_write_access();
                
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
                
                location->update_cache();
                
                if (place) {
                    for (DatedVisit& dv: location->visits)
                        visit_by_id[dv.id].update_dependent_cache();
                }
                
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
                lock_write_access();
                
                maybe_resize(visit_by_id, id);
                Visit& new_visit = visit_by_id[id];
                new_visit.id = id;
                new_visit.visited_at = visited_at;
                new_visit.mark = mark;
                new_visit.user_id = user_id;
                new_visit.location_id = location_id;
                new_visit.update_cache();
                new_visit.update_dependent_cache();
                
                DatedVisit dv = { new_visit.id, new_visit.visited_at };
                
                user->visits.insert(upper_bound(all(user->visits), dv), dv);
                location->visits.insert(upper_bound(all(location->visits), dv), dv);
                location->invalidate_mark_sums();
                
                return 200;
            }
            else {
                successful_update();
                lock_write_access();
                
                if (mark != MAGIC_INTEGER) visit->mark = mark;
                
                // hard updates
                
                if (user_id == visit->user_id) { user_id = MAGIC_INTEGER; user = nullptr; }
                if (location_id == visit->location_id) { location_id = MAGIC_INTEGER; location = nullptr; }
                
                timestamp new_visited_at = visit->visited_at;
                if (visited_at != MAGIC_INTEGER) new_visited_at = visited_at;
                bool visit_time_changed = new_visited_at != visit->visited_at;
                
                bool need_old_location_sums_update = false;
                
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
                    
                    if (new_user->gender != old_user->gender) {
                        need_old_location_sums_update = true;
                    }
                    
                    if (user) visit->user_id = user_id;
                }
                
                if (location || visit_time_changed) {
                    Location* old_location = &location_by_id[visit->location_id];
                    
                    auto it = lower_bound(all(old_location->visits), DatedVisit { id, visit->visited_at });
                    check(it != old_location->visits.end());
                    old_location->visits.erase(it);
                    old_location->invalidate_mark_sums();
                    
                    need_old_location_sums_update = false;
                    
                    Location* new_location = location;
                    if (!new_location)
                        new_location = old_location;
                    
                    DatedVisit dv = { id, new_visited_at };
                    new_location->visits.insert(upper_bound(all(new_location->visits), dv), dv);
                    
                    new_location->invalidate_mark_sums();
                    
                    if (location) visit->location_id = location_id;
                }
                else if (mark != MAGIC_INTEGER) {
                    need_old_location_sums_update = false;
                    location_by_id[visit->location_id].invalidate_mark_sums();
                }
                
                if (need_old_location_sums_update)
                    location_by_id[visit->location_id].invalidate_mark_sums();
                
                visit->visited_at = new_visited_at;
                
                visit->update_cache();
                visit->update_dependent_cache();
                
                return 200;
            }
        }
        
        //printf("VERY STRANGE -- failed to answer POST\n"); fflush(stdout);
        return 400;
    }
    
#undef validate_json
};

void User::update_cache() {
    ResponseBuilder json;
    
    append_str(json, HTTP_OK_PREFIX);
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
    json.embed_content_length(sizeof zero_offset_string, sizeof HTTP_OK_PREFIX - 1);
    
    http_cache = nf_wrap(json.buffer_begin, json.buffer_pos);
}

void Location::update_cache() {
    ResponseBuilder json;
    
    append_str(json, HTTP_OK_PREFIX "{\"id\":");
    json.append_int(id);
    append_str(json, ",\"place\":");
    json.append(place.data(), place.length());
    append_str(json, ",\"country\":");
    json.append(country.data(), country.length());
    append_str(json, ",\"city\":");
    json.append(city.data(), city.length());
    append_str(json, ",\"distance\":");
    json.append_int(distance);
    append_str(json, "}");
    json.embed_content_length(sizeof zero_offset_string, sizeof HTTP_OK_PREFIX - 1);
    
    http_cache = nf_wrap(json.buffer_begin, json.buffer_pos);
}

void Visit::update_cache() {
    ResponseBuilder json;
    
    append_str(json, HTTP_OK_PREFIX "{\"id\":");
    json.append_int(id);
    append_str(json, ",\"location\":");
    json.append_int(location_id);
    append_str(json, ",\"user\":");
    json.append_int(user_id);
    append_str(json, ",\"visited_at\":");
    json.append_int(visited_at);
    append_str(json, ",\"mark\":");
    json.append_int(mark);
    append_str(json, "}");
    json.embed_content_length(sizeof zero_offset_string, sizeof HTTP_OK_PREFIX - 1);
    
    http_cache = nf_wrap(json.buffer_begin, json.buffer_pos);
}

void Visit::update_dependent_cache() {
    ResponseBuilder json;
    
    append_str(json, "{\"mark\":");
                    
    json.append_int(mark);
    append_str(json, ",\"visited_at\":");
    json.append_int(visited_at);
    append_str(json, ",\"place\":");
    json.append(location_by_id[location_id].place.data(), location_by_id[location_id].place.length());
    append_str(json, "},");
    
    json_cache = nf_wrap(json.buffer_begin, json.buffer_pos);
}

void Location::update_mark_sums() {
    int n = visits.size();
    mark_sum.resize(n);
    int sum = 0, male_sum = 0, male_count = 0;
    for (int i = 0; i < n; i++) {
        Visit& visit = visit_by_id[visits[i].id];
        sum += visit.mark;
        if (user_by_id[visit.user_id].gender == 'm') {
            male_sum += visit.mark;
            male_count++;
        }
        
        mark_sum[i] = make_tuple(sum, male_sum, male_count);
    }
}

void Location::invalidate_mark_sums() {
    mark_sum.clear();
}

bool Location::maybe_update_mark_sums() {
    if (mark_sum.size() == 0 && visits.size() > 0) {
        update_mark_sums();
        return true;
    }
    
    return false;
}
