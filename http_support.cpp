/* Instant answer utility */

void write_only_header_answer(int fd, int code) {
    check(code == 200 || code == 400 || code == 404);
    //printf("response %d\n\n", code);
    
    scope_profile(WRITE_RESPONSE);
    //global_t_ready_write = get_ns_timestamp();
    
    //int res = 0, expected = 0;
    if (code == 200) {
        // 3 headers total
#define response_200 header_200 header_content_length_zero header_two_headers header_rn
        imm_write_call(fd, response_200, sizeof response_200 - 1);
        //res = write(fd, response_200, sizeof response_200 - 1);
        //expected = sizeof response_200 - 1;
#undef response_200
    }
    
    if (code == 400) {
#define response_400 header_400 header_content_length_zero header_two_headers header_rn
        imm_write_call(fd, response_400, sizeof response_400 - 1);
        //res = write(fd, response_400, sizeof response_400 - 1);
        //expected = sizeof response_400 - 1;
        //close(fd);
        //was_really_closed = true;
#undef response_400
    }
    
    if (code == 404) {
#define response_404 header_404 header_content_length_zero header_two_headers header_rn
        imm_write_call(fd, response_404, sizeof response_404 - 1);
        //res = write(fd, response_404, sizeof response_404 - 1);
        //expected = sizeof response_404 - 1;
        //close(fd);
        //was_really_closed = true;
#undef response_404
    }
    
#if 0
    if (res != expected) {
        printf("write call returned %d but %d was expected\n", res, expected);
        fflush(stdout);
    }
#endif
}

/* Initial request bufferization & parsing */

int process_get_request(int fd, const char* path, int path_length);
int process_post_request(int fd, const char* path, int path_length, const char* body);
void request_completed(bool is_get, const char* path, int path_length, int code);

struct http_input_request_handler {
    char* request_content;
    int request_length;
    
    int parse(int fd) {
        profile_begin(PARSE_HEADERS);
        size_t prevbuflen = 0, method_len, path_len, num_headers;
        
        // FIXME: 10 here is enough
        struct phr_header headers[15];
        num_headers = sizeof(headers) / sizeof(headers[0]);
        const char* path;
        const char* method;
        int minor_version;
        int pret = phr_parse_request(request_content, request_length, &method, &method_len, &path, &path_len,
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
            profile_end(PARSE_HEADERS);
            if (pret == -1) {
                //printf("Request failed to parse headers:\n'%.*s'\n", request_length, request_content);
                //fflush(stdout);
            }
            
            return pret;
        }
        
        if (method_len == 3) {
            // GET request, can already answer
            profile_end(PARSE_HEADERS);
            
            int code = process_get_request(fd, path, path_len);
            if (code != 200)
                write_only_header_answer(fd, code);
            request_completed(true, path, path_len, code);
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
            
            profile_end(PARSE_HEADERS);
            
            if (content_length < 0) {
                //printf("Request with no content length:\n'%s'\n", request_content.c_str());
                write_only_header_answer(fd, 400);
                return pret;
            }
                
            check(content_length >= 0);
            
            int have_length = request_length - pret;
            if (have_length < content_length) {
                //printf("warning -- large request (%d have, %d need)\n", have_length, content_length); fflush(stdout);
                return -2;
            }
            
            /*if (have_length > content_length) {
                printf("warning -- more data received then promised (%d received, %d content length)\n", have_length, content_length);
            }*/
            
            //write_only_header_answer(fd, 400);
            //return pret;
            
            int code = process_post_request(fd, path, path_len, request_content + pret);
            if (code != 200)
                write_only_header_answer(fd, code);
            request_completed(false, path, path_len, code);
        }
        
        //string response = "HTTP/1.1 200 OK\r\n\r\n";
        //write(fd, response.c_str(), response.length());
        
        return pret;
    }
};

struct instant_answer_request_handler {
    char* request_content;
    int request_length;
    
    int parse(int fd) {
        write_only_header_answer(fd, 200);
        return 1;
    }
};

/* Option parsing */

int process_request_options(RequestHandler& handler, const char* path, const char* path_end)
{
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

/* Timing and validation wrappers */

int process_get_request_raw(int fd, const char* path, int path_length);
int process_post_request_raw(int fd, const char* path, int path_length, const char* body);

//poller_local li global_t_ready;
poller_local bool global_last_request_is_get = false;
poller_local int total_requests = 0;
//poller_local li global_t_polled;

#if 0
li LONG_REQUEST_NS = 250 * (li)1000; // 500 mks is long
#else
li LONG_REQUEST_NS = 400 * (li)1000;
#endif

poller_local int messages_limit = 10;

poller_local li total_reads, total_logic, total_writes;

void request_completed(bool is_get, const char* path, int path_length, int code) {
    if (is_get != global_last_request_is_get)
        total_requests = 0;
    
    global_last_request_is_get = is_get;
    total_requests++;
    
#if 0
    li t_answered = get_ns_timestamp();
    total_reads += global_t_ready - global_t_polled;
    total_logic += global_t_ready_write - global_t_ready;
    total_writes += t_answered - global_t_ready_write;
    
    if (t_answered - global_t_ready > LONG_REQUEST_NS) {
        if (messages_limit > 0) {
            messages_limit--;
            printf("[%d] long %s %.*s (%d): %.3f mks pure, %.3f mks total (%.3f read, %.3f write)\n",
                global_thread_index,
                is_get ? "GET" : "POST", path_length, path, code,
                (global_t_ready_write - global_t_ready) / 1000.0,
                (t_answered - global_t_polled) / 1000.0,
                (global_t_ready - global_t_polled) / 1000.0,
                (t_answered - global_t_ready_write) / 1000.0);
            fflush(stdout);
        }
        else if (messages_limit == 0) {
            printf("message limit exceeded for this thread\n");
            fflush(stdout);
            messages_limit = -1;
        }
    }
    max_request_ns = max(max_request_ns, t_answered - global_t_ready);
#endif
}

int process_get_request(int fd, const char* path, int path_length) {
#ifndef DISABLE_VALIDATE
    string path_backup(path, path + path_length);
#endif
    
    int code = process_get_request_raw(fd, path, path_length);

#ifndef DISABLE_VALIDATE
    validator.check_answer(true, path_backup.c_str(), path_length, code, 0, 0);
#endif
    
    return code;
}

int process_post_request(int fd, const char* path, int path_length, const char* body) {
    int code = process_post_request_raw(fd, path, path_length, body);
    
#ifndef DISABLE_VALIDATE
    validator.check_answer(false, path, path_length, code, 0, 0);
#endif
    return code;
}

/* Path parsing */

int process_get_request_raw(int fd, const char* path, int path_length) {
    //printf("GET %.*s\n", path_length, path); fflush(stdout);
    scope_profile(PARSE_PATH);
    
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
    
    int opts_code = process_request_options(handler, path, path_end);
    if (opts_code != 200)
        return opts_code;
    
    scope_profile_end();
    int code = handler.handle_get(fd);
    return code;
}

int process_post_request_raw(int fd, const char* path, int path_length, const char* body) {
    //printf("POST %.*s '%s'\n", path_length, path, body); fflush(stdout);
    scope_profile(PARSE_PATH);
    
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
    
    scope_profile_end();
    int code = handler.handle_post(fd, body);
    return code;
}
