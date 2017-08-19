/* custom itoa... dont' know why */

const int MAX_INTEGER_SIZE = 30;
thread_local char itoa_buffer[MAX_INTEGER_SIZE];
thread_local int itoa_length;

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

/* Response builder */

thread_local li global_t_ready_write;

const int MAX_RESPONSE_SIZE = 4096 * 2;
thread_local char response_buffer[MAX_RESPONSE_SIZE];

struct ResponseBuilder {
    char* buffer_begin;
    char* buffer_pos;
    char* buffer_end;
    
    ResponseBuilder() {
        buffer_begin = buffer_pos = response_buffer;
        buffer_end = response_buffer + MAX_RESPONSE_SIZE;
    }
    
    void discard_old() {
        if (buffer_begin != response_buffer)
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
#if 0
        usleep((int)1e6); // microseconds
#endif
        
        profile_begin(WRITE_RESPONSE);
        //li t0 = get_ns_timestamp();
        global_t_ready_write = get_ns_timestamp();
        ::write(fd, buffer_begin, buffer_pos - buffer_begin);
        //li t1 = get_ns_timestamp();
        //printf("write call taken %.3f mks\n", (t1 - t0) / 1000.0);
        //close(fd);
        profile_end(WRITE_RESPONSE);
    }
};

#define append_str(builder, str) builder.append(str, sizeof str - 1)
