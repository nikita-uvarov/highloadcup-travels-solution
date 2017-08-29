// No free allocator. Cache everything, answer and die.


// GB
const unsigned NEED_NF_MEMORY = 2.5 * 1000 * 1000 * 1000;
char* nf_memory;
unsigned nf_pos;

int last_tens;

void print_nofree_stats() {
    printf("nofree: %s / %s\n", memory_human_readable(nf_pos).c_str(), memory_human_readable(NEED_NF_MEMORY).c_str()); fflush(stdout);
}

struct fast_string {
    unsigned offset, length;
    
    void imm_write(int fd) {
        imm_write_call(fd, nf_memory + offset, length);
        global_t_ready_write = get_ns_timestamp();
    }
};

void check_allocations() {
    if (nf_pos * (li)10 / NEED_NF_MEMORY != last_tens) {
        last_tens = nf_pos * 10 / NEED_NF_MEMORY;
        printf("%.2f%% non-reusable memory is used\n", nf_pos / (double)NEED_NF_MEMORY * 100); fflush(stdout);
    }
    
    verify(nf_pos < NEED_NF_MEMORY);
}

void initialize_nf() {
    nf_memory = (char*)malloc(NEED_NF_MEMORY);
    verify(nf_memory);
}

char* nf_allocate_mem(unsigned size) {
    char* mem = nf_memory + nf_pos;
    nf_pos += size;
    check_allocations();
    return mem;
}

unsigned nf_allocate_offset(unsigned size) {
    unsigned offset = nf_pos;
    nf_pos += size;
    return offset;
}

fast_string nf_wrap(string s) {
    unsigned offset = nf_allocate_offset(s.length());
    memcpy(nf_memory + offset, s.data(), s.length());
    return fast_string { offset, (unsigned)s.length() };
}

fast_string nf_wrap(char* begin, char* end) {
    unsigned offset = nf_allocate_offset(end - begin);
    memcpy(nf_memory + offset, begin, end - begin);
    return fast_string { offset, (unsigned)(end - begin) };
    
}
