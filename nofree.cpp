// No free allocator. Cache everything, answer and die.

struct fast_string {
    int offset, length;
};

// GB
const int NEED_NF_MEMORY = 2 * 1000 * 1000 * 1000;
char* nf_memory;
int nf_pos;

int last_tens;

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

char* nf_allocate_mem(int size) {
    char* mem = nf_memory + nf_pos;
    nf_pos += size;
    check_allocations();
    return mem;
}

int nf_allocate_offset(int size) {
    int offset = nf_pos;
    nf_pos += size;
    return offset;
}

fast_string nf_wrap(string s) {
    int offset = nf_allocate_offset(s.length());
    memcpy(nf_memory + offset, s.data(), s.length());
    return fast_string { offset, (int)s.length() };
}
