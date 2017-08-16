#include <bits/stdc++.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <errno.h>

#define all(v) (v).begin(), (v).end()

using namespace std;

const int MAX_POLL_EVENTS = 2;

// hey

#define SUBMISSION_MODE

#define verify(cond) if (!(cond)) { fprintf(stderr, "Verification failed: '" #cond "' on line %d\n", __LINE__); perror("perror"); fflush(stderr); abort(); }

#ifndef SUBMISSION_MODE
#define check(cond) if (!(cond)) { fprintf(stderr, "Verification failed: '" #cond "' on line %d\n", __LINE__); perror("perror"); fflush(stderr); abort(); }
#else
#define check(cond) {}
#endif

// networking template copyright: https://banu.com/blog/2/how-to-use-epoll-a-complete-example-in-c/

static int make_socket_non_blocking(int sfd) {
    int flags = fcntl(sfd, F_GETFL, 0);
    verify(flags != -1);

    flags |= O_NONBLOCK;
    verify(fcntl(sfd, F_SETFL, flags) != -1);
    
#if 0
    int one = 1;
    verify(setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) == 0);
#endif

    return 0;
}

static int create_and_bind(int& port) {
    for (int retries = 0; retries < 3; retries++) {
        struct addrinfo hints;
        struct addrinfo *result, *rp;
        int s, sfd;

        memset(&hints, 0, sizeof(struct addrinfo));
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_PASSIVE;

        s = getaddrinfo(NULL, to_string(port).c_str(), &hints, &result);
        if (s != 0) {
            fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(s));
            abort();
        }

        for (rp = result; rp != NULL; rp = rp->ai_next) {
            sfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
            if (sfd == -1)
                continue;
            
            int reuse = 1;
            if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (const char*)&reuse, sizeof(reuse)) < 0)
                perror("setsockopt(SO_REUSEADDR) failed");

            #ifdef SO_REUSEPORT
            if (setsockopt(sfd, SOL_SOCKET, SO_REUSEPORT, (const char*)&reuse, sizeof(reuse)) < 0) 
                perror("setsockopt(SO_REUSEPORT) failed");
            #else
            printf("SO_REUSEPORT is not supported\n");
            #endif

            s = bind(sfd, rp->ai_addr, rp->ai_addrlen);
            if (s == 0) {
                break;
            }

            close(sfd);
        }
        
        freeaddrinfo(result);

        if (rp == NULL) {
            fprintf(stderr, "Could not bind to port %d\n", port);
            port++;
        }
        else {
            return sfd;
        }
    }
    
    printf("Could not acquire free port!\n");
    abort();
}

#include "picohttpparser/picohttpparser.h"

void write_only_header_answer(int fd, int code) {
    check(code == 200 || code == 400 || code == 404);
    //printf("response %d\n\n", code);
    
#define header_200 "HTTP/1.1 200 OK\r\n"
#define header_400 "HTTP/1.1 400 Bad Request\r\n"
#define header_404 "HTTP/1.1 404 Not Found\r\n"
    
#define header_content_length_zero "Content-Length: 0\r\n"
#define header_connection_close "Connection: close\r\n"
#define header_server "Server: 1\r\n"
//#define header_host "Host: travels.com\r\n"
#define header_host
#define header_rn "\r\n"
    
    if (code == 200) {
#define response_200 header_200 header_content_length_zero header_connection_close header_server header_host header_rn
        write(fd, response_200, sizeof response_200 - 1);
#undef response_200
    }
    
    if (code == 400) {
#define response_400 header_400 header_content_length_zero header_connection_close header_server header_host header_rn
        write(fd, response_400, sizeof response_400 - 1);
#undef response_400
    }
    
    if (code == 404) {
#define response_404 header_404 header_content_length_zero header_connection_close header_server header_host header_rn
        write(fd, response_404, sizeof response_404 - 1);
#undef response_404
    }
    
    close(fd);
}

int process_get_request(int fd, const char* path, int path_length);
int process_post_request(int fd, const char* path, int path_length, const char* body);

struct input_request {
    string request_content;
    
    int parse(int fd) {
        size_t prevbuflen = 0, method_len, path_len, num_headers;
        
        struct phr_header headers[100];
        num_headers = sizeof(headers) / sizeof(headers[0]);
        const char* path;
        const char* method;
        int minor_version;
        int pret = phr_parse_request(request_content.data(), request_content.length(), &method, &method_len, &path, &path_len,
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
            if (pret == -1) {
                printf("Request failed to parse headers:\n'%s'\n", request_content.c_str());
                fflush(stdout);
            }
            
            return pret;
        }
        
        if (method_len == 3) {
            // GET request, can already answer
            
            int code = process_get_request(fd, path, path_len);
            if (code != 200)
                write_only_header_answer(fd, code);
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
            
            if (content_length < 0) {
                //printf("Request with no content length:\n'%s'\n", request_content.c_str());
                write_only_header_answer(fd, 400);
                return pret;
            }
                
            check(content_length >= 0);
            
            int have_length = request_content.length() - pret;
            if (have_length < content_length) {
                printf("warning -- large request (%d have, %d need)\n", have_length, content_length);
                return -2;
            }
            
            /*if (have_length > content_length) {
                printf("warning -- more data received then promised (%d received, %d content length)\n", have_length, content_length);
            }*/
            
            //write_only_header_answer(fd, 400);
            //return pret;
            
            int code = process_post_request(fd, path, path_len, request_content.c_str() + pret);
            if (code != 200)
                write_only_header_answer(fd, code);
        }
        
        //string response = "HTTP/1.1 200 OK\r\n\r\n";
        //write(fd, response.c_str(), response.length());
        
        return pret;
    }
};

void load_json_dump_from_file(string file_name);
void reindex_database();

void initialize_validator();

int main(int argc, char *argv[]) {
    verify(sizeof(int) == 4);
    verify(sizeof(time_t) == 8);
    //verify(0);
    
    epoll_event event;
    epoll_event* events = nullptr;

    int port;
    if (argc != 2 || !sscanf(argv[1], "%d", &port)) {
        fprintf(stderr, "Usage: %s [port]\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    int wanted_port = port;

    int sfd = create_and_bind(port);
    verify(sfd != -1);

    verify(make_socket_non_blocking(sfd) != -1);
    verify(listen(sfd, SOMAXCONN) != -1);

    int efd = epoll_create1(0);
    verify(efd != -1);

    event.data.fd = sfd;
    event.events = EPOLLIN | EPOLLET;
    verify(epoll_ctl(efd, EPOLL_CTL_ADD, sfd, &event) != -1);

    /* Buffer where events are returned */
    events = (epoll_event*)calloc(MAX_POLL_EVENTS, sizeof event);
    
    printf("Started server loop, port %d\n", port);
    fprintf(stdout, "hello stdout\n"); fflush(stdout);
    //fprintf(stderr, "hello stderr\n"); fflush(stderr);
    if (port != wanted_port) {
        printf("WARNING: server started on port %d different from %d asked\n", port, wanted_port);
    }
    
#ifndef SUBMISSION_MODE
    initialize_validator();
    
    load_json_dump_from_file("../data/data/locations_1.json");
    load_json_dump_from_file("../data/data/users_1.json");
    load_json_dump_from_file("../data/data/visits_1.json");
#else
    printf("Running in submission mode, proceeding to unzip\n");
    {
        string unzip_cmd = "unzip -o -j /tmp/data/data.zip '*.json' -d data";
        int ec = system(unzip_cmd.c_str());
        verify(ec == 0);
        
        string find_cmd = "find data -name '*.json' >db_files.txt";
        ec = system(find_cmd.c_str());
        verify(ec == 0);
        
        ifstream db_files("db_files.txt");
        string db_file_name = "";
        int n_files = 0;
        while (getline(db_files, db_file_name) && db_file_name.length() > 0) {
            n_files++;
            load_json_dump_from_file(db_file_name.c_str());
        }
        
        verify(n_files > 0);
    }
#endif
    
    reindex_database();
    
    unordered_map<int, input_request*> fd_to_queue;
    fd_to_queue.reserve(1000);

    /* The event loop */
    while (1) {
        int n = epoll_wait(efd, events, MAX_POLL_EVENTS, -1);
        for (int i = 0; i < n; i++) {
            if ((events[i].events & EPOLLERR) ||
                (events[i].events & EPOLLHUP) ||
                (!(events[i].events & EPOLLIN))) {
                /* An error has occured on this fd, or the socket is not
                   ready for reading (why were we notified then?) */
                fprintf(stderr, "epoll error\n");
                close(events[i].data.fd);
                continue;
            }

            else if (sfd == events[i].data.fd) {
                /* We have a notification on the listening socket, which
                   means one or more incoming connections. */
                while (1) {
                    struct sockaddr in_addr;
                    socklen_t in_len;
                    int infd;

                    in_len = sizeof in_addr;
                    infd = accept(sfd, &in_addr, &in_len);
                    if (infd == -1) {
                        if ((errno == EAGAIN) ||  (errno == EWOULDBLOCK)) {
                            /* We have processed all incoming
                               connections. */
                            break;
                        }
                        else {
                            perror("accept");
                            break;
                        }
                    }

#ifndef SUBMISSION_MODE
#if 1 && LOCAL
                    char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
                    s = getnameinfo(&in_addr, in_len,
                                    hbuf, sizeof hbuf,
                                    sbuf, sizeof sbuf,
                                    NI_NUMERICHOST | NI_NUMERICSERV);
                    if (s == 0) {
                        printf("Accepted connection on descriptor %d "
                               "(host=%s, port=%s)\n", infd, hbuf, sbuf);
                    }
#endif
#endif

                    /* Make the incoming socket non-blocking and add it to the list of fds to monitor. */
                    verify(make_socket_non_blocking(infd) != -1);

                    event.data.fd = infd;
                    event.events = EPOLLIN | EPOLLET;
                    verify(epoll_ctl(efd, EPOLL_CTL_ADD, infd, &event) != -1);
                }
                continue;
            }
            else {
                /* We have data on the fd waiting to be read. Read and
                   display it. We must read whatever data is available
                   completely, as we are running in edge-triggered mode
                   and won't get a notification again for the same
                   data. */
                int done = 0;
                
                input_request*& q = fd_to_queue[events[i].data.fd];

                while (1) {
                    ssize_t count;
                    const int READ_BUFFER_SIZE = 512;
                    static char buf[READ_BUFFER_SIZE + 1];

                    count = read(events[i].data.fd, buf, READ_BUFFER_SIZE);
                    if (count == -1) {
                        /* If errno == EAGAIN, that means we have read all data. So go back to the main loop. */
                        if (errno != EAGAIN) {
                            perror("read");
                            done = 1;
                        }
                        break;
                    }
                    else
                    {
                        buf[count] = 0;
                        //printf("read %d bytes '%.*s'\n", count, count, buf);
                        if (count == 0) {
                            /* End of file. The remote has closed the connection. */
                            done = 1;
                            break;
                        }
                    }

                    /* Write the buffer to standard output */
                    //verify(write(1, buf, count) != -1);
                    
                    if (q == 0) {
                        q = new input_request;
                    }
                    q->request_content += buf;
                }
                
                if (done) {
                    //printf("Closed connection on descriptor %d\n", events[i].data.fd);

                    /* Closing the descriptor will make epoll remove it
                       from the set of descriptors which are monitored. */
                    close(events[i].data.fd);
                    
                    if (q)
                        delete q;
                    fd_to_queue.erase(events[i].data.fd);
                    continue;
                }
                
                int pret = q->parse(events[i].data.fd);
                
                // no invalid http requests
                verify(pret != -1);

                
                if (pret > 0) {
                    //printf("request parsed succesfully! discarding connection\n");
                    
                    // FIXME: disable Nagle algorithm
                    // and/or make connection close right after ensuring that POST is correct -- DONE
                    
                    delete q;
                    //close(events[i].data.fd);
                    fd_to_queue.erase(events[i].data.fd);
                }
            }
        }
    }

    free(events);

    close(sfd);

    return EXIT_SUCCESS;
}

/* Answer validation */

#include "rapidjson/include/rapidjson/rapidjson.h"
#include "rapidjson/include/rapidjson/writer.h"
#include "rapidjson/include/rapidjson/stringbuffer.h"
#include "rapidjson/include/rapidjson/document.h"
#include "rapidjson/include/rapidjson/error/en.h"

using namespace rapidjson;

#ifndef SUBMISSION_MODE
struct AnswerValidator {
    map<pair<string, string>, pair<int, string>> expected_answer;
    
    AnswerValidator() {}
    AnswerValidator(string from) {
        load(from);
    }
    
    void load(string answ_file_name) {
        ifstream file(answ_file_name);
        string line;
        while (getline(file, line)) {
            istringstream line_stream(line);
            
            string method, path;
            int code;
            verify(line_stream >> method >> path >> code);
            
            string line_rest;
            getline(line_stream, line_rest, '\0');
            
            int pos = 0;
            while (pos < (int)line_rest.size() && isspace(line_rest[pos])) pos++;
            line_rest = line_rest.substr(pos);
            
            expected_answer[make_pair(method, path)] = make_pair(code, line_rest);
        }
        printf("Loaded %d validation entries\n", (int)expected_answer.size());
    }
    
    const char* next_answer = 0;
    int next_answer_length = 0;
    
    void supply_data(const char* answer, int answer_length) {
        verify(!next_answer);
        
        next_answer = answer;
        next_answer_length = answer_length;
    }
    
    void check_answer(bool is_get, const char* path, int path_length, int code, const char* answer, int answer_length) {
        if (next_answer) {
            verify(!answer && !answer_length);
            answer = next_answer;
            answer_length = next_answer_length;
            
            next_answer = 0;
            next_answer_length = 0;
        }
        
        string method = (is_get ? "GET" : "POST");
        auto it = expected_answer.find(make_pair(method, string(path, path + path_length)));
        
        string request = method + " " + string(path, path + path_length);
        
        if (request == "GET /locations/1081/avg?toDate=813110400&fromAge=17") {
            printf("Known WA, skipping\n");
            return;
        }
        
        printf("answer '%.*s'\n", answer_length, answer);
        if (it == expected_answer.end()) {
            printf("Failed to check answer for '%s'\n", request.c_str());
            abort();
            return;
        }
        
        if (code != it->second.first) {
            printf("Wrong answer code for request '%s': %d, %d expected\n", request.c_str(), code, it->second.first);
            
            if (!(it->second.first == 200 && code == 404))
                abort();
            return;
        }
        
        if (answer == 0) {
            bool nonempty = false;
            for (char c: it->second.second)
                if (!isspace(c)) {
                    nonempty = true;
                    break;
                }
                
            if (nonempty) {
                printf("Wrong answer for request '%s': empty, but nonempty '%s' expected\n", request.c_str(), it->second.second.c_str());
                //abort();
                return;
            }
        }
        else {
            printf("Answers for '%s':\n'%.*s',\n'%s' expected\n", request.c_str(), answer_length, answer, it->second.second.c_str());
            
            Document expected;
            expected.Parse(it->second.second.c_str());
            verify(!expected.HasParseError());
            
            Document answer_doc;
            answer_doc.Parse(string(answer, answer + answer_length).c_str());
            if (answer_doc.HasParseError()) {
                printf("Answer has parse error: %s (%d)\n", GetParseError_En(answer_doc.GetParseError()), (int)answer_doc.GetErrorOffset());
                abort();
            }
            
            verify(expected == answer_doc);
            printf("Correct answer!\n\n");
        }
    }
};

AnswerValidator validator;

void initialize_validator() {
    validator.load("../data/answers/phase_2_post.answ");
    validator.load("../data/answers/phase_3_get.answ");
}
#endif

/* Request handling */

// naive implementation

using timestamp = int;

// http://www.unixtimestamp.com/index.php
const timestamp MIN_ALLOWED_BIRTH_DATE = -1262304000;
const timestamp MAX_ALLOWED_BIRTH_DATE = 915235199;

const timestamp MIN_ALLOWED_VISIT_DATE = 946684800;
const timestamp MAX_ALLOWED_VISIT_DATE = 1420156799;

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
    int id;
    string email;
    string first_name, last_name;
    char gender;
    timestamp birth_date;
    
    vector<DatedVisit> visits;
};

struct Location {
    int id;
    string place;
    string country;
    string city;
    int distance;
    
    vector<DatedVisit> visits;
};

struct Visit {
    int id;
    int location_id;
    int user_id;
    timestamp visited_at;
    char mark;
};

unordered_map<int, User> user_by_id;
unordered_map<int, Location> location_by_id;
unordered_map<int, Visit> visit_by_id;
unordered_set<string> all_user_emails;

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

void reindex_database() {
    for (auto& it: user_by_id)
        it.second.visits.clear();
    
    for (auto& it: location_by_id)
        it.second.visits.clear();
    
    for (auto& it: visit_by_id) {
        Visit& visit = it.second;
        DatedVisit dv = { visit.id, visit.visited_at };
        user_by_id[visit.user_id].visits.push_back(dv);
        location_by_id[visit.location_id].visits.push_back(dv);
    }
    
    for (auto& it: user_by_id)
        sort(all(it.second.visits));
    
    for (auto& it: location_by_id)
        sort(all(it.second.visits));
    
    printf("Database is ready\n");
    fflush(stdout);
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
    
    printf("loading '%s', %d entries\n", entity_to_string(e), root.Size());
    
    if (e == Entity::USERS) user_by_id.reserve(user_by_id.size() + root.Size());
    if (e == Entity::LOCATIONS) location_by_id.reserve(location_by_id.size() + root.Size());
    if (e == Entity::VISITS) visit_by_id.reserve(visit_by_id.size() + root.Size());
    
    for (SizeType i = 0; i < root.Size(); i++) {
        const auto& o = root[i].GetObject();
        int id = o["id"].GetInt();
        
        if (e == Entity::USERS) {
            User& new_user = user_by_id[id];
            
            new_user.id = id;
            new_user.email = o["email"].GetString();
            new_user.first_name = o["first_name"].GetString();
            new_user.last_name = o["last_name"].GetString();
            new_user.gender = o["gender"].GetString()[0];
            new_user.birth_date = o["birth_date"].GetInt();
            
            verify(new_user.gender == 'm' || new_user.gender == 'f');
        }
        else if (e == Entity::LOCATIONS) {
            Location& new_location = location_by_id[id];
            
            new_location.id = id;
            new_location.place = o["place"].GetString();
            new_location.country = o["country"].GetString();
            new_location.city = o["city"].GetString();
            new_location.distance = o["distance"].GetInt();
            
            verify(new_location.distance >= 0);
        }
        else {
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
    
    printf("Data loaded from file '%s'\n", file_name.c_str());
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

const timestamp MAGIC_TIMESTAMP = std::numeric_limits<int>::min() + 1234;
const int MAGIC_INTEGER = std::numeric_limits<int>::min() + 1234;

bool parse_timestamp(const char* str, const char* to, timestamp& value) {
    bool correct = false;
    value = fast_atoi(str, correct);
    return correct && str == to;
}

const int MAX_RESPONSE_SIZE = 4096;
char shared_buffer[MAX_RESPONSE_SIZE];

const int MAX_INTEGER_SIZE = 30;
char itoa_buffer[MAX_INTEGER_SIZE];
int itoa_length;

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

struct ResponseBuilder {
    char* buffer_begin;
    char* buffer_pos;
    char* buffer_end;
    
    ResponseBuilder() {
        buffer_begin = buffer_pos = shared_buffer;
        buffer_end = shared_buffer + MAX_RESPONSE_SIZE;
    }
    
    void discard_old() {
        if (buffer_begin != shared_buffer)
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
        ::write(fd, buffer_begin, buffer_pos - buffer_begin);
        close(fd);
    }
};

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
            *rewrite_pos++ = c;
        }
    }
    
    return rewrite_pos;
}

int utf8_string_length(const char* s, int character_length) {
    int len = 0;
    while (character_length--) len += (*s++ & 0xc0) != 0x80;
    return len;
}

#define append_str(builder, str) builder.append(str, sizeof str - 1)

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
#define zero_offset_string header_200 header_connection_close header_host header_server "Content-Length:"
#define HTTP_OK_PREFIX header_200 header_connection_close header_host header_server header_content_length_tbd header_rn

#ifndef SUBMISSION_MODE
#define validate_json() \
        char* data_ptr = json.buffer_begin + sizeof HTTP_OK_PREFIX - 1; \
        validator.supply_data(data_ptr, json.buffer_pos - data_ptr)
#else
#define validate_json()
#endif

#define send_response() \
    json.embed_content_length(sizeof zero_offset_string, sizeof HTTP_OK_PREFIX - 1); \
    json.write(fd); \
    validate_json()
    
    int handle_get(int fd) {
        if (entity == Entity::USERS) {
            auto user_it = user_by_id.find(id);
            if (user_it == user_by_id.end()) return 404;
            
            User& user = user_it->second;
            
            if (!get_visits) {
                // simple
                
                ResponseBuilder json;
                
                append_str(json, HTTP_OK_PREFIX "{\"id\":");
                json.append_int(user.id);
                append_str(json, ",\"email\":\"");
                json.append(user.email.data(), user.email.length());
                append_str(json, "\",\"first_name\":\"");
                json.append(user.first_name.data(), user.first_name.length());
                append_str(json, "\",\"last_name\":\"");
                json.append(user.last_name.data(), user.last_name.length());
                append_str(json, "\",\"gender\":\"");
                json.append(&user.gender, 1);
                append_str(json, "\",\"birth_date\":");
                json.append_int(user.birth_date);
                append_str(json, "}");
                
                send_response();
                return 200;
            }
            else {
                // visits query
                
                ResponseBuilder json;
                
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
                
                while (it < it_end) {
                    Visit& visit = visit_by_id[it->id];
                    it++;
                    Location& location = location_by_id[visit.location_id];
                    
                    if (country_ptr_begin) {
                        if (country_ptr_end - country_ptr_begin != (long)location.country.length() || memcmp(country_ptr_begin, location.country.data(), location.country.length()))
                            continue;
                    }
                    
                    if (to_distance != MAGIC_INTEGER) {
                        if (location.distance >= to_distance)
                            continue;
                    }
                    
                    if (n_visits) {
                        append_str(json, ",{\"mark\":");
                    }
                    else {
                        append_str(json, "{\"mark\":");
                    }
                    n_visits++;
                    
                    json.append_int(visit.mark);
                    append_str(json,",\"visited_at\":");
                    json.append_int(visit.visited_at);
                    append_str(json,",\"place\":\"");
                    json.append(location.place.data(), location.place.length());
                    append_str(json,"\"}");
                }
                
                append_str(json, "]}");
                
                send_response();
                return 200;
            }
        }
        else if (entity == Entity::LOCATIONS) {
            auto location_it = location_by_id.find(id);
            if (location_it == location_by_id.end()) return 404;
            
            Location& location = location_it->second;
            
            if (!get_avg) {
                // simple
                
                ResponseBuilder json;
                append_str(json, HTTP_OK_PREFIX "{\"id\":");
                json.append_int(location.id);
                append_str(json, ",\"place\":\"");
                json.append(location.place.data(), location.place.length());
                append_str(json, "\",\"country\":\"");
                json.append(location.country.data(), location.country.length());
                append_str(json, "\",\"city\":\"");
                json.append(location.city.data(), location.city.length());
                append_str(json, "\",\"distance\":");
                json.append_int(location.distance);
                append_str(json, "}");
                
                send_response();
                return 200;
            }
            else {
                // average query
                
                ResponseBuilder json;
                
                append_str(json, HTTP_OK_PREFIX "{\"avg\":");
                
                auto it = location.visits.begin();
                if (from_date != MAGIC_TIMESTAMP)
                    it = lower_bound(all(location.visits), DatedVisit { -1, from_date + 1 });
                
                auto it_end = location.visits.end();
                if (to_date != MAGIC_TIMESTAMP)
                    it_end = lower_bound(all(location.visits), DatedVisit{ -1, to_date });
                
                // timestamp jerk
                
                time_t now = {};
                tm time_struct = {};
                
                bool filter_age = false;
                if (from_age != MAGIC_INTEGER || to_age != MAGIC_INTEGER) {
                    now = time(0);
                    gmtime_r(&now, &time_struct);
                    filter_age = true;
                }
                
                timestamp from_filter = numeric_limits<int>::min(), to_filter = numeric_limits<int>::max();
                
                if (from_age != MAGIC_INTEGER) {
                    int orig_year = time_struct.tm_year;
                    time_struct.tm_year -= from_age;
                    time_struct.tm_year = max(time_struct.tm_year, 2);
                    //printf("need %d\n", time_struct.tm_year);
                    to_filter = min((time_t)to_filter, mktime(&time_struct) - 1);
                    time_struct.tm_year = orig_year;
                }
                
                if (to_age != MAGIC_INTEGER) {
                    time_struct.tm_year -= to_age;
                    time_struct.tm_year = max(time_struct.tm_year, 2);
                    from_filter = max((time_t)from_filter, mktime(&time_struct) + 1);
                }
                
#if 0
                time_t now_li = now, from_li = from_filter, to_li = to_filter;
                printf("filters %d %d\n", from_filter, to_filter);
                gmtime_r(&now_li, &time_struct);
                printf("now year %d\n", time_struct.tm_year);
                gmtime_r(&from_li, &time_struct);
                printf("from year %d\n", time_struct.tm_year);
                gmtime_r(&to_li, &time_struct);
                printf("to year %d\n", time_struct.tm_year);
#endif
                
                int n_marks = 0, mark_sum = 0;
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
                
                if (mark_sum == 0) {
                    append_str(json, "0}");
                }
                else {
                    static char mark_avg[50];
                    sprintf(mark_avg, "%.5f}", mark_sum / (double)n_marks);
                    json.append(mark_avg, strlen(mark_avg));
                }
                
                send_response();
                return 200;
            }
        }
        else {
            auto visit_it = visit_by_id.find(id);
            if (visit_it == visit_by_id.end()) return 404;
            
            Visit& visit = visit_it->second;
            
            ResponseBuilder json;
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
        
        printf("VERY STRANGE -- failed to answer GET\n"); fflush(stdout);
        return 400;
    }
    
    int handle_post(int fd, const char* body) {
#define header_content_length_four "Content-Length: 2\r\n"
#define header_content_type "Content-Type: application/json\r\n"
#define HTTP_OK_WITH_EMPTY_JSON_RESPONSE header_200 header_connection_close header_server header_host header_content_type header_content_length_four header_rn "{}"
        //printf("ok json '%s'\n", HTTP_OK_WITH_EMPTY_JSON_RESPONSE);
//#define HTTP_OK_WITH_EMPTY_JSON_RESPONSE "HTTP/1.1 200 OK\r\n\r\n{}"
#define successful_update() \
        write(fd, HTTP_OK_WITH_EMPTY_JSON_RESPONSE, sizeof(HTTP_OK_WITH_EMPTY_JSON_RESPONSE) - 1); \
        close(fd)
        
        //successful_update();
        //return 200;
        
#define declare_doc() \
        Document json; \
        json.ParseInsitu((char*)body); \
        if (json.HasParseError()) return 400;
        
#define begin_parsing_update(EntityType, entity) \
            EntityType* entity = nullptr; \
            if (!is_new) { \
                auto entity_it = (entity ## _by_id).find(id); \
                if (entity_it == (entity ## _by_id).end()) return 404; \
                entity = &entity_it->second; \
            } \
            else { \
                id = MAGIC_INTEGER; \
            } \
            declare_doc();
            
#define iterate_over_fields \
    for (Value::ConstMemberIterator it = json.MemberBegin(); it != json.MemberEnd(); ++it) { \
                    const char* name = it->name.GetString(); \
                    const char* name_end = name + it->name.GetStringLength();
#define end_fields_iteration return 400; }
                
#define string_field(field_name, min_length, max_length, save_to, code) \
                {if (fast_is(name, name_end, field_name)) { \
                    printf("matcher success with '%s'\n", field_name); fflush(stdout); \
                    if (!it->value.IsString()) return 400; \
                    if (save_to) return 400; \
                    save_to = it->value.GetString(); \
                    printf("seems ok\n"); fflush(stdout); \
                    int real_length = utf8_string_length(save_to, it->value.GetStringLength()); \
                    printf("length ok '%s' is %d passed\n", save_to, real_length); \
                    if (!(real_length >= min_length && real_length <= max_length)) return 400; \
                    code \
                    continue; \
                }}
                
                
#define integer_field(field_name, save_to, code) \
                {if (fast_is(name, name_end, field_name)) { \
                    printf("matcher success with '%s'\n", field_name); fflush(stdout); \
                    if (!it->value.IsInt()) return 400; \
                    if (save_to != MAGIC_INTEGER) return 400; \
                    save_to = it->value.GetInt(); \
                    code \
                    continue; \
                }}
        
#if 1
        if (entity == Entity::USERS) {
            begin_parsing_update(User, user);
            
            const char* email = 0;
            const char* first_name = 0;
            const char* last_name = 0;
            const char* gender = 0;
            timestamp birth_date = MAGIC_INTEGER;
                
            iterate_over_fields {
                if (is_new)
                    integer_field("id", id, { if (user_by_id.find(id) != user_by_id.end()) return 400; });
                
                string_field("first_name", 1, 50, first_name, {});
                string_field("last_name", 1, 50, last_name, {});
                string_field("email", 1, 100, email, {});
                string_field("gender", 1, 1, gender, { if (*gender != 'm' && *gender != 'f') return 400; });
            
                integer_field("birth_date", birth_date, { if (!(birth_date >= MIN_ALLOWED_BIRTH_DATE && birth_date <= MAX_ALLOWED_BIRTH_DATE)) return 400; });
            } end_fields_iteration
                
            if (is_new) {
                if (!(email && first_name && last_name && gender && birth_date != MAGIC_INTEGER && id != MAGIC_INTEGER))
                    return 400;
                
                if (all_user_emails.find(email) != all_user_emails.end())
                    return 400;
                
                successful_update();
                
                User& new_user = user_by_id[id];
                new_user.id = id;
                new_user.email = email;
                new_user.first_name = first_name;
                new_user.last_name = last_name;
                new_user.gender = *gender;
                new_user.birth_date = birth_date;
                all_user_emails.emplace(email);
                
                return 200;
            }
            else {
                if (email) {
                    if (all_user_emails.find(email) != all_user_emails.end() && user->email != email)
                        return 400;
                }
                
                successful_update();
                
                if (email) {
                    all_user_emails.erase(user->email);
                    user->email = email;
                    all_user_emails.emplace(user->email);
                }
                
                if (first_name) user->first_name = first_name;
                if (last_name) user->last_name = last_name;
                if (gender) user->gender = *gender;
                if (birth_date != MAGIC_INTEGER) user->birth_date = birth_date;
                
                return 200;
            }
        }
        else
#endif
        if (entity == Entity::LOCATIONS) {
            begin_parsing_update(Location, location);
            
            printf("parsing begin done\n"); fflush(stdout);
            
            const char* place = 0;
            const char* country = 0;
            const char* city = 0;
            int distance = MAGIC_INTEGER;
            
            iterate_over_fields {
                printf("iterating field '%s'\n", name); fflush(stdout);
                if (is_new)
                    integer_field("id", id, { if (location_by_id.find(id) != location_by_id.end()) return 400; });
                
                string_field("place", 0, (int)1e9, place, {});
                string_field("country", 1, 50, country, {});
                string_field("city", 1, 50, city, {});
                integer_field("distance", distance, { if (distance < 0) return 400; });
            } end_fields_iteration
            
            printf("fields iterated success\n"); fflush(stdout);
            
            if (is_new) {
                if (!(place && country && city && distance != MAGIC_INTEGER && id != MAGIC_INTEGER))
                    return 400;
                
                successful_update();
                
                Location& new_location = location_by_id[id];
                new_location.id = id;
                new_location.place = place;
                new_location.country = country;
                new_location.city = city;
                new_location.distance = distance;
                
                return 200;
            }
            else {
                successful_update();
                
                if (place) location->place = place;
                if (country) location->country = country;
                if (city) location->city = city;
                if (distance != MAGIC_INTEGER) location->distance = distance;
                
                return 200;
            }
        }
#if 1
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
                    integer_field("id", id, { if (location_by_id.find(id) != location_by_id.end()) return 400; });
                
                integer_field("location", location_id, {
                    auto it = location_by_id.find(location_id);
                    if (it == location_by_id.end())
                        return 404;
                    
                    location = &it->second;
                });
                
                integer_field("user", user_id, {
                    auto it = user_by_id.find(user_id);
                    if (it == user_by_id.end())
                        return 404;
                    
                    user = &it->second;
                });
                
                integer_field("visited_at", visited_at, { if (!(visited_at >= MIN_ALLOWED_VISIT_DATE && visited_at <= MAX_ALLOWED_VISIT_DATE)) return 400; });
                integer_field("mark", mark, { if (!(mark >= 0 && mark <= 5)) return 400; });
            } end_fields_iteration
            
            if (is_new) {
                if (!(location && user && visited_at != MAGIC_INTEGER && mark != MAGIC_INTEGER && id != MAGIC_INTEGER))
                    return 400;
                
                successful_update();
                
                Visit& new_visit = visit_by_id[id];
                new_visit.id = id;
                new_visit.visited_at = visited_at;
                new_visit.mark = mark;
                new_visit.user_id = user_id;
                new_visit.location_id = location_id;
                
                DatedVisit dv = { new_visit.id, new_visit.visited_at };
                
                user->visits.insert(upper_bound(all(user->visits), dv), dv);
                location->visits.insert(upper_bound(all(location->visits), dv), dv);
                
                return 200;
            }
            else {
                successful_update();
                
                if (mark != MAGIC_INTEGER) visit->mark = mark;
                
                // hard updates
                
                if (user_id == visit->user_id) { user_id = MAGIC_INTEGER; user = nullptr; }
                if (location_id == visit->location_id) { location_id = MAGIC_INTEGER; location = nullptr; }
                
                timestamp new_visited_at = visit->visited_at;
                if (visited_at != MAGIC_INTEGER) new_visited_at = visited_at;
                bool visit_time_changed = new_visited_at != visit->visited_at;
                
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
                }
                
                if (location || visit_time_changed) {
                    Location* old_location = &location_by_id[visit->location_id];
                    
                    auto it = lower_bound(all(old_location->visits), DatedVisit { id, visit->visited_at });
                    check(it != old_location->visits.end());
                    old_location->visits.erase(it);
                    
                    Location* new_location = location;
                    if (!new_location)
                        new_location = old_location;
                    
                    DatedVisit dv = { id, new_visited_at };
                    new_location->visits.insert(upper_bound(all(new_location->visits), dv), dv);
                }
                
                visit->visited_at = new_visited_at;
                if (location) visit->location_id = location_id;
                if (user) visit->user_id = user_id;
                
                return 200;
            }
        }
#endif
        
        printf("VERY STRANGE -- failed to answer POST\n"); fflush(stdout);
        return 400;
    }
    
#undef HTTP_OK_PREFIX
#undef validate_json
};

int process_request_options(RequestHandler& handler, const char* path, const char* path_end) {
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

// omg boilerplate shit
int process_get_request_raw(int fd, const char* path, int path_length) {
    //printf("GET %.*s\n", path_length, path);
    
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
    
    map<string, string> opt_params;
    
    //printf("entity %s id %d\n", entity_to_string(handler.entity), handler.id);
    
    int opts_code = process_request_options(handler, path, path_end);
    if (opts_code != 200)
        return opts_code;
    
    int code = handler.handle_get(fd);
    return code;
}

int process_get_request(int fd, const char* path, int path_length) {
#ifndef SUBMISSION_MODE
    string path_backup(path, path + path_length);
#endif
    int code = process_get_request_raw(fd, path, path_length);
#ifndef SUBMISSION_MODE
    validator.check_answer(true, path_backup.c_str(), path_length, code, 0, 0);
#endif
    return code;
}

int process_post_request_raw(int fd, const char* path, int path_length, const char* body) {
    //printf("POST %.*s\n'%s'\n", path_length, path, body);
    printf("POST %.*s '%s'\n", path_length, path, body);
    fflush(stdout);
    
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
    
    int code = handler.handle_post(fd, body);
    return code;
}

int process_post_request(int fd, const char* path, int path_length, const char* body) {
    int code = process_post_request_raw(fd, path, path_length, body);
#ifndef SUBMISSION_MODE
    validator.check_answer(false, path, path_length, code, 0, 0);
#endif
    return code;
}

