#ifndef DISABLE_VALIDATE
thread_local const char* next_answer = 0;
thread_local int next_answer_length = 0;
    
struct AnswerValidator {
    map<pair<string, string>, pair<int, string>> expected_answer;
    
    AnswerValidator() {}
    AnswerValidator(string from) {
        load(from);
    }
    
    void load(string answ_file_name) {
        ifstream file(answ_file_name);
        verify(file);
        printf("Loading validation from '%s'\n", answ_file_name.c_str());
        verify(file);
        
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
    
    // fixme
    
    void supply_data(const char* answer, int answer_length) {
        verify(!next_answer);
        
        next_answer = answer;
        next_answer_length = answer_length;
    }
    
    mutex validator_mutex;
    
    void check_answer(bool is_get, const char* path, int path_length, int code, const char* answer, int answer_length) {
        if (next_answer) {
            verify(!answer && !answer_length);
            answer = next_answer;
            answer_length = next_answer_length;
            
            next_answer = 0;
            next_answer_length = 0;
        }
        
        unique_lock<mutex> lock(validator_mutex);
        
        static bool post_loaded = false;
        if (!is_get && !post_loaded) {
            
            expected_answer.clear();
            post_loaded = true;
            load("/tmp/answers/phase_2_post.answ");
            load("/tmp/answers/phase_3_get.answ");
            printf("Validator switched to phases 2-3\n");
        }
        
        string method = (is_get ? "GET" : "POST");
        auto it = expected_answer.find(make_pair(method, string(path, path + path_length)));
        
        string request = method + " " + string(path, path + path_length);
        
#if 0
        if (request == "GET /locations/1081/avg?toDate=813110400&fromAge=17") {
            printf("Known WA, skipping\n");
            return;
        }
#endif
        
        //printf("answer '%.*s'\n", answer_length, answer);
        if (it == expected_answer.end()) {
            printf("Failed to check answer for '%s'\n", request.c_str());
            abort();
            return;
        }
        
        if (code != it->second.first) {
            printf("Wrong answer code for request '%s': %d, %d expected\n", request.c_str(), code, it->second.first);
            
            //if (!(it->second.first == 200 && code == 404))
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
            //printf("Answers for '%s':\n'%.*s',\n'%s' expected\n", request.c_str(), answer_length, answer, it->second.second.c_str());
            
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
            //printf("Correct answer!\n\n");
        }
    }
};

AnswerValidator validator;

void initialize_validator() {
    //validator.load("../data/answers/phase_2_post.answ");
    //validator.load("../data/answers/phase_3_get.answ");
    
    validator.load("/tmp/answers/phase_1_get.answ");
}
#endif
