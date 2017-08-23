FROM mybaseimage

COPY main.cpp database.cpp validator.cpp network.cpp profiler.cpp utils.cpp http_support.cpp response_builder.cpp libs ./
RUN g++ -O2 -Wall -Wextra -Wno-missing-field-initializers -DSUBMISSION_MODE -DON_SERVER -o main.elf -std=gnu++11 -flto main.cpp picohttpparser/picohttpparser.c -lpthread
#RUN gcc -march=native -Q --help=target
#CMD (for file in `find /sys/fs/cgroup/cpuset -type f`; do echo -n "$file: "; cat $file; done;);
CMD ("./main.elf" 80 || echo "Solution has crashed")
