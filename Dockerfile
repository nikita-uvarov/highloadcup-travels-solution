FROM mybaseimage

COPY main.cpp database.cpp validator.cpp network.cpp profiler.cpp utils.cpp http_support.cpp response_builder.cpp mtqueue.cpp nofree.cpp spammer.cpp smart_spammer.cpp libs ./
RUN /opt/rh/devtoolset-6/root/bin/g++ -O2 -Wall -Wextra -Wno-missing-field-initializers -DSUBMISSION_MODE -DON_SERVER -o main.elf -std=gnu++17 -march=sandybridge -flto main.cpp picohttpparser/picohttpparser.c -lpthread -lrt && /opt/rh/devtoolset-6/root/bin/g++ -O2 spammer.cpp -o spammer.elf -Wall -Wextra -lpthread && /opt/rh/devtoolset-6/root/bin/g++ -Wall -Wextra -O2 smart_spammer.cpp -o smart_spammer.elf
#RUN gcc -march=native -Q --help=target
#CMD (for file in `find /sys/fs/cgroup/cpuset -type f`; do echo -n "$file: "; cat $file; done;);
CMD uname -a; ("./main.elf" 80 || echo "Solution has crashed")
