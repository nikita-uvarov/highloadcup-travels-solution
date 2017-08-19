FROM mybaseimage

COPY main.cpp database.cpp validator.cpp network.cpp profiler.cpp utils.cpp http_support.cpp response_builder.cpp libs ./
RUN g++ -Wall -Wextra -Wno-missing-field-initializers -DSUBMISSION_MODE -DON_SERVER -oFast -o main.elf -std=gnu++11 -march=native -flto main.cpp picohttpparser/picohttpparser.c -lpthread
#RUN gcc -march=native -Q --help=target
CMD ("./main.elf" 80 || echo "Solution has crashed")
