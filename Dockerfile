FROM mybaseimage

COPY main.cpp libs ./
RUN g++ -DSUBMISSION_MODE -DON_SERVER -oFast -o main.elf -std=gnu++11 -march=native -flto main.cpp picohttpparser/picohttpparser.c
#RUN gcc -march=native -Q --help=target
CMD ("./main.elf" 80 || echo "Solution has crashed")
