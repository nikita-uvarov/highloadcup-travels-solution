#FROM gcc:7.1
FROM centos:7

WORKDIR /root
ADD main.cpp .
COPY picohttpparser/ picohttpparser
COPY rapidjson/ rapidjson

RUN yum -y install gcc gcc-c++ unzip find
RUN g++ -DSUBMISSION_MODE -oFast -o main.elf -std=gnu++11 -march=native -flto main.cpp picohttpparser/picohttpparser.c

EXPOSE 80

CMD ("./main.elf" 80 || echo "Solution has crashed")
