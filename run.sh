#!/bin/bash

sudo docker build -t dumb . && sudo docker run --ulimit memlock=4294967296:4294967296 -it -v /tmp:/tmp -v $(pwd):/var/loadtest -p 80:80 --net host dumb
