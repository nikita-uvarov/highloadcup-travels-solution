#!/bin/bash

sudo docker build -t dumb . && sudo docker run -it -v /tmp/data/:/tmp/data -p 80:80 --net host dumb
