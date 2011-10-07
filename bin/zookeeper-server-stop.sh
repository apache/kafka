#!/bin/sh
ps ax | grep -i 'zookeeper' | grep -v grep | awk '{print $1}' | xargs kill -SIGINT
