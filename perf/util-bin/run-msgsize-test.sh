#!/bin/bash

REMOTE_KAFKA_LOGIN=$1 # user@host format
REMOTE_SIM_LOGIN=$2
TEST_TIME=$3
REPORT_FILE=$4

. `dirname $0`/remote-kafka-env.sh

for i in 1 200 `seq -s " " 1000 1000 10000` ;
do
    kafka_startup
    ssh $REMOTE_SIM_LOGIN "$SIMULATOR_SCRIPT -kafkaServer=$KAFKA_SERVER -numTopic=10  -reportFile=$REPORT_FILE -time=$TEST_TIME -numConsumer=20 -numProducer=40 -xaxis=msgSize -msgSize=$i"
    kafka_cleanup
done
