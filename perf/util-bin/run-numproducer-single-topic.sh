#!/bin/bash

REMOTE_KAFKA_LOGIN=$1 # user@host format
REMOTE_SIM_LOGIN=$2
TEST_TIME=$3
REPORT_FILE=$4

. `dirname $0`/remote-kafka-env.sh

for i in 1 `seq -s " " 10 10 50` ;
do
    kafka_startup
    ssh $REMOTE_SIM_LOGIN "$SIMULATOR_SCRIPT -kafkaServer=$KAFKA_SERVER -numTopic=1  -reportFile=$REPORT_FILE -time=$TEST_TIME -numConsumer=0 -numProducer=$i -xaxis=numProducer"
    kafka_cleanup
done
