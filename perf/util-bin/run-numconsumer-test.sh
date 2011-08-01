#!/bin/bash

REMOTE_KAFKA_LOGIN=$1 # user@host format
REMOTE_SIM_LOGIN=$2
TEST_TIME=$3
REPORT_FILE=$4

. `dirname $0`/remote-kafka-env.sh

kafka_startup
# You need to twidle this time value depending on test time below
ssh $REMOTE_SIM_LOGIN "$SIMULATOR_SCRIPT -kafkaServer=$KAFKA_SERVER -numTopic=1  -reportFile=$REPORT_FILE -time=7 -numConsumer=0 -numProducer=10 -xaxis=numConsumer"
sleep 20

for i in 1 `seq -s " " 10 10 50` ;
do
    ssh $REMOTE_SIM_LOGIN "$SIMULATOR_SCRIPT -kafkaServer=$KAFKA_SERVER -numTopic=1  -reportFile=$REPORT_FILE -time=$TEST_TIME -numConsumer=$i -numProducer=0 -xaxis=numConsumer"
    sleep 10
done

kafka_cleanup
