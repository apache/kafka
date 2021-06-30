#!/bin/bash
# for manual testing consumer needs
BROK_1="kafka1:9091";
ZK_1="zookeeper1:2181";
TOPIC="test_topic";
GROUP_1="consumer_group_1"
LOG_FILE_1="kafka_1.log";

start_consumer() {
  echo "writing to $4"
  DELAY=$5;
  if [ -z $5 ]
  then
    DELAY=0
  fi
    sleep $DELAY;
    source kafka-console-consumer.sh \
    --bootstrap-server $1 \
    --topic $2 \
    --consumer-property request.timeout.ms=60000 \
    --group $3 >> $4
}

start_consumer $BROK_1 $TOPIC $GROUP_1 $LOG_FILE_1;
exit 0;