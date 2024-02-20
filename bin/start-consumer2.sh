#!/bin/bash
# for manual testing consumer needs
BROK_2="kafka2:9092";
ZK_2="zookeeper2:2182";
TOPIC="test_topic";
GROUP_2="consumer_group_2"
LOG_FILE_2="kafka_2.log";

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
    --consumer-property request.timeout.ms=6000000 \
    --group $3 >> $4 &
}
echo -n > $LOG_FILE_2;
start_consumer $BROK_2 $TOPIC $GROUP_2 $LOG_FILE_2;
exit 0;