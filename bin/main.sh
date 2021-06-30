#!/bin/bash
# main script, automate the creation and configuration of topics, start a first producer + consumer
BROK_1="kafka1:9091";
BROK_2="kafka2:9092";
ZK_1="zookeeper1:2181";
ZK_2="zookeeper2:2182";
TOPIC="test_topic";
MAX_ITER="100";
GROUP_1="consumer_group_1"
LOG_FILE_1="kafka_1.log";
LOG_FILE_2="kafka_2.log";

configure_topic() {
  echo "configure $1"
  (
    source kafka-topics.sh \
    --bootstrap-server $1 \
    --create \
    --topic $TOPIC \
    --partitions 30 \
    --replication-factor 1 \
    --config delete.retention.ms=60000
  ) &
}

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
    --group $3 >> $4 &
}

start_producer() {
  c="0";
  while [ $c -le $MAX_ITER ]
  do
    (
      echo -ne "Key$c:$c" | source kafka-console-producer.sh \
    --broker-list $1 \
    --topic $TOPIC \
    --property "parse.key=true" \
    --property "key.separator=:";
    ) &
    sleep 1;
    ((++c))
  done
}
echo -n > $LOG_FILE_1;
echo -n > $LOG_FILE_2;
configure_topic $BROK_1;
configure_topic $BROK_2;
sleep 5;
start_consumer $BROK_1 $TOPIC $GROUP_1 $LOG_FILE_1;
sleep 5;
start_producer $BROK_1;
exit 0;

