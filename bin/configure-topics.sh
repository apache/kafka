#!/bin/bash
# for manual testing initial condition setup
BROK_1="kafka1:9091";
BROK_2="kafka2:9092";
ZK_1="zookeeper1:2181";
ZK_2="zookeeper2:2182";
TOPIC="test_topic";
GROUP_1="consumer_group_1"
GROUP_2="consumer_group_2"
GROUP_3="mirror_group"
CONSUMER_DELAY="10";
LOG_FILE_1="kafka_1.log";
LOG_FILE_2="kafka_2.log";
let "c = 0";

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

echo -n > $LOG_FILE_1;
echo -n > $LOG_FILE_2;
configure_topic $BROK_1;
configure_topic $BROK_2;
exit 0;

