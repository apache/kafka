#!/bin/bash
TOPIC=$1;
source /usr/bin/kafka-mirror-maker \
--consumer.config /root/config/consumer.properties \
--producer.config /root/config/producer.properties \
--whitelist $TOPIC;