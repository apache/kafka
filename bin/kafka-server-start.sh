#!/bin/bash

if [ $# -lt 1 ];
then
	echo "USAGE: $0 server.properties [consumer.properties]"
	exit 1
fi

export JMX_PORT="9999"

$(dirname $0)/kafka-run-class.sh kafka.Kafka $@
