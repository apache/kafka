#!/bin/bash

base_dir=$(dirname $0)
export KAFKA_OPTS="-Xmx512M -server -Dcom.sun.management.jmxremote -Dlog4j.configuration=file:$base_dir/../config/log4j.properties"
$base_dir/kafka-run-class.sh kafka.tools.ReplayLogProducer $@
