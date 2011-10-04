#!/bin/bash

base_dir=$(dirname $0)
$base_dir/kafka-run-class.sh kafka.producer.ConsoleProducer $@
