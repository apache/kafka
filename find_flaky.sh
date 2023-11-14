#!/usr/bin/env bash

set -e
mkdir -p logs/

for i in {1..10}; do 
	echo "run $i, logging to log/flaky_run_$i.log"
	./gradlew -PmaxParallelForks=2 --quiet cleanTest --info :streams:test \
      --tests "org.apache.kafka.streams.integration.ConsistencyVectorIntegrationTest" \
      --tests "org.apache.kafka.streams.integration.EosIntegrationTest" \
      --tests "org.apache.kafka.streams.integration.NamedTopologyIntegrationTest" \
	   > logs/flaky_run_$i.log
done
