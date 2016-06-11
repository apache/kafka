#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Clean-up script to reset a Kafka Streams (v0.10.0.0) application for reprocessing from very beginning
#
# deletes the content of all specified topics by setting topic retention time to 1ms (and restore original value afterwards)
#

if [ $# -lt 1 ]
then
  echo "ERROR: parameter <zookeeper> is missing"
  echo "usage: ./kafka-streams-cleanup.sh [--help] [<zookeeper> <topics>*]"
  exit 1
fi

if [ $1 == '--help' ]
then
  echo "usage: ./kafka-streams-cleanup.sh [--help] [<zookeeper> <topics>*]"
  exit 0
fi

zookeeper=$1;shift
int_sink_topics=$@

for topic in $int_sink_topics
do
  # get current value for "retention.ms"
  line=`./bin/kafka-configs.sh --zookeeper $zookeeper --describe --entity-type topics --entity-name $topic`
  config=`echo $line | cut -d' ' -f 5 | sed 's/,/ /g'`

  for token in $config
  do
    if [[ $token == retention.ms=* ]]
    then
      retention=`echo $token | cut -d'=' -f 2`
    fi
  done

  # set retention time to a tiny value, to trigger Kafka's internal clean-up meachnism
  ./bin/kafka-configs.sh --zookeeper $zookeeper --alter --entity-name $topic --entity-type topics --add-config retention.ms=1

  # restore retention time
  if [ $retention ]
  then
    # original value
    ./bin/kafka-configs.sh --zookeeper $zookeeper --alter --entity-name $topic --entity-type topics --add-config retention.ms=$retention
  else
    # default value
    ./bin/kafka-configs.sh --zookeeper $zookeeper --alter --entity-name $topic --entity-type topics --delete-config retention.ms
  fi
done



