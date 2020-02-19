#!/bin/sh
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
SIGNAL=${SIGNAL:-TERM}
PIDS=$(ps ax | grep -i 'kafka\.Kafka' | grep java | grep -v grep | awk '{print $1}')
PIDS_SUPPORT=$(ps ax | grep -i 'io\.confluent\.support\.metrics\.SupportedKafka' | grep java | grep -v grep | awk '{print $1}')

if [ -z "$PIDS" ]; then
  # Normal Kafka is not running, but maybe we are running the support wrapper?
  if [ -z "${PIDS_SUPPORT}" ]; then
    echo "No kafka server to stop"
    exit 1
  else
    kill -s $SIGNAL $PIDS_SUPPORT
  fi
else
  kill -s $SIGNAL $PIDS
fi
