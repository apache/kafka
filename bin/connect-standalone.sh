#!/bin/bash
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

if [ $# -lt 1 ];
then
        echo "USAGE: $0 [-daemon] connect-standalone.properties [connector1.properties connector2.json ...]"
        exit 1
fi

base_dir=$(dirname $0)

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
    export KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=$base_dir/../config/connect-log4j2.yaml"
elif echo "$KAFKA_LOG4J_OPTS" | grep -qE "log4j\.[^[:space:]]+$"; then
    echo DEPRECATED: A Log4j 1.x configuration file has been detected, which is no longer recommended. >&2
    echo To use a Log4j 2.x configuration, please see https://logging.apache.org/log4j/2.x/migrate-from-log4j1.html#Log4j2ConfigurationFormat for details about Log4j configuration file migration. >&2
    echo You can also use the \$KAFKA_HOME/config/connect-log4j2.yaml file as a starting point. Make sure to remove the Log4j 1.x configuration after completing the migration. >&2
fi

if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
  export KAFKA_HEAP_OPTS="-Xms256M -Xmx2G"
fi

EXTRA_ARGS=${EXTRA_ARGS-'-name connectStandalone'}

COMMAND=$1
case $COMMAND in
  -daemon)
    EXTRA_ARGS="-daemon "$EXTRA_ARGS
    shift
    ;;
  *)
    ;;
esac

exec $(dirname $0)/kafka-run-class.sh $EXTRA_ARGS org.apache.kafka.connect.cli.ConnectStandalone "$@"
