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
  echo "USAGE: $0 [-daemon] [-name servicename] [-loggc] classname [opts]"
  exit 1
fi

base_dir=$(dirname $0)/..

# classpath addition for release
for file in "$base_dir"/libs/*;
do
    CLASSPATH="$CLASSPATH":"$file"
done

# JMX settings
if [ -z "$KAFKA_JMX_OPTS" ]; then
  KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false "
fi

# JMX port to use
if [  $JMX_PORT ]; then
  KAFKA_JMX_OPTS="$KAFKA_JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT "
fi

# Log directory to use
if [ "x$LOG_DIR" = "x" ]; then
  LOG_DIR="$base_dir/logs"
fi

# Log4j settings
if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$base_dir/config/log4j.properties"
fi
KAFKA_LOG4J_OPTS="-Dkafka.logs.dir=$LOG_DIR $KAFKA_LOG4J_OPTS"

# IBM OpenJ9 JVM Performance Options
#
# The choice between -Xgc:concurrentScavenge and gencon (the default) depends on your performance priorities.
#
# Choose gencon (default):
#   - Good for balanced performance between throughput and pause times.
#   - Ideal if maximizing message throughput is the primary goal and occasional short pauses are tolerable.
#
# Choose -Xgc:concurrentScavenge:
#   - Aims for extremely low and consistent pause times, which is critical for latency-sensitive workloads
#     like real-time fraud detection or financial trading.
#   - Best when GC pauses are a known bottleneck for producers or consumers.
#
# The default is set to gencon with concurrentScavenge enabled for a good balance, leaning towards lower pause times.
if [ -z "$KAFKA_JVM_PERFORMANCE_OPTS" ]; then
  KAFKA_JVM_PERFORMANCE_OPTS="-Xgcpolicy:gencon -Xgc:concurrentScavenge -XX:+DisableExplicitGC -Djava.awt.headless=true"
fi

# Process command line arguments
while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    -name)
      DAEMON_NAME=$2
      CONSOLE_OUTPUT_FILE=$LOG_DIR/$DAEMON_NAME.out
      shift 2
      ;;
    -loggc)
      GC_LOG_ENABLED="true"
      shift
      ;;
    -daemon)
      DAEMON_MODE="true"
      shift
      ;;
    *)
      break
      ;;
  esac
done

# GC options
if [ "$GC_LOG_ENABLED" = "true" ]; then
  GC_LOG_FILE_NAME="$DAEMON_NAME-gc.log"
  KAFKA_GC_LOG_OPTS="-Xverbosegclog:$LOG_DIR/$GC_LOG_FILE_NAME,10,100M"
fi

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

# Memory options
#
# The heap size for Kafka components can vary. For Kafka brokers, a good production starting point is 6GB.
# For heavy production loads, consider using machines with 32GB of RAM or more.
# Other components like ksqlDB may require up to 16GB for intensive processing.
# The default of 1GB is a safe starting point for development and testing.
if [ -z "$KAFKA_HEAP_OPTS" ]; then
  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
fi

# Combine all JVM options
ALL_JVM_OPTS="$KAFKA_HEAP_OPTS $KAFKA_JVM_PERFORMANCE_OPTS $KAFKA_GC_LOG_OPTS $KAFKA_JMX_OPTS $KAFKA_LOG4J_OPTS $KAFKA_OPTS"

# Launch mode
if [ "x$DAEMON_MODE" = "xtrue" ]; then
  nohup "$JAVA" $ALL_JVM_OPTS -cp "$CLASSPATH" "$@" > "$CONSOLE_OUTPUT_FILE" 2>&1 < /dev/null &
else
  exec "$JAVA" $ALL_JVM_OPTS -cp "$CLASSPATH" "$@"
fi
