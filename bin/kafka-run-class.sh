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

# create logs directory
if [ "x$LOG_DIR" = "x" ]; then
    LOG_DIR="$base_dir/logs"
fi

if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
fi

if [ -z "$SCALA_VERSION" ]; then
	SCALA_VERSION=2.10.4
fi

if [ -z "$SCALA_BINARY_VERSION" ]; then
	SCALA_BINARY_VERSION=2.10
fi

# run ./gradlew copyDependantLibs to get all dependant jars in a local dir
for file in $base_dir/core/build/dependant-libs-${SCALA_VERSION}*/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/examples/build/libs//kafka-examples*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/contrib/hadoop-consumer/build/libs//kafka-hadoop-consumer*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/contrib/hadoop-producer/build/libs//kafka-hadoop-producer*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/clients/build/libs/kafka-clients*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

# classpath addition for release
for file in $base_dir/libs/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/core/build/libs/kafka_${SCALA_BINARY_VERSION}*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

# JMX settings
if [ -z "$KAFKA_JMX_OPTS" ]; then
  KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false "
fi

# JMX port to use
if [  $JMX_PORT ]; then
  KAFKA_JMX_OPTS="$KAFKA_JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT "
fi

# Log4j settings
if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$base_dir/config/tools-log4j.properties"
fi

KAFKA_LOG4J_OPTS="-Dkafka.logs.dir=$LOG_DIR $KAFKA_LOG4J_OPTS"

# Generic jvm settings you want to add
if [ -z "$KAFKA_OPTS" ]; then
  KAFKA_OPTS=""
fi

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

# Memory options
if [ -z "$KAFKA_HEAP_OPTS" ]; then
  KAFKA_HEAP_OPTS="-Xmx256M"
fi

# JVM performance options
if [ -z "$KAFKA_JVM_PERFORMANCE_OPTS" ]; then
  KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+CMSScavengeBeforeRemark -XX:+DisableExplicitGC -Djava.awt.headless=true"
fi


while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    -name)
      DAEMON_NAME=$2
      CONSOLE_OUTPUT_FILE=$LOG_DIR/$DAEMON_NAME.out
      shift 2
      ;;
    -loggc)
      if [ -z "$KAFKA_GC_LOG_OPTS"] ; then
        GC_LOG_ENABLED="true"
      fi
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
GC_FILE_SUFFIX='-gc.log'
GC_LOG_FILE_NAME=''
if [ "x$GC_LOG_ENABLED" = "xtrue" ]; then
  GC_LOG_FILE_NAME=$DAEMON_NAME$GC_FILE_SUFFIX
  KAFKA_GC_LOG_OPTS="-Xloggc:$LOG_DIR/$GC_LOG_FILE_NAME -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps "
fi

# Launch mode
if [ "x$DAEMON_MODE" = "xtrue" ]; then
  nohup $JAVA $KAFKA_HEAP_OPTS $KAFKA_JVM_PERFORMANCE_OPTS $KAFKA_GC_LOG_OPTS $KAFKA_JMX_OPTS $KAFKA_LOG4J_OPTS -cp $CLASSPATH $KAFKA_OPTS "$@" > "$CONSOLE_OUTPUT_FILE" 2>&1 < /dev/null &
else
  exec $JAVA $KAFKA_HEAP_OPTS $KAFKA_JVM_PERFORMANCE_OPTS $KAFKA_GC_LOG_OPTS $KAFKA_JMX_OPTS $KAFKA_LOG4J_OPTS -cp $CLASSPATH $KAFKA_OPTS "$@"
fi
