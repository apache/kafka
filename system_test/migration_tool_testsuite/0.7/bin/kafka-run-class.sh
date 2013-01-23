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
  echo "USAGE: $0 classname [opts]"
  exit 1
fi

snappy=~/.ivy2/cache/org.xerial.snappy/snappy-java/bundles/snappy-java-1.0.4.1.jar
CLASSPATH=$CLASSPATH:$snappy
library=~/.ivy2/cache/org.scala-lang/scala-library/jars/scala-library-2.8.0.jar
CLASSPATH=$CLASSPATH:$library
compiler=~/.ivy2/cache/org.scala-lang/scala-compiler/jars/scala-compiler-2.8.0.jar
CLASSPATH=$CLASSPATH:$compiler
log4j=~/.ivy2/cache/log4j/log4j/jars/log4j-1.2.15.jar
CLASSPATH=$CLASSPATH:$log4j
slf=~/.ivy2/cache/org.slf4j/slf4j-api/jars/slf4j-api-1.6.4.jar
CLASSPATH=$CLASSPATH:$slf
zookeeper=~/.ivy2/cache/org.apache.zookeeper/zookeeper/jars/zookeeper-3.3.4.jar
CLASSPATH=$CLASSPATH:$zookeeper
jopt=~/.ivy2//cache/net.sf.jopt-simple/jopt-simple/jars/jopt-simple-3.2.jar
CLASSPATH=$CLASSPATH:$jopt

base_dir=$(dirname $0)/../../../..
kafka_07_lib_dir=$(dirname $0)/../lib

# 0.8 - scala jars
for file in $base_dir/project/boot/scala-2.8.0/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

# 0.7 - kafka-0.7.jar, zkclient-0.1.jar, kafka-perf-0.7.0.jar
for file in ${kafka_07_lib_dir}/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

# 0.8 - metrics jar
for file in $base_dir/core/lib/metrics*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

# 0.8 - misc jars
for file in $base_dir/core/lib_managed/scala_2.8.0/compile/*.jar;
do
  if [ ${file##*/} != "sbt-launch.jar" ]; then
    CLASSPATH=$CLASSPATH:$file
  fi
done
if [ -z "$KAFKA_JMX_OPTS" ]; then
  KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false "
fi
if [ -z "$KAFKA_OPTS" ]; then
  KAFKA_OPTS="-Xmx512M -server  -Dlog4j.configuration=file:$base_dir/config/log4j.properties"
fi
if [  $JMX_PORT ]; then
  KAFKA_JMX_OPTS="$KAFKA_JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT "
fi
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

$JAVA $KAFKA_OPTS $KAFKA_JMX_OPTS -cp $CLASSPATH $@
