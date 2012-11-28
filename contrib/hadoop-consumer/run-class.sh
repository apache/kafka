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

base_dir=$(dirname $0)/../..

# include kafka jars
for file in $base_dir/core/target/scala_2.8.0/kafka-*.jar
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/contrib/hadoop-consumer/lib_managed/scala_2.8.0/compile/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

local_dir=$(dirname $0)

# include hadoop-consumer jars
for file in $base_dir/contrib/hadoop-consumer/target/scala_2.8.0/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/contrib/hadoop-consumer/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

CLASSPATH=$CLASSPATH:$base_dir/project/boot/scala-2.8.0/lib/scala-library.jar

echo $CLASSPATH

CLASSPATH=dist:$CLASSPATH:${HADOOP_HOME}/conf

#if [ -z "$KAFKA_OPTS" ]; then
#  KAFKA_OPTS="-Xmx512M -server -Dcom.sun.management.jmxremote"
#fi

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

$JAVA $KAFKA_OPTS -cp $CLASSPATH $@
