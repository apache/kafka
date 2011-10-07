#!/bin/bash

base_dir=$(dirname $0)/../..

for file in $base_dir/project/boot/scala-2.8.0/lib/*.jar;
do
  if [ ${file##*/} != "sbt-launch.jar" ]; then
    CLASSPATH=$CLASSPATH:$file
  fi
done

for file in $base_dir/core/lib_managed/scala_2.8.0/compile/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/core/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/core/target/scala_2.8.0/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/examples/target/scala_2.8.0/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

echo $CLASSPATH

if [ -z "$KAFKA_PERF_OPTS" ]; then
  KAFKA_OPTS="-Xmx512M -server -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=3333 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

$JAVA $KAFKA_OPTS -cp $CLASSPATH kafka.examples.SimpleConsumerDemo $@

