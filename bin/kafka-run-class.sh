#!/bin/bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 classname [opts]"
  exit 1
fi

base_dir=$(dirname $0)/..

for file in $base_dir/project/boot/scala-2.8.0/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/core/target/scala_2.8.0/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/core/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/core/lib_managed/scala_2.8.0/compile/*.jar;
do
  if [ ${file##*/} != "sbt-launch-0.7.7.jar" ]; then
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
