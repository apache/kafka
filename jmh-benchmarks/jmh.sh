#!/usr/bin/env bash


gradleCmd="../gradlew"
libDir="build/libs"


echo "running gradlew :jmh-benchmarks:clean :jmh-benchmarks:shadowJar in quiet mode"

$gradleCmd  -q :jmh-benchmarks:clean :jmh-benchmarks:shadowJar

echo "gradle build done"

echo "running JMH with args [$@]"

java -jar ${libDir}/kafka-jmh-benchmarks-0.10.1.0-SNAPSHOT-all.jar "$@"

echo "JMH benchmarks done"
