#!/bin/bash

find lib/ -not -name piggybank.jar | xargs rm

wget -P lib/ \
http://repo1.maven.org/maven2/org/apache/hadoop/hadoop-core/1.2.1/hadoop-core-1.2.1.jar \
http://repo1.maven.org/maven2/commons-io/commons-io/2.4/commons-io-2.4.jar \
http://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.6/slf4j-api-1.7.6.jar \
http://repo1.maven.org/maven2/com/yammer/metrics/metrics-core/2.2.0/metrics-core-2.2.0.jar \
http://repo1.maven.org/maven2/org/codehaus/jackson/jackson-mapper-asl/1.9.13/jackson-mapper-asl-1.9.13.jar \
http://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar \
http://repo1.maven.org/maven2/commons-httpclient/commons-httpclient/3.1/commons-httpclient-3.1.jar \
http://repo1.maven.org/maven2/log4j/log4j/1.2.16/log4j-1.2.16.jar \
http://repo1.maven.org/maven2/org/codehaus/jackson/jackson-core-asl/1.9.13/jackson-core-asl-1.9.13.jar \
http://repo1.maven.org/maven2/commons-lang/commons-lang/2.6/commons-lang-2.6.jar \
http://repo1.maven.org/maven2/commons-configuration/commons-configuration/1.10/commons-configuration-1.10.jar \
http://repo1.maven.org/maven2/org/scala-lang/scala-library/2.8.0/scala-library-2.8.0.jar

cp ../../core/target/scala-2.8.0/kafka_2.8.0-0.8.0.jar lib
