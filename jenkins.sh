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

# This script is used for verifying changes in Jenkins. In order to provide faster feedback, the tasks are ordered so
# that faster tasks are executed in every module before slower tasks (if possible). For example, the unit tests for all
# the modules are executed before the integration tests.

# Run validation checks (compilation and static analysis)
./gradlew clean compileJava compileScala compileTestJava compileTestScala \
    spotlessScalaCheck checkstyleMain checkstyleTest spotbugsMain rat \
    --profile --no-daemon --continue -PxmlSpotBugsReport=true "$@" \
    || { echo 'Validation steps failed'; exit 1; }

# Run tests
./gradlew unitTest integrationTest \
    --profile --no-daemon --continue -PtestLoggingEvents=started,passed,skipped,failed "$@" \
    || { echo 'Test steps failed'; exit 1; }

# Verify that Kafka Streams archetype compiles
if [ $JAVA_HOME = "/home/jenkins/tools/java/latest11" ] ; then
  echo "Skipping Kafka Streams archetype test for Java 11"
  exit 0
fi

./gradlew streams:install clients:install connect:json:install connect:api:install \
    || { echo 'Could not install kafka-streams.jar (and dependencies) locally`'; exit 1; }

version=`grep "^version=" gradle.properties | cut -d= -f 2` \
    || { echo 'Could not get version from `gradle.properties`'; exit 1; }

cd streams/quickstart \
    || { echo 'Could not change into directory `streams/quickstart`'; exit 1; }

# variable $MAVEN_LATEST__HOME is provided by Jenkins (see build configuration)
mvn=$MAVEN_LATEST__HOME/bin/mvn

$mvn clean install -Dgpg.skip  \
    || { echo 'Could not `mvn install` streams quickstart archetype'; exit 1; }

mkdir test-streams-archetype && cd test-streams-archetype \
    || { echo 'Could not create test directory for stream quickstart archetype'; exit 1; }

echo "Y" | $mvn archetype:generate \
    -DarchetypeCatalog=local \
    -DarchetypeGroupId=org.apache.kafka \
    -DarchetypeArtifactId=streams-quickstart-java \
    -DarchetypeVersion=$version \
    -DgroupId=streams.examples \
    -DartifactId=streams.examples \
    -Dversion=0.1 \
    -Dpackage=myapps \
    || { echo 'Could not create new project using streams quickstart archetype'; exit 1; }

cd streams.examples \
    || { echo 'Could not change into directory `streams.examples`'; exit 1; }

$mvn compile \
    || { echo 'Could not compile streams quickstart archetype project'; exit 1; }

