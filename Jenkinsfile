/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

def setupGradle() {
  // Delete gradle cache to workaround cache corruption bugs, see KAFKA-3167
  dir('.gradle') {
    deleteDir()
  }
  sh './gradlew -version'
}

def doValidation() {
  sh '''
    ./gradlew -PscalaVersion=$SCALA_VERSION clean compileJava compileScala compileTestJava compileTestScala \
        spotlessScalaCheck checkstyleMain checkstyleTest spotbugsMain rat \
        --profile --no-daemon --continue -PxmlSpotBugsReport=true
  '''
}

def doTest() {
  sh '''
    ./gradlew -PscalaVersion=$SCALA_VERSION unitTest integrationTest \
        --profile --no-daemon --continue -PtestLoggingEvents=started,passed,skipped,failed \
        -PignoreFailures=true -PmaxParallelForks=2 -PmaxTestRetries=1 -PmaxTestRetryFailures=5
  '''
  junit '**/build/test-results/**/TEST-*.xml'
}

def doStreamsArchetype() {
  echo 'Verify that Kafka Streams archetype compiles'

  sh '''
    ./gradlew streams:install clients:install connect:json:install connect:api:install \
         || { echo 'Could not install kafka-streams.jar (and dependencies) locally`'; exit 1; }
  '''

  VERSION = sh(script: 'grep "^version=" gradle.properties | cut -d= -f 2', returnStdout: true).trim()

  dir('streams/quickstart') {
    sh '''
      mvn clean install -Dgpg.skip  \
          || { echo 'Could not `mvn install` streams quickstart archetype'; exit 1; }
    '''

    dir('test-streams-archetype') {
      // Note the double quotes for variable interpolation
      sh """ 
        echo "Y" | mvn archetype:generate \
            -DarchetypeCatalog=local \
            -DarchetypeGroupId=org.apache.kafka \
            -DarchetypeArtifactId=streams-quickstart-java \
            -DarchetypeVersion=${VERSION} \
            -DgroupId=streams.examples \
            -DartifactId=streams.examples \
            -Dversion=0.1 \
            -Dpackage=myapps \
            || { echo 'Could not create new project using streams quickstart archetype'; exit 1; }
      """

      dir('streams.examples') {
        sh '''
          mvn compile \
              || { echo 'Could not compile streams quickstart archetype project'; exit 1; }
        '''
      }
    }
  }
}

def tryStreamsArchetype() {
  try {
    doStreamsArchetype()
  } catch(err) {
    echo 'Failed to build Kafka Streams archetype, marking this build UNSTABLE'
    currentBuild.result = 'UNSTABLE'
  }
}


pipeline {
  agent none
  stages {
    stage('Build') {
      parallel {
        stage('JDK 8') {
          agent { label 'ubuntu' }
          tools {
            jdk 'jdk_1.8_latest'
            maven 'maven_3_latest'
          }
          options {
            timeout(time: 8, unit: 'HOURS') 
            timestamps()
          }
          environment {
            SCALA_VERSION=2.12
          }
          steps {
            setupGradle()
            doValidation()
            doTest()
            tryStreamsArchetype()
          }
        }

        stage('JDK 11') {
          agent { label 'ubuntu' }
          tools {
            jdk 'jdk_11_latest'
          }
          options {
            timeout(time: 8, unit: 'HOURS') 
            timestamps()
          }
          environment {
            SCALA_VERSION=2.13
          }
          steps {
            setupGradle()
            doValidation()
            doTest()
            echo 'Skipping Kafka Streams archetype test for Java 11'
          }
        }
       
        stage('JDK 15') {
          agent { label 'ubuntu' }
          tools {
            jdk 'jdk_15_latest'
          }
          options {
            timeout(time: 8, unit: 'HOURS') 
            timestamps()
          }
          environment {
            SCALA_VERSION=2.13
          }
          steps {
            setupGradle()
            doValidation()
            doTest()
            echo 'Skipping Kafka Streams archetype test for Java 15'
          }
        }
      }
    }
  }
}
