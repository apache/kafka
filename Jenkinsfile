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

def doValidation() {
  // Run all the tasks associated with `check` except for `test` - the latter is executed via `doTest`
  sh """
    ./retry_zinc ./gradlew -PscalaVersion=$SCALA_VERSION clean check -x test \
        --profile --continue -PxmlSpotBugsReport=true -PkeepAliveMode="session"
  """
}

def isChangeRequest(env) {
  env.CHANGE_ID != null && !env.CHANGE_ID.isEmpty()
}

def doTestParameterized(env, params, target = "test") {
  sh """./gradlew ${params} ${target} \
      --profile --continue -PkeepAliveMode="session" -PtestLoggingEvents=started,passed,skipped,failed \
      -PignoreFailures=true -PmaxParallelForks=2 -PmaxTestRetries=1 -PmaxTestRetryFailures=10"""
  junit '**/build/test-results/**/TEST-*.xml'
}

def doTest(env) {
  doTestParameterized(env, "-PscalaVersion=$SCALA_VERSION", "test")
}

def streamsTest(env) {
  doTestParameterized(env, "-PstreamsScalaVersion=3", ":streams:streams-scala:test")
}

def doStreamsArchetype() {
  echo 'Verify that Kafka Streams archetype compiles'

  sh '''
    ./gradlew streams:publishToMavenLocal clients:publishToMavenLocal connect:json:publishToMavenLocal connect:api:publishToMavenLocal \
         || { echo 'Could not publish kafka-streams.jar (and dependencies) locally to Maven'; exit 1; }
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

  options {
    disableConcurrentBuilds(abortPrevious: isChangeRequest(env))
  }

  stages {
    stage('Build') {
      parallel {

        stage('JDK 8 and Scala 2.12') {
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
            doValidation()
            doTest(env)
            tryStreamsArchetype()
          }
        }

        stage('JDK 11 and Scala 2.13') {
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
            doValidation()
            doTest(env)
            echo 'Skipping Kafka Streams archetype test for Java 11'
          }
        }

        stage('JDK 17 and Scala 2.13') {
          agent { label 'ubuntu' }
          tools {
            jdk 'jdk_17_latest'
          }
          options {
            timeout(time: 8, unit: 'HOURS')
            timestamps()
          }
          environment {
            SCALA_VERSION=2.13
          }
          steps {
            doValidation()
            doTest(env)
            echo 'Skipping Kafka Streams archetype test for Java 17'
          }
        }

        stage('JDK 21 and Scala 2.13') {
          agent { label 'ubuntu' }
          tools {
            jdk 'jdk_21_latest'
          }
          options {
            timeout(time: 8, unit: 'HOURS')
            timestamps()
          }
          environment {
            SCALA_VERSION=2.13
          }
          steps {
            doValidation()
            doTest(env)
            echo 'Skipping Kafka Streams archetype test for Java 21'
          }
        }

        stage('JDK 21 and Scala 3 (Streams-Scala only)') {
          agent { label 'ubuntu' }
          tools {
            jdk 'jdk_21_latest'
          }
          options {
            timeout(time: 1, unit: 'HOURS')
            timestamps()
          }
          steps {
            streamsTest(env)
          }
        }
      }
    }
  }

  post {
    always {
      script {
        if (!isChangeRequest(env)) {
          node('ubuntu') {
            step([$class: 'Mailer',
                 notifyEveryUnstableBuild: true,
                 recipients: "dev@kafka.apache.org",
                 sendToIndividuals: false])
          }
        }
      }
    }
  }
}
