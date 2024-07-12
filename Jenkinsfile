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

def doTest(env, target = "test") {
  sh """./gradlew -PscalaVersion=$SCALA_VERSION cleanTest ${target} \
      --profile --continue -PkeepAliveMode="session" -PtestLoggingEvents=started,passed,skipped,failed \
      -PignoreFailures=true -PmaxParallelForks=2 -PmaxTestRetries=1 -PmaxTestRetryFailures=10"""
  junit '**/build/test-results/**/TEST-*.xml'
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

            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
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

            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
            doTest(env, target = ':connect:mirror:test')
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
