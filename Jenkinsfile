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
  try {
    sh '''
      ./gradlew -PscalaVersion=$SCALA_VERSION clean compileJava compileScala compileTestJava compileTestScala \
	  spotlessScalaCheck checkstyleMain checkstyleTest spotbugsMain rat \
	  --profile --no-daemon --continue -PxmlSpotBugsReport=true \"$@\"
    '''
  } catch(err) {
    error('Validation checks failed, aborting this build')
  }
}

def doTest() {
  try {
    sh '''
      ./gradlew -PscalaVersion=$SCALA_VERSION unitTest integrationTest \
	  --profile --no-daemon --continue -PtestLoggingEvents=started,passed,skipped,failed "$@"
    '''
  } catch(err) {
    echo 'Some tests failed, marking this build UNSTABLE'
    currentBuild.result = 'UNSTABLE'
  }
}

def doStreamsArchetype() {
  echo 'Verify that Kafka Streams archetype compiles'

  sh '''
    ./gradlew streams:install clients:install connect:json:install connect:api:install \
         || { echo 'Could not install kafka-streams.jar (and dependencies) locally`'; exit 1; }
  '''

  sh '''
    version=`grep "^version=" gradle.properties | cut -d= -f 2` \
        || { echo 'Could not get version from `gradle.properties`'; exit 1; }
  '''

  dir('streams/quickstart') {
    sh '''
      mvn clean install -Dgpg.skip  \
	  || { echo 'Could not `mvn install` streams quickstart archetype'; exit 1; }
    '''

    sh '''
      mkdir test-streams-archetype && cd test-streams-archetype \
	  || { echo 'Could not create test directory for stream quickstart archetype'; exit 1; }
    '''

    sh '''
      echo "Y" | mvn archetype:generate \
	  -DarchetypeCatalog=local \
	  -DarchetypeGroupId=org.apache.kafka \
	  -DarchetypeArtifactId=streams-quickstart-java \
	  -DarchetypeVersion=$version \
	  -DgroupId=streams.examples \
	  -DartifactId=streams.examples \
	  -Dversion=0.1 \
	  -Dpackage=myapps \
	  || { echo 'Could not create new project using streams quickstart archetype'; exit 1; }
    '''

    dir('streams.examples') {
      sh '''
	mvn compile \
	    || { echo 'Could not compile streams quickstart archetype project'; exit 1; }
      '''
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
	    jdk 'JDK 1.8 (latest)'
            maven 'Maven 3.6.3'
	  }
          options {
            timeout(time: 8, unit: 'HOURS') 
            timestamps()
          }
	  environment {
	    SCALA_VERSION=2.12
	  }
	  steps {
	    sh 'gradle -version'
	    doValidation()
            doTest()
            tryStreamsArchetype()
	  }
	  post {
	    success {
	      junit '**/build/test-results/**/TEST-*.xml'
	    }
	    unstable {
	      junit '**/build/test-results/**/TEST-*.xml'
	    }
	  }
	}

	stage('JDK 11') {
          agent { label 'ubuntu' }
	  tools {
	    jdk 'JDK 11 (latest)'
	  }
          options {
            timeout(time: 8, unit: 'HOURS') 
            timestamps()
          }
	  environment {
	    SCALA_VERSION=2.13
	  }
	  steps {
	    sh 'gradle -version'
	    doValidation()
            doTest()
            echo 'Skipping Kafka Streams archetype test for Java 11'
	  }
	  post {
	    success {
	      junit '**/build/test-results/**/TEST-*.xml'
	    }
	    unstable {
	      junit '**/build/test-results/**/TEST-*.xml'
	    }
	  }
	}
       
	stage('JDK 14') {
          agent { label 'ubuntu' }
	  tools {
	    jdk 'JDK 14 (latest)'
	  }
          options {
            timeout(time: 8, unit: 'HOURS') 
            timestamps()
          }
	  environment {
	    SCALA_VERSION=2.13
	  }
	  steps {
	    sh 'gradle -version'
	    doValidation()
            doTest()
            echo 'Skipping Kafka Streams archetype test for Java 14'
	  }
	  post {
	    success {
	      junit '**/build/test-results/**/TEST-*.xml'
	    }
	    unstable {
	      junit '**/build/test-results/**/TEST-*.xml'
	    }
	  }
	}
      }
    }
  }
}
