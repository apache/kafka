pipeline {
  agent { label 'ubuntu' }
  stages {
    stage('build') {
      // First step
      steps {
        echo 'start'
      }

      // Do some parallel stuff
      parallel {
        stage('JDK 8') {
          tools {
	    jdk 'JDK 1.8 (latest)'
	  }
	  steps {
            sh 'gradle -version'
          }
        }

        stage('JDK 11') {
          tools {
	    jdk 'JDK 1.11 (latest)'
	  }
	  steps {
            sh 'gradle -version'
          }
        }
      }
    }
  }
}
