def getRepoURL() {
  sh "git config --get remote.origin.url > .git/remote-url"
  return readFile(".git/remote-url").trim()
}

def getCommitSha() {
  sh "git rev-parse HEAD > .git/current-commit"
  return readFile(".git/current-commit").trim()
}

void setBuildStatus(String context, String message, String state) {
    // workaround https://issues.jenkins-ci.org/browse/JENKINS-38674
    repoUrl = getRepoURL()
    commitSha = getCommitSha()

    echo "Setting status ${repoUrl}:${commitSha} :: ${context} -> ${state} -- ${message}"
    echo "scm.branches: ${scm.branches}"
    echo "scm: ${scm}"
    step([
        $class: "GitHubCommitStatusSetter",
        // Repo needs to be manually specified because it tries to set
        // status on jenkins-common as well -- anything in context --
        // if we aren't specific
        reposSource: [$class: "ManuallyEnteredRepositorySource", url: repoUrl],
        // SHA needs to be manually specified because git commit info
        // isn't available via standard env vars, and statuses might
        // be set before we do the Jenkinsfile-based SCM checkout step
        // that gets commit info.
        commitShaSource: [$class: "ManuallyEnteredShaSource", sha: commitSha],
        // Log errors, default just swallows without logging anything
        errorHandlers: [[$class: 'ShallowAnyErrorHandler']],
        contextSource: [$class: "ManuallyEnteredCommitContextSource", context: context],
        statusResultSource: [ $class: "ConditionalStatusResultSource", results: [[$class: "AnyBuildResult", message: message, state: state]] ]
    ]);
}

void doValidation(String scalaVersion) {
  echo "Scala Version: ${scalaVersion}"
  environment {
    SCALA_VERSION = scalaVersion
  }
  sh '''
    ./gradlew -PscalaVersion=$SCALA_VERSION clean compileJava compileScala compileTestJava compileTestScala \
      spotlessScalaCheck checkstyleMain checkstyleTest spotbugsMain rat \
      --profile --no-daemon --continue -PxmlSpotBugsReport=true \"$@\" \
      || { echo 'Validation steps failed'; exit 1; }
  '''
}

pipeline {
  agent { label 'ubuntu' }
  parallel {
    stage('JDK 8') {
      tools {
	jdk 'JDK 1.8 (latest)'
      }
      steps {
	sh 'gradle -version'
	validation('2.12')
      }
    }

    stage('JDK 11') {
      tools {
	jdk 'JDK 11 (latest)'
      }
      steps {
	sh 'gradle -version'
	validation('2.13')
	// setBuildStatus("continuous-integration/jenkins/test-check-1", "Check is running", "PENDING")
      }
    }
  }
}
