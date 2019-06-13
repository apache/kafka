#!/usr/bin/env groovy

def config = jobConfig {
    cron = '@midnight'
    nodeLabel = 'docker-oraclejdk8'
    testResultSpecs = ['junit': '**/build/test-results/**/TEST-*.xml']
    slackChannel = '#kafka'
    timeoutHours = 4
    runMergeCheck = false
}


def job = {
    // Per KAFKA-7524, Scala 2.12 is the default, yet we currently support the previous minor version.
    stage("Check compilation compatibility with Scala 2.11") {
        sh "gradle"
        sh "./gradlew clean compileJava compileScala compileTestJava compileTestScala " +
                "--stacktrace -PscalaVersion=2.11"
    }

    stage("Compile and validate") {
        sh "./gradlew clean assemble spotlessScalaCheck checkstyleMain checkstyleTest spotbugsMain " +
                "--no-daemon --stacktrace --continue -PxmlSpotBugsReport=true"
    }

    
    if (config.publish && config.isDevJob) {
      configFileProvider([configFile(fileId: 'Gradle Nexus Settings', variable: 'GRADLE_NEXUS_SETTINGS')]) {
          stage("Publish to nexus") {
              sh "./gradlew --init-script ${GRADLE_NEXUS_SETTINGS} --no-daemon uploadArchivesAll"
          }
      }
    }

    stage("Unit Test") {
      sh "./gradlew --no-daemon unitTest --continue --stacktrace || true"
    }

    stage("Integration test") {
        sh "./gradlew integrationTest " +
                "--no-daemon --stacktrace --continue -PtestLoggingEvents=started,passed,skipped,failed -PmaxParallelForks=6 || true"
    }
}

def post = {
    stage('Upload results') {
        // Kafka failed test stdout files
        archiveArtifacts artifacts: '**/testOutput/*.stdout', allowEmptyArchive: true

        def summary = junit '**/build/test-results/**/TEST-*.xml'
        def total = summary.getTotalCount()
        def failed = summary.getFailCount()
        def skipped = summary.getSkipCount()
        summary = "Test results:\n\t"
        summary = summary + ("Passed: " + (total - failed - skipped))
        summary = summary + (", Failed: " + failed)
        summary = summary + (", Skipped: " + skipped)
        return summary;
    }
}

runJob config, job, post
