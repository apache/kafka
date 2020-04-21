#!/usr/bin/env groovy

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

def config = jobConfig {
    cron = '@midnight'
    nodeLabel = 'docker-oraclejdk8'
    testResultSpecs = ['junit': '**/build/test-results/**/TEST-*.xml']
    slackChannel = '#kafka-warn'
    timeoutHours = 4
    runMergeCheck = false
}

def retryFlagsString(jobConfig) {
    if (jobConfig.isPrJob) " -PmaxTestRetries=1 -PmaxTestRetryFailures=5"
    else ""
}

def downstreamBuildFailureOutput = ""
def publishStep(String configSettings) {
  configFileProvider([configFile(fileId: configSettings, variable: 'GRADLE_NEXUS_SETTINGS')]) {
          sh "./gradleAll --init-script ${GRADLE_NEXUS_SETTINGS} --no-daemon uploadArchives"
  }
}
def job = {
    // https://github.com/confluentinc/common-tools/blob/master/confluent/config/dev/versions.json
    def kafkaMuckrakeVersionMap = [
            "2.3": "5.3.x",
            "2.4": "5.4.x",
            "2.5": "5.5.x",
            "trunk": "master",
            "master": "master"
    ]

    stage("Compile and validate") {
        sh "./gradlew clean assemble spotlessScalaCheck checkstyleMain checkstyleTest spotbugsMain " +
                "--no-daemon --stacktrace --continue -PxmlSpotBugsReport=true"
    }

    if (config.publish) {
      stage("Publish to artifactory") {
        if (config.isDevJob) {
          publishStep('Gradle-Artifactory-Settings')
        } else if (config.isPreviewJob) {
          publishStep('Gradle-Artifactory-Preview-Release-Settings')
        }
      }
    }

    stage("Run Tests and build cp-downstream-builds") {
    	def runTestsStepName = "Step run-tests"
    	def downstreamBuildsStepName = "Step cp-downstream-builds"
        def testTargets = [
           runTestsStepName: {
               stage('Run tests') {
                   echo "Running unit and integration tests"
                   sh "./gradlew unitTest integrationTest " +
                           "--no-daemon --stacktrace --continue -PtestLoggingEvents=started,passed,skipped,failed -PmaxParallelForks=4 -PignoreFailures=true" +
                           retryFlagsString(config)
               }
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
        },
        downstreamBuildsStepName: {
            echo "Building cp-downstream-builds"
            if (config.isPrJob) {
                try {
                    def muckrakeBranch = kafkaMuckrakeVersionMap[env.CHANGE_TARGET]
                    def forkRepo = "${env.CHANGE_FORK ?: "confluentinc"}/kafka.git"
                    def forkBranch = env.CHANGE_BRANCH
                    echo "Schedule test-cp-downstream-builds with :"
                    echo "Muckrake branch : ${muckrakeBranch}"
                    echo "PR fork repo : ${forkRepo}"
                    echo "PR fork branch : ${forkBranch}"
                    buildResult = build job: 'test-cp-downstream-builds', parameters: [
                            [$class: 'StringParameterValue', name: 'BRANCH', value: muckrakeBranch],
                            [$class: 'StringParameterValue', name: 'TEST_PATH', value: "muckrake/tests/dummy_test.py"],
                            [$class: 'StringParameterValue', name: 'KAFKA_REPO', value: forkRepo],
                            [$class: 'StringParameterValue', name: 'KAFKA_BRANCH', value: forkBranch]],
                            propagate: true, wait: true
                    downstreamBuildFailureOutput = "cp-downstream-builds result: " + buildResult.getResult();
                    return downstreamBuildFailureOutput
                } catch (Exception e) {
                    currentBuild.result = 'UNSTABLE'
                    downstreamBuildFailureOutput = "cp-downstream-builds result: " + e.getMessage()
                    writeFile file: "downstream/cp-downstream-build-failure.txt", text: downstreamBuildFailureOutput
                    archiveArtifacts artifacts: 'downstream/*.txt'

                    return downstreamBuildFailureOutput
                }
            } else {
                return ""
            }
         }
        ]

        result = parallel testTargets
        // combine results of the two targets into one result string
        return result.runTestsStepName + "\n" + result.downstreamBuildsStepName
    }
}

runJob config, job
echo downstreamBuildFailureOutput
