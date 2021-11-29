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
    cron = '@weekly'
    nodeLabel = 'docker-oraclejdk8'
    testResultSpecs = ['junit': '**/build/test-results/**/TEST-*.xml']
    slackChannel = '#kafka-warn'
    timeoutHours = 4
    runMergeCheck = false
    downStreamValidate = true
    downStreamRepos = ["common",]
    nanoVersion = true
    disableConcurrentBuilds = true
}

def retryFlagsString(jobConfig) {
    if (jobConfig.isPrJob) " -PmaxTestRetries=1 -PmaxTestRetryFailures=5"
    else ""
}

def downstreamBuildFailureOutput = ""
def publishStep(String vaultSecret) {
    withVaultFile([["gradle/${vaultSecret}", "settings_file", "${env.WORKSPACE}/init.gradle", "GRADLE_NEXUS_SETTINGS"]]) {
        sh "./gradlewAll --init-script ${GRADLE_NEXUS_SETTINGS} --no-daemon uploadArchives"
    }
}

def job = {
    // https://github.com/confluentinc/common-tools/blob/master/confluent/config/dev/versions.json
    def kafkaMuckrakeVersionMap = [
            "2.3": "5.3.x",
            "2.4": "5.4.x",
            "2.5": "5.5.x",
            "2.6": "6.0.x",
            "2.7": "6.1.x",
            "2.8": "6.2.x",
            "3.0": "7.0.x",
            "3.1": "7.1.x",
            "trunk": "master",
            "master": "master"
    ]

    if (config.nanoVersion && !config.isReleaseJob) {
        ciTool("ci-update-version ${env.WORKSPACE} kafka", config.isPrJob)
    }

    stage("Check compilation compatibility with Scala 2.12") {
        sh "./gradlew clean assemble spotlessScalaCheck checkstyleMain checkstyleTest spotbugsMain " +
                 "--no-daemon --stacktrace -PxmlSpotBugsReport=true -PscalaVersion=2.12"
    }


    stage("Compile and validate") {
        sh "./gradlew clean assemble publishToMavenLocal spotlessScalaCheck checkstyleMain checkstyleTest spotbugsMain " +
                "--no-daemon --stacktrace -PxmlSpotBugsReport=true"
    }

    if (config.publish) {
      stage("Publish to artifactory") {
        if (!config.isReleaseJob && !config.isPrJob) {
            ciTool("ci-push-tag ${env.WORKSPACE} kafka")
        }

        if (config.isDevJob) {
          publishStep('artifactory_snapshots_settings')
        } else if (config.isPreviewJob) {
          publishStep('artifactory_preview_release_settings')
        }
      }
    }

    if (config.publish && config.isDevJob && !config.isReleaseJob && !config.isPrJob) {
        stage("Start Downstream Builds") {
            def downstreamBranch = kafkaMuckrakeVersionMap[env.BRANCH_NAME]
            config.downStreamRepos.each { repo ->
                build(job: "confluentinc/${repo}/${downstreamBranch}",
                    wait: false,
                    propagate: false
                )
            }
        }
    }

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
            stage('Downstream validation') {
                if (config.isPrJob && config.downStreamValidate) {
                    downStreamValidation(config.nanoVersion, true, true)
                } else {
                    return "skip downStreamValidation"
                }
            }
        }
    ]

    result = parallel testTargets
    // combine results of the two targets into one result string
    return result.runTestsStepName + "\n" + result.downstreamBuildsStepName
}

runJob config, job
echo downstreamBuildFailureOutput
