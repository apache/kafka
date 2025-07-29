// Unified, Parameterized Jenkinsfile for Multi-JVM Kafka Builds
pipeline {
    agent none // Agent will be selected dynamically in stages

    parameters {
        choice(name: 'JDK_TYPE', choices: ['openj9-open', 'openj9-certified', 'hotspot'], description: 'Select the target JDK for the build.')
        booleanParam(name: 'RUN_TESTS', defaultValue: true, description: 'Check to run the full unit and integration test suite.')
        string(name: 'JVM_ARGS_OPENJ9', defaultValue: "-Xmx2G -Xgcpolicy:gencon -Xgcthreads1 -XcompilationThreads1 -XX:+DisableExplicitGC", description: 'Custom JVM arguments for OpenJ9 builds.')
        string(name: 'JVM_ARGS_HOTSPOT', defaultValue: "-Xmx2G -XX:+UseG1GC -XX:+DisableExplicitGC", description: 'Custom JVM arguments for HotSpot builds.')
        string(name: 'SEMERU_OPEN_URL', defaultValue: "https://github.com/ibmruntimes/semeru21-binaries/releases/download/jdk-21.0.8%2B9_openj9-0.53.0/ibm-semeru-open-jdk_x64_linux_21.0.8_9_openj9-0.53.0.tar.gz", description: 'Download URL for Semeru Open Edition JDK.')
        string(name: 'SEMERU_CERTIFIED_URL', defaultValue: "https://github.com/ibmruntimes/semeru21-certified-binaries/releases/download/jdk-21.0.8%2B9_openj9-0.53.0/ibm-semeru-certified-jdk_x64_linux_21.0.8.0.tar.gz", description: 'Download URL for Semeru Certified Edition JDK.')
        string(name: 'GRADLE_MAX_WORKERS', defaultValue: '1', description: 'Value for the --max-workers Gradle flag.')
        string(name: 'GRADLE_MAX_FORKS', defaultValue: '1', description: 'Value for the -PmaxParallelForks Gradle property.')
        string(name: 'GRADLE_MAX_SCALAC_THREADS', defaultValue: '1', description: 'Value for the -PmaxScalacThreads Gradle property.')
    }

    environment {
        DOCKER_REGISTRY_URL     = 'jtsweet0891' // Docker Hub username or registry URL
        DOCKER_IMAGE_BASE_NAME  = 'apache_kafka_on_ibm_j9'
        DOCKER_CREDENTIALS_ID   = 'dockerhub-pat' // ID of Docker Hub credentials in Jenkins
        KAFKA_VERSION           = '4.0.0'
    }

    stages {
        stage('Initialize Build Environment') {
            agent any // Use any available agent for this initial setup stage
            steps {
                script {
                    // Explicitly set the agent label for subsequent stages using if/else
                    def agentLabel = ''
                    if (params.JDK_TYPE == 'openj9-open') {
                        agentLabel = 'dual_xeon_ibm_openj9_jdk21'
                    } else if (params.JDK_TYPE == 'openj9-certified') {
                        agentLabel = 'i7_2600k_ibm_openj9_jdk21_certified'
                    } else if (params.JDK_TYPE == 'hotspot') {
                        agentLabel = 'linux-x64-hotspot-jdk21'
                    }
                    env.AGENT_LABEL = agentLabel

                    // Explicitly set the test status for artifact naming using if/else
                    def testStatus = ''
                    if (params.RUN_TESTS) {
                        testStatus = 'tested'
                    } else {
                        testStatus = 'notest'
                    }
                    env.TEST_STATUS = testStatus

                    // Define artifact names
                    def artifactBaseName = "kafka-${env.KAFKA_VERSION}-jtsweet-feature-openj9-integration-${params.JDK_TYPE}-${testStatus}-b${env.BUILD_NUMBER}"
                    env.TGZ_ARTIFACT_NAME = "${artifactBaseName}.tgz"
                    env.DOCKER_IMAGE_TAG = "${env.KAFKA_VERSION}-${params.JDK_TYPE}-${testStatus}-b${env.BUILD_NUMBER}"
                    
                    // Set JVM options conditionally using parameters
                    if (params.JDK_TYPE.startsWith('openj9')) {
                        env.JAVA_TOOL_OPTIONS = params.JVM_ARGS_OPENJ9
                    } else { // HotSpot
                        env.JAVA_TOOL_OPTIONS = params.JVM_ARGS_HOTSPOT
                    }
                }
            }
        }

        stage('Checkout') {
            agent { label "${env.AGENT_LABEL}" }
            steps {
                echo "Checking out source code on agent: ${env.AGENT_LABEL}"
                checkout scm
            }
        }

        stage('Build and Test') {
            agent { label "${env.AGENT_LABEL}" }
            when { expression { params.RUN_TESTS == true } }
            steps {
                script {
                    def gradleArgs = "--no-build-cache --no-configuration-cache --max-workers=${params.GRADLE_MAX_WORKERS} -PmaxParallelForks=${params.GRADLE_MAX_FORKS} -PmaxScalacThreads=${params.GRADLE_MAX_SCALAC_THREADS} --info --stacktrace"
                    echo "Running Gradle build with tests:./gradlew clean build ${gradleArgs}"
                    sh "./gradlew clean build ${gradleArgs}"
                }
            }
        }

        stage('Package Artifacts') {
            agent { label "${env.AGENT_LABEL}" }
            steps {
                script {
                    def gradleArgs = "--no-build-cache --no-configuration-cache --max-workers=${params.GRADLE_MAX_WORKERS} -PmaxParallelForks=${params.GRADLE_MAX_FORKS} -PmaxScalacThreads=${params.GRADLE_MAX_SCALAC_THREADS} --info --stacktrace"
                    if (params.RUN_TESTS) {
                        echo "Running Gradle packaging:./gradlew releaseTarGz ${gradleArgs}"
                        sh "./gradlew releaseTarGz ${gradleArgs}"
                    } else {
                        echo "Running Gradle packaging without tests:./gradlew clean releaseTarGz -x test ${gradleArgs}"
                        sh "./gradlew clean releaseTarGz -x test ${gradleArgs}"
                    }
                    sh "mv core/build/distributions/kafka_*.tgz ${env.TGZ_ARTIFACT_NAME}"
                    archiveArtifacts artifacts: env.TGZ_ARTIFACT_NAME, followSymlinks: false
                }
            }
        }

        stage('Build and Push Docker Image') {
            agent { label "${env.AGENT_LABEL}" }
            steps {
                script {
                    def jdkUrl = ''
                    if (params.JDK_TYPE == 'openj9-open') {
                        jdkUrl = params.SEMERU_OPEN_URL
                    } else if (params.JDK_TYPE == 'openj9-certified') {
                        jdkUrl = params.SEMERU_CERTIFIED_URL
                    } else {
                        error("HotSpot Docker build is not configured with a specific JDK URL.")
                    }

                    def dockerImageName = "${env.DOCKER_REGISTRY_URL}/${env.DOCKER_IMAGE_BASE_NAME}"
                    def fullImageName = "${dockerImageName}:${env.DOCKER_IMAGE_TAG}"
                    
                    def dockerImage = docker.build(fullImageName, "--build-arg JDK_URL='${jdkUrl}'.")
                    
                    docker.withRegistry("https://index.docker.io/v1/", env.DOCKER_CREDENTIALS_ID) {
                        echo "Pushing image ${fullImageName}"
                        dockerImage.push()

                        if (params.RUN_TESTS) {
                            def latestTag = "${env.KAFKA_VERSION}-${params.JDK_TYPE}-latest-tested"
                            echo "Tagging and pushing latest tested tag: ${latestTag}"
                            dockerImage.push(latestTag)
                        }
                    }
                }
            }
        }
    }

    post {
        agent { label "${env.AGENT_LABEL}" }
        always {
            script {
                if (params.RUN_TESTS) {
                    echo 'Archiving test results...'
                    junit '**/build/test-results/**/*.xml'
                }
            }
            echo 'Pipeline finished.'
            cleanWs()
        }
    }
}
