// =============================================================================
// Jenkins Pipeline: Apache Kafka with IBM Semeru JDK - FULL TEST & BUILD
//
// This pipeline runs the complete test suite (unit and integration) before
// building the classic artifact and all component Docker images.
// =============================================================================
pipeline {
    // NOTE FOR OTHER USERS: This pipeline is configured for a specific agent.
    // To run in a different environment, change the label below.
    agent { label 'dual_xeon_ibm_openj9_jdk21' }

    parameters {
        choice(name: 'JDK_TYPE', choices: ['openj9-open', 'openj9-certified'], description: 'Select the target IBM Semeru JDK type.')
    }

    environment {
        DOCKER_REPO = "jtsweet0891/kafka-jdk-builds"
        DOCKER_CREDENTIALS_ID = 'dockerhub-pat'
        // This is the ID of the GitHub credential you have stored in Jenkins
        GIT_CREDENTIALS_ID = 'github-personal-access-token'
    }

    stages {
        stage('Checkout Source Code') {
            steps {
                cleanWs()
                echo "Checking out the 'feature/openj9-integration' branch..."
                checkout([
                    $class: 'GitSCM',
                    branches: [[name: '*/feature/openj9-integration']],
                    userRemoteConfigs: [[
                        credentialsId: env.GIT_CREDENTIALS_ID,
                        url: 'https://github.com/JTSweet/kafka.git'
                    ]]
                ])
            }
        }

        stage('Initialize Build Environment') {
            steps {
                script {
                    def props = readProperties file: 'gradle.properties'
                    env.KAFKA_VERSION = props['version']
                    // Note: test-status is hardcoded to 'tested' for this pipeline
                    env.TEST_STATUS = "tested"
                    env.FULL_VERSION_TAG = "${env.KAFKA_VERSION}-${params.JDK_TYPE}-${env.TEST_STATUS}-b${env.BUILD_NUMBER}"
                    echo "Building with full version tag: ${env.FULL_VERSION_TAG}"
                }
            }
        }

        stage('Build, Test, and Package Artifacts') {
            steps {
                script {
                    def buildImage = docker.build("kafka-build-ibm-jdk:${env.BUILD_NUMBER}", ".")
                    // Command now INCLUDES all tests by not excluding them
                    def buildCommand = "./gradlew clean releaseTarGz --no-build-cache --no-configuration-cache --no-daemon"

                    buildImage.inside {
                        sh buildCommand
                        sh "./gradlew docker"
                    }
                }
            }
        }

         stage('Test Docker Images') {
            steps {
                script {
                    echo "Starting smoke test for the kafka-kraft Docker image..."
                    def kraftImage = docker.image('kafka-kraft')

                    // Step 1: Generate a unique Cluster ID for the KRaft storage format.
                    def clusterId = sh(returnStdout: true, script: "docker run --rm ${kraftImage.id} bin/kafka-storage.sh random-uuid").trim()
                    echo "Generated KRaft Cluster ID: ${clusterId}"

                    // Step 2: Run a one-off command to format the storage directory.
                    // This uses the default server.properties inside the container.
                    echo "Formatting KRaft storage directory..."
                    sh "docker run --rm ${kraftImage.id} bin/kafka-storage.sh format -t ${clusterId} -c config/kraft/server.properties"

                    // Step 3: Run the container in detached mode for the actual test.
                    kraftImage.withRun('-d') { container ->
                        try {
                            // Wait for the broker to initialize with the newly formatted directory.
                            echo "Waiting for Kafka broker to initialize..."
                            sleep 30

                            // Execute a basic health check command.
                            echo "Executing smoke test command inside the container..."
                            container.exec('bin/kafka-topics.sh --bootstrap-server localhost:9092 --list')

                            echo "Smoke test PASSED. The Kafka broker is responding."
                        } catch (e) {
                            echo "!!! Smoke test FAILED. The container may be unhealthy."
                            sh "docker logs ${container.id}" // Print logs for debugging
                            throw e
                        }
                    }
                }
            }
        }

        stage('Push Docker Images to Docker Hub') {
            steps {
                withCredentials([usernamePassword(credentialsId: env.DOCKER_CREDENTIALS_ID, usernameVariable: 'DOCKER_USER', passwordVariable: 'DOCKER_PASS')]) {
                    script {
                        sh "echo ${DOCKER_PASS} | docker login -u ${DOCKER_USER} --password-stdin"

                        ['', '-kraft'].each { suffix ->
                            def imageName = "kafka${suffix}"
                            def localImage = docker.image(imageName)
                            def remoteImageName = "${env.DOCKER_REPO}:${env.FULL_VERSION_TAG}${suffix}"

                            localImage.tag(remoteImageName)
                            localImage.push()
                        }
                    }
                }
            }
        }

        stage('Archive Classic Artifact') {
            steps {
                script {
                    def buildImageId = sh(returnStdout: true, script: "docker images -q kafka-build-ibm-jdk:${env.BUILD_NUMBER}").trim()
                    def containerId = sh(returnStdout: true, script: "docker create ${buildImageId}").trim()

                    try {
                        def artifactPath = "core/build/distributions/kafka_${env.KAFKA_VERSION}.tgz"
                        def newArtifactName = "kafka-${env.FULL_VERSION_TAG}.tgz"
                        sh "docker cp ${containerId}:/app/${artifactPath} ./build/distributions/${newArtifactName}"

                        archiveArtifacts artifacts: "build/distributions/${newArtifactName}", followSymlinks: false
                    } finally {
                        sh "docker rm ${containerId}"
                    }
                }
            }
        }
    }
    post {
        always {
            cleanWs()
            // Clean up Docker images to save space
            sh "docker rmi jtsweet0891/kafka-jdk-builds:${env.FULL_VERSION_TAG} || true"
            sh "docker rmi jtsweet0891/kafka-jdk-builds:${env.FULL_VERSION_TAG}-kraft || true"
            sh "docker rmi kafka || true"
            sh "docker rmi kafka-kraft || true"
            sh "docker rmi kafka-build-ibm-jdk:${env.BUILD_NUMBER} || true"
        }
    }
}