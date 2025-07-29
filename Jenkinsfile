// Unified, Parameterized Jenkinsfile for Multi-JVM Kafka Builds
pipeline {
    agent none // Agent will be selected dynamically in stages

    parameters {
        choice(name: 'JDK_TYPE', choices: ['openj9-open', 'openj9-certified', 'hotspot'], description: 'Select the target JDK for the build.')
        booleanParam(name: 'RUN_TESTS', defaultValue: true, description: 'Check to run the full unit and integration test suite.')
        string(name: 'JVM_ARGS_OPENJ9', defaultValue: "-Xmx2G -Xgcpolicy:gencon -Xgcthreads1 -XcompilationThreads1 -XX:+DisableExplicitGC", description: 'Custom JVM arguments for OpenJ9 builds.')
        string(name: 'JVM_ARGS_HOTSPOT', defaultValue: "-Xmx2G -XX:+UseG1GC -XX:+DisableExplicitGC", description: 'Custom JVM arguments for HotSpot builds.')
    }

    environment {
        DOCKER_REGISTRY_URL     = 'jtsweet0891' // Docker Hub username or registry URL
        DOCKER_IMAGE_BASE_NAME  = 'apache_kafka_on_ibm_j9'
        DOCKER_CREDENTIALS_ID   = 'dockerhub-pat' // ID of Docker Hub credentials in Jenkins
        KAFKA_VERSION           = '4.0.0'
    }

    stages {
        stage('Initialize Build Environment') {
            agent { label "agent-for-${params.JDK_TYPE}" } // Dynamically select agent based on parameter
            steps {
                script {
                    // Define Test Status for artifact naming
                    def testStatus = params.RUN_TESTS? 'tested' : 'notest'
                    env.TEST_STATUS = testStatus

                    // Define artifact names
                    def artifactBaseName = "kafka-${env.KAFKA_VERSION}-jtsweet-feature-openj9-integration-${params.JDK_TYPE}-${testStatus}-b${env.BUILD_NUMBER}"
                    env.TGZ_ARTIFACT_NAME = "${artifactBaseName}.tgz"
                    env.DOCKER_IMAGE_TAG = "${env.KAFKA_VERSION}-${params.JDK_TYPE}-${testStatus}-b${env.BUILD_NUMBER}"
                    
                    echo "Building with JDK Type: ${params.JDK_TYPE}"
                    echo "Running tests: ${params.RUN_TESTS}"
                    echo "TGZ Artifact Name: ${env.TGZ_ARTIFACT_NAME}"
                    echo "Docker Image Tag: ${env.DOCKER_IMAGE_TAG}"

                    // Set JVM options conditionally using parameters
                    if (params.JDK_TYPE.startsWith('openj9')) {
                        env.JAVA_TOOL_OPTIONS = params.JVM_ARGS_OPENJ9
                    } else { // HotSpot
                        env.JAVA_TOOL_OPTIONS = params.JVM_ARGS_HOTSPOT
                    }
                    echo "Using JAVA_TOOL_OPTIONS: ${env.JAVA_TOOL_OPTIONS}"
                }
            }
        }

        stage('Checkout') {
            agent { label "agent-for-${params.JDK_TYPE}" }
            steps {
                echo 'Checking out source code...'
                checkout scm
            }
        }

        stage('Build and Test') {
            agent { label "agent-for-${params.JDK_TYPE}" }
            steps {
                script {
                    if (params.RUN_TESTS) {
                        echo 'Compiling source and running all unit and integration tests...'
                        sh './gradlew clean build'
                    } else {
                        echo 'Skipping tests as per configuration.'
                    }
                }
            }
        }

        stage('Package Artifacts') {
            agent { label "agent-for-${params.JDK_TYPE}" }
            when { expression { params.RUN_TESTS == true } } // Only run if tests passed
            steps {
                echo 'Packaging classic artifact...'
                sh './gradlew releaseTarGz'
                script {
                    // Rename the artifact to our standard convention
                    sh "mv core/build/distributions/kafka_*.tgz ${env.TGZ_ARTIFACT_NAME}"
                    archiveArtifacts artifacts: env.TGZ_ARTIFACT_NAME, followSymlinks: false
                }
            }
        }

        stage('Package Artifacts (No Tests)') {
            agent { label "agent-for-${params.JDK_TYPE}" }
            when { expression { params.RUN_TESTS == false } } // Only run for no-test builds
            steps {
                echo 'Building Kafka distributable artifact without running tests...'
                sh './gradlew clean releaseTarGz -x test'
                script {
                    // Rename the artifact to our standard convention
                    sh "mv core/build/distributions/kafka_*.tgz ${env.TGZ_ARTIFACT_NAME}"
                    archiveArtifacts artifacts: env.TGZ_ARTIFACT_NAME, followSymlinks: false
                }
            }
        }

        stage('Build and Push Docker Image') {
            agent { label "agent-for-${params.JDK_TYPE}" }
            steps {
                script {
                    def dockerImageName = "${env.DOCKER_REGISTRY_URL}/${env.DOCKER_IMAGE_BASE_NAME}"
                    def fullImageName = "${dockerImageName}:${env.DOCKER_IMAGE_TAG}"
                    
                    def dockerImage = docker.build(fullImageName, ".")
                    
                    docker.withRegistry("https://index.docker.io/v1/", env.DOCKER_CREDENTIALS_ID) {
                        echo "Pushing image ${fullImageName}"
                        dockerImage.push()

                        // Optionally, tag and push a 'latest' tag for the tested build
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
