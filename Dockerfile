# Use a stable, well-supported base image like Ubuntu 20.04 (Focal Fossa)
# This provides a consistent foundation for both the build and final runtime environments.
ARG BASE_IMAGE=ubuntu:focal
FROM ${BASE_IMAGE} as base

# Accept the JDK download URL as a build-time argument.
# This makes the Dockerfile flexible for both Open and Certified editions.
ARG JDK_URL
ARG JDK_SHA256

# Install necessary tools for downloading and extracting the JDK
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    tar \
    gzip \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt

# Download, verify, and extract the specified JDK from the provided URL
RUN wget -O jdk.tar.gz "${JDK_URL}" && \
    echo "${JDK_SHA256} *jdk.tar.gz" | sha256sum -c - && \
    mkdir -p /opt/java && \
    tar -zxf jdk.tar.gz -C /opt/java --strip-components=1 && \
    rm jdk.tar.gz

# Set JAVA_HOME and add it to the system PATH
ENV JAVA_HOME=/opt/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Stage 1: The Builder
# This stage inherits the base with the correct JDK already installed.
FROM base as builder

# Install build dependencies (Gradle)
ARG GRADLE_VERSION=8.5
RUN apt-get update && apt-get install -y --no-install-recommends \
    unzip \
    && wget https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip -P /tmp \
    && unzip -d /opt/gradle /tmp/gradle-${GRADLE_VERSION}-bin.zip \
    && rm /tmp/gradle-${GRADLE_VERSION}-bin.zip \
    && rm -rf /var/lib/apt/lists/*
ENV PATH="/opt/gradle/gradle-${GRADLE_VERSION}/bin:${PATH}"

# Copy the application source code
WORKDIR /app
COPY..

# Grant execution permissions to the Gradle wrapper
RUN chmod +x./gradlew

# Build the classic distributable artifact
RUN./gradlew releaseTarGz -x test

# Unpack the artifact for easy copying in the next stage
RUN mkdir -p /app/dist && \
    tar -xzf./core/build/distributions/kafka_*.tgz -C /app/dist --strip-components=1

# Stage 2: The Final Production Image
# This stage also inherits the base with the full JDK for diagnostics.
FROM base

# Define environment variables for Kafka configuration
ENV KAFKA_HOME=/opt/kafka
ENV PATH="${KAFKA_HOME}/bin:${PATH}"

# Create a non-root user and group for security
RUN groupadd -r kafka && useradd -r -g kafka -d ${KAFKA_HOME} kafka

# Create Kafka directories and set permissions
RUN mkdir -p ${KAFKA_HOME} /var/lib/kafka/data /var/log/kafka && \
    chown -R kafka:kafka ${KAFKA_HOME} /var/lib/kafka /var/log/kafka

# Set the working directory
WORKDIR ${KAFKA_HOME}

# Copy the built Kafka distribution from the builder stage
COPY --from=builder --chown=kafka:kafka /app/dist .

# Switch to the non-root user
USER kafka

# Expose the default Kafka port
EXPOSE 9092

# Define the default command to run when the container starts
CMD ["kafka-server-start.sh", "config/kraft/server.properties"]
