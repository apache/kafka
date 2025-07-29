# Stage 1: The Builder
# Use an IBM Semeru OpenJ9 JDK image that has the necessary build tools.
# For simplicity, we'll use a standard OpenJDK image and install Gradle.
# A pre-built image with both could also be used.
FROM ibm-semeru-runtimes:open-21-jdk as builder

# Install build dependencies
USER root
RUN apt-get update && apt-get install -y unzip

# Install Gradle
ARG GRADLE_VERSION=8.5
RUN apt-get install -y wget && \
    wget https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip -P /tmp && \
    unzip -d /opt/gradle /tmp/gradle-${GRADLE_VERSION}-bin.zip
ENV PATH="/opt/gradle/gradle-${GRADLE_VERSION}/bin:${PATH}"

# Copy the application source code into the container
WORKDIR /app
COPY..

# Grant execution permissions to the Gradle wrapper
RUN chmod +x./gradlew

# Build the classic distributable artifact
# The -x test flag skips running tests, as per the requirement for the first Jenkins job.
# For the second job, this flag would be removed from the command in the Jenkinsfile.
RUN./gradlew releaseTarGz -x test

# Unpack the artifact for easy copying in the next stage
RUN mkdir -p /app/dist && \
    tar -xzf./core/build/distributions/kafka_*.tgz -C /app/dist --strip-components=1

# Stage 2: The Final Production Image
# Start from a full IBM Semeru OpenJ9 JDK image for diagnostics
FROM ibm-semeru-runtimes:open-21-jdk

# Define environment variables for Kafka configuration
ENV KAFKA_HOME=/opt/kafka
ENV PATH="${KAFKA_HOME}/bin:${PATH}"

# Create a non-root user and group for security
RUN groupadd -r kafka && useradd -r -g kafka -d /opt/kafka kafka

# Create Kafka directories and set permissions
RUN mkdir -p $KAFKA_HOME /var/lib/kafka/data /var/log/kafka && \
    chown -R kafka:kafka $KAFKA_HOME /var/lib/kafka /var/log/kafka

# Set the working directory
WORKDIR $KAFKA_HOME

# Copy the built Kafka distribution from the builder stage
COPY --from=builder --chown=kafka:kafka /app/dist.

# Switch to the non-root user
USER kafka

# Expose the default Kafka port
EXPOSE 9092

# Define the default command to run when the container starts
# This will start the Kafka broker using our modified scripts
CMD ["kafka-server-start.sh", "config/kraft/server.properties"]
