# =============================================================================
# STAGE 1: Build Stage
#
# This stage uses the full IBM Semeru OpenJ9 JDK to compile the Kafka source
# code and create the final distribution tarball.
# =============================================================================
FROM ibm-semeru-runtimes:open-21-jdk as builder

LABEL maintainer="Your Name <youremail@example.com>"
LABEL description="Builder stage for Apache Kafka on IBM Semeru JDK"

# Set the working directory for the build.
WORKDIR /app

# Grant execution permissions to the Gradle wrapper script before copying.
# This avoids issues with file permissions in the build context.
COPY gradlew .
COPY gradle ./gradle
RUN chmod +x ./gradlew

# Copy the rest of the project source code.
COPY . .

# Execute the build to create the final distribution tarball.
# We use releaseTarGz to get the complete package.
RUN ./gradlew releaseTarGz -x test -x integrationTest --no-build-cache --no-configuration-cache --no-daemon

# =============================================================================
# STAGE 2: Runtime Stage
#
# This stage creates the final, lean Docker image.
# It uses the full JDK as requested for access to diagnostic tools.
# =============================================================================
FROM ibm-semeru-runtimes:open-21-jdk

LABEL maintainer="Your Name <youremail@example.com>"
LABEL description="Apache Kafka running on IBM Semeru (OpenJ9) JDK 21"

# Set environment variables for Kafka home and add Kafka's bin to the PATH.
ENV KAFKA_HOME=/opt/kafka
ENV PATH="${KAFKA_HOME}/bin:${PATH}"

# Create a non-root user and group for better security.
RUN groupadd -r kafka && useradd -r -g kafka kafka
RUN mkdir -p /opt/kafka && chown -R kafka:kafka /opt/kafka

# Set the working directory for the running Kafka application.
WORKDIR /opt/kafka

# Copy ONLY the final distribution tarball from the builder stage.
# The path is corrected to where releaseTarGz places the artifact.
COPY --from=builder /app/core/build/distributions/kafka_*.tgz .

# Unpack the distribution archive, strip the top-level directory,
# and remove the .tgz file to save space.
RUN tar -xzf kafka_*.tgz --strip-components=1 && rm kafka_*.tgz

# Switch to the non-root user.
USER kafka

# Expose the default Kafka ports for client and KRaft connections.
EXPOSE 9092 9093

# Define the command to run when the container starts.
CMD ["kafka-server-start.sh", "config/kraft/server.properties"]