# Stage 1: Create a verified base image with the official OpenJ9 JDK
FROM ubuntu:22.04 as jdk-base
ARG JDK_URL=https://github.com/ibmruntimes/semeru21-binaries/releases/download/jdk-21.0.7%2B6_openj9-0.51.0/ibm-semeru-open-jdk_x64_linux_21.0.7_6_openj9-0.51.0.tar.gz
ENV JAVA_HOME=/opt/java/semeru-openj9-jdk-21
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install dependencies and download the specified JDK
RUN apt-get update && apt-get install -y wget && \
    mkdir -p ${JAVA_HOME} && \
    wget -O /tmp/semeru-jdk.tar.gz ${JDK_URL} && \
    tar -xzf /tmp/semeru-jdk.tar.gz -C ${JAVA_HOME} --strip-components=1 && \
    rm /tmp/semeru-jdk.tar.gz && \
    apt-get purge -y wget && apt-get autoremove -y && apt-get clean

# Verify the installation
RUN java -version

# ---
# Stage 2: Build the Kafka artifact using the verified JDK base image
FROM jdk-base as builder
ARG GRADLE_VERSION=8.5

# Install build tools
RUN apt-get update && apt-get install -y unzip wget && \
    wget -O /tmp/gradle.zip https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip && \
    unzip -d /opt/gradle /tmp/gradle.zip && \
    rm /tmp/gradle.zip
ENV PATH="/opt/gradle/gradle-${GRADLE_VERSION}/bin:${PATH}"

# Copy source code and build
WORKDIR /app
COPY . .
RUN chmod +x ./gradlew
ENV GRADLE_OPTS="-Dorg.gradle.jvmargs='-Xmx4g'"
RUN ./gradlew releaseTarGz --max-workers=1 -PmaxParallelForks=1 -PmaxScalacThreads=1 -x test -x integrationTest

# ---
# Stage 3: Create the minimal JRE-based final image
FROM ubuntu:22.04 as jre-image
# CORRECTED: Set the final JRE home path
ENV JAVA_HOME=/opt/java/semeru-openj9-jre-21
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV KAFKA_HOME=/opt/kafka

# CORRECTED: Copy ONLY the JRE sub-directory from the full JDK in the jdk-base stage
COPY --from=jdk-base /opt/java/semeru-openj9-jdk-21/jre ${JAVA_HOME}

# Standard Kafka setup
RUN groupadd -r kafka && useradd -r -g kafka -d ${KAFKA_HOME} kafka && \
    mkdir -p ${KAFKA_HOME} /var/lib/kafka/data /var/log/kafka
COPY --from=builder /app/core/build/distributions/kafka_*.tgz /tmp/kafka.tgz
RUN tar -xzf /tmp/kafka.tgz -C ${KAFKA_HOME} --strip-components=1 && \
    rm /tmp/kafka.tgz && \
    chown -R kafka:kafka ${KAFKA_HOME} /var/lib/kafka /var/log/kafka

WORKDIR ${KAFKA_HOME}
USER kafka
EXPOSE 9092
CMD ["kafka-server-start.sh", "config/kraft/server.properties"]

# ---
# Stage 4: Create the full JDK-based final image
FROM ubuntu:22.04 as jdk-image
ENV JAVA_HOME=/opt/java/semeru-openj9-jdk-21
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV KAFKA_HOME=/opt/kafka

# Copy the FULL JDK from our verified JDK base image
COPY --from=jdk-base ${JAVA_HOME} ${JAVA_HOME}

# Standard Kafka setup
RUN groupadd -r kafka && useradd -r -g kafka -d ${KAFKA_HOME} kafka && \
    mkdir -p ${KAFKA_HOME} /var/lib/kafka/data /var/log/kafka
COPY --from=builder /app/core/build/distributions/kafka_*.tgz /tmp/kafka.tgz
RUN tar -xzf /tmp/kafka.tgz -C ${KAFKA_HOME} --strip-components=1 && \
    rm /tmp/kafka.tgz && \
    chown -R kafka:kafka ${KAFKA_HOME} /var/lib/kafka /var/log/kafka

WORKDIR ${KAFKA_HOME}
USER kafka
EXPOSE 9092
CMD ["kafka-server-start.sh", "config/kraft/server.properties"]