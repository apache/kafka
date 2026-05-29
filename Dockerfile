# Licensed to the Apache Software Foundation (ASF)
# under one or more contributor license agreements.
#
# Enhanced MirrorMaker 2 Image
# Based on Apache Kafka 4.0.0
# Only modifies MirrorSourceTask.java

# ─────────────────────────────────────────────
# Stage 1: COMPILE
# Compile only MirrorSourceTask.java
# ─────────────────────────────────────────────
FROM eclipse-temurin:17-jdk-alpine AS compiler

WORKDIR /build

# Copy the existing connect-mirror jar from
# official Kafka image to use as classpath
COPY --from=apache/kafka:4.0.0 \
    /opt/kafka/libs/ \
    /build/libs/

# Copy our modified source file
COPY connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java \
    /build/MirrorSourceTask.java

# Compile MirrorSourceTask.java against existing jars
RUN javac \
    -cp "/build/libs/*" \
    -d /build/classes \
    /build/MirrorSourceTask.java

# Find the connect-mirror jar and update it
# with our modified class file
RUN MIRROR_JAR=$(ls /build/libs/connect-mirror-[0-9]*.jar) && \
    echo "Found jar: $MIRROR_JAR" && \
    cp "$MIRROR_JAR" /build/connect-mirror-modified.jar && \
    cd /build/classes && \
    jar uf /build/connect-mirror-modified.jar \
    org/apache/kafka/connect/mirror/MirrorSourceTask.class \
    'org/apache/kafka/connect/mirror/MirrorSourceTask$DataLossException.class'

# ─────────────────────────────────────────────
# Stage 2: RUNTIME
# Use official Kafka image and replace MM2 jar
# ─────────────────────────────────────────────
FROM apache/kafka:4.0.0

USER root

# Remove original connect-mirror jar
RUN MIRROR_JAR=$(ls /opt/kafka/libs/connect-mirror-[0-9]*.jar) && \
    rm -f "$MIRROR_JAR"

# Copy our modified jar
COPY --from=compiler \
    /build/connect-mirror-modified.jar \
    /opt/kafka/libs/connect-mirror-4.0.0.jar

USER appuser