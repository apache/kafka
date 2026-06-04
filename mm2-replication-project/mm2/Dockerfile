# =============================================================================
# Enhanced MirrorMaker 2 image
#
# Stage 1 builds the modified `connect-mirror` module (containing the Task 2/3
# changes) from the Kafka fork. Stage 2 starts from the official Apache Kafka
# image and overlays the freshly built MirrorMaker 2 jars, so the running
# MirrorMaker 2 uses the enhanced code.
#
# Build context: the root of the Kafka fork checkout (this repo root).
#   docker build -f mm2-replication-project/mm2/Dockerfile -t <user>/enhanced-mm2:latest .
# =============================================================================

# ---- Stage 1: build the fork ------------------------------------------------
FROM eclipse-temurin:21-jdk AS build

WORKDIR /kafka
# Copy the entire fork. (A .dockerignore excludes build output and .git to keep
# the context small.)
COPY . .

# Build only the connect-mirror module and its dependencies. This produces the
# enhanced connect-mirror jar under connect/mirror/build/libs and copies all
# runtime dependencies under connect/mirror/build/dependant-libs.
RUN ./gradlew --no-daemon :connect:mirror:copyDependantLibs :connect:mirror:jar

# ---- Stage 2: runtime image -------------------------------------------------
FROM apache/kafka:4.0.0

USER root

# Replace the stock connect-mirror jar(s) with the enhanced build. The official
# image ships MirrorMaker 2 jars under /opt/kafka/libs; overwriting the
# connect-mirror-*.jar there is sufficient because only MirrorSourceTask /
# LogTruncationException changed.
COPY --from=build /kafka/connect/mirror/build/libs/connect-mirror-*.jar /opt/kafka/libs/
COPY --from=build /kafka/connect/mirror/build/dependant-libs/ /opt/kafka/libs/

# Bundled MM2 configuration.
COPY mm2-replication-project/mm2/mm2.properties /opt/kafka/config/mm2.properties

USER appuser

# Run MirrorMaker 2 in dedicated mode against the bundled config.
ENTRYPOINT ["/opt/kafka/bin/connect-mirror-maker.sh", "/opt/kafka/config/mm2.properties"]
