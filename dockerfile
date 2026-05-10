# Stage 1: Build the Java code
FROM gradle:8-jdk17 AS builder
WORKDIR /app
# Copy your modified Kafka repository into the container
COPY . /app/
# Fix Windows line endings and compile all necessary Kafka modules and their dependencies
RUN sed -i 's/\r$//' gradlew && ./gradlew \
    :clients:jar \
    :connect:api:jar \
    :connect:runtime:jar \
    :connect:json:jar \
    :connect:mirror:jar \
    :connect:mirror-client:jar \
    :connect:mirror:copyDependantLibs \
    -x test
 
# Stage 2: Create the actual Kafka Image
FROM apache/kafka:3.7.0
USER root
 
# Move all dependant libs and our jars into the Kafka libs directory
# We use a temporary directory first to avoid conflicts during copy
COPY --from=builder /app/clients/build/libs/kafka-clients-*.jar /opt/kafka/libs/
COPY --from=builder /app/connect/api/build/libs/connect-api-*.jar /opt/kafka/libs/
COPY --from=builder /app/connect/runtime/build/libs/connect-runtime-*.jar /opt/kafka/libs/
COPY --from=builder /app/connect/json/build/libs/connect-json-*.jar /opt/kafka/libs/
COPY --from=builder /app/connect/mirror/build/libs/connect-mirror-*.jar /opt/kafka/libs/
COPY --from=builder /app/connect/mirror-client/build/libs/connect-mirror-client-*.jar /opt/kafka/libs/
COPY --from=builder /app/connect/mirror/build/dependant-libs/*.jar /opt/kafka/libs/
 
# Ensure we remove potential duplicate jars with different versions (3.7.0 vs 4.4.0)
# This is a bit risky but necessary for the newer version to work.
# We'll rely on the newer jars we just copied.
RUN rm -f /opt/kafka/libs/connect-mirror-3.7.0.jar \
          /opt/kafka/libs/connect-mirror-client-3.7.0.jar \
          /opt/kafka/libs/kafka-clients-3.7.0.jar \
          /opt/kafka/libs/connect-api-3.7.0.jar \
          /opt/kafka/libs/connect-runtime-3.7.0.jar \
          /opt/kafka/libs/connect-json-3.7.0.jar
 
USER appuser
