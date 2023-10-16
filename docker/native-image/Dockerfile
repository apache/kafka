FROM golang:1.21-bullseye AS build-ub
WORKDIR /build
RUN useradd --no-log-init --create-home --shell /bin/bash appuser
COPY --chown=appuser:appuser resources/ub/ ./
RUN go build -ldflags="-w -s" ./ub.go
USER appuser
RUN go test ./...


FROM ghcr.io/graalvm/graalvm-community:17 AS build-native-image

WORKDIR /app

COPY kafka_2.13-3.7.0-SNAPSHOT.tgz kafka_2.13-3.7.0-SNAPSHOT.tgz
COPY native-image-configs native-image-configs

RUN tar -xzf kafka_2.13-3.7.0-SNAPSHOT.tgz ; \
    rm kafka_2.13-3.7.0-SNAPSHOT.tgz ; \
    cd kafka_2.13-3.7.0-SNAPSHOT ;  \
    native-image --no-fallback \
        --allow-incomplete-classpath \
        --report-unsupported-elements-at-runtime \
        --install-exit-handlers \
        -H:+ReportExceptionStackTraces \
        -H:ReflectionConfigurationFiles=/app/native-image-configs/reflect-config.json \
        -H:JNIConfigurationFiles=/app/native-image-configs/jni-config.json \
        -H:ResourceConfigurationFiles=/app/native-image-configs/resource-config.json \
        -H:SerializationConfigurationFiles=/app/native-image-configs/serialization-config.json \
        -H:PredefinedClassesConfigurationFiles=/app/native-image-configs/predefined-classes-config.json \
        -H:DynamicProxyConfigurationFiles=/app/native-image-configs/proxy-config.json \
        --verbose \
        -cp "libs/*" kafka.KafkaNativeWrapper


FROM alpine:latest
RUN apk update && \
    apk add --no-cache gcompat && \
    mkdir -p /etc/kafka/config
WORKDIR /app

COPY --from=build-ub /build/ub /usr/bin
COPY --from=build-native-image /app/kafka_2.13-3.7.0-SNAPSHOT/kafka.kafkanativewrapper .
#COPY --from=build-native-image /app/kafka_2.13-3.7.0-SNAPSHOT/config/kraft/server.properties server.properties
#COPY --from=build-native-image /app/kafka_2.13-3.7.0-SNAPSHOT/config/log4j.properties log4j.properties
COPY resources/docker_scripts/include/etc/kafka/docker /etc/kafka/resources
COPY singleScript /etc/kafka/resources/

CMD ["/etc/kafka/resources/singleScript"]
