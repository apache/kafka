# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
COPY resources/common_scripts /etc/kafka/resources
COPY launch /etc/kafka/resources/

CMD ["/etc/kafka/resources/launch"]
