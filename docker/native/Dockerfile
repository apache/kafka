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

FROM golang:latest AS build-utility
WORKDIR /build
RUN useradd --no-log-init --create-home --shell /bin/bash appuser
COPY --chown=appuser:appuser resources/utility/ ./

# Generate utility executable for dealing with env variables
RUN go build -ldflags="-w -s" ./utility.go
USER appuser
RUN go test ./...


FROM ghcr.io/graalvm/graalvm-community:21 AS build-native-image

ARG kafka_url

WORKDIR /app

ENV KAFKA_URL=$kafka_url
COPY native-image-configs native-image-configs

RUN mkdir kafka; \
    microdnf install wget; \
    wget -nv -O kafka.tgz "$KAFKA_URL"; \
    tar xfz kafka.tgz -C kafka --strip-components 1; \
    rm kafka.tgz ; \
    cd kafka ;  \
    native-image --no-fallback \
        --enable-http \
        --enable-https \
        --allow-incomplete-classpath \
        --report-unsupported-elements-at-runtime \
        --install-exit-handlers \
        --enable-monitoring=jmxserver,jmxclient,heapdump,jvmstat \
        -H:+ReportExceptionStackTraces \
        -H:+EnableAllSecurityServices \
        -H:EnableURLProtocols=http,https \
        -H:AdditionalSecurityProviders=sun.security.jgss.SunProvider \
        -H:ReflectionConfigurationFiles=/app/native-image-configs/reflect-config.json \
        -H:JNIConfigurationFiles=/app/native-image-configs/jni-config.json \
        -H:ResourceConfigurationFiles=/app/native-image-configs/resource-config.json \
        -H:SerializationConfigurationFiles=/app/native-image-configs/serialization-config.json \
        -H:PredefinedClassesConfigurationFiles=/app/native-image-configs/predefined-classes-config.json \
        -H:DynamicProxyConfigurationFiles=/app/native-image-configs/proxy-config.json \
        --verbose \
        -cp "libs/*" kafka.KafkaNativeWrapper \
        -o kafka.Kafka


FROM alpine:latest

EXPOSE 9092

ARG build_date

LABEL org.label-schema.name="kafka" \
      org.label-schema.description="Apache Kafka" \
      org.label-schema.build-date="${build_date}" \
      org.label-schema.vcs-url="https://github.com/apache/kafka" \
      maintainer="Apache Kafka"

RUN apk update ; \
    apk add --no-cache gcompat ; \
    apk add --no-cache bash ; \
    mkdir -p /etc/kafka/secrets /var/lib/kafka/data /mnt/shared/config ; \
    mkdir -p /etc/kafka/docker ; \
    mkdir -p /opt/kafka /opt/kafka/config

COPY --chown=appuser:appuser --from=build-utility /build/utility /usr/bin/
COPY --from=build-native-image /app/kafka/kafka.Kafka /opt/kafka/
COPY --from=build-native-image /app/kafka/config/kraft/server.properties /etc/kafka/docker/
COPY --from=build-native-image /app/kafka/config/log4j.properties /etc/kafka/docker/
COPY --from=build-native-image /app/kafka/config/tools-log4j.properties /etc/kafka/docker/
COPY resources/common-scripts /etc/kafka/docker/
COPY launch /etc/kafka/docker/

VOLUME ["/etc/kafka/secrets", "/var/lib/kafka/data", "/mnt/shared/config"]

#CMD ["sleep", "3000"]
CMD ["/etc/kafka/docker/run"]
