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

FROM ghcr.io/graalvm/graalvm-community:21 AS build-native-image

ARG kafka_url

WORKDIR /app

ENV KAFKA_URL=$kafka_url
ENV NATIVE_IMAGE_PATH="native-image"
ENV KAFKA_DIR="/app/kafka"
ENV NATIVE_CONFIGS_DIR="/app/native-image-configs"
ENV KAFKA_LIBS_DIR="$KAFKA_DIR/libs"
ENV TARGET_PATH="$KAFKA_DIR/kafka.Kafka"

COPY native-image-configs $NATIVE_CONFIGS_DIR
COPY native_command.sh native_command.sh

RUN mkdir $KAFKA_DIR; \
    microdnf install wget; \
    wget -nv -O kafka.tgz "$KAFKA_URL"; \
    wget -nv -O kafka.tgz.asc "$KAFKA_URL.asc"; \
    tar xfz kafka.tgz -C $KAFKA_DIR --strip-components 1; \
    wget -nv -O KEYS https://downloads.apache.org/kafka/KEYS; \
    gpg --import KEYS; \
    gpg --batch --verify kafka.tgz.asc kafka.tgz; \
    rm kafka.tgz ; \
# Build the native-binary of the apache kafka using graalVM native-image.
    /app/native_command.sh $NATIVE_IMAGE_PATH $NATIVE_CONFIGS_DIR $KAFKA_LIBS_DIR $TARGET_PATH


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
    mkdir -p /etc/kafka/docker /mnt/shared/config /opt/kafka/config /etc/kafka/secrets ; \
    adduser -h /home/appuser -D --shell /bin/bash appuser ; \
    chown appuser:root -R /etc/kafka /opt/kafka /mnt/shared/config ; \
    chmod -R ug+w /etc/kafka /opt/kafka /mnt/shared/config ;

COPY --chown=appuser:root --from=build-native-image /app/kafka/kafka.Kafka /opt/kafka/
COPY --chown=appuser:root --from=build-native-image /app/kafka/config/kraft/reconfig-server.properties /etc/kafka/docker/
COPY --chown=appuser:root --from=build-native-image /app/kafka/config/log4j.properties /etc/kafka/docker/
COPY --chown=appuser:root --from=build-native-image /app/kafka/config/tools-log4j.properties /etc/kafka/docker/
COPY --chown=appuser:root resources/common-scripts /etc/kafka/docker/
COPY --chown=appuser:root launch /etc/kafka/docker/

USER appuser

VOLUME ["/etc/kafka/secrets", "/mnt/shared/config"]

CMD ["/etc/kafka/docker/run"]
