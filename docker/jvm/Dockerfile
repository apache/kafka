###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
###############################################################################

ARG GOLANG_VERSION=1.21.1

FROM golang:${GOLANG_VERSION} AS build-ub
WORKDIR /build
RUN useradd --no-log-init --create-home --shell /bin/bash appuser
COPY --chown=appuser:appuser resources/ub/ ./
RUN go build -ldflags="-w -s" ./ub.go
USER appuser
RUN go test ./...


FROM eclipse-temurin:21-jre

# exposed ports
EXPOSE 9092

USER root

# Get kafka from https://archive.apache.org/dist/kafka and pass the url through build arguments
ARG kafka_url
ARG build_date


LABEL org.label-schema.name="kafka" \
      org.label-schema.description="Apache Kafka" \
      org.label-schema.build-date="${build_date}" \
      org.label-schema.vcs-url="https://github.com/apache/kafka" \
      org.label-schema.schema-version="1.0" \
      maintainer="apache"

ENV KAFKA_URL=$kafka_url

RUN set -eux ; \
    apt-get update ; \
    apt-get upgrade -y ; \
    apt-get install -y --no-install-recommends curl wget gpg dirmngr gpg-agent; \
    mkdir opt/kafka; \
    wget -nv -O kafka.tgz "$KAFKA_URL"; \
    wget -nv -O kafka.tgz.asc "$KAFKA_URL.asc"; \
    tar xfz kafka.tgz -C /opt/kafka --strip-components 1; \
    wget -nv -O KEYS https://downloads.apache.org/kafka/KEYS; \
    gpg --import KEYS; \
    gpg --batch --verify kafka.tgz.asc kafka.tgz; \
    mkdir -p /var/lib/kafka/data /etc/kafka/secrets /var/log/kafka /var/lib/zookeeper; \
    mkdir -p /etc/kafka/docker /usr/logs; \
    useradd --no-log-init --create-home --shell /bin/bash appuser; \
    chown appuser:appuser -R /etc/kafka/ /usr/logs /opt/kafka; \
    chown appuser:root -R /etc/kafka /var/lib/kafka /etc/kafka/secrets /var/lib/kafka /etc/kafka /var/log/kafka /var/lib/zookeeper; \
    chmod -R ug+w /etc/kafka /var/lib/kafka /var/lib/kafka /etc/kafka/secrets /etc/kafka /var/log/kafka /var/lib/zookeeper; \
    rm kafka.tgz; \
    rm kafka.tgz.asc;

COPY --from=build-ub /build/ub /usr/bin
COPY --chown=appuser:appuser resources/common-scripts /etc/kafka/docker
COPY --chown=appuser:appuser launch /etc/kafka/docker/launch

USER appuser

VOLUME ["/etc/kafka/secrets", "/var/lib/kafka/data"]

CMD ["/etc/kafka/docker/run"]
