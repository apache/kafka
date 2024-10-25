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

FROM eclipse-temurin:21-jre-alpine AS build-jsa

# Get kafka from https://archive.apache.org/dist/kafka and pass the url through build arguments
ARG kafka_url
ARG GPG_KEY

COPY jsa_launch /etc/kafka/docker/jsa_launch

RUN set -eux ; \
    apk add --no-cache wget gcompat gpg gpg-agent procps bash; \
    wget -nv -O kafka.tgz "$kafka_url"; \
    wget -nv -O kafka.tgz.asc "$kafka_url.asc"; \
    export GNUPGHOME="$(mktemp -d)"; \
    gpg --batch --keyserver hkp://keys.openpgp.org --recv-keys "$GPG_KEY" || \
    gpg --batch --keyserver keyserver.ubuntu.com --recv-keys "$GPG_KEY" ; \
    gpg --batch --verify kafka.tgz.asc kafka.tgz; \
    gpgconf --kill all; \
    rm -rf "$GNUPGHOME" kafka.tgz.asc; \
    mkdir opt/kafka; \
    tar xfz kafka.tgz -C /opt/kafka --strip-components 1; \
    # Generate jsa files using dynamic CDS for kafka server start command and kafka storage format command
    /etc/kafka/docker/jsa_launch


FROM eclipse-temurin:21-jre-alpine

# exposed ports
EXPOSE 9092

# Get kafka from https://archive.apache.org/dist/kafka and pass the url through build arguments
ARG kafka_url
ARG build_date
ARG GPG_KEY

LABEL org.opencontainers.image.title="kafka" \
      org.opencontainers.image.description="Apache Kafka" \
      org.opencontainers.image.created="${build_date}" \
      org.opencontainers.image.source="https://github.com/apache/kafka" \
      maintainer="Apache Kafka"

RUN set -eux ; \
    apk add --no-cache wget gcompat gpg gpg-agent procps bash; \
    wget -nv -O kafka.tgz "$kafka_url"; \
    wget -nv -O kafka.tgz.asc "$kafka_url.asc"; \
    export GNUPGHOME="$(mktemp -d)"; \
    gpg --batch --keyserver hkp://keys.openpgp.org --recv-keys "$GPG_KEY" || \
    gpg --batch --keyserver keyserver.ubuntu.com --recv-keys "$GPG_KEY" ; \
    gpg --batch --verify kafka.tgz.asc kafka.tgz; \
    gpgconf --kill all; \
    rm -rf "$GNUPGHOME" kafka.tgz.asc; \
    mkdir opt/kafka; \
    tar xfz kafka.tgz -C /opt/kafka --strip-components 1; \
    mkdir -p /var/lib/kafka/data /etc/kafka/secrets; \
    mkdir -p /etc/kafka/docker /usr/logs /mnt/shared/config; \
    adduser -h /home/appuser -D --shell /bin/bash appuser; \
    chown appuser:appuser -R /usr/logs /opt/kafka /mnt/shared/config; \
    chown appuser:root -R /var/lib/kafka /etc/kafka/secrets /etc/kafka; \
    chmod -R ug+w /etc/kafka /var/lib/kafka /etc/kafka/secrets; \
    cp /opt/kafka/config/log4j.properties /etc/kafka/docker/log4j.properties; \
    cp /opt/kafka/config/tools-log4j.properties /etc/kafka/docker/tools-log4j.properties; \
    cp /opt/kafka/config/kraft/server.properties /etc/kafka/docker/server.properties; \
    rm kafka.tgz; \
    apk del wget gpg gpg-agent; 

COPY --from=build-jsa kafka.jsa /opt/kafka/kafka.jsa
COPY --from=build-jsa storage.jsa /opt/kafka/storage.jsa
COPY --chown=appuser:appuser resources/common-scripts /etc/kafka/docker
COPY --chown=appuser:appuser launch /etc/kafka/docker/launch

USER appuser

VOLUME ["/etc/kafka/secrets", "/var/lib/kafka/data", "/mnt/shared/config"]

CMD ["/etc/kafka/docker/run"]
