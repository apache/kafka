##
# Copyright 2023 Aiven Oy
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##
# Kafka 3.4.x
FROM confluentinc/cp-kafka:7.4.0

ARG _SCALA_VERSION
ARG _KAFKA_VERSION
ENV _KAFKA_FULL_VERSION "kafka_${_SCALA_VERSION}-${_KAFKA_VERSION}"

USER root
COPY core/build/distributions/${_KAFKA_FULL_VERSION}.tgz /
RUN cd / \
  && tar -xf ${_KAFKA_FULL_VERSION}.tgz \
  && rm -r /usr/share/java/kafka/* \
  && cp /${_KAFKA_FULL_VERSION}/libs/* /usr/share/java/kafka/ \
  && ln -s /usr/share/java/kafka/${_KAFKA_FULL_VERSION}.jar /usr/share/java/kafka/kafka.jar \
  && rm -r /${_KAFKA_FULL_VERSION}.tgz /${_KAFKA_FULL_VERSION}

# Add test jars with local implementations.
COPY clients/build/libs/kafka-clients-${_KAFKA_VERSION}-test.jar /usr/share/java/kafka/
COPY storage/build/libs/kafka-storage-${_KAFKA_VERSION}-test.jar /usr/share/java/kafka/
COPY storage/api/build/libs/kafka-storage-api-${_KAFKA_VERSION}-test.jar /usr/share/java/kafka/

# Restore the user.
USER appuser
