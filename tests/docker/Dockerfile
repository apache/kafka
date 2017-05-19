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

FROM openjdk:8

MAINTAINER Apache Kafka dev@kafka.apache.org
VOLUME ["/opt/kafka-dev"]
ENV MIRROR="http://apache.cs.utah.edu/"

ADD ssh /root/.ssh
RUN chmod 600 /root/.ssh/id_rsa
RUN apt update && apt install -y unzip wget curl jq coreutils openssh-server net-tools vim python-pip python-dev libffi-dev libssl-dev
RUN pip install -U pip && pip install --upgrade cffi ducktape==0.6.0
# Test with multiple Scala version
RUN mkdir -p "/opt/kafka-0.8.2.2" && curl -s "${MIRROR}kafka/0.8.2.2/kafka_2.10-0.8.2.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-0.8.2.2"
RUN mkdir -p "/opt/kafka-0.9.0.1" && curl -s "${MIRROR}kafka/0.9.0.1/kafka_2.11-0.9.0.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-0.9.0.1"
RUN mkdir -p "/opt/kafka-0.10.0.1" && curl -s "${MIRROR}kafka/0.10.0.1/kafka_2.11-0.10.0.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-0.10.0.1"
RUN mkdir -p "/opt/kafka-0.10.1.1" && curl -s "${MIRROR}kafka/0.10.1.1/kafka_2.11-0.10.1.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-0.10.1.1"

CMD service ssh start && tail -f /dev/null
