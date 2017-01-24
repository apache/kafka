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
RUN apt update
RUN apt install -y unzip wget curl jq coreutils openssh-server net-tools vim openjdk-8-jdk python-pip python-dev libffi-dev libssl-dev
RUN pip install -U pip
RUN pip install --upgrade cffi
RUN pip install ducktape==0.6.0

VOLUME ["/kafka_src"]

ENV MIRROR="http://apache.cs.utah.edu/"
RUN wget -q "${MIRROR}kafka/0.8.2.2/kafka_2.10-0.8.2.2.tgz" -O "/tmp/kafka_2.10-0.8.2.2.tgz" && tar xfz /tmp/kafka_2.10-0.8.2.2.tgz -C /opt && mv "/opt/kafka_2.10-0.8.2.2" "/opt/kafka-0.8.2.2"
RUN wget -q "${MIRROR}kafka/0.9.0.1/kafka_2.10-0.9.0.1.tgz" -O "/tmp/kafka_2.10-0.9.0.1.tgz" && tar xfz /tmp/kafka_2.10-0.9.0.1.tgz -C /opt && mv "/opt/kafka_2.10-0.9.0.1" "/opt/kafka-0.9.0.1"
RUN wget -q "${MIRROR}kafka/0.10.0.1/kafka_2.10-0.10.0.1.tgz" -O "/tmp/kafka_2.10-0.10.0.1.tgz" && tar xfz /tmp/kafka_2.10-0.10.0.1.tgz -C /opt && mv "/opt/kafka_2.10-0.10.0.1" "/opt/kafka-0.10.0.1"
RUN wget -q "${MIRROR}kafka/0.10.1.1/kafka_2.10-0.10.1.1.tgz" -O "/tmp/kafka_2.10-0.10.1.1.tgz" && tar xfz /tmp/kafka_2.10-0.10.1.1.tgz -C /opt && mv "/opt/kafka_2.10-0.10.1.1" "/opt/kafka-0.10.1.1"

RUN rm /tmp/kafka_*.tgz
ADD ssh /root/.ssh
RUN chmod 600 /root/.ssh/id_rsa

CMD service ssh start && tail -f /dev/null
