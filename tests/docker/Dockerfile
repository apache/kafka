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
ENV MIRROR="http://mirror.olnevhost.net/pub/apache/"

# Set the timezone.
ENV TZ="/usr/share/zoneinfo/America/Los_Angeles"

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# Set the ducker.creator label so that we know that this is a ducker image.  This will make it
# visible to 'ducker purge'.  The ducker.creator label also lets us know what UNIX user built this
# image.
ARG ducker_creator=default
LABEL ducker.creator=$ducker_creator

# Update Linux and install necessary utilities.
RUN apt update && apt install -y sudo netcat iptables rsync unzip wget curl jq coreutils openssh-server net-tools vim python-pip python-dev libffi-dev libssl-dev cmake pkg-config libfuse-dev && apt-get -y clean
RUN pip install -U pip setuptools && pip install --upgrade cffi virtualenv pyasn1 boto3 pycrypto pywinrm && pip install --upgrade ducktape==0.7.1

# Set up ssh
COPY ./ssh-config /root/.ssh/config
RUN ssh-keygen -q -t rsa -N '' -f /root/.ssh/id_rsa && cp -f /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

# Install binary test dependencies.
ENV MIRROR="http://mirrors.ocf.berkeley.edu/apache/"
RUN mkdir -p "/opt/kafka-0.8.2.2" && curl -s "${MIRROR}kafka/0.8.2.2/kafka_2.10-0.8.2.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-0.8.2.2"
RUN mkdir -p "/opt/kafka-0.9.0.1" && curl -s "${MIRROR}kafka/0.9.0.1/kafka_2.11-0.9.0.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-0.9.0.1"
RUN mkdir -p "/opt/kafka-0.10.0.1" && curl -s "${MIRROR}kafka/0.10.0.1/kafka_2.11-0.10.0.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-0.10.0.1"
RUN mkdir -p "/opt/kafka-0.10.1.1" && curl -s "${MIRROR}kafka/0.10.1.1/kafka_2.11-0.10.1.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-0.10.1.1"
RUN mkdir -p "/opt/kafka-0.10.2.1" && curl -s "${MIRROR}kafka/0.10.2.1/kafka_2.11-0.10.2.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-0.10.2.1"
RUN mkdir -p "/opt/kafka-0.11.0.0" && curl -s "${MIRROR}kafka/0.11.0.0/kafka_2.11-0.11.0.0.tgz" | tar xz --strip-components=1 -C "/opt/kafka-0.11.0.0"

# Set up the ducker user.
RUN useradd -ms /bin/bash ducker && mkdir -p /home/ducker/ && rsync -aiq /root/.ssh/ /home/ducker/.ssh && chown -R ducker /home/ducker/ /mnt/ && echo 'ducker ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers
USER ducker

CMD sudo service ssh start && tail -f /dev/null
