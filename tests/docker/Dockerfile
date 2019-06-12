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

ARG jdk_version=openjdk:8
FROM $jdk_version

MAINTAINER Apache Kafka dev@kafka.apache.org
VOLUME ["/opt/kafka-dev"]

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
RUN apt update && apt install -y sudo netcat iptables rsync unzip wget curl jq coreutils openssh-server net-tools vim python-pip python-dev libffi-dev libssl-dev cmake pkg-config libfuse-dev  && apt-get -y clean
RUN python -m pip install -U pip==9.0.3;
RUN pip install --upgrade cffi virtualenv pyasn1 boto3 pycrypto pywinrm ipaddress enum34 && pip install --upgrade ducktape==0.7.5

# Set up ssh
COPY ./ssh-config /root/.ssh/config
# NOTE: The paramiko library supports the PEM-format private key, but does not support the RFC4716 format.
RUN ssh-keygen -m PEM -q -t rsa -N '' -f /root/.ssh/id_rsa && cp -f /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
RUN echo 'PermitUserEnvironment yes' >> /etc/ssh/sshd_config

# Install binary test dependencies.
# we use the same versions as in vagrant/base.sh
ARG KAFKA_MIRROR="https://s3-us-west-2.amazonaws.com/kafka-packages"
RUN mkdir -p "/opt/kafka-0.8.2.2" && chmod a+rw /opt/kafka-0.8.2.2 && curl -s "$KAFKA_MIRROR/kafka_2.11-0.8.2.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-0.8.2.2"
RUN mkdir -p "/opt/kafka-0.9.0.1" && chmod a+rw /opt/kafka-0.9.0.1 && curl -s "$KAFKA_MIRROR/kafka_2.11-0.9.0.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-0.9.0.1"
RUN mkdir -p "/opt/kafka-0.10.0.1" && chmod a+rw /opt/kafka-0.10.0.1 && curl -s "$KAFKA_MIRROR/kafka_2.11-0.10.0.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-0.10.0.1"
RUN mkdir -p "/opt/kafka-0.10.1.1" && chmod a+rw /opt/kafka-0.10.1.1 && curl -s "$KAFKA_MIRROR/kafka_2.11-0.10.1.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-0.10.1.1"
RUN mkdir -p "/opt/kafka-0.10.2.2" && chmod a+rw /opt/kafka-0.10.2.2 && curl -s "$KAFKA_MIRROR/kafka_2.11-0.10.2.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-0.10.2.2"
RUN mkdir -p "/opt/kafka-0.11.0.3" && chmod a+rw /opt/kafka-0.11.0.3 && curl -s "$KAFKA_MIRROR/kafka_2.11-0.11.0.3.tgz" | tar xz --strip-components=1 -C "/opt/kafka-0.11.0.3"
RUN mkdir -p "/opt/kafka-1.0.2" && chmod a+rw /opt/kafka-1.0.2 && curl -s "$KAFKA_MIRROR/kafka_2.11-1.0.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-1.0.2"
RUN mkdir -p "/opt/kafka-1.1.1" && chmod a+rw /opt/kafka-1.1.1 && curl -s "$KAFKA_MIRROR/kafka_2.11-1.1.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-1.1.1"
RUN mkdir -p "/opt/kafka-2.0.1" && chmod a+rw /opt/kafka-2.0.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.0.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.0.1"
RUN mkdir -p "/opt/kafka-2.1.1" && chmod a+rw /opt/kafka-2.1.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.1.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.1.1"
RUN mkdir -p "/opt/kafka-2.2.0" && chmod a+rw /opt/kafka-2.2.0 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.2.0.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.2.0"

# Streams test dependencies
RUN curl -s "$KAFKA_MIRROR/kafka-streams-0.10.0.1-test.jar" -o /opt/kafka-0.10.0.1/libs/kafka-streams-0.10.0.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-0.10.1.1-test.jar" -o /opt/kafka-0.10.1.1/libs/kafka-streams-0.10.1.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-0.10.2.2-test.jar" -o /opt/kafka-0.10.2.2/libs/kafka-streams-0.10.2.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-0.11.0.3-test.jar" -o /opt/kafka-0.11.0.3/libs/kafka-streams-0.11.0.3-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-1.0.2-test.jar" -o /opt/kafka-1.0.2/libs/kafka-streams-1.0.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-1.1.1-test.jar" -o /opt/kafka-1.1.1/libs/kafka-streams-1.1.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.0.1-test.jar" -o /opt/kafka-2.0.1/libs/kafka-streams-2.0.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.1.1-test.jar" -o /opt/kafka-2.1.1/libs/kafka-streams-2.1.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.2.0-test.jar" -o /opt/kafka-2.2.0/libs/kafka-streams-2.2.0-test.jar

# The version of Kibosh to use for testing.
# If you update this, also update vagrant/base.sy
ARG KIBOSH_VERSION="d85ac3ec44be0700efe605c16289fd901cfdaa13"

# Install Kibosh
RUN apt-get install fuse
RUN cd /opt && git clone -q  https://github.com/confluentinc/kibosh.git && cd "/opt/kibosh" && git reset --hard $KIBOSH_VERSION && mkdir "/opt/kibosh/build" && cd "/opt/kibosh/build" && ../configure && make -j 2

# Set up the ducker user.
RUN useradd -ms /bin/bash ducker && mkdir -p /home/ducker/ && rsync -aiq /root/.ssh/ /home/ducker/.ssh && chown -R ducker /home/ducker/ /mnt/ /var/log/ && echo "PATH=$(runuser -l ducker -c 'echo $PATH'):$JAVA_HOME/bin" >> /home/ducker/.ssh/environment && echo 'PATH=$PATH:'"$JAVA_HOME/bin" >> /home/ducker/.profile && echo 'ducker ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers
USER ducker

CMD sudo service ssh start && tail -f /dev/null
