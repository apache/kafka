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
# we have to install git since it is included in openjdk:8 but not openjdk:11
RUN apt update && apt install -y sudo git netcat iptables rsync unzip wget curl jq coreutils openssh-server net-tools vim python3-pip python3-dev libffi-dev libssl-dev cmake pkg-config libfuse-dev iperf traceroute iproute2 && apt-get -y clean
RUN python3 -m pip install -U pip==21.1.1;
RUN pip3 install --upgrade cffi virtualenv pyasn1 boto3 pycrypto pywinrm ipaddress enum34 debugpy && pip3 install --upgrade "ducktape>0.8"

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
RUN mkdir -p "/opt/kafka-2.2.2" && chmod a+rw /opt/kafka-2.2.2 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.2.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.2.2"
RUN mkdir -p "/opt/kafka-2.3.1" && chmod a+rw /opt/kafka-2.3.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.3.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.3.1"
RUN mkdir -p "/opt/kafka-2.4.1" && chmod a+rw /opt/kafka-2.4.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.4.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.4.1"
RUN mkdir -p "/opt/kafka-2.5.1" && chmod a+rw /opt/kafka-2.5.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.5.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.5.1"
RUN mkdir -p "/opt/kafka-2.6.2" && chmod a+rw /opt/kafka-2.6.2 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.6.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.6.2"
RUN mkdir -p "/opt/kafka-2.7.1" && chmod a+rw /opt/kafka-2.7.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.7.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.7.1"
RUN mkdir -p "/opt/kafka-2.8.2" && chmod a+rw /opt/kafka-2.8.2 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.8.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.8.2"
RUN mkdir -p "/opt/kafka-3.0.2" && chmod a+rw /opt/kafka-3.0.2 && curl -s "$KAFKA_MIRROR/kafka_2.12-3.0.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-3.0.2"
RUN mkdir -p "/opt/kafka-3.1.2" && chmod a+rw /opt/kafka-3.1.2 && curl -s "$KAFKA_MIRROR/kafka_2.12-3.1.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-3.1.2"
RUN mkdir -p "/opt/kafka-3.2.3" && chmod a+rw /opt/kafka-3.2.3 && curl -s "$KAFKA_MIRROR/kafka_2.12-3.2.3.tgz" | tar xz --strip-components=1 -C "/opt/kafka-3.2.3"
RUN mkdir -p "/opt/kafka-3.3.1" && chmod a+rw /opt/kafka-3.3.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-3.3.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-3.3.1"

# Streams test dependencies
RUN curl -s "$KAFKA_MIRROR/kafka-streams-0.10.0.1-test.jar" -o /opt/kafka-0.10.0.1/libs/kafka-streams-0.10.0.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-0.10.1.1-test.jar" -o /opt/kafka-0.10.1.1/libs/kafka-streams-0.10.1.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-0.10.2.2-test.jar" -o /opt/kafka-0.10.2.2/libs/kafka-streams-0.10.2.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-0.11.0.3-test.jar" -o /opt/kafka-0.11.0.3/libs/kafka-streams-0.11.0.3-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-1.0.2-test.jar" -o /opt/kafka-1.0.2/libs/kafka-streams-1.0.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-1.1.1-test.jar" -o /opt/kafka-1.1.1/libs/kafka-streams-1.1.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.0.1-test.jar" -o /opt/kafka-2.0.1/libs/kafka-streams-2.0.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.1.1-test.jar" -o /opt/kafka-2.1.1/libs/kafka-streams-2.1.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.2.2-test.jar" -o /opt/kafka-2.2.2/libs/kafka-streams-2.2.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.3.1-test.jar" -o /opt/kafka-2.3.1/libs/kafka-streams-2.3.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.4.1-test.jar" -o /opt/kafka-2.4.1/libs/kafka-streams-2.4.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.5.1-test.jar" -o /opt/kafka-2.5.1/libs/kafka-streams-2.5.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.6.2-test.jar" -o /opt/kafka-2.6.2/libs/kafka-streams-2.6.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.7.1-test.jar" -o /opt/kafka-2.7.1/libs/kafka-streams-2.7.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.8.2-test.jar" -o /opt/kafka-2.8.2/libs/kafka-streams-2.8.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-3.0.2-test.jar" -o /opt/kafka-3.0.2/libs/kafka-streams-3.0.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-3.1.2-test.jar" -o /opt/kafka-3.1.2/libs/kafka-streams-3.1.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-3.2.3-test.jar" -o /opt/kafka-3.2.3/libs/kafka-streams-3.2.3-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-3.3.1-test.jar" -o /opt/kafka-3.3.1/libs/kafka-streams-3.3.1-test.jar

# The version of Kibosh to use for testing.
# If you update this, also update vagrant/base.sh
ARG KIBOSH_VERSION="8841dd392e6fbf02986e2fb1f1ebf04df344b65a"

# Aligning uid inside/outside docker enables containers to modify files of kafka source (mounted in /opt/kafka-dev")
# By default, the outside user id is 1000 (UID_MIN). The known exception in QA is travis which gives non-1000 id.
ARG UID="1000"

# Install Kibosh
RUN apt-get install fuse
RUN cd /opt && git clone -q  https://github.com/confluentinc/kibosh.git && cd "/opt/kibosh" && git reset --hard $KIBOSH_VERSION && mkdir "/opt/kibosh/build" && cd "/opt/kibosh/build" && ../configure && make -j 2

# Set up the ducker user.
RUN useradd -u $UID -ms /bin/bash ducker \
  && mkdir -p /home/ducker/ \
  && rsync -aiq /root/.ssh/ /home/ducker/.ssh \
  && chown -R ducker /home/ducker/ /mnt/ /var/log/ \
  && echo "PATH=$(runuser -l ducker -c 'echo $PATH'):$JAVA_HOME/bin" >> /home/ducker/.ssh/environment \
  && echo 'PATH=$PATH:'"$JAVA_HOME/bin" >> /home/ducker/.profile \
  && echo 'ducker ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers

USER ducker

CMD sudo service ssh start && tail -f /dev/null
