#!/bin/sh
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

# Override this section from the script to include the com.sun.management.jmxremote.rmi.port property.
if [ -z "${KAFKA_JMX_OPTS-}" ]; then
    export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true \
    -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.ssl=false "
fi

# The JMX client needs to be able to connect to java.rmi.server.hostname.
# The default for bridged n/w is the bridged IP so you will only be able to connect from another docker container.
# For host n/w, this is the IP that the hostname on the host resolves to.

# If you have more than one n/w configured, hostname -i gives you all the IPs,
# the default is to pick the first IP (or network).
export KAFKA_JMX_HOSTNAME=${KAFKA_JMX_HOSTNAME:-$(hostname -i | cut -d" " -f1)}

if [ "${KAFKA_JMX_PORT-}" ]; then
    export JMX_PORT=$KAFKA_JMX_PORT
    export KAFKA_JMX_OPTS="${KAFKA_JMX_OPTS-} -Djava.rmi.server.hostname=$KAFKA_JMX_HOSTNAME \
    -Dcom.sun.management.jmxremote.local.only=false \
    -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT \
    -Dcom.sun.management.jmxremote.port=$JMX_PORT"
fi

# Invoke the docker wrapper to setup property files and format storage
result=$(/opt/kafka/kafka.Kafka setup \
      --default-configs-dir /etc/kafka/docker \
      --mounted-configs-dir /mnt/shared/config \
      --final-configs-dir /opt/kafka/config \
      -Dlog4j.configuration=file:/opt/kafka/config/tools-log4j.properties 2>&1) || \
      echo $result | grep -i "already formatted" || \
      { echo $result && (exit 1) }

echo "WARNING: THIS IS AN EXPERIMENTAL DOCKER IMAGE RECOMMENDED FOR LOCAL TESTING AND DEVELOPMENT PURPOSES."

KAFKA_LOG4J_CMD_OPTS="-Dkafka.logs.dir=/opt/kafka/logs/ -Dlog4j.configuration=file:/opt/kafka/config/log4j.properties"
exec /opt/kafka/kafka.Kafka start --config /opt/kafka/config/server.properties $KAFKA_LOG4J_CMD_OPTS $KAFKA_JMX_OPTS ${KAFKA_OPTS-}
