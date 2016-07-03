#!/usr/bin/env bash
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

# Usage: zk.sh <zkid> <num_zk>

set -e

ZKID=$1
NUM_ZK=$2
JMX_PORT=$3

kafka_dir=/opt/kafka-trunk
cd $kafka_dir

cp $kafka_dir/config/zookeeper.properties $kafka_dir/config/zookeeper-$ZKID.properties
echo "initLimit=5" >> $kafka_dir/config/zookeeper-$ZKID.properties
echo "syncLimit=2" >> $kafka_dir/config/zookeeper-$ZKID.properties
echo "quorumListenOnAllIPs=true" >> $kafka_dir/config/zookeeper-$ZKID.properties
for i in `seq 1 $NUM_ZK`; do
    echo "server.${i}=zk${i}:2888:3888" >> $kafka_dir/config/zookeeper-$ZKID.properties
done

mkdir -p /tmp/zookeeper
echo "$ZKID" > /tmp/zookeeper/myid

echo "Killing ZooKeeper"
bin/zookeeper-server-stop.sh || true
sleep 5 # Because zookeeper-server-stop.sh doesn't actually wait
echo "Starting ZooKeeper"
if [[  -n $JMX_PORT ]]; then
  export JMX_PORT=$JMX_PORT
  export KAFKA_JMX_OPTS="-Djava.rmi.server.hostname=zk$ZKID -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false "
fi
bin/zookeeper-server-start.sh config/zookeeper-$ZKID.properties 1>> /tmp/zk.log 2>> /tmp/zk.log &
