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

#!/bin/bash
apt-get -y update
apt-get install -y openjdk-6-jre

chmod a+rw /opt
cd /opt
ln -s /vagrant kafka
cd kafka
IP=$(ifconfig  | grep 'inet addr:'| grep 168 | grep 192|cut -d: -f2 | awk '{ print $1}')
sed 's/broker.id=0/'broker.id=$1'/' /opt/kafka/config/server.properties > /tmp/prop1.tmp
sed 's/#advertised.host.name=<hostname routable by clients>/'advertised.host.name=$IP'/' /tmp/prop1.tmp > /tmp/prop2.tmp
sed 's/#host.name=localhost/'host.name=$IP'/' /tmp/prop2.tmp > /tmp/prop3.tmp
sed 's/zookeeper.connect=localhost:2181/'zookeeper.connect=192.168.50.5:2181'/' /tmp/prop3.tmp > /opt/server.properties

bin/kafka-server-start.sh /opt/server.properties 1>> /tmp/broker.log 2>> /tmp/broker.log &
