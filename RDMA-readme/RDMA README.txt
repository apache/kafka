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

Important facts:
The replication mechanisms within the Kafka clusters are designed only to work within a single
cluster, not between multiple clusters.

#############################################################
How to compile:
#######################################################
Install required packages:
- Scala at least 2.11
- gradle

Compilation steps
1. Install libdisni C library

2. run gradle in kafka home directory
$ gradle

3. Compile files with
$ ./gradlew jar

Important comments
You may also need to add libdisni C library to LD_LIBRARY_PATH if it 
was not installed in the standard location: /usr/local/lib

#####################################
How to run Kafka.
#####################################


0. Clean after old experimants.
On zookeeper machine
$ rm -r /tmp/zookeeper/
On broker machines
$ rm -r /tmp/kafka-logs/
 
1. Run Zookeeper. 
$ bin/zookeeper-server-start.sh config/zookeeper.properties

It will run Zookeeper at port 2181

2. Run a broker with a given config_file. More about config files see later in the current file
$ bin/kafka-server-start.sh ${config_file}

For example:
bin/kafka-server-start.sh config/server.properties
bin/kafka-server-start.sh config/rdma-broker.properties

You will need to run a single per each node.


3. Create topics before tests with required number of partitions and replication factor. 
Option 1, directly via broker (Recommended): 
> bin/kafka-topics.sh --create --bootstrap-server <BROKER_ADDRESS> --replication-factor 1 --partitions 1 --topic testname
where  <BROKER_ADDRESS>  is a address of one of brokers in the format host:port., e.g. 10.10.10.10:8787
It also can be a list with , separator e.g. HOST1:PORT1,HOST2:PORT2 like 10.10.10.10:8787,11.11.11.11:8787

Option 2, via zookeeper (DEPRECATED): 
> bin/kafka-topics.sh --create --zookeeper  <ZOOKEEPER_ADDRESS>  --replication-factor 1 --partitions 1 --topic testname
where  <ZOOKEEPER_ADDRESS>  is a address of the zookeeper in the format host:port., e.g. 10.10.10.10:2181

The command above will create topic with the name "testname" with 1 partition and replication factor 1.


4. Run one of clients:
We will use ProducerPerformance, ConsumerPerformance, EndToEndLatency

bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance --help
bin/kafka-run-class.sh kafka.tools.ConsumerPerformance --help
bin/kafka-run-class.sh kafka.tools.EndToEndLatency --help

First two also have bash scripts: 

bin/kafka-producer-perf-test.sh --help
bin/kafka-consumer-perf-test.sh --help

#####################################
About kafka brokers and their config file
#####################################


Please read comments in the example config file rdma-broker.properties

 
#####################################
About kafka clients and their config file
#####################################

1. ProducerPerformance
You can always read help
bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance --help

Example of running without RDMA
bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance --print-metrics --topic testname --num-records 100000 --throughput 1000 --record-size 100 --producer-props bootstrap.servers=example.com:9092 buffer.memory=67108864 batch.size=1000 acks=1

Example of running with RDMA
bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance --print-metrics --topic testname --num-records 100000 --throughput 1000 --record-size 100 --producer-props bootstrap.servers=example.com:9092 buffer.memory=67108864 batch.size=1000 acks=1 --with-rdma 

To add custom config for producer use --producer.config option


2. ConsumerPerformance
You can always read help
bin/kafka-run-class.sh kafka.tools.ConsumerPerformance  --help

Example of running without RDMA
bin/kafka-run-class.sh kafka.tools.ConsumerPerformance   --broker-list example.com:9092 --messages 200000  --topic testname --print-metrics --fetch-size 1000 --reporting-interval 1000 --show-detailed-stats

Example of running with RDMA
bin/kafka-run-class.sh kafka.tools.ConsumerPerformance  --broker-list example.com:9092 --messages 200000  --topic testname --print-metrics --fetch-size 1000 --reporting-interval 1000 --show-detailed-stats --with-rdma

To add custom config for consumer use --consumer.config option


3. EndToEndLatency
You can always read help
bin/kafka-run-class.sh kafka.tools.EndToEndLatency --help

Example of running without RDMA
bin/kafka-run-class.sh kafka.tools.EndToEndLatency  --broker-list example.com:9092 --messages 1000 --topic testname --producer_acks 1 --size 100

Example of running with RDMA producer
bin/kafka-run-class.sh kafka.tools.EndToEndLatency  --broker-list example.com:9092 --messages 1000 --topic testname --producer_acks 1 --size 100 --with-rdma-producer

Example of running with RDMA consumer
bin/kafka-run-class.sh kafka.tools.EndToEndLatency  --broker-list example.com:9092 --messages 1000 --topic testname --producer_acks 1 --size 100 --with-rdma-consumer

Example of running with RDMA producer and RDMA consumer
bin/kafka-run-class.sh kafka.tools.EndToEndLatency  --broker-list example.com:9092 --messages 1000 --topic testname --producer_acks 1 --size 100 --with-rdma-producer --with-rdma-consumer


To add custom config for consumer use --consumer.config option
To add custom config for producer use --producer.config option


#####################################
About kafka client config files
#####################################


Please read comments in the example config file producer.properties

Please read comments in the example config file consumer.properties


########################################
Others
########################################
You can also have a look at examples from somebody
https://gist.github.com/ueokande/b96eadd798fff852551b80962862bfb3

EndToEndLatency will be different as I rewrote it! 




 


