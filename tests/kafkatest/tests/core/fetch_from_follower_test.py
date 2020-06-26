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

import time
from collections import defaultdict

from ducktape.mark.resource import cluster

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.monitor.jmx import JmxTool
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int


class FetchFromFollowerTest(ProduceConsumeValidateTest):

    RACK_AWARE_REPLICA_SELECTOR = "org.apache.kafka.common.replica.RackAwareReplicaSelector"
    METADATA_MAX_AGE_MS = 3000

    def __init__(self, test_context):
        super(FetchFromFollowerTest, self).__init__(test_context=test_context)
        self.jmx_tool = JmxTool(test_context, jmx_poll_ms=100)
        self.topic = "test_topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context,
                                  num_nodes=3,
                                  zk=self.zk,
                                  topics={
                                      self.topic: {
                                          "partitions": 1,
                                          "replication-factor": 3,
                                          "configs": {"min.insync.replicas": 1}},
                                  },
                                  server_prop_overides=[
                                      ["replica.selector.class", self.RACK_AWARE_REPLICA_SELECTOR]
                                  ],
                                  per_node_server_prop_overrides={
                                      1: [("broker.rack", "rack-a")],
                                      2: [("broker.rack", "rack-b")],
                                      3: [("broker.rack", "rack-c")]
                                  })

        self.producer_throughput = 1000
        self.num_producers = 1
        self.num_consumers = 1

    def min_cluster_size(self):
        return super(FetchFromFollowerTest, self).min_cluster_size() + self.num_producers * 2 + self.num_consumers * 2

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    @cluster(num_nodes=9)
    def test_consumer_preferred_read_replica(self):
        """
        This test starts up brokers with "broker.rack" and "replica.selector.class" configurations set. The replica
        selector is set to the rack-aware implementation. One of the brokers has a different rack than the other two.
        We then use a console consumer with the "client.rack" set to the same value as the differing broker. After
        producing some records, we verify that the client has been informed of the preferred replica and that all the
        records are properly consumed.
        """

        # Find the leader, configure consumer to be on a different rack
        leader_node = self.kafka.leader(self.topic, 0)
        leader_idx = self.kafka.idx(leader_node)
        non_leader_idx = 2 if leader_idx != 2 else 1
        non_leader_rack = "rack-b" if leader_idx != 2 else "rack-a"

        self.logger.debug("Leader %d %s" % (leader_idx, leader_node))
        self.logger.debug("Non-Leader %d %s" % (non_leader_idx, non_leader_rack))

        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka, self.topic,
                                           throughput=self.producer_throughput)
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka, self.topic,
                                        client_id="console-consumer", group_id="test-consumer-group-1",
                                        consumer_timeout_ms=60000, message_validator=is_int,
                                        consumer_properties={"client.rack": non_leader_rack, "metadata.max.age.ms": self.METADATA_MAX_AGE_MS})

        # Start up and let some data get produced
        self.start_producer_and_consumer()
        time.sleep(self.METADATA_MAX_AGE_MS * 2. / 1000)

        consumer_node = self.consumer.nodes[0]
        consumer_idx = self.consumer.idx(consumer_node)
        read_replica_attribute = "preferred-read-replica"
        read_replica_mbean = "kafka.consumer:type=consumer-fetch-manager-metrics,client-id=%s,topic=%s,partition=%d" % \
                  ("console-consumer", self.topic, 0)
        self.jmx_tool.jmx_object_names = [read_replica_mbean]
        self.jmx_tool.jmx_attributes = [read_replica_attribute]
        self.jmx_tool.start_jmx_tool(consumer_idx, consumer_node)

        # Wait for at least one interval of "metadata.max.age.ms"
        time.sleep(self.METADATA_MAX_AGE_MS * 2. / 1000)

        # Read the JMX output
        self.jmx_tool.read_jmx_output(consumer_idx, consumer_node)

        all_captured_preferred_read_replicas = defaultdict(int)
        self.logger.debug(self.jmx_tool.jmx_stats)

        for ts, data in self.jmx_tool.jmx_stats[0].items():
            for k, v in data.items():
                if k.endswith(read_replica_attribute):
                    all_captured_preferred_read_replicas[int(v)] += 1

        self.logger.debug("Saw the following preferred read replicas %s",
                          dict(all_captured_preferred_read_replicas.items()))

        assert all_captured_preferred_read_replicas[non_leader_idx] > 0, \
            "Expected to see broker %d (%s) as a preferred replica" % (non_leader_idx, non_leader_rack)

        # Validate consumed messages
        self.stop_producer_and_consumer()
        self.validate()
