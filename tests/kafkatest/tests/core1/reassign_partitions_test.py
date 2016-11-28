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

from ducktape.mark import parametrize
from ducktape.utils.util import wait_until

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
import random

class ReassignPartitionsTest(ProduceConsumeValidateTest):
    """
    These tests validate partition reassignment.
    Create a topic with few partitions, load some data, trigger partition re-assignment with and without broker failure,
    check that partition re-assignment can complete and there is no data loss.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ReassignPartitionsTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context, num_nodes=4, zk=self.zk, topics={self.topic: {
                                                                    "partitions": 20,
                                                                    "replication-factor": 3,
                                                                    'configs': {"min.insync.replicas": 2}}
                                                                })
        self.num_partitions = 20
        self.timeout_sec = 60
        self.producer_throughput = 1000
        self.num_producers = 1
        self.num_consumers = 1

    def setUp(self):
        self.zk.start()

    def min_cluster_size(self):
        # Override this since we're adding services outside of the constructor
        return super(ReassignPartitionsTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    def clean_bounce_some_brokers(self):
        """Bounce every other broker"""
        for node in self.kafka.nodes[::2]:
            self.kafka.restart_node(node, clean_shutdown=True)

    def reassign_partitions(self, bounce_brokers):
        partition_info = self.kafka.parse_describe_topic(self.kafka.describe_topic(self.topic))
        self.logger.debug("Partitions before reassignment:" + str(partition_info))

        # jumble partition assignment in dictionary
        seed = random.randint(0, 2 ** 31 - 1)
        self.logger.debug("Jumble partition assignment with seed " + str(seed))
        random.seed(seed)
        # The list may still be in order, but that's ok
        shuffled_list = range(0, self.num_partitions)
        random.shuffle(shuffled_list)

        for i in range(0, self.num_partitions):
            partition_info["partitions"][i]["partition"] = shuffled_list[i]
        self.logger.debug("Jumbled partitions: " + str(partition_info))

        # send reassign partitions command
        self.kafka.execute_reassign_partitions(partition_info)

        if bounce_brokers:
            # bounce a few brokers at the same time
            self.clean_bounce_some_brokers()

        # Wait until finished or timeout
        wait_until(lambda: self.kafka.verify_reassign_partitions(partition_info), timeout_sec=self.timeout_sec, backoff_sec=.5)

    @parametrize(security_protocol="PLAINTEXT", bounce_brokers=True)
    @parametrize(security_protocol="PLAINTEXT", bounce_brokers=False)
    def test_reassign_partitions(self, bounce_brokers, security_protocol):
        """Reassign partitions tests.
        Setup: 1 zk, 3 kafka nodes, 1 topic with partitions=3, replication-factor=3, and min.insync.replicas=2

            - Produce messages in the background
            - Consume messages in the background
            - Reassign partitions
            - If bounce_brokers is True, also bounce a few brokers while partition re-assignment is in progress
            - When done reassigning partitions and bouncing brokers, stop producing, and finish consuming
            - Validate that every acked message was consumed
        """

        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol
        new_consumer = False if  self.kafka.security_protocol == "PLAINTEXT" else True
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka, self.topic, throughput=self.producer_throughput)
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka, self.topic, new_consumer=new_consumer, consumer_timeout_ms=60000, message_validator=is_int)
        self.kafka.start()

        self.run_produce_consume_validate(core_test_action=lambda: self.reassign_partitions(bounce_brokers))
