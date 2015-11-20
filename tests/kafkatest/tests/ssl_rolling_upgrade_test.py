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


from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer, is_int
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
import time
import random


class TestRollingSSLUpgrade(ProduceConsumeValidateTest):
    """Tests a rolling upgrade from PLAINTEXT to an SSL enabled cluster.
    """

    def __init__(self, test_context):
        super(TestRollingSSLUpgrade, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = "test_topic"
        self.producer_throughput = 1000
        self.num_producers = 1
        self.num_consumers = 1
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk, topics={self.topic: {
            "partitions": 3,
            "replication-factor": 3,
            "min.insync.replicas": 2}})
        self.zk.start()


    def create_producer_and_consumer(self):
        self.producer = VerifiableProducer(
            self.test_context, self.num_producers, self.kafka, self.topic,
            throughput=self.producer_throughput)

        self.consumer = ConsoleConsumer(
            self.test_context, self.num_consumers, self.kafka, self.topic,
            consumer_timeout_ms=30000, message_validator=is_int, new_consumer=True)

        self.consumer.group_id = "unique-test-group-" + str(random.random())


    def roll_in_interbroker_security_then_disable_plaintext(self):
        self.kafka.interbroker_security_protocol = "SSL"
        self.kafka.security_protocol = "SSL"

        # Roll cluster to include inter broker security protocol.
        # Keep Plaintext enabled so plaintext nodes can communicate during the upgrade
        self.kafka.open_port('PLAINTEXT')
        self.kafka.open_port('SSL')
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            self.kafka.start_node(node)

        # Roll cluster to disable PLAINTEXT port
        self.kafka.close_port('PLAINTEXT')
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            self.kafka.start_node(node)


    def add_ssl_port(self):
        self.kafka.security_protocol = "SSL"
        self.kafka.open_port("SSL")

        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            self.kafka.start_node(node)


    def test_rolling_upgrade_phase_one(self):
        """
        Start with a PLAINTEXT cluster, add SSL on a second port, via a rolling upgrade, ensuring we could produce
        and consume throughout over PLAINTEXT. Finally check we can produce and consume via SSL.
        """

        self.kafka.interbroker_security_protocol = "PLAINTEXT"
        self.kafka.security_protocol = "PLAINTEXT"
        self.kafka.start()

        # Rolling upgrade to SSL ensuring the Plaintext consumer continues to run
        self.create_producer_and_consumer()
        self.run_produce_consume_validate(self.add_ssl_port)

        # Now a second port is opened using SSL make sure we can produce and consume to it
        self.kafka.security_protocol = "SSL"
        self.create_producer_and_consumer()
        self.run_produce_consume_validate(lambda: time.sleep(1))


    def test_rolling_upgrade_phase_two(self):
        """
        Start with a PLAINTEXT cluster with a second SSL port open (i.e. result of phase one).
        Start an SSL enabled Producer and Consumer.
        Rolling upgrade to add inter-broker SSL
        Rolling upgrade again to disable PLAINTEXT
        Ensure the producer and consumer ran throughout
        """

        #Given we have a broker with an additional SSL enabled port
        self.kafka.interbroker_security_protocol = "PLAINTEXT"
        self.kafka.security_protocol = "SSL"
        self.kafka.start()

        #Create an SSL Producer and Consumer
        self.create_producer_and_consumer()

        #Run, ensuring we can consume throughout the rolling upgrade
        self.run_produce_consume_validate(self.roll_in_interbroker_security_then_disable_plaintext)
