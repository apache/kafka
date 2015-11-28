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


class ZooKeeperSecurityUpgradeTest(ProduceConsumeValidateTest):
    """Tests a rolling upgrade for zookeeper.
    """

    def __init__(self, test_context):
        super(ZooKeeperSecurityUpgradeTest, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = "test_topic"
        self.producer_throughput = 100
        self.num_producers = 1
        self.num_consumers = 1

        self.zk = ZookeeperService(self.test_context, num_nodes=3)


        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk, topics={self.topic: {
            "partitions": 3,
            "replication-factor": 3,
            "min.insync.replicas": 2}})


    def create_producer_and_consumer(self):
        self.producer = VerifiableProducer(
            self.test_context, self.num_producers, self.kafka, self.topic,
            throughput=self.producer_throughput)

        self.consumer = ConsoleConsumer(
            self.test_context, self.num_consumers, self.kafka, self.topic,
            consumer_timeout_ms=60000, message_validator=is_int, new_consumer=True)

        self.consumer.group_id = "group"

    def zk_migration(self):
        # generate jaas login file
        jaas_login = self.zk.gen_jaas_login_digest(self.zk.nodes)
        self.logger.warn("Login file: %s" % jaas_login)
        self.zk.gen_jaas_login_digest(self.kafka.nodes)

        # change zk config (auth provider + jaas login)
        self.zk.set_kafka_opts("-Dzookeeper.authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider -Djava.security.auth.login.config=%s" % jaas_login)
        
        # restart zk
        for node in self.zk.nodes:
            self.zk.stop_node(node)
            self.zk.start_node(node)
        
        # restart broker with jaas login
        for node in self.kafka.nodes:
            self.kafka.security_config.set_zk_jaas_login(jaas_login)
            self.kafka.stop_node(node)
            self.kafka.start_node(node)

        # run migration tool
        for node in self.zk.nodes:
            self.zk.zookeeper_migration(node, "secure")

        # restart broker with zookeeper.set.acl=true
        self.kafka.zk_sasl_enabled = "true"
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            self.kafka.start_node(node)

    def test_zk_security_upgrade(self):
        self.zk.start()
        #kafka has the correct sasl options hacked in
        self.kafka.start()

        #Create Secured Producer and Consumer
        self.create_producer_and_consumer()

        #Roll in the security protocol. Disable Plaintext. Ensure we can produce and Consume throughout
        self.run_produce_consume_validate(self.zk_migration)