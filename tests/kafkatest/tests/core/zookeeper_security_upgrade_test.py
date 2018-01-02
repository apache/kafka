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

from ducktape.mark import matrix, ignore
from ducktape.mark.resource import cluster

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.services.security.kafka_acls import ACLs
from kafkatest.utils import is_int

class ZooKeeperSecurityUpgradeTest(ProduceConsumeValidateTest):
    """Tests a rolling upgrade for zookeeper.
    """

    def __init__(self, test_context):
        super(ZooKeeperSecurityUpgradeTest, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = "test_topic"
        self.group = "group"
        self.producer_throughput = 100
        self.num_producers = 1
        self.num_consumers = 1
        self.acls = ACLs(self.test_context)

        self.zk = ZookeeperService(self.test_context, num_nodes=3)

        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk, topics={self.topic: {
            "partitions": 3,
            "replication-factor": 3,
            'configs': {"min.insync.replicas": 2}}})

    def create_producer_and_consumer(self):
        self.producer = VerifiableProducer(
            self.test_context, self.num_producers, self.kafka, self.topic,
            throughput=self.producer_throughput)

        self.consumer = ConsoleConsumer(
            self.test_context, self.num_consumers, self.kafka, self.topic,
            consumer_timeout_ms=60000, message_validator=is_int)

        self.consumer.group_id = self.group

    @property
    def no_sasl(self):
        return self.kafka.security_protocol == "PLAINTEXT" or self.kafka.security_protocol == "SSL"

    @property
    def is_secure(self):
        return self.kafka.security_protocol == "SASL_PLAINTEXT" \
               or self.kafka.security_protocol == "SSL" \
               or self.kafka.security_protocol == "SASL_SSL"

    def run_zk_migration(self):
        # change zk config (auth provider + jaas login)
        self.zk.kafka_opts = self.zk.security_system_properties
        self.zk.zk_sasl = True
        if self.no_sasl:
            self.kafka.start_minikdc(self.zk.zk_principals)
        # restart zk
        for node in self.zk.nodes:
            self.zk.stop_node(node)
            self.zk.start_node(node)

        # restart broker with jaas login
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            self.kafka.start_node(node)

        # run migration tool
        for node in self.zk.nodes:
            self.zk.zookeeper_migration(node, "secure")

        # restart broker with zookeeper.set.acl=true and acls
        self.kafka.zk_set_acl = True
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            self.kafka.start_node(node)

    @cluster(num_nodes=9)
    @matrix(security_protocol=["PLAINTEXT", "SSL", "SASL_SSL", "SASL_PLAINTEXT"])
    def test_zk_security_upgrade(self, security_protocol):
        self.zk.start()
        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol

        # set acls
        if self.is_secure:
            self.kafka.authorizer_class_name = KafkaService.SIMPLE_AUTHORIZER
            self.acls.set_acls(security_protocol, self.kafka, self.topic, self.group)

        if self.no_sasl:
            self.kafka.start()
        else:
            self.kafka.start(self.zk.zk_principals)

        #Create Producer and Consumer
        self.create_producer_and_consumer()

        #Run upgrade
        self.run_produce_consume_validate(self.run_zk_migration)
