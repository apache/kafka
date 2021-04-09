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

from ducktape.mark.resource import cluster

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int

class ZookeeperTlsEncryptOnlyTest(ProduceConsumeValidateTest):
    """Tests TLS encryption-only (ssl.clientAuth=none) connectivity to zookeeper.
    """

    def __init__(self, test_context):
        super(ZookeeperTlsEncryptOnlyTest, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = "test_topic"
        self.group = "group"
        self.producer_throughput = 100
        self.num_producers = 1
        self.num_consumers = 1

        self.zk = ZookeeperService(self.test_context, num_nodes=3,
                                   zk_client_port = False, zk_client_secure_port = True, zk_tls_encrypt_only = True)

        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk, zk_client_secure=True, topics={self.topic: {
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

    def perform_produce_consume_validation(self):
        self.create_producer_and_consumer()
        self.run_produce_consume_validate()
        self.producer.free()
        self.consumer.free()

    @cluster(num_nodes=9)
    def test_zk_tls_encrypt_only(self):
        self.zk.start()
        self.kafka.security_protocol = self.kafka.interbroker_security_protocol = "PLAINTEXT"

        self.kafka.start()

        self.perform_produce_consume_validation()

        # Make sure the ZooKeeper command line is able to talk to a TLS-enabled, encrypt-only ZooKeeper quorum
        # Test both create() and query(), each of which leverages the ZooKeeper command line
        # This tests the code in org.apache.zookeeper.ZooKeeperMainWithTlsSupportForKafka
        path="/foo"
        value="{\"bar\": 0}"
        self.zk.create(path, value=value)
        if self.zk.query(path) != value:
            raise Exception("Error creating and then querying a znode using the CLI with a TLS-enabled ZooKeeper quorum")

        # Make sure the ConfigCommand CLI is able to talk to a TLS-enabled, encrypt-only ZooKeeper quorum
        # This is necessary for the bootstrap use case despite direct ZooKeeper connectivity being deprecated
        self.zk.describe(self.topic)

        # Make sure the AclCommand CLI is able to talk to a TLS-enabled, encrypt-only ZooKeeper quorum
        # This is necessary for the bootstrap use case despite direct ZooKeeper connectivity being deprecated
        self.zk.list_acls(self.topic)
