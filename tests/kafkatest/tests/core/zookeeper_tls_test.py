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

class ZookeeperTlsTest(ProduceConsumeValidateTest):
    """Tests TLS connectivity to zookeeper.
    """

    def __init__(self, test_context):
        super(ZookeeperTlsTest, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = "test_topic"
        self.group = "group"
        self.producer_throughput = 100
        self.num_producers = 1
        self.num_consumers = 1

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

    def perform_produce_consume_validation(self):
        self.create_producer_and_consumer()
        self.run_produce_consume_validate()
        self.producer.free()
        self.consumer.free()

    def enable_zk_tls(self):
        self.test_context.logger.debug("Enabling the TLS port in Zookeeper (we won't use it from Kafka yet)")
        # change zk config (enable TLS, but also keep non-TLS)
        self.zk.zk_client_secure_port = True
        self.zk.restart_cluster()
        # bounce a Kafka broker -- allows us to detect a broker restart failure as a simple sanity check
        self.kafka.stop_node(self.kafka.nodes[0])
        self.kafka.start_node(self.kafka.nodes[0])

    def enable_kafka_zk_tls(self):
        self.test_context.logger.debug("Configuring Kafka to use the TLS port in Zookeeper")
        # change Kafka config (enable TLS to Zookeeper) and restart the Kafka cluster
        self.kafka.zk_client_secure = True
        self.kafka.restart_cluster()

    def disable_zk_non_tls(self):
        self.test_context.logger.debug("Disabling the non-TLS port in Zookeeper (as a simple sanity check)")
        # change zk config (disable non-TLS, keep TLS) and restart the ZooKeeper cluster
        self.zk.zk_client_port = False
        self.zk.restart_cluster()
        # bounce a Kafka broker -- allows us to detect a broker restart failure as a simple sanity check
        self.kafka.stop_node(self.kafka.nodes[0])
        self.kafka.start_node(self.kafka.nodes[0])

    @cluster(num_nodes=9)
    def test_zk_tls(self):
        self.zk.start()
        self.kafka.security_protocol = self.kafka.interbroker_security_protocol = "PLAINTEXT"

        self.kafka.start()

        # Enable TLS port in Zookeeper in addition to the regular non-TLS port
        # Bounces the ZooKeeper cluster (and a single broker as a sanity check)
        self.enable_zk_tls()

        # Leverage ZooKeeper TLS port in Kafka
        # Bounces the Kafka cluster
        self.enable_kafka_zk_tls()
        self.perform_produce_consume_validation()

        # Disable ZooKeeper non-TLS port to make sure we aren't using it
        # Bounces the ZooKeeper cluster (and a single broker as a sanity check)
        self.disable_zk_non_tls()

        # Make sure the ZooKeeper command line is able to talk to a TLS-enabled ZooKeeper quorum
        # Test both create() and query(), each of which leverages the ZooKeeper command line
        # This tests the code in org.apache.zookeeper.ZooKeeperMainWithTlsSupportForKafka
        path="/foo"
        value="{\"bar\": 0}"
        self.zk.create(path, value=value)
        if self.zk.query(path) != value:
            raise Exception("Error creating and then querying a znode using the CLI with a TLS-enabled ZooKeeper quorum")

        # Make sure the ConfigCommand CLI is able to talk to a TLS-enabled ZooKeeper quorum
        # This is necessary for the bootstrap use case despite direct ZooKeeper connectivity being deprecated
        self.zk.describeUsers()

        # Make sure the AclCommand CLI is able to talk to a TLS-enabled ZooKeeper quorum
        # This is necessary for the bootstrap use case despite direct ZooKeeper connectivity being deprecated
        self.zk.list_acls(self.topic)

        #
        # Test zookeeper.set.acl with just TLS mutual authentication (no SASL)
        #
        # Step 1: run migration tool
        self.zk.zookeeper_migration(self.zk.nodes[0], "secure")
        # Step 2: restart brokers with zookeeper.set.acl=true and acls (with TLS but no SASL)
        self.kafka.zk_set_acl = True
        self.kafka.restart_cluster()
        self.perform_produce_consume_validation()

        #
        # Test zookeeper.set.acl with both SASL and TLS mutual authentication
        #
        # Step 1: remove ACLs created previously
        self.kafka.zk_set_acl = False
        self.kafka.restart_cluster()
        self.zk.zookeeper_migration(self.zk.nodes[0], "unsecure")
        # Step 2: enable ZooKeeper SASL authentication, but don't take advantage of it in Kafka yet
        self.zk.zk_sasl = True
        self.kafka.start_minikdc_if_necessary(self.zk.zk_principals)
        self.zk.restart_cluster()
        # bounce a Kafka broker -- allows us to detect a broker restart failure as a simple sanity check
        self.kafka.stop_node(self.kafka.nodes[0])
        self.kafka.start_node(self.kafka.nodes[0])
        # Step 3: run migration tool
        self.zk.zookeeper_migration(self.zk.nodes[0], "secure")
        # Step 4: restart brokers with zookeeper.set.acl=true and acls (with both TLS and SASL)
        self.kafka.zk_set_acl = True
        self.kafka.restart_cluster()
        self.perform_produce_consume_validation()
