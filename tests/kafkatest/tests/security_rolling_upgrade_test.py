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
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.utils import is_int
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from ducktape.mark import matrix
from kafkatest.services.security.kafka_acls import ACLs
import time


class TestSecurityRollingUpgrade(ProduceConsumeValidateTest):
    """Tests a rolling upgrade from PLAINTEXT to a secured cluster
    """

    def __init__(self, test_context):
        super(TestSecurityRollingUpgrade, self).__init__(test_context=test_context)

    def setUp(self):
        self.acls = ACLs()
        self.topic = "test_topic"
        self.group = "group"
        self.producer_throughput = 100
        self.num_producers = 1
        self.num_consumers = 1
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk, topics={self.topic: {
            "partitions": 3,
            "replication-factor": 3,
            'configs': {"min.insync.replicas": 2}}})
        self.zk.start()

    def create_producer_and_consumer(self):
        self.producer = VerifiableProducer(
            self.test_context, self.num_producers, self.kafka, self.topic,
            throughput=self.producer_throughput)

        self.consumer = ConsoleConsumer(
            self.test_context, self.num_consumers, self.kafka, self.topic,
            consumer_timeout_ms=60000, message_validator=is_int, new_consumer=True)

        self.consumer.group_id = "group"

    def bounce(self):
        self.kafka.start_minikdc()
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            self.kafka.start_node(node)
            time.sleep(10)

    def roll_in_secured_settings(self, client_protocol, broker_protocol):

        # Roll cluster to include inter broker security protocol.
        self.kafka.interbroker_security_protocol = broker_protocol
        self.kafka.open_port(client_protocol)
        self.kafka.open_port(broker_protocol)
        self.bounce()

        # Roll cluster to disable PLAINTEXT port
        self.kafka.close_port('PLAINTEXT')
        self.kafka.authorizer_class_name = KafkaService.SIMPLE_AUTHORIZER
        self.acls.set_acls(client_protocol, self.kafka, self.zk, self.topic, self.group)
        self.acls.set_acls(broker_protocol, self.kafka, self.zk, self.topic, self.group)
        self.bounce()

    def open_secured_port(self, client_protocol):
        self.kafka.security_protocol = client_protocol
        self.kafka.open_port(client_protocol)
        self.kafka.start_minikdc()
        self.bounce()

    @matrix(client_protocol=["SSL", "SASL_PLAINTEXT", "SASL_SSL"])
    def test_rolling_upgrade_phase_one(self, client_protocol):
        """
        Start with a PLAINTEXT cluster, open a SECURED port, via a rolling upgrade, ensuring we could produce
        and consume throughout over PLAINTEXT. Finally check we can produce and consume the new secured port.
        """
        self.kafka.interbroker_security_protocol = "PLAINTEXT"
        self.kafka.security_protocol = "PLAINTEXT"
        self.kafka.start()

        # Create PLAINTEXT producer and consumer
        self.create_producer_and_consumer()

        # Rolling upgrade, opening a secure protocol, ensuring the Plaintext producer/consumer continues to run
        self.run_produce_consume_validate(self.open_secured_port, client_protocol)

        # Now we can produce and consume via the secured port
        self.kafka.security_protocol = client_protocol
        self.create_producer_and_consumer()
        self.run_produce_consume_validate(lambda: time.sleep(1))

    @matrix(client_protocol=["SASL_SSL", "SSL", "SASL_PLAINTEXT"], broker_protocol=["SASL_SSL", "SSL", "SASL_PLAINTEXT"])
    def test_rolling_upgrade_phase_two(self, client_protocol, broker_protocol):
        """
        Start with a PLAINTEXT cluster with a second Secured port open (i.e. result of phase one).
        Start an Producer and Consumer via the SECURED port
        Incrementally upgrade to add inter-broker be the secure protocol
        Incrementally upgrade again to add ACLs as well as disabling the PLAINTEXT port
        Ensure the producer and consumer ran throughout
        """
        #Given we have a broker that has both secure and PLAINTEXT ports open
        self.kafka.security_protocol = client_protocol
        self.kafka.interbroker_security_protocol = "PLAINTEXT"
        self.kafka.start()

        #Create Secured Producer and Consumer
        self.create_producer_and_consumer()

        #Roll in the security protocol. Disable Plaintext. Ensure we can produce and Consume throughout
        self.run_produce_consume_validate(self.roll_in_secured_settings, client_protocol, broker_protocol)
