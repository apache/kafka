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

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.services.security.security_config import SslStores
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
import time

class TestSslStores(SslStores):
    def __init__(self):
        super(TestSslStores, self).__init__()
        self.invalid_hostname = False
        self.generate_ca()
        self.generate_truststore()

    def hostname(self, node):
        if (self.invalid_hostname):
            return "invalidhost"
        else:
            return super(TestSslStores, self).hostname(node)

class SecurityTest(ProduceConsumeValidateTest):
    """
    These tests validate security features.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(SecurityTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context, num_nodes=1, zk=self.zk, topics={self.topic: {
                                                                    "partitions": 2,
                                                                    "replication-factor": 1}
                                                                })
        self.num_partitions = 2
        self.timeout_sec = 10000
        self.producer_throughput = 1000
        self.num_producers = 1
        self.num_consumers = 1

    def setUp(self):
        self.zk.start()

    @parametrize(security_protocol='PLAINTEXT', interbroker_security_protocol='SSL')
    @parametrize(security_protocol='SSL', interbroker_security_protocol='PLAINTEXT')
    def test_client_ssl_endpoint_validation_failure(self, security_protocol, interbroker_security_protocol):
        """
        Test that invalid hostname in certificate results in connection failures.
        When security_protocol=SSL, client SSL handshakes are expected to fail due to hostname verification failure.
        When security_protocol=PLAINTEXT and interbroker_security_protocol=SSL, controller connections fail
        with hostname verification failure. Hence clients are expected to fail with LEADER_NOT_AVAILABLE.
        """

        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = interbroker_security_protocol
        SecurityConfig.ssl_stores = TestSslStores()

        SecurityConfig.ssl_stores.invalid_hostname = True
        self.kafka.start()
        self.create_producer_and_consumer()
        self.producer.log_level = "TRACE"
        self.producer.start()
        self.consumer.start()
        time.sleep(10)
        assert self.producer.num_acked == 0, "Messages published successfully, endpoint validation did not fail with invalid hostname"
        error = 'SSLHandshakeException' if security_protocol is 'SSL' else 'LEADER_NOT_AVAILABLE'
        for node in self.producer.nodes:
            node.account.ssh("grep %s %s" % (error, self.producer.LOG_FILE))
        for node in self.consumer.nodes:
            node.account.ssh("grep %s %s" % (error, self.consumer.LOG_FILE))

        self.producer.stop()
        self.consumer.stop()
        self.producer.log_level = "INFO"

        SecurityConfig.ssl_stores.invalid_hostname = False
        for node in self.kafka.nodes:
            self.kafka.restart_node(node, clean_shutdown=True)
        self.create_producer_and_consumer()
        self.run_produce_consume_validate()

    def create_producer_and_consumer(self):
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka, self.topic, throughput=self.producer_throughput)
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka, self.topic, new_consumer=True, consumer_timeout_ms=10000, message_validator=is_int)

