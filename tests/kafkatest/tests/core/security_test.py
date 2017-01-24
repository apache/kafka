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

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from ducktape.errors import TimeoutError

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.services.security.security_config import SslStores
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int

class TestSslStores(SslStores):
    def __init__(self, local_scratch_dir, valid_hostname=True):
        super(TestSslStores, self).__init__(local_scratch_dir)
        self.valid_hostname = valid_hostname
        self.generate_ca()
        self.generate_truststore()

    def hostname(self, node):
        if self.valid_hostname:
            return super(TestSslStores, self).hostname(node)
        else:
            return "invalidhostname"

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

    def producer_consumer_have_expected_error(self, error):
        try:
            for node in self.producer.nodes:
                node.account.ssh("grep %s %s" % (error, self.producer.LOG_FILE))
            for node in self.consumer.nodes:
                node.account.ssh("grep %s %s" % (error, self.consumer.LOG_FILE))
        except RemoteCommandError:
            return False

        return True

    @cluster(num_nodes=7)
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
        SecurityConfig.ssl_stores = TestSslStores(self.test_context.local_scratch_dir, valid_hostname=False)

        self.kafka.start()
        self.create_producer_and_consumer()
        self.producer.log_level = "TRACE"

        self.producer.start()
        self.consumer.start()
        try:
            wait_until(lambda: self.producer.num_acked > 0, timeout_sec=5)

            # Fail quickly if messages are successfully acked
            raise RuntimeError("Messages published successfully but should not have!"
                               " Endpoint validation did not fail with invalid hostname")
        except TimeoutError:
            # expected
            pass

        error = 'SSLHandshakeException' if security_protocol == 'SSL' else 'LEADER_NOT_AVAILABLE'
        wait_until(lambda: self.producer_consumer_have_expected_error(error), timeout_sec=5)

        self.producer.stop()
        self.consumer.stop()
        self.producer.log_level = "INFO"

        SecurityConfig.ssl_stores.valid_hostname = True
        for node in self.kafka.nodes:
            self.kafka.restart_node(node, clean_shutdown=True)

        self.create_producer_and_consumer()
        self.run_produce_consume_validate()

    def create_producer_and_consumer(self):
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka, self.topic, throughput=self.producer_throughput)
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka, self.topic, consumer_timeout_ms=10000, message_validator=is_int)

