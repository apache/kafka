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

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int_with_prefix
from kafkatest.version import DEV_BRANCH, LATEST_3_1


class ClientTelemetryTest(ProduceConsumeValidateTest):
    """
    These tests validate that we can use a client against brokers that don't support or aren't configured
    client telemetry.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ClientTelemetryTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.zk = ZookeeperService(test_context, num_nodes=3) if quorum.for_test(test_context) == quorum.zk else None
        self.kafka = KafkaService(
            test_context,
            num_nodes=3,
            zk=self.zk,
            topics={
                self.topic: {
                    "partitions": 10,
                    "replication-factor": 2
                }
            }
        )
        self.num_partitions = 10
        self.timeout_sec = 60
        self.producer_throughput = 1000
        self.num_producers = 2
        self.messages_per_producer = 1000
        self.num_consumers = 1

    def setUp(self):
        if self.zk:
            self.zk.start()

    def min_cluster_size(self):
        # Override this since we're adding services outside the constructor
        return super(ClientTelemetryTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    @cluster(num_nodes=9)
    def test_broker_pre_support(self):
        self.run_expecting_log_message(
            LATEST_3_1,
            "The broker generated an error for the GetTelemetrySubscriptions API request"
        )

    @cluster(num_nodes=9)
    def test_no_plugin_configured(self):
        self.run_expecting_log_message(
            DEV_BRANCH,
            "The broker does not have any client metrics plugin configured"
        )

    def run_expecting_log_message(self, broker_version, message):
        """
        We are testing that clients that support client telemetry still function as expected when they
        are used against brokers that are not set up for it in some way. Right now the two ways are:

        1. The broker(s) are of a version before client telemetry existed
        2. The broker(s) are not configured for client telemetry

        :type broker_version: KafkaVersion
        :type message: str
        """
        self.kafka.set_version(broker_version)
        self.kafka.security_protocol = "PLAINTEXT"
        self.kafka.interbroker_security_protocol = self.kafka.security_protocol
        self.producer = VerifiableProducer(self.test_context,
                                           self.num_producers,
                                           self.kafka,
                                           self.topic,
                                           throughput=self.producer_throughput,
                                           message_validator=is_int_with_prefix)
        self.consumer = ConsoleConsumer(self.test_context,
                                        self.num_consumers,
                                        self.kafka,
                                        self.topic,
                                        consumer_timeout_ms=60000,
                                        message_validator=is_int_with_prefix)
        self.kafka.start()

        # First make sure we can execute basic message production and consumption as expected...
        self.run_produce_consume_validate(
            lambda: wait_until(
                lambda: self.producer.each_produced_at_least(self.messages_per_producer),
                timeout_sec=120,
                backoff_sec=1,
                err_msg="Producer did not produce all messages in reasonable amount of time"
            )
        )

        # ...but also make sure that the clients include logging that warn that the broker doesn't support
        # client telemetry so there's no silent failure.
        self.assert_message(message)

    def assert_message(self, message):
        try:
            for node in self.producer.nodes:
                node.account.ssh("grep '%s' %s" % (message, self.producer.LOG_FILE))
            for node in self.consumer.nodes:
                self.consumer.has_log_message(node, message)
        except RemoteCommandError:
            assert False, "The client logs are expected to include the log message: \"%s\"" % message
