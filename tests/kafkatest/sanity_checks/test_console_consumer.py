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

import time

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.utils.remote_account import line_count, file_exists


class ConsoleConsumerTest(Test):
    """Sanity checks on console consumer service class."""
    def __init__(self, test_context):
        super(ConsoleConsumerTest, self).__init__(test_context)

        self.topic = "topic"
        self.zk = ZookeeperService(test_context, num_nodes=1) if quorum.for_test(test_context) == quorum.zk else None
        self.kafka = KafkaService(self.test_context, num_nodes=1, zk=self.zk, zk_chroot="/kafka",
                                  topics={self.topic: {"partitions": 1, "replication-factor": 1}})
        self.consumer = ConsoleConsumer(self.test_context, num_nodes=1, kafka=self.kafka, topic=self.topic)

    def setUp(self):
        if self.zk:
            self.zk.start()

    @cluster(num_nodes=3)
    @matrix(security_protocol=['PLAINTEXT', 'SSL'], metadata_quorum=quorum.all_kraft)
    @cluster(num_nodes=4)
    @matrix(security_protocol=['SASL_SSL'], sasl_mechanism=['PLAIN'], metadata_quorum=quorum.all_kraft)
    @matrix(security_protocol=['SASL_SSL'], sasl_mechanism=['SCRAM-SHA-256', 'SCRAM-SHA-512']) # SCRAM not yet supported with KRaft
    @matrix(security_protocol=['SASL_PLAINTEXT', 'SASL_SSL'], metadata_quorum=quorum.all_kraft)
    def test_lifecycle(self, security_protocol, sasl_mechanism='GSSAPI', metadata_quorum=quorum.zk):
        """Check that console consumer starts/stops properly, and that we are capturing log output."""

        self.kafka.security_protocol = security_protocol
        self.kafka.client_sasl_mechanism = sasl_mechanism
        self.kafka.interbroker_sasl_mechanism = sasl_mechanism
        self.kafka.start()

        self.consumer.security_protocol = security_protocol

        t0 = time.time()
        self.consumer.start()
        node = self.consumer.nodes[0]

        wait_until(lambda: self.consumer.alive(node),
            timeout_sec=20, backoff_sec=.2, err_msg="Consumer was too slow to start")
        self.logger.info("consumer started in %s seconds " % str(time.time() - t0))

        # Verify that log output is happening
        wait_until(lambda: file_exists(node, ConsoleConsumer.LOG_FILE), timeout_sec=10,
                   err_msg="Timed out waiting for consumer log file to exist.")
        wait_until(lambda: line_count(node, ConsoleConsumer.LOG_FILE) > 0, timeout_sec=1,
                   backoff_sec=.25, err_msg="Timed out waiting for log entries to start.")

        # Verify no consumed messages
        assert line_count(node, ConsoleConsumer.STDOUT_CAPTURE) == 0

        self.consumer.stop_node(node)
