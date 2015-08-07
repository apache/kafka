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

from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer

import time


def file_exists(node, file):
    try:
        node.account.ssh("cat " + file, allow_fail=False)
        return True
    except:
        return False


class ConsoleConsumerTest(Test):
    """Sanity checks on console consumer service class."""
    def __init__(self, test_context):
        super(ConsoleConsumerTest, self).__init__(test_context)

        self.topic = "topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context, num_nodes=1, zk=self.zk,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 1}})
        self.consumer = ConsoleConsumer(test_context, num_nodes=1, kafka=self.kafka, topic=self.topic)

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    def test_lifecycle(self):
        t0 = time.time()
        self.consumer.start()
        node = self.consumer.nodes[0]

        if not wait_until(lambda: self.consumer.alive(node), timeout_sec=10, backoff_sec=.2):
            raise Exception("Consumer was too slow to start")
        self.logger.info("consumer started in %s seconds " % str(time.time() - t0))

        # Verify that log output is happening
        if not wait_until(lambda: file_exists(node, ConsoleConsumer.LOG_FILE), timeout_sec=10):
            raise Exception("Timed out waiting for log file to exist")
        consumer_log_lines = [line for line in node.account.ssh_capture("cat %s" % ConsoleConsumer.LOG_FILE)]
        assert len(consumer_log_lines) > 0

        # Verify no consumed messages
        consumed = [line for line in node.account.ssh_capture("cat %s" % ConsoleConsumer.STDOUT_CAPTURE)]
        assert len(consumed) == 0

        self.consumer.stop_node(node)
        if not wait_until(lambda: not self.consumer.alive(node), timeout_sec=10, backoff_sec=.2):
            raise Exception("Took too long for consumer to die.")


