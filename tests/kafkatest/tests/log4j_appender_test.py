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


from ducktape.utils.util import wait_until

from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka_log4j_appender import KafkaLog4jAppender

import time

TOPIC = "topic-log4j-appender"
MAX_MESSAGES = 100

def file_exists(node, file):
    """Quick and dirty check for existence of remote file."""
    try:
        node.account.ssh("cat " + file, allow_fail=False)
        return True
    except:
        return False


def line_count(node, file):
    """Return the line count of file on node"""
    out = [line for line in node.account.ssh_capture("wc -l %s" % file)]
    if len(out) != 1:
        raise Exception("Expected single line of output from wc -l")

    return int(out[0].strip().split(" ")[0])

class Log4jAppenderTest(KafkaTest):
    """
    Tests KafkaLog4jAppender using
    """
    def __init__(self, test_context):
        super(Log4jAppenderTest, self).__init__(test_context, num_zk=1, num_brokers=1, topics={
            TOPIC: {'partitions': 1, 'replication-factor': 1}
        })
        self.context = test_context
        self.num_nodes = 1

        self.appender = KafkaLog4jAppender(self.context, self.num_nodes, self.kafka, TOPIC, MAX_MESSAGES)
        self.consumer = ConsoleConsumer(self.context, num_nodes=self.num_nodes, kafka=self.kafka, topic=TOPIC, consumer_timeout_ms=1000)

    def test_log4j_appender(self):
        """
        Tests
        :return:
        """
        self.appender.start()
        self.appender.wait()

        t0 = time.time()
        self.consumer.start()
        node = self.consumer.nodes[0]

        wait_until(lambda: self.consumer.alive(node),
            timeout_sec=10, backoff_sec=.2, err_msg="Consumer was too slow to start")
        self.logger.info("consumer started in %s seconds " % str(time.time() - t0))

        # Verify that log output is happening
        wait_until(lambda: file_exists(node, ConsoleConsumer.LOG_FILE), timeout_sec=10,
                   err_msg="Timed out waiting for logging to start.")
        assert line_count(node, ConsoleConsumer.LOG_FILE) > 0

        # Verify consumed messages count
        expected_lines_count = MAX_MESSAGES * 2  # two times to account for new lines introduced by log4j
        wait_until(lambda: len(self.consumer.messages_consumed[1]) == expected_lines_count, timeout_sec=10,
                   err_msg="Timed out waiting to consume expected number of messages.")
        assert line_count(node, ConsoleConsumer.STDOUT_CAPTURE) == expected_lines_count

        self.consumer.stop_node(node)