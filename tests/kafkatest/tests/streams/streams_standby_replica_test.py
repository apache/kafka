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
from kafkatest.services.kafka import KafkaService
from kafkatest.services.streams import StreamsRepeatingIntegerKeyProducerService
from kafkatest.services.streams import StreamsStandbyTaskService
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.zookeeper import ZookeeperService


class StreamsStandbyTask(Test):
    """
    This test validates using standby tasks helps with rebalance times
    additionally verifies standby replicas continue to work in the
    face of continual changes to streams code base
    """

    streams_source_topic = "standbyTaskSource1"
    streams_sink_topic_1 = "standbyTaskSink1"
    streams_sink_topic_2 = "standbyTaskSink2"

    num_messages = 60000

    def __init__(self, test_context):
        super(StreamsStandbyTask, self).__init__(test_context=test_context)
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context,
                                  num_nodes=3,
                                  zk=self.zk,
                                  topics={
                                      self.streams_source_topic: {'partitions': 6, 'replication-factor': 1},
                                      self.streams_sink_topic_1: {'partitions': 1, 'replication-factor': 1},
                                      self.streams_sink_topic_2: {'partitions': 1, 'replication-factor': 1}
                                  })

    def get_consumer(self, topic, num_messages):
        return VerifiableConsumer(self.test_context,
                                  1,
                                  self.kafka,
                                  topic,
                                  "stream-broker-resilience-verify-consumer",
                                  max_messages=num_messages)

    def assert_consume(self, test_state, topic, num_messages=5):
        consumer = self.get_consumer(topic, num_messages)
        consumer.start()

        wait_until(lambda: consumer.total_consumed() >= num_messages,
                   timeout_sec=120,
                   err_msg="At %s streams did not process messages in 60 seconds " % test_state)

    @staticmethod
    def get_configs(extra_configs=""):
        # Consumer max.poll.interval > min(max.block.ms, ((retries + 1) * request.timeout)
        consumer_poll_ms = "consumer.max.poll.interval.ms=50000"
        retries_config = "producer.retries=2"
        request_timeout = "producer.request.timeout.ms=15000"
        max_block_ms = "producer.max.block.ms=30000"

        # java code expects configs in key=value,key=value format
        updated_configs = consumer_poll_ms + "," + retries_config + "," + request_timeout + "," + max_block_ms + extra_configs

        return updated_configs

    def wait_for_verification(self, processor, message, file, num_lines=1, timeout_sec=20):
        wait_until(lambda: self.verify_from_file(processor, message, file) >= num_lines,
                   timeout_sec=timeout_sec,
                   err_msg="Did expect to read '%s' from %s" % (message, processor.node.account))

    @staticmethod
    def verify_from_file(processor, message, file):
        result = processor.node.account.ssh_output("grep '%s' %s | wc -l" % (message, file), allow_fail=False)
        return int(result)


    def setUp(self):
        self.zk.start()
        self.kafka.start()

    def test_standby_tasks_rebalance(self):

        driver_configs = "num_messages=%s,topics=%s" % (str(self.num_messages), self.streams_source_topic)

        driver = StreamsRepeatingIntegerKeyProducerService(self.test_context, self.kafka, driver_configs)
        driver.start()

        configs = self.get_configs(",sourceTopic=%s,sinkTopic1=%s,sinkTopic2=%s" % (self.streams_source_topic,
                                                                                    self.streams_sink_topic_1,
                                                                                    self.streams_sink_topic_2))

        processor_1 = StreamsStandbyTaskService(self.test_context, self.kafka, configs)
        processor_2 = StreamsStandbyTaskService(self.test_context, self.kafka, configs)
        processor_3 = StreamsStandbyTaskService(self.test_context, self.kafka, configs)

        processor_1.start()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:6 STANDBY_TASKS:0", processor_1.STDOUT_FILE)

        processor_2.start()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_1.STDOUT_FILE)
        self.wait_for_verification(processor_2, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_2.STDOUT_FILE)

        processor_3.start()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:2 STANDBY_TASKS:2", processor_1.STDOUT_FILE)
        self.wait_for_verification(processor_2, "ACTIVE_TASKS:2 STANDBY_TASKS:2", processor_2.STDOUT_FILE)
        self.wait_for_verification(processor_3, "ACTIVE_TASKS:2 STANDBY_TASKS:2", processor_3.STDOUT_FILE)

        processor_1.stop()

        self.wait_for_verification(processor_2, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_2.STDOUT_FILE, num_lines=2)
        self.wait_for_verification(processor_3, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_3.STDOUT_FILE)

        processor_2.stop()

        self.wait_for_verification(processor_3, "ACTIVE_TASKS:6 STANDBY_TASKS:0", processor_3.STDOUT_FILE)

        processor_1.start()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_1.STDOUT_FILE)
        self.wait_for_verification(processor_3, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_3.STDOUT_FILE, num_lines=2)

        processor_2.start()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:2 STANDBY_TASKS:2", processor_1.STDOUT_FILE)
        self.wait_for_verification(processor_2, "ACTIVE_TASKS:2 STANDBY_TASKS:2", processor_2.STDOUT_FILE)
        self.wait_for_verification(processor_3, "ACTIVE_TASKS:2 STANDBY_TASKS:2", processor_3.STDOUT_FILE, num_lines=2)

        processor_3.stop()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_1.STDOUT_FILE, num_lines=2)
        self.wait_for_verification(processor_2, "ACTIVE_TASKS:3 STANDBY_TASKS:3", processor_2.STDOUT_FILE)

        processor_1.stop()

        self.wait_for_verification(processor_2, "ACTIVE_TASKS:6 STANDBY_TASKS:0", processor_2.STDOUT_FILE)

        processor_3.start()
        processor_1.start()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:2 STANDBY_TASKS:2", processor_1.STDOUT_FILE)
        self.wait_for_verification(processor_3, "ACTIVE_TASKS:2 STANDBY_TASKS:2", processor_3.STDOUT_FILE)
        self.wait_for_verification(processor_2, "ACTIVE_TASKS:2 STANDBY_TASKS:2", processor_2.STDOUT_FILE, num_lines=2)

        self.assert_consume("assert all messages consumed from %s" % self.streams_sink_topic_1, self.streams_sink_topic_1, self.num_messages)
        self.assert_consume("assert all messages consumed from %s" % self.streams_sink_topic_2, self.streams_sink_topic_2, self.num_messages)

        self.wait_for_verification(driver, "Producer shut down now, sent total [{0}] of requested [{0}]".format(str(self.num_messages)),
                                   driver.STDOUT_FILE)


