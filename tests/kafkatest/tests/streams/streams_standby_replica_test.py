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

from kafkatest.services.streams import StreamsRepeatingIntegerKeyProducerService
from kafkatest.services.streams import StreamsStandbyTaskService
from kafkatest.tests.streams.base_streams_test import BaseStreamsTest


class StreamsStandbyTask(BaseStreamsTest):
    """
    This test validates using standby tasks helps with rebalance times
    additionally verifies standby replicas continue to work in the
    face of continual changes to streams code base
    """

    streams_source_topic = "standbyTaskSource1"
    streams_sink_topic_1 = "standbyTaskSink1"
    streams_sink_topic_2 = "standbyTaskSink2"
    client_id = "stream-broker-resilience-verify-consumer"

    num_messages = 60000

    def __init__(self, test_context):
        super(StreamsStandbyTask, self).__init__(test_context,
                                                 topics={
                                                     self.streams_source_topic: {'partitions': 6, 'replication-factor': 1},
                                                     self.streams_sink_topic_1: {'partitions': 1, 'replication-factor': 1},
                                                     self.streams_sink_topic_2: {'partitions': 1, 'replication-factor': 1}
                                                 })

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

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:3 STANDBY_TASKS:[^0]", processor_1.STDOUT_FILE)
        self.wait_for_verification(processor_2, "ACTIVE_TASKS:3 STANDBY_TASKS:[^0]", processor_2.STDOUT_FILE)

        processor_3.start()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:2 STANDBY_TASKS:[^0]", processor_1.STDOUT_FILE)
        self.wait_for_verification(processor_2, "ACTIVE_TASKS:2 STANDBY_TASKS:[^0]", processor_2.STDOUT_FILE)
        self.wait_for_verification(processor_3, "ACTIVE_TASKS:2 STANDBY_TASKS:[^0]", processor_3.STDOUT_FILE)

        processor_1.stop()

        self.wait_for_verification(processor_2, "ACTIVE_TASKS:3 STANDBY_TASKS:[^0]", processor_2.STDOUT_FILE, num_lines=2)
        self.wait_for_verification(processor_3, "ACTIVE_TASKS:3 STANDBY_TASKS:[^0]", processor_3.STDOUT_FILE)

        processor_2.stop()

        self.wait_for_verification(processor_3, "ACTIVE_TASKS:6 STANDBY_TASKS:0", processor_3.STDOUT_FILE)

        processor_1.start()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:3 STANDBY_TASKS:[^0]", processor_1.STDOUT_FILE)
        self.wait_for_verification(processor_3, "ACTIVE_TASKS:3 STANDBY_TASKS:[^0]", processor_3.STDOUT_FILE, num_lines=2)

        processor_2.start()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:2 STANDBY_TASKS:[^0]", processor_1.STDOUT_FILE)
        self.wait_for_verification(processor_2, "ACTIVE_TASKS:2 STANDBY_TASKS:[^0]", processor_2.STDOUT_FILE)
        self.wait_for_verification(processor_3, "ACTIVE_TASKS:2 STANDBY_TASKS:[^0]", processor_3.STDOUT_FILE, num_lines=2)

        processor_3.stop()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:3 STANDBY_TASKS:[^0]", processor_1.STDOUT_FILE, num_lines=2)
        self.wait_for_verification(processor_2, "ACTIVE_TASKS:3 STANDBY_TASKS:[^0]", processor_2.STDOUT_FILE)

        processor_1.stop()

        self.wait_for_verification(processor_2, "ACTIVE_TASKS:6 STANDBY_TASKS:0", processor_2.STDOUT_FILE)

        processor_3.start()
        processor_1.start()

        self.wait_for_verification(processor_1, "ACTIVE_TASKS:2 STANDBY_TASKS:[^0]", processor_1.STDOUT_FILE)
        self.wait_for_verification(processor_3, "ACTIVE_TASKS:2 STANDBY_TASKS:[^0]", processor_3.STDOUT_FILE)
        self.wait_for_verification(processor_2, "ACTIVE_TASKS:2 STANDBY_TASKS:[^0]", processor_2.STDOUT_FILE, num_lines=2)

        self.assert_consume(self.client_id, "assert all messages consumed from %s" % self.streams_sink_topic_1, self.streams_sink_topic_1, self.num_messages)
        self.assert_consume(self.client_id, "assert all messages consumed from %s" % self.streams_sink_topic_2, self.streams_sink_topic_2, self.num_messages)

        self.wait_for_verification(driver, "Producer shut down now, sent total {0} of requested {0}".format(str(self.num_messages)),
                                   driver.STDOUT_FILE)


