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
from ducktape.tests.test import Test
from kafkatest.services.kafka import KafkaService
from kafkatest.services.streams import StreamsOptimizedUpgradeTestService
from kafkatest.services.streams import StreamsResetter
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.streams.utils import stop_processors

class StreamsOptimizedTest(Test):
    """
    Test doing upgrades of a Kafka Streams application
    that is un-optimized initially then optimized
    """

    input_topic = 'inputTopic'
    aggregation_topic = 'aggregationTopic'
    reduce_topic = 'reduceTopic'
    join_topic = 'joinTopic'
    operation_pattern = 'AGGREGATED\|REDUCED\|JOINED'
    stopped_message = 'OPTIMIZE_TEST Streams Stopped'

    def __init__(self, test_context):
        super(StreamsOptimizedTest, self).__init__(test_context)
        self.topics = {
            self.input_topic: {'partitions': 6},
            self.aggregation_topic: {'partitions': 6},
            self.reduce_topic: {'partitions': 6},
            self.join_topic: {'partitions': 6}
        }

        self.zookeeper = ZookeeperService(self.test_context, num_nodes=1)
        self.kafka = KafkaService(self.test_context, num_nodes=3,
                                  zk=self.zookeeper, topics=self.topics)

        self.producer = VerifiableProducer(self.test_context,
                                           1,
                                           self.kafka,
                                           self.input_topic,
                                           throughput=1000,
                                           acks=1)

    def test_upgrade_optimized_topology(self):
        self.zookeeper.start()
        self.kafka.start()

        processor1 = StreamsOptimizedUpgradeTestService(self.test_context, self.kafka)
        processor2 = StreamsOptimizedUpgradeTestService(self.test_context, self.kafka)
        processor3 = StreamsOptimizedUpgradeTestService(self.test_context, self.kafka)

        processors = [processor1, processor2, processor3]

        # produce records continually during the test
        self.producer.start()

        # start all processors unoptimized
        for processor in processors:
            self.set_topics(processor)
            processor.CLEAN_NODE_ENABLED = False
            self.verify_running_repartition_topic_count(processor, 4)

        self.verify_processing(processors, verify_individual_operations=False)

        stop_processors(processors, self.stopped_message)

        self.reset_application()

        # start again with topology optimized
        for processor in processors:
            processor.OPTIMIZED_CONFIG = 'all'
            self.verify_running_repartition_topic_count(processor, 1)

        self.verify_processing(processors, verify_individual_operations=True)

        stop_processors(processors, self.stopped_message)

        self.producer.stop()
        self.kafka.stop()
        self.zookeeper.stop()

    def reset_application(self):
        resetter = StreamsResetter(self.test_context, self.kafka, topic = self.input_topic, applicationId = 'StreamsOptimizedTest')
        resetter.start()
        # resetter is not long-term running but it would be better to check the pid by stopping it
        resetter.stop()

    @staticmethod
    def verify_running_repartition_topic_count(processor, repartition_topic_count):
        node = processor.node
        with node.account.monitor_log(processor.STDOUT_FILE) as monitor:
            processor.start()
            monitor.wait_until('REBALANCING -> RUNNING with REPARTITION TOPIC COUNT=%s' % repartition_topic_count,
                               timeout_sec=120,
                               err_msg="Never saw 'REBALANCING -> RUNNING with REPARTITION TOPIC COUNT=%s' message "
                                       % repartition_topic_count + str(processor.node.account))

    def verify_processing(self, processors, verify_individual_operations):
        for processor in processors:
            if not self.all_source_subtopology_tasks(processor):
                if verify_individual_operations:
                    for operation in self.operation_pattern.split('\|'):
                        self.do_verify(processor, operation)
                else:
                    self.do_verify(processor, self.operation_pattern)
            else:
                self.logger.info("Skipping processor %s with all source tasks" % processor.node.account)

    def do_verify(self, processor, pattern):
        self.logger.info("Verifying %s processing pattern in STDOUT_FILE" % pattern)
        with processor.node.account.monitor_log(processor.STDOUT_FILE) as monitor:
            monitor.wait_until(pattern,
                               timeout_sec=60,
                               err_msg="Never saw processing of %s " % pattern + str(processor.node.account))

    def all_source_subtopology_tasks(self, processor):
        retries = 0
        while retries < 5:
            found = list(processor.node.account.ssh_capture("sed -n 's/.*current active tasks: \[\(\(0_[0-9], \)\{3\}0_[0-9]\)\].*/\1/p' %s" % processor.LOG_FILE, allow_fail=True))
            self.logger.info("Returned %s from assigned task check" % found)
            if len(found) > 0:
                return True
            retries += 1
            time.sleep(1)

        return False

    def set_topics(self, processor):
        processor.INPUT_TOPIC = self.input_topic
        processor.AGGREGATION_TOPIC = self.aggregation_topic
        processor.REDUCE_TOPIC = self.reduce_topic
        processor.JOIN_TOPIC = self.join_topic
