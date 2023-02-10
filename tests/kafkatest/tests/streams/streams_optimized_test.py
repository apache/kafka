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

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService, quorum
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

        self.zookeeper = (
            ZookeeperService(self.test_context, 1)
            if quorum.for_test(self.test_context) == quorum.zk
            else None
        )
        self.kafka = KafkaService(self.test_context, num_nodes=3,
                                  zk=self.zookeeper, topics=self.topics, controller_num_nodes_override=1)

        self.producer = VerifiableProducer(self.test_context,
                                           1,
                                           self.kafka,
                                           self.input_topic,
                                           throughput=1000,
                                           acks=1)

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=[quorum.remote_kraft])
    def test_upgrade_optimized_topology(self, metadata_quorum):
        if self.zookeeper:
            self.zookeeper.start()
        self.kafka.start()

        processor1 = StreamsOptimizedUpgradeTestService(self.test_context, self.kafka)
        processor2 = StreamsOptimizedUpgradeTestService(self.test_context, self.kafka)
        processor3 = StreamsOptimizedUpgradeTestService(self.test_context, self.kafka)

        processors = [processor1, processor2, processor3]

        self.logger.info("produce records continually during the test")
        self.producer.start()

        self.logger.info("start all processors unoptimized")
        for processor in processors:
            self.set_topics(processor)
            processor.CLEAN_NODE_ENABLED = False
            self.verify_running_repartition_topic_count(processor, 4)

        self.logger.info("verify unoptimized")
        self.verify_processing(processors, verify_individual_operations=False)

        self.logger.info("stop unoptimized")
        stop_processors(processors, self.stopped_message)

        self.logger.info("reset")
        self.reset_application()
        for processor in processors:
            processor.node.account.ssh("mv " + processor.LOG_FILE + " " + processor.LOG_FILE + ".1", allow_fail=False)
            processor.node.account.ssh("mv " + processor.STDOUT_FILE + " " + processor.STDOUT_FILE + ".1", allow_fail=False)
            processor.node.account.ssh("mv " + processor.STDERR_FILE + " " + processor.STDERR_FILE + ".1", allow_fail=False)
            processor.node.account.ssh("mv " + processor.CONFIG_FILE + " " + processor.CONFIG_FILE + ".1", allow_fail=False)

        self.logger.info("start again with topology optimized")
        for processor in processors:
            processor.OPTIMIZED_CONFIG = 'all'
            self.verify_running_repartition_topic_count(processor, 1)

        self.logger.info("verify optimized")
        self.verify_processing(processors, verify_individual_operations=True)

        self.logger.info("stop optimized")
        stop_processors(processors, self.stopped_message)

        self.logger.info("teardown")
        self.producer.stop()
        self.kafka.stop()
        if self.zookeeper:
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
        # This test previously had logic to account for skewed assignments, in which not all processors may
        # receive active assignments. I don't think this will happen anymore, but keep an eye out if we see
        # test failures here. If that does resurface, note that the prior implementation was not correct.
        # A better approach would be to make sure we see processing of each partition across the whole cluster
        # instead of just expecting to see each node perform some processing.
        for processor in processors:
            if verify_individual_operations:
                for operation in self.operation_pattern.split('\|'):
                    self.do_verify(processor, operation)
            else:
                self.do_verify(processor, self.operation_pattern)

    def do_verify(self, processor, pattern):
        self.logger.info("Verifying %s processing pattern in STDOUT_FILE" % pattern)
        self.logger.info(list(processor.node.account.ssh_capture("ls -lh %s" % (processor.STDOUT_FILE), allow_fail=True)))
        wait_until(
            lambda: processor.node.account.ssh("grep --max-count 1 '%s' %s" % (pattern, processor.STDOUT_FILE), allow_fail=True) == 0,
            timeout_sec=60
        )

    def set_topics(self, processor):
        processor.INPUT_TOPIC = self.input_topic
        processor.AGGREGATION_TOPIC = self.aggregation_topic
        processor.REDUCE_TOPIC = self.reduce_topic
        processor.JOIN_TOPIC = self.join_topic
