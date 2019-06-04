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
from kafkatest.services.kafka import KafkaService
from kafkatest.services.streams import StreamsStaticMembershipService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService

class StreamsStaticMembershipTest(Test):
    """
    Tests using static membership when broker points to minimum supported
    version (2.3) or higher.
    """

    input_topic = 'inputTopic'
    pattern = 'PROCESSED'

    def __init__(self, test_context):
        super(StreamsStaticMembershipTest, self).__init__(test_context)
        self.topics = {
            self.input_topic: {'partitions': 18},
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

    def test_rolling_bounces_will_not_trigger_rebalance_under_static_membership(self):
        self.logger.info("code rolling bounce test started")
        self.zookeeper.start()
        self.kafka.start()

        numThreads = 3
        processor1 = StreamsStaticMembershipService(self.test_context, self.kafka, "consumer-A", numThreads)
        processor2 = StreamsStaticMembershipService(self.test_context, self.kafka, "consumer-B", numThreads)
        processor3 = StreamsStaticMembershipService(self.test_context, self.kafka, "consumer-C", numThreads)

        processors = [processor1, processor2, processor3]

        self.producer.start()

        for processor in processors:
            processor.CLEAN_NODE_ENABLED = False
            self.set_topics(processor)
            self.verify_running(processor, 'REBALANCING -> RUNNING')

        self.verify_processing(processors)

        # do several rolling bounces
        numBounces = 3
        for i in range(0, numBounces):
            for processor in processors:
                self.verify_stopped(processor)
                self.verify_running(processor, 'REBALANCING -> RUNNING')

        stableGeneration = 3
        for processor in processors:
            generations = self.extract_generation_from_logs(processor)
            for generation in generations[-(numBounces * numThreads):]:
                assert stableGeneration == int(generation), \
                    "Stream rolling bounce have caused unexpected generation bump %d" % int(generation)

        self.verify_processing(processors)

        self.stop_processors(processors)

        self.producer.stop()
        self.kafka.stop()
        self.zookeeper.stop()

    @staticmethod
    def extract_generation_from_logs(processor):
        return list(processor.node.account.ssh_capture("grep \"Successfully joined group with generation\" %s| awk \'{for(i=1;i<=NF;i++) {if ($i == \"generation\") beginning=i+1; if($i== \"(org.apache.kafka.clients.consumer.internals.AbstractCoordinator)\") ending=i }; for (j=beginning;j<ending;j++) printf $j; printf \"\\n\"}\'" % processor.LOG_FILE, allow_fail=True))

    @staticmethod
    def verify_running(processor, message):
        node = processor.node
        with node.account.monitor_log(processor.STDOUT_FILE) as monitor:
            processor.start()
            monitor.wait_until(message,
                               timeout_sec=60,
                               err_msg="Never saw '%s' message " % message + str(processor.node.account))

    @staticmethod
    def verify_stopped(processor):
        node = processor.node
        with node.account.monitor_log(processor.STDOUT_FILE) as monitor:
            processor.stop()
            monitor.wait_until('Static membership test closed',
                               timeout_sec=60,
                               err_msg="'Static membership test closed' message" + str(processor.node.account))

    def verify_processing(self, processors):
        for processor in processors:
            with processor.node.account.monitor_log(processor.STDOUT_FILE) as monitor:
                monitor.wait_until(self.pattern,
                                   timeout_sec=60,
                                   err_msg="Never saw processing of %s " % self.pattern + str(processor.node.account))

    def stop_processors(self, processors):
        for processor in processors:
            self.verify_stopped(processor)

    def set_topics(self, processor):
        processor.INPUT_TOPIC = self.input_topic
