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
from kafkatest.services.streams import StaticMemberTestService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.streams.utils import verify_stopped, stop_processors, verify_running, extract_generation_from_logs, extract_generation_id

class StreamsStaticMembershipTest(Test):
    """
    Tests using static membership when broker points to minimum supported
    version (2.3) or higher.
    """

    input_topic = 'inputTopic'
    pattern = 'PROCESSED'
    running_message = 'REBALANCING -> RUNNING'
    stopped_message = 'Static membership test closed'

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
        self.zookeeper.start()
        self.kafka.start()

        numThreads = 3
        processor1 = StaticMemberTestService(self.test_context, self.kafka, "consumer-A", numThreads)
        processor2 = StaticMemberTestService(self.test_context, self.kafka, "consumer-B", numThreads)
        processor3 = StaticMemberTestService(self.test_context, self.kafka, "consumer-C", numThreads)

        processors = [processor1, processor2, processor3]

        self.producer.start()

        for processor in processors:
            processor.CLEAN_NODE_ENABLED = False
            self.set_topics(processor)
            verify_running(processor, self.running_message)

        self.verify_processing(processors)

        # do several rolling bounces
        num_bounces = 3
        for i in range(0, num_bounces):
            for processor in processors:
                verify_stopped(processor, self.stopped_message)
                verify_running(processor, self.running_message)

        stable_generation = -1
        for processor in processors:
            generations = extract_generation_from_logs(processor)
            num_bounce_generations = num_bounces * numThreads
            assert num_bounce_generations <= len(generations), \
                "Smaller than minimum expected %d generation messages, actual %d" % (num_bounce_generations, len(generations))

            for generation in generations[-num_bounce_generations:]:
                generation = extract_generation_id(generation)
                if stable_generation == -1:
                    stable_generation = generation
                assert stable_generation == generation, \
                    "Stream rolling bounce have caused unexpected generation bump %d" % generation

        self.verify_processing(processors)

        stop_processors(processors, self.stopped_message)

        self.producer.stop()
        self.kafka.stop()
        self.zookeeper.stop()

    def verify_processing(self, processors):
        for processor in processors:
            with processor.node.account.monitor_log(processor.STDOUT_FILE) as monitor:
                monitor.wait_until(self.pattern,
                                   timeout_sec=60,
                                   err_msg="Never saw processing of %s " % self.pattern + str(processor.node.account))

    def set_topics(self, processor):
        processor.INPUT_TOPIC = self.input_topic
