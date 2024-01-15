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
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.streams import StreamsNamedRepartitionTopicService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.streams.utils import verify_stopped, stop_processors, verify_running

class StreamsNamedRepartitionTopicTest(Test):
    """
    Tests using a named repartition topic by starting
    application then doing a rolling upgrade with added
    operations and the application still runs
    """

    input_topic = 'inputTopic'
    aggregation_topic = 'aggregationTopic'
    pattern = 'AGGREGATED'
    stopped_message = 'NAMED_REPARTITION_TEST Streams Stopped'

    def __init__(self, test_context):
        super(StreamsNamedRepartitionTopicTest, self).__init__(test_context)
        self.topics = {
            self.input_topic: {'partitions': 6},
            self.aggregation_topic: {'partitions': 6}
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

    @cluster(num_nodes=8)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_upgrade_topology_with_named_repartition_topic(self, metadata_quorum):
        if self.zookeeper:
            self.zookeeper.start()
        self.kafka.start()

        processor1 = StreamsNamedRepartitionTopicService(self.test_context, self.kafka)
        processor2 = StreamsNamedRepartitionTopicService(self.test_context, self.kafka)
        processor3 = StreamsNamedRepartitionTopicService(self.test_context, self.kafka)

        processors = [processor1, processor2, processor3]

        self.producer.start()

        for processor in processors:
            processor.CLEAN_NODE_ENABLED = False
            self.set_topics(processor)
            verify_running(processor, 'REBALANCING -> RUNNING')

        self.verify_processing(processors)

        # do rolling upgrade
        for processor in processors:
            verify_stopped(processor, self.stopped_message)
            #  will tell app to add operations before repartition topic
            processor.ADD_ADDITIONAL_OPS = 'true'
            verify_running(processor, 'UPDATED Topology')

        self.verify_processing(processors)

        stop_processors(processors, self.stopped_message)

        self.producer.stop()
        self.kafka.stop()
        if self.zookeeper:
            self.zookeeper.stop()

    def verify_processing(self, processors):
        for processor in processors:
            with processor.node.account.monitor_log(processor.STDOUT_FILE) as monitor:
                monitor.wait_until(self.pattern,
                                   timeout_sec=60,
                                   err_msg="Never saw processing of %s " % self.pattern + str(processor.node.account))

    def set_topics(self, processor):
        processor.INPUT_TOPIC = self.input_topic
        processor.AGGREGATION_TOPIC = self.aggregation_topic
