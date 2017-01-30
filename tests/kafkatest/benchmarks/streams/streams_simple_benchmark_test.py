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

from ducktape.mark.resource import cluster

from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.performance.streams_performance import StreamsSimpleBenchmarkService


class StreamsSimpleBenchmarkTest(KafkaTest):
    """
    Simple benchmark of Kafka Streams.
    """

    def __init__(self, test_context):
        super(StreamsSimpleBenchmarkTest, self).__init__(test_context, num_zk=1, num_brokers=1,topics={
            'simpleBenchmarkSourceTopic' : { 'partitions': 1, 'replication-factor': 1 },
            'simpleBenchmarkSinkTopic' : { 'partitions': 1, 'replication-factor': 1 },
            'joinSourceTopic1KStreamKStream' : { 'partitions': 1, 'replication-factor': 1 },
            'joinSourceTopic2KStreamKStream' : { 'partitions': 1, 'replication-factor': 1 },
            'joinSourceTopic1KStreamKTable' : { 'partitions': 1, 'replication-factor': 1 },
            'joinSourceTopic2KStreamKTable' : { 'partitions': 1, 'replication-factor': 1 },
            'joinSourceTopic1KTableKTable' : { 'partitions': 1, 'replication-factor': 1 },
            'joinSourceTopic2KTableKTable' : { 'partitions': 1, 'replication-factor': 1 }
        })

        self.driver = StreamsSimpleBenchmarkService(test_context, self.kafka, 1000000L)

    @cluster(num_nodes=3)
    def test_simple_benchmark(self):
        """
        Run simple Kafka Streams benchmark
        """

        self.driver.start()
        self.driver.wait()
        self.driver.stop()
        node = self.driver.node
        node.account.ssh("grep Performance %s" % self.driver.STDOUT_FILE, allow_fail=False)

        return self.driver.collect_data(node)
