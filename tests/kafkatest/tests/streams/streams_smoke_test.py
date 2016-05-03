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

from ducktape.mark import ignore

from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsSmokeTestJobRunnerService
import time

class StreamsSmokeTest(KafkaTest):
    """
    Simple test of Kafka Streams.
    """

    def __init__(self, test_context):
        super(StreamsSmokeTest, self).__init__(test_context, num_zk=1, num_brokers=1, topics={
            'echo' : { 'partitions': 5, 'replication-factor': 1 },
            'data' : { 'partitions': 5, 'replication-factor': 1 },
            'min' : { 'partitions': 5, 'replication-factor': 1 },
            'max' : { 'partitions': 5, 'replication-factor': 1 },
            'sum' : { 'partitions': 5, 'replication-factor': 1 },
            'dif' : { 'partitions': 5, 'replication-factor': 1 },
            'cnt' : { 'partitions': 5, 'replication-factor': 1 },
            'avg' : { 'partitions': 5, 'replication-factor': 1 },
            'wcnt' : { 'partitions': 5, 'replication-factor': 1 },
            'tagg' : { 'partitions': 5, 'replication-factor': 1 }
        })

        self.driver = StreamsSmokeTestDriverService(test_context, self.kafka)
        self.processor1 = StreamsSmokeTestJobRunnerService(test_context, self.kafka)
        self.processor2 = StreamsSmokeTestJobRunnerService(test_context, self.kafka)
        self.processor3 = StreamsSmokeTestJobRunnerService(test_context, self.kafka)
        self.processor4 = StreamsSmokeTestJobRunnerService(test_context, self.kafka)

    def test_streams(self):
        """
        Start a few smoke test clients, then repeat start a new one, stop (cleanly) running one a few times.
        Ensure that all results (stats on values computed by Kafka Streams) are correct.
        """

        self.driver.start()

        self.processor1.start()
        self.processor2.start()

        time.sleep(15);

        self.processor3.start()
        self.processor1.stop()

        time.sleep(15);

        self.processor4.start();

        self.driver.wait()
        self.driver.stop()

        self.processor2.stop()
        self.processor3.stop()
        self.processor4.stop()

        node = self.driver.node
        node.account.ssh("grep SUCCESS %s" % self.driver.STDOUT_FILE, allow_fail=False)
