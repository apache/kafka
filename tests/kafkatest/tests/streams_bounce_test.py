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

class StreamsBounceTest(KafkaTest):
    """
    Simple test of Kafka Streams.
    """

    def __init__(self, test_context):
        super(StreamsBounceTest, self).__init__(test_context, num_zk=1, num_brokers=2, topics={
            'echo' : { 'partitions': 5, 'replication-factor': 2 },
            'data' : { 'partitions': 5, 'replication-factor': 2 },
            'min' : { 'partitions': 5, 'replication-factor': 2 },
            'max' : { 'partitions': 5, 'replication-factor': 2 },
            'sum' : { 'partitions': 5, 'replication-factor': 2 },
            'dif' : { 'partitions': 5, 'replication-factor': 2 },
            'cnt' : { 'partitions': 5, 'replication-factor': 2 },
            'avg' : { 'partitions': 5, 'replication-factor': 2 },
            'wcnt' : { 'partitions': 5, 'replication-factor': 2 },
            'tagg' : { 'partitions': 5, 'replication-factor': 2 }
        })

        self.driver = StreamsSmokeTestDriverService(test_context, self.kafka)
        self.processor1 = StreamsSmokeTestJobRunnerService(test_context, self.kafka)

    def test_bounce(self):
        """
        Start a smoke test client, then abort (kill -9) and restart it a few times.
        Ensure that all records are delivered.
        """

        self.driver.start()

        self.processor1.start()

        time.sleep(15);

        self.processor1.abortThenRestart()

        time.sleep(15);

        # enable this after we add change log partition replicas
        #self.kafka.signal_leader("data")

        #time.sleep(15);

        self.processor1.abortThenRestart()

        self.driver.wait()
        self.driver.stop()

        self.processor1.stop()

        node = self.driver.node
        node.account.ssh("grep ALL-RECORDS-DELIVERED %s" % self.driver.STDOUT_FILE, allow_fail=False)
