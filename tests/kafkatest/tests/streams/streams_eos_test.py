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
from kafkatest.services.streams import StreamsEosTestDriverService, StreamsEosTestJobRunnerService, StreamsEosTestVerifyRunnerService
import time


class StreamsEosTest(KafkaTest):
    """
    Test of Kafka Streams exactly-once semantics
    """

    def __init__(self, test_context):
        super(StreamsEosTest, self).__init__(test_context, num_zk=1, num_brokers=3, topics={
            'data' : { 'partitions': 5, 'replication-factor': 2 },
            'echo' : { 'partitions': 5, 'replication-factor': 2 },
            'min' : { 'partitions': 5, 'replication-factor': 2 },
            'sum' : { 'partitions': 5, 'replication-factor': 2 }
        })
        self.driver = StreamsEosTestDriverService(test_context, self.kafka)
        self.processor1 = StreamsEosTestJobRunnerService(test_context, self.kafka)
        self.processor2 = StreamsEosTestJobRunnerService(test_context, self.kafka)
        self.verifier = StreamsEosTestVerifyRunnerService(test_context, self.kafka)

    @cluster(num_nodes=8)
    def test_rebalance(self):
        """
        Starts and stops two test clients a few times.
        Ensure that all records are delivered exactly-once.
        """

        self.driver.start()
        self.processor1.start()

        time.sleep(30)

        self.processor2.start()

        time.sleep(30)
        self.processor1.stop()

        time.sleep(30)
        self.processor1.start()

        time.sleep(30)
        self.processor2.stop()

        time.sleep(30)

        self.driver.stop()

        self.processor1.stop()
        self.processor2.stop()

        self.verifier.start()
        self.verifier.wait()

        self.verifier.node.account.ssh("grep ALL-RECORDS-DELIVERED %s" % self.verifier.STDOUT_FILE, allow_fail=False)

    @cluster(num_nodes=8)
    def test_failure_and_recovery(self):
        """
        Starts two test clients, then abort (kill -9) and restart them a few times.
        Ensure that all records are delivered exactly-once.
        """

        self.driver.start()
        self.processor1.start()
        self.processor2.start()

        time.sleep(30)
        self.processor1.abortThenRestart()

        time.sleep(30)
        self.processor1.abortThenRestart()

        time.sleep(30)
        self.processor2.abortThenRestart()

        time.sleep(30)

        self.driver.stop()

        self.processor1.stop()
        self.processor2.stop()

        self.verifier.start()
        self.verifier.wait()

        self.verifier.node.account.ssh("grep ALL-RECORDS-DELIVERED %s" % self.verifier.STDOUT_FILE, allow_fail=False)
