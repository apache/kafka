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
from ducktape.mark import ignore

from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.streams import StreamsEosTestDriverService, StreamsEosTestJobRunnerService, \
    StreamsComplexEosTestJobRunnerService, StreamsEosTestVerifyRunnerService, StreamsComplexEosTestVerifyRunnerService
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
            'sum' : { 'partitions': 5, 'replication-factor': 2 },
            'repartition' : { 'partitions': 5, 'replication-factor': 2 },
            'max' : { 'partitions': 5, 'replication-factor': 2 },
            'cnt' : { 'partitions': 5, 'replication-factor': 2 }
        })
        self.driver = StreamsEosTestDriverService(test_context, self.kafka)
        self.test_context = test_context

    @ignore
    @cluster(num_nodes=8)
    def test_rebalance_simple(self):
        self.run_rebalance(StreamsEosTestJobRunnerService(self.test_context, self.kafka),
                           StreamsEosTestJobRunnerService(self.test_context, self.kafka),
                           StreamsEosTestVerifyRunnerService(self.test_context, self.kafka))

    @ignore
    @cluster(num_nodes=8)
    def test_rebalance_complex(self):
        self.run_rebalance(StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka),
                           StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka),
                           StreamsComplexEosTestVerifyRunnerService(self.test_context, self.kafka))

    def run_rebalance(self, processor1, processor2, verifier):
        """
        Starts and stops two test clients a few times.
        Ensure that all records are delivered exactly-once.
        """

        self.driver.start()
        processor1.start()

        time.sleep(120)

        processor2.start()

        time.sleep(120)
        processor1.stop()

        time.sleep(120)
        processor1.start()

        time.sleep(120)
        processor2.stop()

        time.sleep(120)

        self.driver.stop()

        processor1.stop()
        processor2.stop()

        verifier.start()
        verifier.wait()

        verifier.node.account.ssh("grep ALL-RECORDS-DELIVERED %s" % verifier.STDOUT_FILE, allow_fail=False)

    @ignore
    @cluster(num_nodes=8)
    def test_failure_and_recovery(self):
        self.run_failure_and_recovery(StreamsEosTestJobRunnerService(self.test_context, self.kafka),
                                      StreamsEosTestJobRunnerService(self.test_context, self.kafka),
                                      StreamsEosTestVerifyRunnerService(self.test_context, self.kafka))

    @ignore
    @cluster(num_nodes=8)
    def test_failure_and_recovery_complex(self):
        self.run_failure_and_recovery(StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka),
                                      StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka),
                                      StreamsComplexEosTestVerifyRunnerService(self.test_context, self.kafka))

    def run_failure_and_recovery(self, processor1, processor2, verifier):
        """
        Starts two test clients, then abort (kill -9) and restart them a few times.
        Ensure that all records are delivered exactly-once.
        """

        self.driver.start()
        processor1.start()
        processor2.start()

        time.sleep(120)
        processor1.abortThenRestart()

        time.sleep(120)
        processor1.abortThenRestart()

        time.sleep(120)
        processor2.abortThenRestart()

        time.sleep(120)

        self.driver.stop()

        processor1.stop()
        processor2.stop()

        verifier.start()
        verifier.wait()

        verifier.node.account.ssh("grep ALL-RECORDS-DELIVERED %s" % verifier.STDOUT_FILE, allow_fail=False)
