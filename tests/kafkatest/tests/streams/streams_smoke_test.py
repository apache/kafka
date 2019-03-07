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

from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsSmokeTestJobRunnerService, StreamsSmokeTestEOSJobRunnerService


class StreamsSmokeTest(KafkaTest):
    """
    Simple test of Kafka Streams.
    """

    def __init__(self, test_context):
        super(StreamsSmokeTest, self).__init__(test_context, num_zk=1, num_brokers=3, topics={
            'echo' : { 'partitions': 5, 'replication-factor': 1 },
            'data' : { 'partitions': 5, 'replication-factor': 1 },
            'min' : { 'partitions': 5, 'replication-factor': 1 },
            'min-suppressed' : { 'partitions': 5, 'replication-factor': 1 },
            'min-raw' : { 'partitions': 5, 'replication-factor': 1 },
            'max' : { 'partitions': 5, 'replication-factor': 1 },
            'sum' : { 'partitions': 5, 'replication-factor': 1 },
            'sws-raw' : { 'partitions': 5, 'replication-factor': 1 },
            'sws-suppressed' : { 'partitions': 5, 'replication-factor': 1 },
            'dif' : { 'partitions': 5, 'replication-factor': 1 },
            'cnt' : { 'partitions': 5, 'replication-factor': 1 },
            'avg' : { 'partitions': 5, 'replication-factor': 1 },
            'wcnt' : { 'partitions': 5, 'replication-factor': 1 },
            'tagg' : { 'partitions': 5, 'replication-factor': 1 }
        })

        self.test_context = test_context
        self.driver = StreamsSmokeTestDriverService(test_context, self.kafka)

    @cluster(num_nodes=8)
    @matrix(eos=[True, False], crash=[True, False])
    def test_streams(self, eos, crash):
        #
        if eos:
            processor1 = StreamsSmokeTestEOSJobRunnerService(self.test_context, self.kafka)
            processor2 = StreamsSmokeTestEOSJobRunnerService(self.test_context, self.kafka)
            processor3 = StreamsSmokeTestEOSJobRunnerService(self.test_context, self.kafka)
        else:
            processor1 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka)
            processor2 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka)
            processor3 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka)



        with processor1.node.account.monitor_log(processor1.STDOUT_FILE) as monitor1:
            processor1.start()
            monitor1.wait_until('REBALANCING -> RUNNING',
                               timeout_sec=60,
                               err_msg="Never saw 'REBALANCING -> RUNNING' message " + str(processor1.node.account)
                               )

            self.driver.start()

            monitor1.wait_until('processed',
                                timeout_sec=30,
                                err_msg="Didn't see any processing messages " + str(processor1.node.account)
                                )

            # make sure we're not already done processing (which would invalidate the test)
            self.driver.node.account.ssh("! grep 'Result Verification' %s" % self.driver.STDOUT_FILE, allow_fail=False)

            processor1.stop_nodes(not crash)

        with processor2.node.account.monitor_log(processor2.STDOUT_FILE) as monitor2:
            processor2.start()
            monitor2.wait_until('REBALANCING -> RUNNING',
                                timeout_sec=120,
                                err_msg="Never saw 'REBALANCING -> RUNNING' message " + str(processor2.node.account)
                                )
            monitor2.wait_until('processed',
                                timeout_sec=30,
                                err_msg="Didn't see any processing messages " + str(processor2.node.account)
                                )

        # make sure we're not already done processing (which would invalidate the test)
        self.driver.node.account.ssh("! grep 'Result Verification' %s" % self.driver.STDOUT_FILE, allow_fail=False)

        processor2.stop_nodes(not crash)

        with processor3.node.account.monitor_log(processor3.STDOUT_FILE) as monitor3:
            processor3.start()
            monitor3.wait_until('REBALANCING -> RUNNING',
                                timeout_sec=120,
                                err_msg="Never saw 'REBALANCING -> RUNNING' message " + str(processor3.node.account)
                                )
            # there should still be some data left for this processor to work on.
            monitor3.wait_until('processed',
                                timeout_sec=30,
                                err_msg="Didn't see any processing messages " + str(processor3.node.account)
                                )

        self.driver.wait()
        self.driver.stop()

        processor3.stop()

        if crash and not eos:
            self.driver.node.account.ssh("grep -E 'SUCCESS|PROCESSED-MORE-THAN-GENERATED' %s" % self.driver.STDOUT_FILE, allow_fail=False)
        else:
            self.driver.node.account.ssh("grep SUCCESS %s" % self.driver.STDOUT_FILE, allow_fail=False)
