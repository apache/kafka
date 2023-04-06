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
from kafkatest.services.kafka import quorum
from kafkatest.services.streams import StreamsSmokeTestShutdownDeadlockService


class StreamsShutdownDeadlockTest(KafkaTest):
    """
    Simple test of Kafka Streams.
    """

    def __init__(self, test_context):
        super(StreamsShutdownDeadlockTest, self).__init__(test_context, num_zk=1, num_brokers=1, topics={
            'source' : { 'partitions': 1, 'replication-factor': 1 }
        })

        self.driver = StreamsSmokeTestShutdownDeadlockService(test_context, self.kafka)

    @cluster(num_nodes=3)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_shutdown_wont_deadlock(self, metadata_quorum):
        """
        Start ShutdownDeadLockTest, wait for upt to 1 minute, and check that the process exited.
        If it hasn't exited then fail as it is deadlocked
        """

        self.driver.start()

        self.driver.wait(timeout_sec=60)

        self.driver.stop_nodes(clean_shutdown=False)

        self.driver.stop()

