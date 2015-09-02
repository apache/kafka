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

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka import KAFKA_0_8_2_1


class KafkaVersionTest(Test):
    """Sanity checks on kafka versioning."""
    def __init__(self, test_context):
        super(KafkaVersionTest, self).__init__(test_context)

        self.topic = "topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)

    def setUp(self):
        self.zk.start()

    def test_0_8_2(self):
        """Test that we can start a single-node 0.8.2.1 cluster."""
        self.kafka = KafkaService(self.test_context, num_nodes=1, zk=self.zk,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 1}})
        self.kafka.update_version(KAFKA_0_8_2_1)
        self.kafka.start()

    def test_multi_version(self):
        """Test that we can bring up a 2-node cluster, one on version 0.8.2.1, the other on trunk."""
        self.kafka = KafkaService(self.test_context, num_nodes=2, zk=self.zk,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 2}})
        self.kafka.update_version(version=KAFKA_0_8_2_1, idx=2)
        self.kafka.start()