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
from kafkatest.services.kafka import KafkaService, quorum


class KafkaTest(Test):
    """
    Helper class that manages setting up a Kafka cluster. Use this if the
    default settings for Kafka are sufficient for your test; any customization
    needs to be done manually. Your run() method should call tearDown and
    setUp. The Zookeeper and Kafka services are available as the fields
    KafkaTest.zk and KafkaTest.kafka.
    """
    def __init__(self, test_context, num_zk, num_brokers, topics=None):
        super(KafkaTest, self).__init__(test_context)
        self.num_zk = num_zk
        self.num_brokers = num_brokers
        self.topics = topics

        self.zk = ZookeeperService(test_context, self.num_zk) if quorum.for_test(test_context) == quorum.zk else None

        self.kafka = KafkaService(
            test_context, self.num_brokers,
            self.zk, topics=self.topics,
            controller_num_nodes_override=self.num_zk)

    def setUp(self):
        if self.zk:
            self.zk.start()
        self.kafka.start()