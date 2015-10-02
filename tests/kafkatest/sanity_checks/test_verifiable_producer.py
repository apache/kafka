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
from ducktape.utils.util import wait_until

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka.version import LATEST_0_8_2
from kafkatest.services.verifiable_producer import VerifiableProducer


class TestVerifiableProducer(Test):
    """Sanity checks on verifiable producer service class."""
    def __init__(self, test_context):
        super(TestVerifiableProducer, self).__init__(test_context)

        self.topic = "topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context, num_nodes=1, zk=self.zk,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 1}})

        self.num_messages = 1000
        # This will produce to source kafka cluster
        self.producer = VerifiableProducer(test_context, num_nodes=1, kafka=self.kafka, topic=self.topic,
                                           max_messages=self.num_messages, throughput=1000)

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    def test_version(self):
        """Test that we can start VerifiableProducer using 0.8.2 jar, and validate that a small number of
        messages can be produced/acknowledged.
        """
        self.producer.nodes[0].version = LATEST_0_8_2
        self.producer.start()
        wait_until(lambda: self.producer.num_acked > 5, timeout_sec=5,
             err_msg="Producer failed to start in a reasonable amount of time.")
        self.producer.wait()

        num_produced = self.producer.num_acked
        assert num_produced == self.num_messages, "num_produced: %d, num_messages: %d" % (num_produced, self.num_messages)

    def test_normal_operation(self):
        """Test startup of single-node VerifiableProducer, and validate that a small number of messages can
        be produced/acknowledged.
        """
        self.producer.start()
        wait_until(lambda: self.producer.num_acked > 5, timeout_sec=5,
             err_msg="Producer failed to start in a reasonable amount of time.")
        self.producer.wait()

        num_produced = self.producer.num_acked
        assert num_produced == self.num_messages, "num_produced: %d, num_messages: %d" % (num_produced, self.num_messages)


