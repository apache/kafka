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
from ducktape.mark.resource import cluster
from kafkatest.services.kafka import KafkaService
from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsSmokeTestJobRunnerService
import time
import signal

def broker_node(test, topic, broker_type):
    """ Discover node of requested type. For leader type, discovers leader for our topic and partition 0
    """
    if broker_type == "leader":
        node = test.kafka.leader(topic, partition=0)
    elif broker_type == "controller":
        node = test.kafka.controller()
    else:
        raise Exception("Unexpected broker type %s." % (broker_type))

    return node

def clean_shutdown(test, topic, broker_type):
    """Discover broker node of requested type and shut it down cleanly.
    """
    node = broker_node(test, topic, broker_type)
    test.kafka.signal_node(node, sig=signal.SIGTERM)

        
class StreamsBrokerBounceTest(Test):
    """
    Simple test of Kafka Streams with brokers failing
    """

    def __init__(self, test_context):
        super(StreamsBrokerBounceTest, self).__init__(test_context)
        self.topics = {
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
        }

    @cluster(num_nodes=5)
    def test_broker_bounce(self):
        """
        Start a smoke test client, then kill a few brokers and ensure data is still received
        Ensure that all records are delivered.
        """

        #############
        # SETUP PHASE
        #############
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()
        
        self.kafka = KafkaService(self.test_context, num_nodes=2, zk=self.zk, topics=self.topics)
        self.kafka.start()
        

        self.driver = StreamsSmokeTestDriverService(self.test_context, self.kafka)
        self.processor1 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka)

        
        self.driver.start()
        self.processor1.start()

        for topic in self.topics.keys():
            clean_shutdown(self, topic, "leader")


        self.driver.wait()
        self.driver.stop()

        self.processor1.stop()

        node = self.driver.node
        node.account.ssh("grep ALL-RECORDS-DELIVERED %s" % self.driver.STDOUT_FILE, allow_fail=False)
