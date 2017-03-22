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
from ducktape.mark import matrix
from ducktape.mark import parametrize
from kafkatest.services.kafka import KafkaService
from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsSmokeTestJobRunnerService
import time
import signal
from random import randint

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

def hard_shutdown(test, topic, broker_type):
    """Discover broker node of requested type and shut it down with a hard kill."""
    node = broker_node(test, topic, broker_type)
    test.kafka.signal_node(node, sig=signal.SIGKILL)

    
failures = {
    "clean_shutdown": clean_shutdown,
    "hard_shutdown": hard_shutdown
}
        
class StreamsBrokerBounceTest(Test):
    """
    Simple test of Kafka Streams with brokers failing
    """

    def __init__(self, test_context):
        super(StreamsBrokerBounceTest, self).__init__(test_context)
        self.replication = 3
        self.partitions = 3
        self.topics = {
            'echo' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": 2}},
            'data' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": 2} },
            'min' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'max' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'sum' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'dif' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'cnt' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'avg' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'wcnt' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": 2} },
            'tagg' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": 2} }
        }

    @cluster(num_nodes=7)
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown"],
            broker_type=["leader", "controller"],
            sleep_time_secs=[0, 15])
    def test_broker_bounce(self, failure_mode, broker_type, sleep_time_secs):
        """
        Start a smoke test client, then kill a few brokers and ensure data is still received
        Ensure that all records are delivered.
        """

        # Setup phase
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()
        
        self.kafka = KafkaService(self.test_context, num_nodes=self.replication,
                                  zk=self.zk, topics=self.topics)
        self.kafka.start()
        

        # Start test harness
        self.driver = StreamsSmokeTestDriverService(self.test_context, self.kafka)
        self.processor1 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka)

        
        self.driver.start()
        self.processor1.start()

        # Sleep to allow test to run for a bit
        time.sleep(sleep_time_secs)

        # Pick a random topic and bounce it's leader
        topic_index = randint(0, len(self.topics.keys()) - 1)
        topic = self.topics.keys()[topic_index]
        failures[failure_mode](self, topic, broker_type)

        # End test
        self.driver.wait()
        self.driver.stop()

        self.processor1.stop()

        node = self.driver.node
        
        # Capture all three options for now, since for the purposes of this test, we are only testing Kafka Streams
        # being robust in face of failure, not Kafka's data loss or duplicates.
        output = node.account.ssh_capture("grep -E 'ALL-RECORDS-DELIVERED|PROCESSED-MORE-THAN-GENERATED|PROCESSED-LESS-THAN-GENERATED' %s" % self.driver.STDOUT_FILE, allow_fail=False)

        data = {}
        for line in output:
            data["result"] = line
        return data
