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
from ducktape.mark import parametrize, ignore
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

def signal_node(test, node, sig):
    test.kafka.signal_node(node, sig)
    
def clean_shutdown(test, topic, broker_type):
    """Discover broker node of requested type and shut it down cleanly.
    """
    node = broker_node(test, topic, broker_type)
    signal_node(test, node, signal.SIGTERM)

def hard_shutdown(test, topic, broker_type):
    """Discover broker node of requested type and shut it down with a hard kill."""
    node = broker_node(test, topic, broker_type)
    signal_node(test, node, signal.SIGKILL)

    
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

    def fail_broker_type(self, failure_mode, broker_type):
        # Pick a random topic and bounce it's leader
        topic_index = randint(0, len(self.topics.keys()) - 1)
        topic = self.topics.keys()[topic_index]
        failures[failure_mode](self, topic, broker_type)

    def fail_many_brokers(self, failure_mode, num_failures):
        sig = signal.SIGTERM
        if (failure_mode == "clean_shutdown"):
            sig = signal.SIGTERM
        else:
            sig = signal.SIGKILL
            
        for num in range(0, num_failures - 1):
            signal_node(self, self.kafka.nodes[num], sig)

        
    def setup_system(self):
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

    def collect_results(self):
         # End test
        self.driver.wait()
        self.driver.stop()

        self.processor1.stop()

        node = self.driver.node
        
        # Success is declared if all records are processed or if there are duplicates, but not if data is lost
        # We have set up the test so that data is not lost
        output = node.account.ssh_capture("grep -E 'ALL-RECORDS-DELIVERED|PROCESSED-MORE-THAN-GENERATED' %s" % self.driver.STDOUT_FILE, allow_fail=False)

        data = {}
        for line in output:
            data["result"] = line
        return data

    @cluster(num_nodes=7)
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown"],
            broker_type=["leader", "controller"],
            sleep_time_secs=[0, 15])
    def test_broker_type_bounce(self, failure_mode, broker_type, sleep_time_secs):
        """
        Start a smoke test client, then kill a particular broker and ensure data is still received
        Record if records are delivered
        """
        self.setup_system() 

        # Sleep to allow test to run for a bit
        time.sleep(sleep_time_secs)

        # Fail brokers
        self.fail_broker_type(failure_mode, broker_type);

        return self.collect_results()

    
    @cluster(num_nodes=7)
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown"],
            num_failures=[2])
    def test_many_brokers_bounce(self, failure_mode, num_failures):
        """
        Start a smoke test client, then kill a few brokers and ensure data is still received
        Record if records are delivered
        """
        self.setup_system() 

        # Sleep to allow test to run for a bit
        time.sleep(15)

        # Fail brokers
        self.fail_many_brokers(failure_mode, num_failures);

        return self.collect_results()
