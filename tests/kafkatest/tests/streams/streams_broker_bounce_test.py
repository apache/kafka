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

from ducktape.utils.util import wait_until
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

def clean_bounce(test, topic, broker_type):
    """Chase the leader of one partition and restart it cleanly a few times (5 times)."""
    for i in range(5):
        prev_broker_node = broker_node(test, topic, broker_type)
        test.kafka.restart_node(prev_broker_node, clean_shutdown=True)


def hard_bounce(test, topic, broker_type):
    """Chase the leader and restart it with a hard kill. Do this a few times (5)."""
    for i in range(5):
        prev_broker_node = broker_node(test, topic, broker_type)
        test.kafka.signal_node(prev_broker_node, sig=signal.SIGKILL)

        # Since this is a hard kill, we need to make sure the process is down and that
        # zookeeper has registered the loss by expiring the broker's session timeout.

        wait_until(lambda: len(test.kafka.pids(prev_broker_node)) == 0 and not test.kafka.is_registered(prev_broker_node),
                   timeout_sec=test.kafka.zk_session_timeout + 5,
                   err_msg="Failed to see timely deregistration of hard-killed broker %s" % str(prev_broker_node.account))

        test.kafka.start_node(prev_broker_node)
    

    
failures = {
    "clean_shutdown": clean_shutdown,
    "hard_shutdown": hard_shutdown,
    "clean_bounce": clean_bounce,
    "hard_bounce": hard_bounce
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
                       'configs': {"min.insync.replicas": 2} },
            '__consumer_offsets' : { 'partitions': 50, 'replication-factor': self.replication,
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

    def confirm_topics_on_all_brokers(self, expected_topic_set):
        for node in self.kafka.nodes:
            match_count = 0
            # need to iterate over topic_list_generator as kafka.list_topics()
            # returns a python generator so values are fetched lazily
            # so we can't just compare directly we must iterate over what's returned
            topic_list_generator = self.kafka.list_topics(node=node)
            for topic in topic_list_generator:
                if topic in expected_topic_set:
                    match_count += 1

            if len(expected_topic_set) != match_count:
                return False

        return True

        
    def setup_system(self, start_processor=True):
        # Setup phase
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()

        self.kafka = KafkaService(self.test_context, num_nodes=self.replication, zk=self.zk, topics=self.topics)
        self.kafka.start()

        # allow some time for topics to be created
        wait_until(lambda: self.confirm_topics_on_all_brokers(set(self.topics.keys())),
                   timeout_sec=60,
                   err_msg="Broker did not create all topics in 60 seconds ")

        # Start test harness
        self.driver = StreamsSmokeTestDriverService(self.test_context, self.kafka)
        self.processor1 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka)

        self.driver.start()

        if (start_processor):
           self.processor1.start()

    def collect_results(self, sleep_time_secs):
        data = {}
        # End test
        self.driver.wait()
        self.driver.stop()

        self.processor1.stop()

        node = self.driver.node
        
        # Success is declared if streams does not crash when sleep time > 0
        # It should give an exception when sleep time is 0 since we kill the brokers immediately
        # and the topic manager cannot create internal topics with the desired replication factor
        if (sleep_time_secs == 0):
            output_streams = self.processor1.node.account.ssh_capture("grep SMOKE-TEST-CLIENT-EXCEPTION %s" % self.processor1.STDOUT_FILE, allow_fail=False)
        else:
            output_streams = self.processor1.node.account.ssh_capture("grep SMOKE-TEST-CLIENT-CLOSED %s" % self.processor1.STDOUT_FILE, allow_fail=False)
            
        for line in output_streams:
            data["Client closed"] = line

        # Currently it is hard to guarantee anything about Kafka since we don't have exactly once.
        # With exactly once in place, success will be defined as ALL-RECORDS-DELIEVERD and SUCCESS
        output = node.account.ssh_capture("grep -E 'ALL-RECORDS-DELIVERED|PROCESSED-MORE-THAN-GENERATED|PROCESSED-LESS-THAN-GENERATED' %s" % self.driver.STDOUT_FILE, allow_fail=False)
        for line in output:
            data["Records Delivered"] = line
        output = node.account.ssh_capture("grep -E 'SUCCESS|FAILURE' %s" % self.driver.STDOUT_FILE, allow_fail=False)
        for line in output:
            data["Logic Success/Failure"] = line
            
        
        return data

    @cluster(num_nodes=7)
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown", "clean_bounce", "hard_bounce"],
            broker_type=["leader", "controller"],
            sleep_time_secs=[120])
    def test_broker_type_bounce(self, failure_mode, broker_type, sleep_time_secs):
        """
        Start a smoke test client, then kill one particular broker and ensure data is still received
        Record if records are delivered. 
        """
        self.setup_system() 

        # Sleep to allow test to run for a bit
        time.sleep(sleep_time_secs)

        # Fail brokers
        self.fail_broker_type(failure_mode, broker_type)

        return self.collect_results(sleep_time_secs)

    @ignore
    @cluster(num_nodes=7)
    @matrix(failure_mode=["clean_shutdown"],
            broker_type=["controller"],
            sleep_time_secs=[0])
    def test_broker_type_bounce_at_start(self, failure_mode, broker_type, sleep_time_secs):
        """
        Start a smoke test client, then kill one particular broker immediately before streams stats
        Streams should throw an exception since it cannot create topics with the desired
        replication factor of 3
        """
        self.setup_system(start_processor=False)

        # Sleep to allow test to run for a bit
        time.sleep(sleep_time_secs)

        # Fail brokers
        self.fail_broker_type(failure_mode, broker_type)

        self.processor1.start()

        return self.collect_results(sleep_time_secs)

    @cluster(num_nodes=7)
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown", "clean_bounce", "hard_bounce"],
            num_failures=[2])
    def test_many_brokers_bounce(self, failure_mode, num_failures):
        """
        Start a smoke test client, then kill a few brokers and ensure data is still received
        Record if records are delivered
        """
        self.setup_system() 

        # Sleep to allow test to run for a bit
        time.sleep(120)

        # Fail brokers
        self.fail_many_brokers(failure_mode, num_failures)

        return self.collect_results(120)

    @cluster(num_nodes=7)
    @matrix(failure_mode=["clean_bounce", "hard_bounce"],
            num_failures=[3])
    def test_all_brokers_bounce(self, failure_mode, num_failures):
        """
        Start a smoke test client, then kill a few brokers and ensure data is still received
        Record if records are delivered
        """
        self.setup_system() 

        # Sleep to allow test to run for a bit
        time.sleep(120)

        # Fail brokers
        self.fail_many_brokers(failure_mode, num_failures)

        return self.collect_results(120)
