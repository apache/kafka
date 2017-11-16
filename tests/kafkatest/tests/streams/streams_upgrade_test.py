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
from ducktape.tests.test import Test
from ducktape.mark import parametrize, ignore
from kafkatest.services.kafka import KafkaService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsSmokeTestJobRunnerService
from kafkatest.version import LATEST_0_10_1, LATEST_0_10_2, LATEST_0_11_0, DEV_BRANCH, KafkaVersion
import time


class StreamsUpgradeTest(Test):
    """
    Tests rolling upgrades and downgrades of the Kafka Streams library.
    """

    def __init__(self, test_context):
        super(StreamsUpgradeTest, self).__init__(test_context)
        self.replication = 3
        self.partitions = 1
        self.isr = 2
        self.topics = {
            'echo' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": self.isr}},
            'data' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": self.isr} },
            'min' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": self.isr} },
            'max' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": self.isr} },
            'sum' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": self.isr} },
            'dif' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": self.isr} },
            'cnt' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": self.isr} },
            'avg' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": self.isr} },
            'wcnt' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": self.isr} },
            'tagg' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": self.isr} }
        }
        

    def perform_streams_upgrade(self, to_version):
        self.logger.info("First pass bounce - rolling streams upgrade")

        # get the node running the streams app
        node = self.processor1.node
        self.processor1.stop()

        # change it's version. This will automatically make it pick up a different
        # JAR when it starts again
        node.version = KafkaVersion(to_version)
        self.processor1.start()

    def perform_broker_upgrade(self, to_version):
        self.logger.info("First pass bounce - rolling broker upgrade")
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            node.version = KafkaVersion(to_version)
            self.kafka.start_node(node)


    @cluster(num_nodes=6)
    @parametrize(from_version=str(LATEST_0_10_1), to_version=str(DEV_BRANCH))
    @parametrize(from_version=str(LATEST_0_10_2), to_version=str(DEV_BRANCH))
    @parametrize(from_version=str(LATEST_0_10_1), to_version=str(LATEST_0_11_0))
    @parametrize(from_version=str(LATEST_0_10_2), to_version=str(LATEST_0_11_0))
    @parametrize(from_version=str(LATEST_0_11_0), to_version=str(LATEST_0_10_2))
    @parametrize(from_version=str(DEV_BRANCH), to_version=str(LATEST_0_10_2))
    def test_upgrade_downgrade_streams(self, from_version, to_version):
        """
        Start a smoke test client, then abort (kill -9) and restart it a few times.
        Ensure that all records are delivered.

        Note, that just like tests/core/upgrade_test.py, a prerequisite for this test to succeed
        if the inclusion of all parametrized versions of kafka in kafka/vagrant/base.sh 
        (search for get_kafka()). For streams in particular, that means that someone has manually
        copies the kafka-stream-$version-test.jar in the right S3 bucket as shown in base.sh.
        """
        # Setup phase
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()

        # number of nodes needs to be >= 3 for the smoke test
        self.kafka = KafkaService(self.test_context, num_nodes=3,
                                  zk=self.zk, version=KafkaVersion(from_version), topics=self.topics)
        self.kafka.start()
        
        # allow some time for topics to be created
        time.sleep(10)
        
        self.driver = StreamsSmokeTestDriverService(self.test_context, self.kafka)
        self.processor1 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka)

        
        self.driver.start()
        self.processor1.start()
        time.sleep(15)

        self.perform_streams_upgrade(to_version)

        time.sleep(15)
        self.driver.wait()
        self.driver.stop()

        self.processor1.stop()

        node = self.driver.node
        node.account.ssh("grep ALL-RECORDS-DELIVERED %s" % self.driver.STDOUT_FILE, allow_fail=False)
        self.processor1.node.account.ssh_capture("grep SMOKE-TEST-CLIENT-CLOSED %s" % self.processor1.STDOUT_FILE, allow_fail=False)



    @cluster(num_nodes=6)
    @parametrize(from_version=str(LATEST_0_10_2), to_version=str(DEV_BRANCH))
    def test_upgrade_brokers(self, from_version, to_version):
        """
        Start a smoke test client then perform rolling upgrades on the broker. 
        """
        # Setup phase
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()

        # number of nodes needs to be >= 3 for the smoke test
        self.kafka = KafkaService(self.test_context, num_nodes=3,
                                  zk=self.zk, version=KafkaVersion(from_version), topics=self.topics)
        self.kafka.start()
        
        # allow some time for topics to be created
        time.sleep(10)
        
        self.driver = StreamsSmokeTestDriverService(self.test_context, self.kafka)
        self.processor1 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka)

        
        self.driver.start()
        self.processor1.start()
        time.sleep(15)

        self.perform_broker_upgrade(to_version)

        time.sleep(15)
        self.driver.wait()
        self.driver.stop()

        self.processor1.stop()

        node = self.driver.node
        node.account.ssh("grep ALL-RECORDS-DELIVERED %s" % self.driver.STDOUT_FILE, allow_fail=False)
        self.processor1.node.account.ssh_capture("grep SMOKE-TEST-CLIENT-CLOSED %s" % self.processor1.STDOUT_FILE, allow_fail=False)
