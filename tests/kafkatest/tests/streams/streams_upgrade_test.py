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
from ducktape.mark import parametrize
from kafkatest.services.kafka import KafkaService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsSmokeTestJobRunnerService
from kafkatest.version import LATEST_0_10_1, LATEST_0_10_2, DEV_BRANCH, KafkaVersion
import time


class StreamsUpgradeTest(Test):
    """
    Simple test of Kafka Streams.
    """

    def __init__(self, test_context):
        super(StreamsUpgradeTest, self).__init__(test_context)
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
        

    def perform_upgrade(self, to_version):
        self.logger.info("First pass bounce - rolling upgrade")
        node = self.processor1.node
        self.processor1.stop()
        node.version = KafkaVersion(to_version)
        self.processor1.start()
        
    @cluster(num_nodes=6)
    @parametrize(from_version=str(DEV_BRANCH), to_version=str(LATEST_0_10_1))
    @parametrize(from_version=str(LATEST_0_10_2), to_version=str(DEV_BRANCH))
    def test_upgrade(self, from_version, to_version):
        """
        Start a smoke test client, then abort (kill -9) and restart it a few times.
        Ensure that all records are delivered.
        """
        # Setup phase
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()
        
        self.kafka = KafkaService(self.test_context, num_nodes=self.replication,
                                  zk=self.zk, version=KafkaVersion(from_version), topics=self.topics)
        self.kafka.start()
        
        self.driver = StreamsSmokeTestDriverService(self.test_context, self.kafka)
        self.processor1 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka)

        
        self.driver.start()
        self.processor1.start()
        time.sleep(15)

        self.perform_upgrade(to_version)

        time.sleep(15)
        self.driver.wait()
        self.driver.stop()

        self.processor1.stop()

        node = self.driver.node
        node.account.ssh("grep ALL-RECORDS-DELIVERED %s" % self.driver.STDOUT_FILE, allow_fail=False)
