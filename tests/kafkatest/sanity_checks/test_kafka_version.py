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
from kafkatest.services.kafka import KafkaService, property
from kafkatest.services.kafka.version import LATEST_0_8_2

import re


def kafka_jar_versions(proc_string):
    """Return all kafka versions explicitly in the process classpath"""
    versions = re.findall("kafka_[0-9\.]+-([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)", proc_string)
    return set(versions)


def is_kafka_version(node, version):
    """Heuristic to check that only the specified version appears in the classpath of the kafka process"""
    lines = [l for l in node.account.ssh_capture("ps ax | grep kafka.properties | grep -v grep")]
    assert len(lines) == 1

    return kafka_jar_versions(lines[0]) == {str(version)}


class KafkaVersionTest(Test):
    """Sanity checks on kafka versioning."""
    def __init__(self, test_context):
        super(KafkaVersionTest, self).__init__(test_context)

        self.topic = "topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)

    def setUp(self):
        self.zk.start()

    def test_0_8_2(self):
        """Test kafka service node-versioning api - verify that we can bring up a single-node 0.8.2.X cluster."""
        self.kafka = KafkaService(self.test_context, num_nodes=1, zk=self.zk,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 1}})
        node = self.kafka.nodes[0]
        node.version = LATEST_0_8_2
        self.kafka.start()

        assert is_kafka_version(node, LATEST_0_8_2)

    def test_multi_version(self):
        """Test kafka service node-versioning api - ensure we can bring up a 2-node cluster, one on version 0.8.2.X,
        the other on trunk."""
        self.kafka = KafkaService(self.test_context, num_nodes=2, zk=self.zk,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 2}})
        self.kafka.nodes[1].version = LATEST_0_8_2
        self.kafka.nodes[1].config[property.INTER_BROKER_PROTOCOL_VERSION] = "0.8.2.X"
        self.kafka.start()

        assert not is_kafka_version(self.kafka.nodes[0], LATEST_0_8_2)
        assert is_kafka_version(self.kafka.nodes[1], LATEST_0_8_2)
