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

import os
import time
from random import randint

from ducktape.mark import parametrize
from ducktape.tests.test import TestContext

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from ducktape.tests.test import Test
from kafkatest.version import DEV_BRANCH, LATEST_0_10_0, LATEST_0_10_1, V_0_10_1_0, KafkaVersion

def get_broker_features(broker_version):
    features = {}
    if (broker_version < V_0_10_1_0):
        features["offsets-for-times-supported"] = False
        features["cluster-id-supported"] = False
        features["expect-record-too-large-exception"] = True
    else:
        features["offsets-for-times-supported"] = True
        features["cluster-id-supported"] = True
        features["expect-record-too-large-exception"] = False
    return features

def run_command(node, cmd, ssh_log_file):
    with open(ssh_log_file, 'w') as f:
        f.write("Running %s\n" % cmd)
        try:
            for line in node.account.ssh_capture(cmd):
                f.write(line)
        except Exception as e:
            f.write("** Command failed!")
            print e
            raise e


class ClientCompatibilityFeaturesTest(Test):
    """
    Tests clients for the presence or absence of specific features when communicating with brokers with various
    versions. Relies on ClientCompatibilityTest.java for much of the functionality.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ClientCompatibilityFeaturesTest, self).__init__(test_context=test_context)

        self.zk = ZookeeperService(test_context, num_nodes=3)

        # Generate a unique topic name
        topic_name = "client_compat_features_topic_%d%d" % (int(time.time()), randint(0, 2147483647))
        self.topics = { topic_name: {
            "partitions": 1, # Use only one partition to avoid worrying about ordering
            "replication-factor": 3
            }}
        self.kafka = KafkaService(test_context, num_nodes=3, zk=self.zk, topics=self.topics)

    def invoke_compatibility_program(self, features):
        # Run the compatibility test on the first Kafka node.
        node = self.zk.nodes[0]
        cmd = ("%s org.apache.kafka.tools.ClientCompatibilityTest "
               "--bootstrap-server %s "
               "--offsets-for-times-supported %s "
               "--cluster-id-supported %s "
               "--expect-record-too-large-exception %s "
               "--topic %s " % (self.zk.path.script("kafka-run-class.sh", node),
                               self.kafka.bootstrap_servers(),
                               features["offsets-for-times-supported"],
                               features["cluster-id-supported"],
                               features["expect-record-too-large-exception"],
                               self.topics.keys()[0]))
        results_dir = TestContext.results_dir(self.test_context, 0)
        os.makedirs(results_dir)
        ssh_log_file = "%s/%s" % (results_dir, "client_compatibility_test_output.txt")
        try:
          self.logger.info("Running %s" % cmd)
          run_command(node, cmd, ssh_log_file)
        except Exception as e:
          self.logger.info("** Command failed.  See %s for log messages." % ssh_log_file)
          raise e

    @parametrize(broker_version=str(DEV_BRANCH))
    @parametrize(broker_version=str(LATEST_0_10_0))
    @parametrize(broker_version=str(LATEST_0_10_1))
    def run_compatibility_test(self, broker_version):
        self.zk.start()
        self.kafka.set_version(KafkaVersion(broker_version))
        self.kafka.start()
        features = get_broker_features(broker_version)
        self.invoke_compatibility_program(features)
