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

import errno
import time
from random import randint

from ducktape.mark import matrix, parametrize
from ducktape.mark.resource import cluster
from ducktape.tests.test import TestContext

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService, quorum
from ducktape.tests.test import Test
from kafkatest.version import DEV_BRANCH, LATEST_0_10_0, LATEST_0_10_1, LATEST_0_10_2, LATEST_0_11_0, LATEST_1_0, \
    LATEST_1_1, LATEST_2_0, LATEST_2_1, LATEST_2_2, LATEST_2_3, LATEST_2_4, LATEST_2_5, LATEST_2_6, LATEST_2_7, \
    LATEST_2_8, LATEST_3_0, LATEST_3_1, LATEST_3_2, LATEST_3_3, LATEST_3_4, LATEST_3_5, LATEST_3_6, LATEST_3_7, \
    V_0_11_0_0, V_0_10_1_0, KafkaVersion

def get_broker_features(broker_version):
    features = {}
    if broker_version < V_0_10_1_0:
        features["create-topics-supported"] = False
        features["offsets-for-times-supported"] = False
        features["cluster-id-supported"] = False
        features["expect-record-too-large-exception"] = True
    else:
        features["create-topics-supported"] = True
        features["offsets-for-times-supported"] = True
        features["cluster-id-supported"] = True
        features["expect-record-too-large-exception"] = False
    if broker_version < V_0_11_0_0:
        features["describe-acls-supported"] = False
        features["describe-configs-supported"] = False
        features["idempotent-producer-supported"] = False
    else:
        features["describe-acls-supported"] = True
        features["describe-configs-supported"] = True
        features["idempotent-producer-supported"] = True
    return features

def run_command(node, cmd, ssh_log_file):
    with open(ssh_log_file, 'w') as f:
        f.write("Running %s\n" % cmd)
        try:
            for line in node.account.ssh_capture(cmd):
                f.write(line)
        except Exception as e:
            f.write("** Command failed!")
            print(e, flush=True)
            raise


class ClientCompatibilityFeaturesTest(Test):
    """
    Tests clients for the presence or absence of specific features when communicating with brokers with various
    versions. Relies on ClientCompatibilityTest.java for much of the functionality.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ClientCompatibilityFeaturesTest, self).__init__(test_context=test_context)

        self.zk = ZookeeperService(test_context, num_nodes=3) if quorum.for_test(test_context) == quorum.zk else None

        # Generate a unique topic name
        topic_name = "client_compat_features_topic_%d%d" % (int(time.time()), randint(0, 2147483647))
        self.topics = { topic_name: {
            "partitions": 1, # Use only one partition to avoid worrying about ordering
            "replication-factor": 3
            }}
        self.kafka = KafkaService(test_context, num_nodes=3, zk=self.zk, topics=self.topics)
        # Always use the latest version of org.apache.kafka.tools.ClientCompatibilityTest
        # so store away the path to the DEV version before we set the Kafka version
        self.dev_script_path = self.kafka.path.script("kafka-run-class.sh", self.kafka.nodes[0])

    def invoke_compatibility_program(self, features):
        # Run the compatibility test on the first Kafka node.
        node = self.kafka.nodes[0]
        cmd = ("%s org.apache.kafka.tools.ClientCompatibilityTest "
               "--bootstrap-server %s "
               "--num-cluster-nodes %d "
               "--topic %s " % (self.dev_script_path,
                               self.kafka.bootstrap_servers(),
                               len(self.kafka.nodes),
                               list(self.topics.keys())[0]))
        for k, v in features.items():
            cmd = cmd + ("--%s %s " % (k, v))
        results_dir = TestContext.results_dir(self.test_context, 0)
        try:
            os.makedirs(results_dir)
        except OSError as e:
            if e.errno == errno.EEXIST and os.path.isdir(results_dir):
                pass
            else:
                raise
        ssh_log_file = "%s/%s" % (results_dir, "client_compatibility_test_output.txt")
        try:
          self.logger.info("Running %s" % cmd)
          run_command(node, cmd, ssh_log_file)
        except Exception as e:
          self.logger.info("** Command failed.  See %s for log messages." % ssh_log_file)
          raise

    @cluster(num_nodes=7)
    @matrix(broker_version=[str(DEV_BRANCH)], metadata_quorum=quorum.all_non_upgrade)
    @parametrize(broker_version=str(LATEST_0_10_0))
    @parametrize(broker_version=str(LATEST_0_10_1))
    @parametrize(broker_version=str(LATEST_0_10_2))
    @parametrize(broker_version=str(LATEST_0_11_0))
    @parametrize(broker_version=str(LATEST_1_0))
    @parametrize(broker_version=str(LATEST_1_1))
    @parametrize(broker_version=str(LATEST_2_0))
    @parametrize(broker_version=str(LATEST_2_1))
    @parametrize(broker_version=str(LATEST_2_2))
    @parametrize(broker_version=str(LATEST_2_3))
    @parametrize(broker_version=str(LATEST_2_4))
    @parametrize(broker_version=str(LATEST_2_5))
    @parametrize(broker_version=str(LATEST_2_6))
    @parametrize(broker_version=str(LATEST_2_7))
    @parametrize(broker_version=str(LATEST_2_8))
    @parametrize(broker_version=str(LATEST_3_0))
    @parametrize(broker_version=str(LATEST_3_1))
    @parametrize(broker_version=str(LATEST_3_2))
    @parametrize(broker_version=str(LATEST_3_3))
    @parametrize(broker_version=str(LATEST_3_4))
    @parametrize(broker_version=str(LATEST_3_5))
    @parametrize(broker_version=str(LATEST_3_6))
    @parametrize(broker_version=str(LATEST_3_7))
    def run_compatibility_test(self, broker_version, metadata_quorum=quorum.zk):
        if self.zk:
            self.zk.start()
        self.kafka.set_version(KafkaVersion(broker_version))
        self.kafka.start()
        features = get_broker_features(broker_version)
        self.invoke_compatibility_program(features)
