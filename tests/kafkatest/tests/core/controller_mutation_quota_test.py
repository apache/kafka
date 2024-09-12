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
from ducktape.mark import matrix
from ducktape.tests.test import Test

from kafkatest.services.trogdor.produce_bench_workload import ProduceBenchWorkloadService, ProduceBenchWorkloadSpec
from kafkatest.services.trogdor.consume_bench_workload import ConsumeBenchWorkloadService, ConsumeBenchWorkloadSpec
from kafkatest.services.trogdor.task_spec import TaskSpec
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.trogdor.trogdor import TrogdorService
from kafkatest.services.zookeeper import ZookeeperService

import time


class ControllerMutationQuotaTest(Test):
    """Tests throttled partition changes via the kafka-topics CLI as follows:

    1. Create a topic with N partitions (creating N partitions)
    2. Alter the number of topic partitons to 2N (adding N partitions)
    3. Delete topic (removing 2N partitions)
    3. Create topic with 1 partition

    The mutation quota should be checked at the beginning of each operation,
    so the checks are expected to be as follows assuming 1 window for the entire set of operations:

    Before step 2: alter will be allowed if quota > N
    Before step 3: delete will be allowed if quota > 2N
    Before step 4: create will be allowed if quota > 4N

    Therefore, if we want steps 1-3 to succeed and step 4 to fail we need the 2N < quota < 4N

    For example, we can achieve the desired behavior (steps 1-3 succeed, step 4 fails)
    for N = 10 partitions by setting the following configs:

    controller.quota.window.num=10
    controller.quota.window.size.seconds=200
    controller_mutation_rate=X where (20 < 2000X < 40), therefore (0.01 < X < 0.02)
    """
    def __init__(self, test_context):
        super(ControllerMutationQuotaTest, self).__init__(test_context=test_context)
        self.test_context = test_context
        self.zk = ZookeeperService(test_context, num_nodes=1) if quorum.for_test(test_context) == quorum.zk else None
        self.window_num = 10
        self.window_size_seconds = 200 # must be long enough such that all CLI commands fit into it

        self.kafka = KafkaService(self.test_context, num_nodes=1, zk=self.zk,
            server_prop_overrides=[
                ["quota.window.num", "%s" % self.window_num],
                ["controller.quota.window.size.seconds", "%s" % self.window_size_seconds]
            ],
            controller_num_nodes_override=1)

    def setUp(self):
        if self.zk:
            self.zk.start()
        self.kafka.start()

    def teardown(self):
        # Need to increase the timeout due to partition count
        self.kafka.stop()
        if self.zk:
            self.zk.stop()

    @cluster(num_nodes=2)
    @matrix(metadata_quorum=quorum.all)
    def test_controller_mutation_quota(self, metadata_quorum=quorum.zk):
        self.partition_count = 10
        mutation_rate = 3 * self.partition_count / (self.window_num * self.window_size_seconds)

        # first invoke the steps with no mutation quota to ensure it succeeds in general
        self.mutate_partitions("topic_succeed", True)

        # now apply the mutation quota configs
        node = self.kafka.nodes[0]
        alter_mutation_quota_cmd = "%s --entity-type users --entity-default --alter --add-config 'controller_mutation_rate=%f'" % \
               (self.kafka.kafka_configs_cmd_with_optional_security_settings(node, force_use_zk_connection=False), mutation_rate)
        node.account.ssh(alter_mutation_quota_cmd)

        # now invoke the same steps with the mutation quota applied to ensure it fails
        self.mutate_partitions("topic_fail", False)

    def mutate_partitions(self, topic_prefix, expected_to_succeed):
        try:
            # create the topic
            topic_name = topic_prefix
            topic_cfg = {
                "topic": topic_name,
                "partitions": self.partition_count,
                "replication-factor": 1,
            }
            self.kafka.create_topic(topic_cfg)

            # alter the partition count
            node = self.kafka.nodes[0]
            alter_topic_cmd = "%s --alter --topic %s --partitions %i" % \
                   (self.kafka.kafka_topics_cmd_with_optional_security_settings(node, force_use_zk_connection=False),
                   topic_name, 2 * self.partition_count)
            node.account.ssh(alter_topic_cmd)

            # delete the topic
            self.kafka.delete_topic(topic_name)
        except RuntimeError as e:
            fail_msg = "Failure: Unexpected Exception - %s" % str(e)
            self.logger.error(fail_msg)
            raise RuntimeError(fail_msg)

        # create a second topic with a single partition
        topic_cfg = {
            "topic": "%s_b" % topic_prefix,
            "partitions": 1,
            "replication-factor": 1,
        }
        fail_msg = ""
        try:
            self.kafka.create_topic(topic_cfg)
            if not expected_to_succeed:
                fail_msg = "Failure: Did not encounter controller mutation quota violation when one was expected"
            else:
                self.logger.info("Success: Encountered no quota violation as expected")
        except RuntimeError as e:
            if expected_to_succeed:
                fail_msg = "Failure: Unexpected Exception - %s" % str(e)
            else:
                self.logger.info("Success: Encountered quota violation exception as expected")
        if fail_msg:
            self.logger.error(fail_msg)
            raise RuntimeError(fail_msg)