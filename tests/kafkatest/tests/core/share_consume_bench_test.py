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

import json
from ducktape.utils.util import wait_until
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.trogdor.produce_bench_workload import ProduceBenchWorkloadService, ProduceBenchWorkloadSpec
from kafkatest.services.trogdor.share_consume_bench_workload import ShareConsumeBenchWorkloadService, ShareConsumeBenchWorkloadSpec
from kafkatest.services.trogdor.task_spec import TaskSpec
from kafkatest.services.trogdor.trogdor import TrogdorService


class ShareConsumeBenchTest(Test):
    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ShareConsumeBenchTest, self).__init__(test_context)
        self.kafka = KafkaService(test_context, num_nodes=3, zk=None)
        self.producer_workload_service = ProduceBenchWorkloadService(test_context, self.kafka)
        self.share_consumer_workload_service = ShareConsumeBenchWorkloadService(test_context, self.kafka)
        self.topics_with_multiple_partitions = {"share_consume_bench_topic[0-5]": {"numPartitions": 5, "replicationFactor": 3}}
        self.topic_with_single_partitions = {"share_consume_bench_topic6": {"numPartitions": 1, "replicationFactor": 3}}
        self.trogdor = TrogdorService(context=self.test_context,
                                      client_services=[self.kafka, self.producer_workload_service,
                                                       self.share_consumer_workload_service])
        self.share_group="share-group"

    def setUp(self):
        self.trogdor.start()
        self.kafka.start()

    def teardown(self):
        self.trogdor.stop()
        self.kafka.stop()

    def produce_messages(self, topics, max_messages=10000):
        produce_spec = ProduceBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                self.producer_workload_service.producer_node,
                                                self.producer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=1000,
                                                max_messages=max_messages,
                                                producer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                inactive_topics={},
                                                active_topics=topics)
        produce_workload = self.trogdor.create_task("produce_workload", produce_spec)
        produce_workload.wait_for_done(timeout_sec=180)
        self.logger.debug("Produce workload finished")

    @cluster(num_nodes=10)
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        use_share_groups=[True],
    )
    def test_share_consume_bench(self, metadata_quorum, use_share_groups=True):
        """
        Runs a ShareConsumeBench workload to consume messages
        """
        self.produce_messages(self.topics_with_multiple_partitions)
        share_consume_spec = ShareConsumeBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                self.share_consumer_workload_service.share_consumer_node,
                                                self.share_consumer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=1000,
                                                max_messages=10000,
                                                consumer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                active_topics=["share_consume_bench_topic[0-5]"],
                                                share_group=self.share_group)
        wait_until(lambda: self.kafka.set_share_group_offset_reset_strategy(group=self.share_group, strategy="earliest"),
                   timeout_sec=20, backoff_sec=2, err_msg="share.auto.offset.reset not set to earliest")
        share_consume_workload = self.trogdor.create_task("share_consume_workload", share_consume_spec)
        share_consume_workload.wait_for_done(timeout_sec=360)
        self.logger.debug("Share consume workload finished")
        tasks = self.trogdor.tasks()
        self.logger.info("TASKS: %s\n" % json.dumps(tasks, sort_keys=True, indent=2))

    @cluster(num_nodes=10)
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        use_share_groups=[True],
    )
    def test_two_share_consumers_in_a_group_topics(self, metadata_quorum, use_share_groups=True):
        """
        Runs two share consumers in the same share group to read messages from topics.
        """
        self.produce_messages(self.topics_with_multiple_partitions)
        share_consume_spec = ShareConsumeBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                self.share_consumer_workload_service.share_consumer_node,
                                                self.share_consumer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=1000,
                                                max_messages=2000, # both should read at least 2k messages
                                                consumer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                threads_per_worker=2,
                                                active_topics=["share_consume_bench_topic[0-5]"],
                                                share_group=self.share_group)
        wait_until(lambda: self.kafka.set_share_group_offset_reset_strategy(group=self.share_group, strategy="earliest"),
                   timeout_sec=20, backoff_sec=2, err_msg="share.auto.offset.reset not set to earliest")
        share_consume_workload = self.trogdor.create_task("share_consume_workload", share_consume_spec)
        share_consume_workload.wait_for_done(timeout_sec=360)
        self.logger.debug("Share consume workload finished")
        tasks = self.trogdor.tasks()
        self.logger.info("TASKS: %s\n" % json.dumps(tasks, sort_keys=True, indent=2))

    @cluster(num_nodes=10)
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        use_share_groups=[True],
    )
    def test_one_share_consumer_subscribed_to_single_topic(self, metadata_quorum, use_share_groups=True):
        """
        Runs one share consumers in a share group to read messages from topic with single partition.
        """
        self.produce_messages(self.topic_with_single_partitions)
        share_consume_spec = ShareConsumeBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                self.share_consumer_workload_service.share_consumer_node,
                                                self.share_consumer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=1000,
                                                max_messages=10000,
                                                consumer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                active_topics=["share_consume_bench_topic6"],
                                                share_group=self.share_group)
        wait_until(lambda: self.kafka.set_share_group_offset_reset_strategy(group=self.share_group, strategy="earliest"),
                   timeout_sec=20, backoff_sec=2, err_msg="share.auto.offset.reset not set to earliest")
        share_consume_workload = self.trogdor.create_task("share_consume_workload", share_consume_spec)
        share_consume_workload.wait_for_done(timeout_sec=360)
        self.logger.debug("Share consume workload finished")
        tasks = self.trogdor.tasks()
        self.logger.info("TASKS: %s\n" % json.dumps(tasks, sort_keys=True, indent=2))

    @cluster(num_nodes=10)
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        use_share_groups=[True],
    )
    def test_multiple_share_consumers_subscribed_to_single_topic(self, metadata_quorum, use_share_groups=True):
        """
        Runs multiple share consumers in a share group to read messages from topic with single partition.
        """
        self.produce_messages(self.topic_with_single_partitions)
        share_consume_spec = ShareConsumeBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                self.share_consumer_workload_service.share_consumer_node,
                                                self.share_consumer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=1000,
                                                max_messages=100, # all should read at least 100 messages
                                                consumer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                threads_per_worker=5,
                                                active_topics=["share_consume_bench_topic6"],
                                                share_group=self.share_group)
        wait_until(lambda: self.kafka.set_share_group_offset_reset_strategy(group=self.share_group, strategy="earliest"),
                   timeout_sec=20, backoff_sec=2, err_msg="share.auto.offset.reset not set to earliest")
        share_consume_workload = self.trogdor.create_task("share_consume_workload", share_consume_spec)
        share_consume_workload.wait_for_done(timeout_sec=360)
        self.logger.debug("Share consume workload finished")
        tasks = self.trogdor.tasks()
        self.logger.info("TASKS: %s\n" % json.dumps(tasks, sort_keys=True, indent=2))