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
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.trogdor.produce_bench_workload import ProduceBenchWorkloadService, ProduceBenchWorkloadSpec
from kafkatest.services.trogdor.consume_bench_workload import ConsumeBenchWorkloadService, ConsumeBenchWorkloadSpec
from kafkatest.services.trogdor.task_spec import TaskSpec
from kafkatest.services.trogdor.trogdor import TrogdorService
from kafkatest.services.zookeeper import ZookeeperService


class ConsumeBenchTest(Test):
    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ConsumeBenchTest, self).__init__(test_context)
        self.zk = ZookeeperService(test_context, num_nodes=3) if quorum.for_test(test_context) == quorum.zk else None
        self.kafka = KafkaService(test_context, num_nodes=3, zk=self.zk)
        self.producer_workload_service = ProduceBenchWorkloadService(test_context, self.kafka)
        self.consumer_workload_service = ConsumeBenchWorkloadService(test_context, self.kafka)
        self.consumer_workload_service_2 = ConsumeBenchWorkloadService(test_context, self.kafka)
        self.active_topics = {"consume_bench_topic[0-5]": {"numPartitions": 5, "replicationFactor": 3}}
        self.trogdor = TrogdorService(context=self.test_context,
                                      client_services=[self.kafka, self.producer_workload_service,
                                                       self.consumer_workload_service,
                                                       self.consumer_workload_service_2])

    def setUp(self):
        self.trogdor.start()
        if self.zk:
            self.zk.start()
        self.kafka.start()

    def teardown(self):
        self.trogdor.stop()
        self.kafka.stop()
        if self.zk:
            self.zk.stop()

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
        topics=[
            ["consume_bench_topic[0-5]"], # topic subscription
            ["consume_bench_topic[0-5]:[0-4]"] # manual topic assignment
        ],
        metadata_quorum=[quorum.zk],
        use_new_coordinator=[False]
    )
    @matrix(
        topics=[
            ["consume_bench_topic[0-5]"], # topic subscription
            ["consume_bench_topic[0-5]:[0-4]"] # manual topic assignment
        ],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True, False]
    )
    def test_consume_bench(self, topics, metadata_quorum=quorum.zk, use_new_coordinator=False):
        """
        Runs a ConsumeBench workload to consume messages
        """
        self.produce_messages(self.active_topics)
        consume_spec = ConsumeBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                self.consumer_workload_service.consumer_node,
                                                self.consumer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=1000,
                                                max_messages=10000,
                                                consumer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                active_topics=topics)
        consume_workload = self.trogdor.create_task("consume_workload", consume_spec)
        consume_workload.wait_for_done(timeout_sec=360)
        self.logger.debug("Consume workload finished")
        tasks = self.trogdor.tasks()
        self.logger.info("TASKS: %s\n" % json.dumps(tasks, sort_keys=True, indent=2))

    @cluster(num_nodes=10)
    @matrix(
        metadata_quorum=[quorum.zk],
        use_new_coordinator=[False]
    )
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True, False]
    )
    def test_single_partition(self, metadata_quorum=quorum.zk, use_new_coordinator=False):
        """
        Run a ConsumeBench against a single partition
        """
        active_topics = {"consume_bench_topic": {"numPartitions": 2, "replicationFactor": 3}}
        self.produce_messages(active_topics, 5000)
        consume_spec = ConsumeBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                self.consumer_workload_service.consumer_node,
                                                self.consumer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=1000,
                                                max_messages=2500,
                                                consumer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                active_topics=["consume_bench_topic:1"])
        consume_workload = self.trogdor.create_task("consume_workload", consume_spec)
        consume_workload.wait_for_done(timeout_sec=180)
        self.logger.debug("Consume workload finished")
        tasks = self.trogdor.tasks()
        self.logger.info("TASKS: %s\n" % json.dumps(tasks, sort_keys=True, indent=2))

    @cluster(num_nodes=10)
    @matrix(
        metadata_quorum=[quorum.zk],
        use_new_coordinator=[False]
    )
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True, False]
    )
    def test_multiple_consumers_random_group_topics(self, metadata_quorum=quorum.zk, use_new_coordinator=False):
        """
        Runs multiple consumers group to read messages from topics.
        Since a consumerGroup isn't specified, each consumer should read from all topics independently
        """
        self.produce_messages(self.active_topics, max_messages=5000)
        consume_spec = ConsumeBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                self.consumer_workload_service.consumer_node,
                                                self.consumer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=1000,
                                                max_messages=5000, # all should read exactly 5k messages
                                                consumer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                threads_per_worker=5,
                                                active_topics=["consume_bench_topic[0-5]"])
        consume_workload = self.trogdor.create_task("consume_workload", consume_spec)
        consume_workload.wait_for_done(timeout_sec=360)
        self.logger.debug("Consume workload finished")
        tasks = self.trogdor.tasks()
        self.logger.info("TASKS: %s\n" % json.dumps(tasks, sort_keys=True, indent=2))

    @cluster(num_nodes=10)
    @matrix(
        metadata_quorum=[quorum.zk],
        use_new_coordinator=[False]
    )
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True, False]
    )
    def test_two_consumers_specified_group_topics(self, metadata_quorum=quorum.zk, use_new_coordinator=False):
        """
        Runs two consumers in the same consumer group to read messages from topics.
        Since a consumerGroup is specified, each consumer should dynamically get assigned a partition from group
        """
        self.produce_messages(self.active_topics)
        consume_spec = ConsumeBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                self.consumer_workload_service.consumer_node,
                                                self.consumer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=1000,
                                                max_messages=2000, # both should read at least 2k messages
                                                consumer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                threads_per_worker=2,
                                                consumer_group="testGroup",
                                                active_topics=["consume_bench_topic[0-5]"])
        consume_workload = self.trogdor.create_task("consume_workload", consume_spec)
        consume_workload.wait_for_done(timeout_sec=360)
        self.logger.debug("Consume workload finished")
        tasks = self.trogdor.tasks()
        self.logger.info("TASKS: %s\n" % json.dumps(tasks, sort_keys=True, indent=2))

    @cluster(num_nodes=10)
    @matrix(
        metadata_quorum=[quorum.zk],
        use_new_coordinator=[False]
    )
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True, False]
    )
    def test_multiple_consumers_random_group_partitions(self, metadata_quorum=quorum.zk, use_new_coordinator=False):
        """
        Runs multiple consumers in to read messages from specific partitions.
        Since a consumerGroup isn't specified, each consumer will get assigned a random group
        and consume from all partitions
        """
        self.produce_messages(self.active_topics, max_messages=20000)
        consume_spec = ConsumeBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                self.consumer_workload_service.consumer_node,
                                                self.consumer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=1000,
                                                max_messages=2000,
                                                consumer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                threads_per_worker=4,
                                                active_topics=["consume_bench_topic1:[0-4]"])
        consume_workload = self.trogdor.create_task("consume_workload", consume_spec)
        consume_workload.wait_for_done(timeout_sec=360)
        self.logger.debug("Consume workload finished")
        tasks = self.trogdor.tasks()
        self.logger.info("TASKS: %s\n" % json.dumps(tasks, sort_keys=True, indent=2))

    @cluster(num_nodes=10)
    @matrix(
        metadata_quorum=[quorum.zk],
        use_new_coordinator=[False]
    )
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True, False]
    )
    def test_multiple_consumers_specified_group_partitions_should_raise(self, metadata_quorum=quorum.zk, use_new_coordinator=False):
        """
        Runs multiple consumers in the same group to read messages from specific partitions.
        It is an invalid configuration to provide a consumer group and specific partitions.
        """
        expected_error_msg = 'explicit partition assignment'
        self.produce_messages(self.active_topics, max_messages=20000)
        consume_spec = ConsumeBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                self.consumer_workload_service.consumer_node,
                                                self.consumer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=1000,
                                                max_messages=2000,
                                                consumer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                threads_per_worker=4,
                                                consumer_group="fail_group",
                                                active_topics=["consume_bench_topic1:[0-4]"])
        consume_workload = self.trogdor.create_task("consume_workload", consume_spec)
        try:
            consume_workload.wait_for_done(timeout_sec=360)
            raise Exception("Should have raised an exception due to an invalid configuration")
        except RuntimeError as e:
            if expected_error_msg not in str(e):
                raise RuntimeError("Unexpected Exception - " + str(e))
            self.logger.info(e)

