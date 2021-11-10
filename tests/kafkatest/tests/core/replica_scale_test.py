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


class ReplicaScaleTest(Test):
    def __init__(self, test_context):
        super(ReplicaScaleTest, self).__init__(test_context=test_context)
        self.test_context = test_context
        self.zk = ZookeeperService(test_context, num_nodes=1) if quorum.for_test(test_context) == quorum.zk else None
        self.kafka = KafkaService(self.test_context, num_nodes=8, zk=self.zk, controller_num_nodes_override=1)

    def setUp(self):
        if self.zk:
            self.zk.start()
        self.kafka.start()

    def teardown(self):
        # Need to increase the timeout due to partition count
        for node in self.kafka.nodes:
            self.kafka.stop_node(node, clean_shutdown=False, timeout_sec=60)
        self.kafka.stop()
        if self.zk:
            self.zk.stop()

    @cluster(num_nodes=12)
    @matrix(topic_count=[50], partition_count=[34], replication_factor=[3], metadata_quorum=quorum.all_non_upgrade)
    def test_produce_consume(self, topic_count, partition_count, replication_factor, metadata_quorum=quorum.zk):
        topics_create_start_time = time.time()
        for i in range(topic_count):
            topic = "replicas_produce_consume_%d" % i
            print("Creating topic %s" % topic, flush=True)  # Force some stdout for Jenkins
            topic_cfg = {
                "topic": topic,
                "partitions": partition_count,
                "replication-factor": replication_factor,
                "configs": {"min.insync.replicas": 2}
            }
            self.kafka.create_topic(topic_cfg)

        topics_create_end_time = time.time()
        self.logger.info("Time to create topics: %d" % (topics_create_end_time - topics_create_start_time))

        producer_workload_service = ProduceBenchWorkloadService(self.test_context, self.kafka)
        consumer_workload_service = ConsumeBenchWorkloadService(self.test_context, self.kafka)
        trogdor = TrogdorService(context=self.test_context,
                                 client_services=[self.kafka, producer_workload_service, consumer_workload_service])
        trogdor.start()

        produce_spec = ProduceBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                producer_workload_service.producer_node,
                                                producer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=150000,
                                                max_messages=3400000,
                                                producer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                inactive_topics={},
                                                active_topics={"replicas_produce_consume_[0-2]": {
                                                    "numPartitions": partition_count, "replicationFactor": replication_factor
                                                }})
        produce_workload = trogdor.create_task("replicas-produce-workload", produce_spec)
        produce_workload.wait_for_done(timeout_sec=600)
        print("Completed produce bench", flush=True)  # Force some stdout for Travis

        consume_spec = ConsumeBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                consumer_workload_service.consumer_node,
                                                consumer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=150000,
                                                max_messages=3400000,
                                                consumer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                active_topics=["replicas_produce_consume_[0-2]"])
        consume_workload = trogdor.create_task("replicas-consume-workload", consume_spec)
        consume_workload.wait_for_done(timeout_sec=600)
        self.logger.info("Completed consume bench")

        trogdor.stop()

    @cluster(num_nodes=12)
    @matrix(topic_count=[50], partition_count=[34], replication_factor=[3], metadata_quorum=quorum.all_non_upgrade)
    def test_clean_bounce(self, topic_count, partition_count, replication_factor, metadata_quorum=quorum.zk):
        topics_create_start_time = time.time()
        for i in range(topic_count):
            topic = "topic-%04d" % i
            print("Creating topic %s" % topic, flush=True)  # Force some stdout for Jenkins
            topic_cfg = {
                "topic": topic,
                "partitions": partition_count,
                "replication-factor": replication_factor,
                "configs": {"min.insync.replicas": 2}
            }
            self.kafka.create_topic(topic_cfg)
        topics_create_end_time = time.time()
        self.logger.info("Time to create topics: %d" % (topics_create_end_time - topics_create_start_time))

        restart_times = []
        for node in self.kafka.nodes:
            broker_bounce_start_time = time.time()
            self.kafka.stop_node(node, clean_shutdown=True, timeout_sec=600)
            self.kafka.start_node(node, timeout_sec=600)
            broker_bounce_end_time = time.time()
            restart_times.append(broker_bounce_end_time - broker_bounce_start_time)
            self.logger.info("Time to restart %s: %d" % (node.name, broker_bounce_end_time - broker_bounce_start_time))

        self.logger.info("Total time to restart: %s" % sum(restart_times))

        delete_start_time = time.time()
        for i in range(topic_count):
            topic = "topic-%04d" % i
            self.logger.info("Deleting topic %s" % topic)
            self.kafka.delete_topic(topic)
        delete_end_time = time.time()
        self.logger.info("Time to delete topics: %d" % (delete_end_time - delete_start_time))
