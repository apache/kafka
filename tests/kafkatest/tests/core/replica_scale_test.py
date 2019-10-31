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
from ducktape.utils.util import wait_until

from kafkatest.services.trogdor.produce_bench_workload import ProduceBenchWorkloadService, ProduceBenchWorkloadSpec
from kafkatest.services.trogdor.consume_bench_workload import ConsumeBenchWorkloadService, ConsumeBenchWorkloadSpec
from kafkatest.services.trogdor.task_spec import TaskSpec
from kafkatest.services.kafka import KafkaService
from kafkatest.services.trogdor.trogdor import TrogdorService
from kafkatest.services.zookeeper import ZookeeperService

import json
import time


class ReplicaScaleTest(Test):
    def __init__(self, test_context):
        super(ReplicaScaleTest, self).__init__(test_context=test_context)
        self.test_context = test_context
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(self.test_context, num_nodes=8, zk=self.zk)
        self.producer_workload_service = ProduceBenchWorkloadService(self.test_context, self.kafka)
        self.consumer_workload_service = ConsumeBenchWorkloadService(self.test_context, self.kafka)
        self.trogdor = TrogdorService(context=self.test_context,
                                      client_services=[self.kafka, self.producer_workload_service, self.consumer_workload_service])
        self.active_topics = {"100k_replicas_bench[0-9]": {"numPartitions": 34, "replicationFactor": 3}}
        self.inactive_topics = {"100k_replicas_bench[10-99]": {"numPartitions": 34, "replicationFactor": 3}}

    def setUp(self):
        self.trogdor.start()
        self.zk.start()
        self.kafka.start()

    def teardown(self):
        # Need to increase the timeout due to partition count
        for node in self.kafka.nodes:
            self.kafka.stop_node(node, clean_shutdown=False, timeout_sec=60)
        self.kafka.stop()
        self.zk.stop()
        self.trogdor.stop()

    @cluster(num_nodes=12)
    def test_100k_bench(self):
        t0 = time.time()
        for i in range(10):
            topic = "100k_replicas_bench%d" % i
            self.logger.info("Creating topic %s" % topic)
            topic_cfg = {
                "topic": topic,
                "partitions": 34,
                "replication-factor": 3,
                "configs": {"min.insync.replicas": 1}
            }
            self.kafka.create_topic(topic_cfg)

        t1 = time.time()
        self.logger.info("Time to create topics: %d" % (t1-t0))

        produce_spec = ProduceBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                self.producer_workload_service.producer_node,
                                                self.producer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=10000,
                                                max_messages=3400000,
                                                producer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                inactive_topics={},
                                                active_topics={"100k_replicas_bench[0-2]": {"numPartitions": 34, "replicationFactor": 3}})
        produce_workload = self.trogdor.create_task("100k-replicas-produce-workload", produce_spec)
        produce_workload.wait_for_done(timeout_sec=3600)

        consume_spec = ConsumeBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                self.consumer_workload_service.consumer_node,
                                                self.consumer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=10000,
                                                max_messages=3400000,
                                                consumer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                active_topics=["100k_replicas_bench[0-2]"])
        consume_workload = self.trogdor.create_task("100k-replicas-consume_workload", consume_spec)
        consume_workload.wait_for_done(timeout_sec=600)

    @cluster(num_nodes=12)
    def test_100k_clean_bounce(self):

        t0 = time.time()
        for i in range(1000):
            topic = "topic-%04d" % i
            self.logger.info("Creating topic %s" % topic)
            topic_cfg = {
                "topic": topic,
                "partitions": 34,
                "replication-factor": 3,
                "configs": {"min.insync.replicas": 1}
            }
            self.kafka.create_topic(topic_cfg)

        t1 = time.time()
        self.logger.info("Time to create topics: %d" % (t1-t0))

        restart_times = []
        for node in self.kafka.nodes:
            t2 = time.time()
            self.kafka.stop_node(node, clean_shutdown=True, timeout_sec=600)
            self.afka.start_node(node, timeout_sec=600)
            t3 = time.time()
            restart_times.append(t3-t2)
            self.logger.info("Time to restart %s: %d" % (node.name, t3-t2))

        self.logger.info("Restart times: %s" % restart_times)

        for i in range(1000):
            topic = "topic-%04d" % i
            self.logger.info("Deleting topic %s" % topic)
            topic_cfg = {
                "topic": topic,
                "partitions": 34,
                "replication-factor": 3,
                "configs": {"min.insync.replicas": 1}
            }
            self.kafka.create_topic(topic_cfg)
