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
from ducktape.tests.test import Test
from kafkatest.services.kafka import KafkaService
from kafkatest.services.trogdor.produce_bench_workload import ProduceBenchWorkloadService, ProduceBenchWorkloadSpec
from kafkatest.services.trogdor.task_spec import TaskSpec
from kafkatest.services.trogdor.trogdor import TrogdorService
from kafkatest.services.zookeeper import ZookeeperService


class ProduceBenchTest(Test):
    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ProduceBenchTest, self).__init__(test_context)
        self.zk = ZookeeperService(test_context, num_nodes=3)
        self.kafka = KafkaService(test_context, num_nodes=3, zk=self.zk)
        self.workload_service = ProduceBenchWorkloadService(test_context, self.kafka)
        self.trogdor = TrogdorService(context=self.test_context,
                                      client_services=[self.kafka, self.workload_service])

    def setUp(self):
        self.trogdor.start()
        self.zk.start()
        self.kafka.start()

    def teardown(self):
        self.trogdor.stop()
        self.kafka.stop()
        self.zk.stop()

    def test_produce_bench(self):
        active_topics={"produce_bench_topic[0-1]":{"numPartitions":1, "replicationFactor":3}}
        inactive_topics={"produce_bench_topic[2-9]":{"numPartitions":1, "replicationFactor":3}}
        spec = ProduceBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                        self.workload_service.producer_node,
                                        self.workload_service.bootstrap_servers,
                                        target_messages_per_sec=1000,
                                        max_messages=100000,
                                        producer_conf={},
                                        inactive_topics=inactive_topics,
                                        active_topics=active_topics)
        workload1 = self.trogdor.create_task("workload1", spec)
        workload1.wait_for_done(timeout_sec=360)
        tasks = self.trogdor.tasks()
        self.logger.info("TASKS: %s\n" % json.dumps(tasks, sort_keys=True, indent=2))
