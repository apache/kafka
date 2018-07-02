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
from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test

from kafkatest.services.kafka import KafkaService
from kafkatest.services.performance import ProducerPerformanceService, compute_aggregate_throughput, \
    ConsumerPerformanceService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.version import DEV_BRANCH, LATEST_0_10, KafkaVersion, LATEST_1_1, LATEST_0_10_0


class DownconversionMemoryTest(Test):
    def __init__(self, test_context):
        super(DownconversionMemoryTest, self).__init__(test_context=test_context)
        '''
        Test Setup:
        ==========
        - Java heap size = 190MB
        - 1M messages, 1kB each ==> 1GB of total messages
        - Split into 200 partitions ==> approximately 5MB per partition
        - 1 consumer with `fetch.max.bytes` = 200MB and `max.partition.fetch.bytes` = 1MB
        - Each fetch consumes min(1MB*200, 200MB) = 200MB i.e. 1MB from each partition for a total of 200MB
        - Success criteria:
            - Must always run out of memory if not using lazy down-conversion
            - Must never run out of memory if using lazy down-conversion
        '''
        self.heap_size = 190
        self.max_messages = 1024 * 1024
        self.message_size = 1024
        self.batch_size = self.message_size * 50
        self.num_partitions = 200
        self.max_fetch_size = 200 * 1024 * 1024
        self.num_producers = 1
        self.num_consumers = 1
        self.topics = ["test_topic"]
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.heap_dump_path = "/mnt/heapdump/"
        self.consumer_version = LATEST_0_10

    def setUp(self):
        self.zk.start()
        super(DownconversionMemoryTest, self).setUp()

    def tearDown(self):
        for node in self.kafka.nodes:
            node.account.ssh("rm -rf %s" % self.heap_dump_path, allow_fail=False)
        super(DownconversionMemoryTest, self).tearDown()

    def startBroker(self):
        self.kafka = KafkaService(self.test_context, num_nodes=1, zk=self.zk, version=self.broker_version,
                                  topics={topic:
                                              {"partitions": self.num_partitions,
                                               "replication-factor": 1,
                                               "configs": {"min.insync.replicas": 1}}
                                          for topic in self.topics},
                                  heap_opts="-Xmx%dM -Xms%dM -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%s" % (self.heap_size, self.heap_size, self.heap_dump_path))
        for node in self.kafka.nodes:
            node.account.ssh("mkdir -p %s" % self.heap_dump_path, allow_fail=False)
        self.kafka.log_level = "INFO"
        self.kafka.start()

    def produceMessages(self):
        for topic in self.topics:
            producer = ProducerPerformanceService(self.test_context, self.num_producers, self.kafka, topic=topic,
                num_records=self.max_messages, record_size=self.message_size, throughput=-1,
                version=self.producer_version, settings={'batch.size': self.batch_size})
            producer.run()
            self.logger.debug("Producer throughput for topic %s:" % topic)
            self.logger.debug(compute_aggregate_throughput(producer))

    def consumeMessages(self):
        for topic in self.topics:
            consumer = ConsumerPerformanceService(self.test_context, self.num_consumers, self.kafka, topic=topic,
                                                  messages=self.max_messages, version=self.consumer_version,
                                                  consumer_config={"fetch.max.bytes": self.max_fetch_size},
                                                  timeout_sec=self.consumer_timeout_sec)
            consumer.run()

    @cluster(num_nodes=12)
    @parametrize(version=str(LATEST_1_1))
    @parametrize(version=str(DEV_BRANCH))
    def test_downconversion(self, version):
        self.broker_version = KafkaVersion(version)
        self.producer_version = self.broker_version

        out_of_memory = False
        if KafkaVersion(version) <= LATEST_1_1:
            expect_out_of_memory = True
            self.consumer_timeout_sec = 60
        else:
            expect_out_of_memory = False
            self.consumer_timeout_sec = None

        self.startBroker()
        self.produceMessages()
        self.consumeMessages()

        for node in self.kafka.nodes:
            if self.kafka.file_exists(node, self.heap_dump_path + "*.hprof"):
                out_of_memory = True

        assert expect_out_of_memory == out_of_memory, "Unexpected state. Expected: %d Actual: %d" % (expect_out_of_memory, out_of_memory)

