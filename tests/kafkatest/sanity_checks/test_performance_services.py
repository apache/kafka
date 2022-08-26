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

from ducktape.mark import matrix, parametrize
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.performance import ProducerPerformanceService, ConsumerPerformanceService, EndToEndLatencyService
from kafkatest.services.performance import latency, compute_aggregate_throughput
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.version import DEV_BRANCH, LATEST_0_8_2, LATEST_0_9, LATEST_1_1, KafkaVersion


class PerformanceServiceTest(Test):
    def __init__(self, test_context):
        super(PerformanceServiceTest, self).__init__(test_context)
        self.record_size = 100
        self.num_records = 10000
        self.topic = "topic"

        self.zk = ZookeeperService(test_context, 1) if quorum.for_test(test_context) == quorum.zk else None

    def setUp(self):
        if self.zk:
            self.zk.start()

    @cluster(num_nodes=5)
    # We are keeping 0.8.2 here so that we don't inadvertently break support for it. Since this is just a sanity check,
    # the overhead should be manageable.
    @parametrize(version=str(LATEST_0_8_2), new_consumer=False)
    @parametrize(version=str(LATEST_0_9), new_consumer=False)
    @parametrize(version=str(LATEST_0_9))
    @parametrize(version=str(LATEST_1_1), new_consumer=False)
    @cluster(num_nodes=5)
    @matrix(version=[str(DEV_BRANCH)], metadata_quorum=quorum.all)
    def test_version(self, version=str(LATEST_0_9), new_consumer=True, metadata_quorum=quorum.zk):
        """
        Sanity check out producer performance service - verify that we can run the service with a small
        number of messages. The actual stats here are pretty meaningless since the number of messages is quite small.
        """
        version = KafkaVersion(version)
        self.kafka = KafkaService(
            self.test_context, 1,
            self.zk, topics={self.topic: {'partitions': 1, 'replication-factor': 1}}, version=version)
        self.kafka.start()

        # check basic run of producer performance
        self.producer_perf = ProducerPerformanceService(
            self.test_context, 1, self.kafka, topic=self.topic,
            num_records=self.num_records, record_size=self.record_size,
            throughput=1000000000,  # Set impossibly for no throttling for equivalent behavior between 0.8.X and 0.9.X
            version=version,
            settings={
                'acks': 1,
                'batch.size': 8*1024,
                'buffer.memory': 64*1024*1024})
        self.producer_perf.run()
        producer_perf_data = compute_aggregate_throughput(self.producer_perf)
        assert producer_perf_data['records_per_sec'] > 0

        # check basic run of end to end latency
        self.end_to_end = EndToEndLatencyService(
            self.test_context, 1, self.kafka,
            topic=self.topic, num_records=self.num_records, version=version)
        self.end_to_end.run()
        end_to_end_data = latency(self.end_to_end.results[0]['latency_50th_ms'],  self.end_to_end.results[0]['latency_99th_ms'], self.end_to_end.results[0]['latency_999th_ms'])

        # check basic run of consumer performance service
        self.consumer_perf = ConsumerPerformanceService(
            self.test_context, 1, self.kafka, new_consumer=new_consumer,
            topic=self.topic, version=version, messages=self.num_records)
        self.consumer_perf.group = "test-consumer-group"
        self.consumer_perf.run()
        consumer_perf_data = compute_aggregate_throughput(self.consumer_perf)
        assert consumer_perf_data['records_per_sec'] > 0

        return {
            "producer_performance": producer_perf_data,
            "end_to_end_latency": end_to_end_data,
            "consumer_performance": consumer_perf_data
        }
