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

from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.performance import ProducerPerformanceService
from kafkatest.services.console_consumer import ConsoleConsumer, is_int

import random
import signal
import time

class QuotaTest(Test):
    """
    These tests verify that quota provides expected functionality -- they run
    producer, broker, and consumer with different clientId and quota configuration and
    check that the observed throughput is close to the value we expect.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(QuotaTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.logger.info("use topic " + self.topic)

        # quota related parameters
        self.quota_config = {"quota_producer_default": 2500000,
                             "quota_consumer_default": 2000000,
                             "quota_producer_bytes_per_second_overrides": "overridden_id=3750000",
                             "quota_consumer_bytes_per_second_overrides": "overridden_id=3000000"}
        self.maximum_deviation_percentage = 50.0
        self.num_records = 100000
        self.record_size = 3000

        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context, num_nodes=1, zk=self.zk,
                                  topics={self.topic: {"partitions": 6, "replication-factor": 1, "min.insync.replicas": 1}},
                                  quota_config=self.quota_config)
        self.num_producers = 1
        self.num_consumers = 2

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    def min_cluster_size(self):
        """Override this since we're adding services outside of the constructor"""
        return super(QuotaTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    def run_clients(self, producer_id, producer_num, consumer_id, consumer_num):
        self.produced_num = {}
        self.producer_average_bps = {}
        self.producer_maximum_bps = {}

        self.consumed_num = {}
        self.consumer_average_bps = {}
        self.consumer_maximum_bps = {}

        # Produce all messages
        producer = ProducerPerformanceService(
            self.test_context, producer_num, self.kafka,
            topic=self.topic, num_records=self.num_records, record_size=self.record_size, throughput=-1, client_id=producer_id,
            jmx_object_name="kafka.producer:type=producer-metrics,client-id=%s" % producer_id, jmx_attributes="outgoing-byte-rate")
        producer.run()
        self.produced_num[producer.client_id] = sum([value["records"] for value in producer.results])
        self.producer_average_bps[producer.client_id] = producer.average_jmx_value
        self.producer_maximum_bps[producer.client_id] = producer.maximum_jmx_value

        # Consume all messages
        consumer = ConsoleConsumer(self.test_context, consumer_num, self.kafka, self.topic, consumer_timeout_ms=30000, client_id=consumer_id,
            jmx_object_name="kafka.consumer:type=ConsumerTopicMetrics,name=BytesPerSec,clientId=%s" % consumer_id,
            jmx_attributes="OneMinuteRate")
        consumer.run()

        for idx, messages in consumer.messages_consumed.iteritems():
            assert len(messages)>0, "consumer %d didn't consume any message before timeout" % idx

        self.consumed_num[consumer.client_id] = sum([len(value) for value in consumer.messages_consumed.values()])
        self.consumer_average_bps[consumer.client_id] = consumer.average_jmx_value
        self.consumer_maximum_bps[consumer.client_id] = consumer.maximum_jmx_value

        success, msg = self.validate()
        assert success, msg

    def validate(self):
        """
        For each client_id we validate that:
        1) number of consumed messages equals number of produced messages
        2) maximum_producer_throughput <= producer_quota * (1 + maximum_deviation_percentage/100)
        3) maximum_consumer_throughput <= consumer_quota * (1 + maximum_deviation_percentage/100)
        """
        success = True
        msg = ""

        for client_id, num in self.produced_num.iteritems():
            self.logger.info("producer %s produced %d messages" % (client_id, num))

        for client_id, num in self.consumed_num.iteritems():
            self.logger.info("consumer %s consumed %d messages" % (client_id, num))

        if sum(self.produced_num.values()) != sum(self.consumed_num.values()):
            success = False
            msg += "number of produced messages %d doesn't equal number of consumed messages %d" % \
                   (sum(self.produced_num.values()), sum(self.consumed_num.values()))

        for client_id, maximum_bps in self.producer_maximum_bps.iteritems():
            quota_bps = self.getProducerQuota(client_id)
            self.logger.info("producer %s has maximum throughput %.2f bps, average throughput %.2f bps with quota %.2f bps" %
                             (client_id, maximum_bps, self.producer_average_bps[client_id], quota_bps))
            if maximum_bps > quota_bps*(self.maximum_deviation_percentage/100+1):
                success = False
                msg += "maximum producer throughput %.2f bps exceeded quota %.2f bps by more than %.1f%%" % \
                       (maximum_bps, quota_bps, self.maximum_deviation_percentage)

        for client_id, maximum_bps in self.consumer_maximum_bps.iteritems():
            quota_bps = self.getConsumerQuota(client_id)
            self.logger.info("consumer %s has maximum throughput %.2f bps, average throughput %.2f bps with quota %.2f bps" %
                             (client_id, maximum_bps, self.consumer_average_bps[client_id], quota_bps))
            if maximum_bps > quota_bps*(self.maximum_deviation_percentage/100+1):
                success = False
                msg += "maximum consumer throughput %.2f bps exceeded quota %.2f bps by more than %.1f%%" % \
                       (maximum_bps, quota_bps, self.maximum_deviation_percentage)

        return success, msg

    def getProducerQuota(self, client_id):
        overridden_quotas = {value.split('=')[0]:value.split('=')[1] for value in self.quota_config["quota_producer_bytes_per_second_overrides"].split(',')}
        if client_id in overridden_quotas:
            return float(overridden_quotas[client_id])
        return self.quota_config["quota_producer_default"]

    def getConsumerQuota(self, client_id):
        overridden_quotas = {value.split('=')[0]:value.split('=')[1] for value in self.quota_config["quota_consumer_bytes_per_second_overrides"].split(',')}
        if client_id in overridden_quotas:
            return float(overridden_quotas[client_id])
        return self.quota_config["quota_consumer_default"]

    def test_default_quota(self):
        self.run_clients(producer_id="default_id", producer_num=1, consumer_id="default_id", consumer_num=1)

    def test_overridden_quota(self):
        self.run_clients(producer_id="overridden_id", producer_num=1, consumer_id="overridden_id", consumer_num=1)

    def test_shared_quota(self):
        self.run_clients(producer_id="overridden_id", producer_num=1, consumer_id="overridden_id", consumer_num=2)