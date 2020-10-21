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

import time
import math
from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from kafkatest.services.performance import ProducerPerformanceService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.utils import is_int


class ThrottlingTest(ProduceConsumeValidateTest):
    """Tests throttled partition reassignment. This is essentially similar
    to the reassign_partitions_test, except that we throttle the reassignment
    and verify that it takes a sensible amount of time given the throttle
    and the amount of data being moved.

    Since the correctness is time dependent, this test also simplifies the
    cluster topology. In particular, we fix the number of brokers, the
    replication-factor, the number of partitions, the partition size, and
    the number of partitions being moved so that we can accurately predict
    the time throttled reassignment should take.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ThrottlingTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        # Because we are starting the producer/consumer/validate cycle _after_
        # seeding the cluster with big data (to test throttling), we need to
        # Start the consumer from the end of the stream. further, we need to
        # ensure that the consumer is fully started before the producer starts
        # so that we don't miss any messages. This timeout ensures the sufficient
        # condition.
        self.consumer_init_timeout_sec =  60
        self.num_brokers = 6
        self.num_partitions = 3
        self.kafka = KafkaService(test_context,
                                  num_nodes=self.num_brokers,
                                  zk=self.zk,
                                  topics={
                                      self.topic: {
                                          "partitions": self.num_partitions,
                                          "replication-factor": 2,
                                          "configs": {
                                              "segment.bytes": 64 * 1024 * 1024
                                          }
                                      }
                                  })
        self.producer_throughput = 1000
        self.timeout_sec = 400
        self.num_records = 2000
        self.record_size = 4096 * 100  # 400 KB
        # 1 MB per partition on average.
        self.partition_size = (self.num_records * self.record_size) // self.num_partitions
        self.num_producers = 2
        self.num_consumers = 1
        self.throttle = 4 * 1024 * 1024  # 4 MB/s

    def setUp(self):
        self.zk.start()

    def min_cluster_size(self):
        # Override this since we're adding services outside of the constructor
        return super(ThrottlingTest, self).min_cluster_size() +\
            self.num_producers + self.num_consumers

    def clean_bounce_some_brokers(self):
        """Bounce every other broker"""
        for node in self.kafka.nodes[::2]:
            self.kafka.restart_node(node, clean_shutdown=True)

    def reassign_partitions(self, bounce_brokers, throttle):
        """This method reassigns partitions using a throttle. It makes an
        assertion about the minimum amount of time the reassignment should take
        given the value of the throttle, the number of partitions being moved,
        and the size of each partition.
        """
        partition_info = self.kafka.parse_describe_topic(
            self.kafka.describe_topic(self.topic))
        self.logger.debug("Partitions before reassignment:" +
                          str(partition_info))
        max_num_moves = 0
        for i in range(0, self.num_partitions):
            old_replicas = set(partition_info["partitions"][i]["replicas"])
            new_part = (i+1) % self.num_partitions
            new_replicas = set(partition_info["partitions"][new_part]["replicas"])
            max_num_moves = max(len(new_replicas - old_replicas), max_num_moves)
            partition_info["partitions"][i]["partition"] = new_part
        self.logger.debug("Jumbled partitions: " + str(partition_info))

        self.kafka.execute_reassign_partitions(partition_info,
                                               throttle=throttle)
        start = time.time()
        if bounce_brokers:
            # bounce a few brokers at the same time
            self.clean_bounce_some_brokers()

        # Wait until finished or timeout
        size_per_broker = max_num_moves * self.partition_size
        self.logger.debug("Max amount of data transfer per broker: %fb",
                          size_per_broker)
        estimated_throttled_time = math.ceil(float(size_per_broker) /
                                             self.throttle)
        estimated_time_with_buffer = estimated_throttled_time * 2
        self.logger.debug("Waiting %ds for the reassignment to complete",
                          estimated_time_with_buffer)
        wait_until(lambda: self.kafka.verify_reassign_partitions(partition_info),
                   timeout_sec=estimated_time_with_buffer, backoff_sec=.5)
        stop = time.time()
        time_taken = stop - start
        self.logger.debug("Transfer took %d second. Estimated time : %ds",
                          time_taken,
                          estimated_throttled_time)
        assert time_taken >= estimated_throttled_time * 0.9, \
            ("Expected rebalance to take at least %ds, but it took %ds" % (
                estimated_throttled_time,
                time_taken))

    @cluster(num_nodes=10)
    @parametrize(bounce_brokers=True)
    @parametrize(bounce_brokers=False)
    def test_throttled_reassignment(self, bounce_brokers):
        security_protocol = 'PLAINTEXT'
        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol

        producer_id = 'bulk_producer'
        bulk_producer = ProducerPerformanceService(
            context=self.test_context, num_nodes=1, kafka=self.kafka,
            topic=self.topic, num_records=self.num_records,
            record_size=self.record_size, throughput=-1, client_id=producer_id)


        self.producer = VerifiableProducer(context=self.test_context,
                                           num_nodes=1,
                                           kafka=self.kafka, topic=self.topic,
                                           message_validator=is_int,
                                           throughput=self.producer_throughput)

        self.consumer = ConsoleConsumer(self.test_context,
                                        self.num_consumers,
                                        self.kafka,
                                        self.topic,
                                        consumer_timeout_ms=60000,
                                        message_validator=is_int,
                                        from_beginning=False,
                                        wait_until_partitions_assigned=True)

        self.kafka.start()
        bulk_producer.run()
        self.run_produce_consume_validate(core_test_action=
                                          lambda: self.reassign_partitions(bounce_brokers, self.throttle))

        self.logger.debug("Bulk producer outgoing-byte-rates: %s",
                          (metric.value for k, metrics in
                          bulk_producer.metrics(group='producer-metrics', name='outgoing-byte-rate', client_id=producer_id) for
                          metric in metrics)
        )