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

from ducktape.utils.util import wait_until
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from kafkatest.services.kafka import config_property
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.utils.remote_account import path_exists

def select_node(test, broker_type, topic):
    """ Discover node of requested type. For leader type, discovers leader for our topic and partition 0
    """
    if broker_type == "leader":
        node = test.kafka.leader(topic, partition=0)
    elif broker_type == "follower":
        leader = test.kafka.leader(topic, partition=0)
        node = [replica for replica in test.kafka.replicas(topic, partition=0) if replica != leader][0]
    elif broker_type == "controller":
        node = test.kafka.controller()
    else:
        raise Exception("Unexpected broker type %s." % (broker_type))

    return node


class LogDirFailureTest(ProduceConsumeValidateTest):
    """
    Note that consuming is a bit tricky, at least with console consumer. The goal is to consume all messages
    (foreach partition) in the topic. In this case, waiting for the last message may cause the consumer to stop
    too soon since console consumer is consuming multiple partitions from a single thread and therefore we lose
    ordering guarantees.

    Waiting on a count of consumed messages can be unreliable: if we stop consuming when num_consumed == num_acked,
    we might exit early if some messages are duplicated (though not an issue here since producer retries==0)

    Therefore rely here on the consumer.timeout.ms setting which times out on the interval between successively
    consumed messages. Since we run the producer to completion before running the consumer, this is a reliable
    indicator that nothing is left to consume.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(LogDirFailureTest, self).__init__(test_context=test_context)

        self.topic1 = "test_topic_1"
        self.topic2 = "test_topic_2"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context,
                                  num_nodes=3,
                                  zk=self.zk,
                                  topics={
                                      self.topic1: {"partitions": 1, "replication-factor": 3, "configs": {"min.insync.replicas": 1}},
                                      self.topic2: {"partitions": 1, "replication-factor": 3, "configs": {"min.insync.replicas": 2}}
                                  },
                                  # Set log.roll.ms to 3 seconds so that broker will detect disk error sooner when it creates log segment
                                  # Otherwise broker will still be able to read/write the log file even if the log directory is inaccessible.
                                  server_prop_overrides=[
                                      [config_property.OFFSETS_TOPIC_NUM_PARTITIONS, "1"],
                                      [config_property.LOG_FLUSH_INTERVAL_MESSAGE, "5"],
                                      [config_property.REPLICA_HIGHWATERMARK_CHECKPOINT_INTERVAL_MS, "60000"],
                                      [config_property.LOG_ROLL_TIME_MS, "3000"]
                                  ])

        self.producer_throughput = 1000
        self.num_producers = 1
        self.num_consumers = 1

    def setUp(self):
        self.zk.start()

    def min_cluster_size(self):
        """Override this since we're adding services outside of the constructor"""
        return super(LogDirFailureTest, self).min_cluster_size() + self.num_producers * 2 + self.num_consumers * 2

    @cluster(num_nodes=9)
    @matrix(bounce_broker=[False, True], broker_type=["leader", "follower"], security_protocol=["PLAINTEXT"])
    def test_replication_with_disk_failure(self, bounce_broker, security_protocol, broker_type):
        """Replication tests.
        These tests verify that replication provides simple durability guarantees by checking that data acked by
        brokers is still available for consumption in the face of various failure scenarios.

        Setup: 1 zk, 3 kafka nodes, 1 topic with partitions=3, replication-factor=3, and min.insync.replicas=2
               and another topic with partitions=3, replication-factor=3, and min.insync.replicas=1
            - Produce messages in the background
            - Consume messages in the background
            - Drive broker failures (shutdown, or bounce repeatedly with kill -15 or kill -9)
            - When done driving failures, stop producing, and finish consuming
            - Validate that every acked message was consumed
        """

        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol
        self.kafka.start()

        try:
            # Initialize producer/consumer for topic2
            self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka, self.topic2,
                                               throughput=self.producer_throughput)
            self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka, self.topic2, group_id="test-consumer-group-1",
                                            consumer_timeout_ms=60000, message_validator=is_int)
            self.start_producer_and_consumer()

            # Get a replica of the partition of topic2 and make its log directory offline by changing the log dir's permission.
            # We assume that partition of topic2 is created in the second log directory of respective brokers.
            broker_node = select_node(self, broker_type, self.topic2)
            broker_idx = self.kafka.idx(broker_node)
            assert broker_idx in self.kafka.isr_idx_list(self.topic2), \
                   "Broker %d should be in isr set %s" % (broker_idx, str(self.kafka.isr_idx_list(self.topic2)))

            # Verify that topic1 and the consumer offset topic is in the first log directory and topic2 is in the second log directory
            topic_1_partition_0 = KafkaService.DATA_LOG_DIR_1 + "/test_topic_1-0"
            topic_2_partition_0 = KafkaService.DATA_LOG_DIR_2 + "/test_topic_2-0"
            offset_topic_partition_0 = KafkaService.DATA_LOG_DIR_1 + "/__consumer_offsets-0"
            for path in [topic_1_partition_0, topic_2_partition_0, offset_topic_partition_0]:
                assert path_exists(broker_node, path), "%s should exist" % path

            self.logger.debug("Making log dir %s inaccessible" % (KafkaService.DATA_LOG_DIR_2))
            cmd = "chmod a-w %s -R" % (KafkaService.DATA_LOG_DIR_2)
            broker_node.account.ssh(cmd, allow_fail=False)

            if bounce_broker:
                self.kafka.restart_node(broker_node, clean_shutdown=True)

            # Verify the following:
            # 1) The broker with offline log directory is not the leader of the partition of topic2
            # 2) The broker with offline log directory is not in the ISR
            # 3) The broker with offline log directory is still online
            # 4) Messages can still be produced and consumed from topic2
            wait_until(lambda: self.kafka.leader(self.topic2, partition=0) != broker_node,
                       timeout_sec=60,
                       err_msg="Broker %d should not be leader of topic %s and partition 0" % (broker_idx, self.topic2))
            assert self.kafka.alive(broker_node), "Broker %d should be still online" % (broker_idx)
            wait_until(lambda: broker_idx not in self.kafka.isr_idx_list(self.topic2),
                       timeout_sec=60,
                       err_msg="Broker %d should not be in isr set %s" % (broker_idx, str(self.kafka.isr_idx_list(self.topic2))))

            self.stop_producer_and_consumer()
            self.validate()

            # Shutdown all other brokers so that the broker with offline log dir is the only online broker
            offline_nodes = []
            for node in self.kafka.nodes:
                if broker_node != node:
                    offline_nodes.append(node)
                    self.logger.debug("Hard shutdown broker %d" % (self.kafka.idx(node)))
                    self.kafka.stop_node(node)

            # Verify the following:
            # 1) The broker with offline directory is the only in-sync broker of the partition of topic1
            # 2) Messages can still be produced and consumed from topic1
            self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka, self.topic1,
                                               throughput=self.producer_throughput, offline_nodes=offline_nodes)
            self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka, self.topic1, group_id="test-consumer-group-2",
                                            consumer_timeout_ms=90000, message_validator=is_int)
            self.consumer_start_timeout_sec = 90
            self.start_producer_and_consumer()

            assert self.kafka.isr_idx_list(self.topic1) == [broker_idx], \
                   "In-sync replicas of topic %s and partition 0 should be %s" % (self.topic1, str([broker_idx]))

            self.stop_producer_and_consumer()
            self.validate()

        except BaseException as e:
            for s in self.test_context.services:
                self.mark_for_collect(s)
            raise
