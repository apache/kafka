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

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from kafkatest.services.kafka import config_property
from kafkatest.services.kafka import KafkaService, quorum, consumer_group, TopicPartition
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
import random
import time

class ReassignPartitionsTest(ProduceConsumeValidateTest):
    """
    These tests validate partition reassignment.
    Create a topic with few partitions, load some data, trigger partition re-assignment with and without broker failure,
    check that partition re-assignment can complete and there is no data loss.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ReassignPartitionsTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.num_partitions = 20
        # We set the min.insync.replicas to match the replication factor because
        # it makes the test more stringent. If min.isr = 2 and
        # replication.factor=3, then the test would tolerate the failure of
        # reassignment for upto one replica per partition, which is not
        # desirable for this test in particular.
        self.kafka = KafkaService(test_context, num_nodes=4, zk=None,
                                  server_prop_overrides=[
                                      [config_property.LOG_ROLL_TIME_MS, "5000"],
                                      [config_property.LOG_INITIAL_TASK_DELAY_MS, "100"],
                                      [config_property.LOG_DELETE_DELAY_MS, "1000"],
                                      [config_property.LOG_RETENTION_CHECK_INTERVAL_MS, "5000"]
                                  ],
                                  topics={self.topic: {
                                      "partitions": self.num_partitions,
                                      "replication-factor": 3,
                                      'configs': {
                                          "min.insync.replicas": 3,
                                      }}
                                  },
                                  controller_num_nodes_override=1)
        self.timeout_sec = 60
        self.producer_throughput = 1000
        self.num_producers = 1
        self.num_consumers = 1

    def min_cluster_size(self):
        # Override this since we're adding services outside of the constructor
        return super(ReassignPartitionsTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    def clean_bounce_some_brokers(self):
        """Bounce every other broker"""
        for node in self.kafka.nodes[::2]:
            self.kafka.restart_node(node, clean_shutdown=True)

    def reassign_partitions(self, bounce_brokers):
        partition_info = self.kafka.parse_describe_topic(self.kafka.describe_topic(self.topic))
        self.logger.debug("Partitions before reassignment:" + str(partition_info))

        # jumble partition assignment in dictionary
        seed = random.randint(0, 2 ** 31 - 1)
        self.logger.debug("Jumble partition assignment with seed " + str(seed))
        random.seed(seed)
        # The list may still be in order, but that's ok
        shuffled_list = list(range(0, self.num_partitions))
        random.shuffle(shuffled_list)

        for i in range(0, self.num_partitions):
            partition_info["partitions"][i]["partition"] = shuffled_list[i]
        self.logger.debug("Jumbled partitions: " + str(partition_info))

        def check_all_partitions():
            acked_partitions = self.producer.acked_by_partition
            for i in range(self.num_partitions):
                if TopicPartition(self.topic, i) not in acked_partitions:
                    return False
            return True

        # ensure all partitions have data so we don't hit OutOfOrderExceptions due to broker restarts
        wait_until(check_all_partitions,
                   timeout_sec=60,
                   err_msg="Failed to produce to all partitions in 60s")

        # send reassign partitions command
        self.kafka.execute_reassign_partitions(partition_info)

        if bounce_brokers:
            # bounce a few brokers at the same time
            self.clean_bounce_some_brokers()

        # Wait until finished or timeout
        wait_until(lambda: self.kafka.verify_reassign_partitions(partition_info),
                   timeout_sec=self.timeout_sec, backoff_sec=.5)

    def move_start_offset(self):
        """We move the start offset of the topic by writing really old messages
        and waiting for them to be cleaned up.
        """
        producer = VerifiableProducer(self.test_context, 1, self.kafka, self.topic,
                                      throughput=-1, enable_idempotence=True,
                                      create_time=1000)
        producer.start()
        wait_until(lambda: producer.num_acked > 0,
                   timeout_sec=30,
                   err_msg="Failed to get an acknowledgement for %ds" % 30)
        # Wait 8 seconds to let the topic be seeded with messages that will
        # be deleted. The 8 seconds is important, since we should get 2 deleted
        # segments in this period based on the configured log roll time and the
        # retention check interval.
        time.sleep(8)
        producer.stop()
        self.logger.info("Seeded topic with %d messages which will be deleted" %\
                         producer.num_acked)
        # Since the configured check interval is 5 seconds, we wait another
        # 6 seconds to ensure that at least one more cleaning so that the last
        # segment is deleted. An altenate to using timeouts is to poll each
        # partition until the log start offset matches the end offset. The
        # latter is more robust.
        time.sleep(6)

    @cluster(num_nodes=8)
    @matrix(
        bounce_brokers=[True, False],
        reassign_from_offset_zero=[True, False],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[False]
    )
    @matrix(
        bounce_brokers=[True, False],
        reassign_from_offset_zero=[True, False],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        group_protocol=consumer_group.all_group_protocols
    )
    def test_reassign_partitions(self, bounce_brokers, reassign_from_offset_zero, metadata_quorum, use_new_coordinator=False, group_protocol=None):
        """Reassign partitions tests.
        Setup: 1 controller, 4 kafka nodes, 1 topic with partitions=20, replication-factor=3,
        and min.insync.replicas=3

            - Produce messages in the background
            - Consume messages in the background
            - Reassign partitions
            - If bounce_brokers is True, also bounce a few brokers while partition re-assignment is in progress
            - When done reassigning partitions and bouncing brokers, stop producing, and finish consuming
            - Validate that every acked message was consumed
            """
        self.kafka.start()
        if not reassign_from_offset_zero:
            self.move_start_offset()

        self.producer = VerifiableProducer(self.test_context, self.num_producers,
                                           self.kafka, self.topic,
                                           throughput=self.producer_throughput,
                                           enable_idempotence=True,
                                           # This test aims to verify the reassignment without failure, assuming that all partitions have data.
                                           # To avoid the reassignment behavior being affected by the `BuiltInPartitioner` (due to the key not being set),
                                           # we set a key for the message to ensure both even data distribution across all partitions.
                                           repeating_keys=100)
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers,
                                        self.kafka, self.topic,
                                        consumer_timeout_ms=60000,
                                        message_validator=is_int,
                                        consumer_properties=consumer_group.maybe_set_group_protocol(group_protocol))

        self.enable_idempotence=True
        self.run_produce_consume_validate(core_test_action=lambda: self.reassign_partitions(bounce_brokers))
