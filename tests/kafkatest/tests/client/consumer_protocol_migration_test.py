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
from ducktape.utils.util import wait_until
from ducktape.mark.resource import cluster

from kafkatest.tests.verifiable_consumer_test import VerifiableConsumerTest
from kafkatest.services.kafka import TopicPartition, quorum, consumer_group

class ConsumerProtocolMigrationTest(VerifiableConsumerTest):
    TOPIC = "test_topic"
    NUM_PARTITIONS = 6

    RANGE = "org.apache.kafka.clients.consumer.RangeAssignor"
    ROUND_ROBIN = "org.apache.kafka.clients.consumer.RoundRobinAssignor"
    STICKY = "org.apache.kafka.clients.consumer.StickyAssignor"
    COOPERATIVE_STICKEY = "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
    all_assignment_strategies = [RANGE, ROUND_ROBIN, COOPERATIVE_STICKEY, STICKY]

    def __init__(self, test_context):
        super(ConsumerProtocolMigrationTest, self).__init__(test_context, num_consumers=2, num_producers=1,
                                                            num_zk=0, num_brokers=2, topics={
                self.TOPIC : { 'partitions': self.NUM_PARTITIONS, 'replication-factor': 1 }
            })

    @cluster(num_nodes=6)
    @matrix(
        clean_shutdown=[True, False],
        enable_autocommit=[True, False],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        consumer_group_migration_policy=["bidirectional"],
        assignment_strategy=all_assignment_strategies,
        bounce_mode=["all", "rolling"]
    )
    def test_boucing_consumer(self, clean_shutdown, enable_autocommit, metadata_quorum, use_new_coordinator,
                              consumer_group_migration_policy, assignment_strategy, bounce_mode):
        producer = self.setup_producer(self.TOPIC)
        consumer = self.setup_consumer(self.TOPIC, group_protocol=consumer_group.classic_group_protocol,
                                       assignment_strategy=assignment_strategy, enable_autocommit=enable_autocommit)
        self.mark_for_collect(consumer, 'verifiable_consumer_stdout')
        consumer.start()

        producer.start()
        self.await_produced_messages(producer)

        consumer.start()
        self.await_all_members(consumer)
        self.await_consumed_messages(consumer)

        consumer.group_protocol = consumer_group.consumer_group_protocol

        if bounce_mode == "all":
            for node in consumer.nodes:
                consumer.stop_node(node, clean_shutdown)

            wait_until(lambda: len(consumer.dead_nodes()) == self.num_consumers, timeout_sec=10,
                       err_msg="Timed out waiting for the consumers to shutdown")

            for node in consumer.nodes:
                consumer.start_node(node)

            self.await_all_members(consumer)
            self.await_consumed_messages(consumer)
        else:
            for node in consumer.nodes:
                consumer.stop_node(node, clean_shutdown)

                wait_until(lambda: len(consumer.dead_nodes()) == 1,
                           timeout_sec=self.session_timeout_sec+5,
                           err_msg="Timed out waiting for the consumer to shutdown")

                consumer.start_node(node)

                self.await_all_members(consumer)
                self.await_consumed_messages(consumer)

        consumer.stop_all()
