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

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.mark import matrix
from ducktape.mark import parametrize
from ducktape.mark.resource import cluster

from kafkatest.services.kafka import quorum
from kafkatest.tests.end_to_end import EndToEndTest
from kafkatest.services.kafka import config_property
from kafkatest.services.trogdor.network_partition_fault_spec import NetworkPartitionFaultSpec
from kafkatest.services.trogdor.task_spec import TaskSpec
from kafkatest.services.trogdor.trogdor import TrogdorService

import signal
import time

class ReplicationReplicaFailureTest(EndToEndTest):

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ReplicationReplicaFailureTest, self).__init__(test_context=test_context, topic=None)

    @cluster(num_nodes=7)
    @matrix(metadata_quorum=quorum.all_non_upgrade)
    def test_replication_with_replica_failure(self, metadata_quorum=quorum.zk):
        """
        This test verifies that replication shrinks the ISR when a replica is not fetching anymore.
        It also verifies that replication provides simple durability guarantees by checking that data acked by
        brokers is still available for consumption.

        Setup: 1 zk/KRaft controller, 3 kafka nodes, 1 topic with partitions=1, replication-factor=3, and min.insync.replicas=2
          - Produce messages in the background
          - Consume messages in the background
          - Partition a follower
          - Validate that the ISR was shrunk
          - Stop producing and finish consuming
          - Validate that every acked message was consumed
        """
        self.create_zookeeper_if_necessary()
        if self.zk:
            self.zk.start()

        self.create_kafka(num_nodes=3,
                          server_prop_overrides=[["replica.lag.time.max.ms", "10000"]],
                          controller_num_nodes_override=1)
        self.kafka.start()

        self.trogdor = TrogdorService(context=self.test_context,
                                      client_services=[self.kafka])
        self.trogdor.start()

        # If ZK is used, the partition leader is put on the controller node
        # to avoid partitioning the controller later on in the test.
        if self.zk:
            controller = self.kafka.controller()
            assignment = [self.kafka.idx(controller)] + [self.kafka.idx(node) for node in self.kafka.nodes if node != controller]
        else:
            assignment = [self.kafka.idx(node) for node in self.kafka.nodes]

        self.topic = "test_topic"
        self.kafka.create_topic({"topic": self.topic,
                                 "replica-assignment": ":".join(map(str, assignment)),
                                 "configs": {"min.insync.replicas": 2}})

        self.logger.info("Created topic %s with assignment %s", self.topic, ", ".join(map(str, assignment)))

        self.create_producer()
        self.producer.start()

        self.create_consumer()
        self.consumer.start()

        self.await_startup()

        leader = self.kafka.leader(self.topic, partition=0)
        replicas = self.kafka.replicas(self.topic, partition=0)

        # One of the followers is picked to be partitioned.
        follower_to_partition = [replica for replica in replicas if replica != leader][0]
        self.logger.info("Partitioning follower %s (%s) from the other brokers", self.kafka.idx(follower_to_partition), follower_to_partition.name)
        partition_spec = NetworkPartitionFaultSpec(0, 5*60*1000,
            [[follower_to_partition], [node for node in self.kafka.nodes if node != follower_to_partition]])
        partition = self.trogdor.create_task("partition", partition_spec)

        def current_isr():
            try:
                # Due to the network partition, the kafka-topics command could fail if it tries
                # to connect to the partitioned broker. Therefore we catch the error here and retry.
                return set(self.kafka.isr_idx_list(self.topic, partition=0, node=leader, offline_nodes=[follower_to_partition]))
            except RemoteCommandError as e:
                return set()

        # Verify that ISR is shrunk.
        expected_isr = {self.kafka.idx(replica) for replica in replicas if replica != follower_to_partition}
        wait_until(lambda: current_isr() == expected_isr,
                   timeout_sec=120, backoff_sec=1, err_msg="ISR should have been shrunk.")

        # Wait until the network partition is removed.
        partition.stop()
        partition.wait_for_done(timeout_sec=300)

        # Verify that ISR is expanded.
        expected_isr = {self.kafka.idx(replica) for replica in replicas}
        wait_until(lambda: current_isr() == expected_isr,
                   timeout_sec=120, backoff_sec=1, err_msg="ISR should have been expanded.")

        self.run_validation(producer_timeout_sec=120, min_records=25000)
