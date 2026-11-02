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
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.kafka.util import fix_opts_for_new_jvm


class ReassignPartitionsOverrideTest(Test):
    """
    Overrides a reassignment while the replica it is adding is still out of the ISR, and checks
    that the controller keeps tracking that replica as adding until it catches up. Otherwise the
    override completes as soon as the replica it adds catches up, and the partition is left with
    an out-of-sync replica that no reassignment accounts for.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ReassignPartitionsOverrideTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.partition = 0
        # Broker 4 is the replica that must stay out of the ISR while the reassignment is
        # overridden. It has to be alive, because kafka-reassign-partitions.sh rejects targets
        # that are not registered, so it is held back with a 1 byte/s follower throttle instead.
        self.throttled_broker = 4
        self.kafka = KafkaService(test_context, num_nodes=5, zk=None,
                                  topics={self.topic: {"replica-assignment": "1:2:3"}},
                                  controller_num_nodes_override=1)
        # Enough data that the throttled follower cannot catch up within the test, since the
        # throttle only kicks in after the first fetch.
        self.num_records = 50000
        self.record_size = 1000
        self.timeout_sec = 120

    def reassignment(self, replicas):
        return {"version": 1, "partitions": [
            {"topic": self.topic, "partition": self.partition, "replicas": replicas}
        ]}

    def current_reassignment(self):
        return self.kafka.list_partition_reassignments().get((self.topic, self.partition))

    def isr(self):
        return set(self.kafka.isr_idx_list(self.topic, self.partition))

    def replicas(self):
        description = self.kafka.describe_topic(self.topic)
        partitions = self.kafka.parse_describe_topic(description)["partitions"]
        return [p["replicas"] for p in partitions if p["partition"] == self.partition][0]

    def produce_records(self):
        node = self.kafka.nodes[0]
        cmd = fix_opts_for_new_jvm(node)
        cmd += "%s --topic %s --num-records %d --record-size %d --throughput -1 " \
               "--producer-props bootstrap.servers=%s acks=all" % \
               (self.kafka.path.script("kafka-producer-perf-test.sh", node), self.topic,
                self.num_records, self.record_size, self.kafka.bootstrap_servers())
        self.kafka.run_cli_tool(node, cmd)

    def alter_configs(self, entity_type, entity_name, action, configs):
        node = self.kafka.nodes[0]
        cmd = fix_opts_for_new_jvm(node)
        cmd += "%s --alter --entity-type %s --entity-name %s --%s %s" % \
               (self.kafka.kafka_configs_cmd_with_optional_security_settings(node, False),
                entity_type, entity_name, action, configs)
        self.kafka.run_cli_tool(node, cmd)

    def throttle_replica(self, broker):
        """Throttle replication of the partition onto the given broker down to 1 byte/s."""
        self.alter_configs("brokers", broker, "add-config",
                           "leader.replication.throttled.rate=1,follower.replication.throttled.rate=1")
        self.alter_configs("topics", self.topic, "add-config",
                           "leader.replication.throttled.replicas=%d:%d,follower.replication.throttled.replicas=%d:%d" %
                           (self.partition, broker, self.partition, broker))

    def unthrottle_replica(self, broker):
        self.alter_configs("topics", self.topic, "delete-config",
                           "leader.replication.throttled.replicas,follower.replication.throttled.replicas")
        self.alter_configs("brokers", broker, "delete-config",
                           "leader.replication.throttled.rate,follower.replication.throttled.rate")

    def assert_out_of_sync_replicas_are_tracked(self):
        """Every replica outside the ISR must be either being added or being removed."""
        reassignment = self.current_reassignment()
        tracked = set()
        if reassignment is not None:
            tracked.update(reassignment["adding"])
            tracked.update(reassignment["removing"])
        out_of_sync = set(self.replicas()) - self.isr()
        assert out_of_sync <= tracked, \
            "Replicas %s are out of sync but no reassignment accounts for them: reassignment=%s" % \
            (sorted(out_of_sync), reassignment)

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_override_keeps_still_adding_replica(self, metadata_quorum):
        """Override an in-progress reassignment and check the still-adding replica is kept.
        Setup: 1 controller, 5 brokers, 1 topic with one partition assigned to brokers 1, 2, 3.

            - Produce enough data that a throttled replica cannot catch up during the test
            - Throttle replication of the partition onto broker 4 down to 1 byte/s
            - Reassign the partition to [1, 2, 4]: broker 4 is adding, broker 3 is removing
            - Override the reassignment with [1, 2, 4, 5] while broker 4 is still out of the ISR
            - Wait for broker 5 to join the ISR
            - Check that the reassignment is still in progress, still adding brokers 4 and 5 and
              still removing broker 3, and that broker 3 is still in the ISR
            - Remove the throttle and check that the reassignment completes with replicas
              [1, 2, 4, 5] all in the ISR
        """
        self.kafka.start()
        wait_until(lambda: self.isr() == {1, 2, 3},
                   timeout_sec=self.timeout_sec,
                   err_msg="Timed out waiting for the initial ISR [1, 2, 3]")
        self.produce_records()
        self.throttle_replica(self.throttled_broker)

        first = self.reassignment([1, 2, self.throttled_broker])
        self.kafka.execute_reassign_partitions(first)
        wait_until(lambda: self.current_reassignment() is not None and
                           self.current_reassignment()["adding"] == [self.throttled_broker] and
                           self.current_reassignment()["removing"] == [3],
                   timeout_sec=self.timeout_sec,
                   err_msg="Timed out waiting for the first reassignment to be in progress")
        assert self.throttled_broker not in self.isr(), \
            "Broker %d joined the ISR despite the throttle: isr=%s" % \
            (self.throttled_broker, sorted(self.isr()))

        second = self.reassignment([1, 2, self.throttled_broker, 5])
        self.kafka.execute_reassign_partitions(second, additional=True)
        wait_until(lambda: 5 in self.isr(),
                   timeout_sec=self.timeout_sec,
                   err_msg="Timed out waiting for broker 5 to join the ISR")

        # Broker 4 is still throttled, so the override must not have completed: it must still
        # list broker 4 as adding next to broker 5, and broker 3 must not have been removed yet.
        reassignment = self.current_reassignment()
        assert reassignment is not None, \
            "The reassignment completed while broker %d was still out of the ISR: replicas=%s isr=%s" % \
            (self.throttled_broker, self.replicas(), sorted(self.isr()))
        assert sorted(reassignment["adding"]) == [self.throttled_broker, 5], \
            "Expected brokers %s to be adding, got %s" % ([self.throttled_broker, 5], reassignment)
        assert reassignment["removing"] == [3], \
            "Expected broker 3 to be removing, got %s" % reassignment
        assert not self.kafka.verify_reassign_partitions(second), \
            "--verify reported the reassignment as complete while broker %d is out of the ISR" % \
            self.throttled_broker
        assert self.isr() == {1, 2, 3, 5}, \
            "Expected ISR [1, 2, 3, 5], got %s" % sorted(self.isr())
        self.assert_out_of_sync_replicas_are_tracked()

        self.unthrottle_replica(self.throttled_broker)
        wait_until(lambda: self.kafka.verify_reassign_partitions(second),
                   timeout_sec=self.timeout_sec, backoff_sec=.5,
                   err_msg="Timed out waiting for the reassignment to complete after removing the throttle")
        wait_until(lambda: self.isr() == {1, 2, self.throttled_broker, 5},
                   timeout_sec=self.timeout_sec,
                   err_msg="Timed out waiting for the final ISR [1, 2, 4, 5]")
        assert self.replicas() == [1, 2, self.throttled_broker, 5], \
            "Expected replicas [1, 2, 4, 5], got %s" % self.replicas()
        assert self.current_reassignment() is None, \
            "Expected no reassignment in progress, got %s" % self.current_reassignment()
        self.assert_out_of_sync_replicas_are_tracked()
