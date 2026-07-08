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

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.tests.verifiable_share_consumer_test import VerifiableShareConsumerTest


class ShareConsumerDLQTest(VerifiableShareConsumerTest):
    """System tests for share group dead-letter queues (KIP-1191).

    These tests exercise the real distributed cluster/consumer topology that the JUnit
    integration test suite (ShareConsumerDLQTest, clients-integration-tests) cannot: multiple
    brokers, real consumer processes, and end-to-end DLQ topic content verification.
    """

    TOPIC = {"name": "dlq-source-topic", "partitions": 1, "replication_factor": 1}

    num_consumers = 1
    num_producers = 1
    num_brokers = 3

    share_group_id = "dlq-test-group"
    total_messages = 300

    default_timeout_sec = 180

    def __init__(self, test_context):
        super(ShareConsumerDLQTest, self).__init__(test_context, num_consumers=self.num_consumers,
            num_producers=self.num_producers, num_zk=0, num_brokers=self.num_brokers,
            topics={self.TOPIC["name"]: {"partitions": self.TOPIC["partitions"],
                                          "replication-factor": self.TOPIC["replication_factor"]}})

        # DLQ (KIP-1191) is gated behind share.version=2, which is not yet the production
        # default (LATEST_PRODUCTION=SV_1) -- bootstrap the cluster with it explicitly. KafkaTest's
        # constructor already built a throwaway self.kafka above; nothing has started yet, but
        # ducktape allocates cluster nodes at Service.__init__ time (not at start()), so the
        # throwaway service's nodes (and its isolated controller quorum's, if any) must be freed
        # before building the replacement, or the cluster runs out of nodes.
        self.kafka.free()
        if self.kafka.isolated_controller_quorum:
            self.kafka.isolated_controller_quorum.free()
        self.kafka = KafkaService(test_context, self.num_brokers, self.zk, topics=self.topics,
                                   controller_num_nodes_override=self.num_zk, share_version="2")

    def create_dlq_topic(self, name):
        self.kafka.create_topic({
            "topic": name,
            "partitions": 1,
            "replication-factor": 1,
            "configs": {"errors.deadletterqueue.group.enable": "true"}
        })

    def setup_dlq_group_config(self, dlq_topic, copy_record_enable=None):
        wait_until(lambda: self.kafka.set_share_group_offset_reset_strategy(group=self.share_group_id, strategy="earliest"),
                   timeout_sec=20, backoff_sec=2, err_msg="share.auto.offset.reset not set to earliest")
        wait_until(lambda: self.kafka.set_share_group_dlq_config(group=self.share_group_id, topic_name=dlq_topic,
                                                                  copy_record_enable=copy_record_enable),
                   timeout_sec=20, backoff_sec=2, err_msg="DLQ config not applied to share group")

    def read_dlq_topic(self, dlq_topic, min_messages, timeout_sec=None):
        """Consume dlq_topic with a plain (non-share) VerifiableConsumer and return the collected record_data events."""
        timeout_sec = timeout_sec or self.default_timeout_sec
        dlq_records = []

        def on_dlq_record(event, node):
            dlq_records.append(event)

        dlq_consumer = VerifiableConsumer(self.test_context, 1, self.kafka, dlq_topic,
                                           group_id="%s-dlq-verifier" % self.share_group_id,
                                           on_record_consumed=on_dlq_record)
        dlq_consumer.start()
        wait_until(lambda: dlq_consumer.total_consumed() >= min_messages, timeout_sec=timeout_sec,
                   err_msg="Timed out waiting to consume %d records from DLQ topic %s" % (min_messages, dlq_topic))
        dlq_consumer.stop_all()
        return dlq_records

    @cluster(num_nodes=10)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_single_partition_dlq_reject(self, metadata_quorum=quorum.isolated_kraft):
        """Every record is REJECTed and must be written to the DLQ topic exactly once, headers-only
        (copy-record left at its default of false)."""
        dlq_topic = "dlq.reject-single"
        self.create_dlq_topic(dlq_topic)
        self.setup_dlq_group_config(dlq_topic)

        producer = self.setup_producer(self.TOPIC["name"], max_messages=self.total_messages)
        consumer = self.setup_share_group(self.TOPIC["name"], group_id=self.share_group_id,
                                           acknowledgement_mode="sync", ack_pattern=["reject"])

        producer.start()
        self.await_produced_messages(producer, min_messages=self.total_messages, timeout_sec=self.default_timeout_sec)

        consumer.start()
        self.await_all_members(consumer, timeout_sec=self.default_timeout_sec)

        wait_until(lambda: consumer.total_rejected() >= self.total_messages, timeout_sec=self.default_timeout_sec,
                   err_msg="Timed out waiting for all records to be rejected")

        producer.stop()
        consumer.stop_all()

        dlq_records = self.read_dlq_topic(dlq_topic, self.total_messages)
        assert len(dlq_records) == self.total_messages
        assert all(record["value"] is None for record in dlq_records), \
            "Expected DLQ records to carry no value when copy-record is disabled"

    @cluster(num_nodes=10)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_single_partition_dlq_release(self, metadata_quorum=quorum.isolated_kraft):
        """Every record is RELEASEd repeatedly; once its delivery count exceeds the (lowered) limit
        it must be written to the DLQ topic."""
        dlq_topic = "dlq.release-single"
        delivery_count_limit = 2
        self.create_dlq_topic(dlq_topic)
        self.setup_dlq_group_config(dlq_topic)
        wait_until(lambda: self.kafka.set_share_group_delivery_count_limit(group=self.share_group_id, limit=delivery_count_limit),
                   timeout_sec=20, backoff_sec=2, err_msg="group.share.delivery.count.limit not set")

        producer = self.setup_producer(self.TOPIC["name"], max_messages=self.total_messages)
        consumer = self.setup_share_group(self.TOPIC["name"], group_id=self.share_group_id,
                                           acknowledgement_mode="sync", ack_pattern=["release"])

        producer.start()
        self.await_produced_messages(producer, min_messages=self.total_messages, timeout_sec=self.default_timeout_sec)

        consumer.start()
        self.await_all_members(consumer, timeout_sec=self.default_timeout_sec)

        producer.stop()

        # Assert via DLQ topic content, not consumer-side release counts, since RELEASE keeps
        # redelivering (and re-releasing) the same offsets until the delivery count is exceeded.
        # The consumer must stay running while we wait: it's what drives the redelivery cycles
        # that eventually push each record over the delivery count limit and into the DLQ.
        dlq_records = self.read_dlq_topic(dlq_topic, self.total_messages, timeout_sec=self.default_timeout_sec * 2)
        assert len(dlq_records) == self.total_messages

        consumer.stop_all()

    @cluster(num_nodes=10)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_single_partition_dlq_mixed(self, metadata_quorum=quorum.isolated_kraft):
        """Records are cycled reject/release/accept by (offset % 3); reject+release offsets must
        eventually land in the DLQ topic with the original value copied (copy-record enabled),
        while accept offsets never reach the DLQ."""
        dlq_topic = "dlq.mixed-single"
        delivery_count_limit = 2
        self.create_dlq_topic(dlq_topic)
        self.setup_dlq_group_config(dlq_topic, copy_record_enable=True)
        wait_until(lambda: self.kafka.set_share_group_delivery_count_limit(group=self.share_group_id, limit=delivery_count_limit),
                   timeout_sec=20, backoff_sec=2, err_msg="group.share.delivery.count.limit not set")

        producer = self.setup_producer(self.TOPIC["name"], max_messages=self.total_messages)
        consumer = self.setup_share_group(self.TOPIC["name"], group_id=self.share_group_id,
                                           acknowledgement_mode="sync", ack_pattern=["reject", "release", "accept"])

        producer.start()
        self.await_produced_messages(producer, min_messages=self.total_messages, timeout_sec=self.default_timeout_sec)

        consumer.start()
        self.await_all_members(consumer, timeout_sec=self.default_timeout_sec)

        expected_dlq_count = sum(1 for offset in range(self.total_messages) if offset % 3 != 2)
        expected_accepted_count = self.total_messages - expected_dlq_count

        wait_until(lambda: consumer.total_accepted() >= expected_accepted_count, timeout_sec=self.default_timeout_sec,
                   err_msg="Timed out waiting for all accept-pattern records to be accepted")

        producer.stop()

        # The consumer must stay running while we wait: released offsets only reach the DLQ after
        # repeated redelivery, which the accepted-count check above does not guarantee has finished.
        dlq_records = self.read_dlq_topic(dlq_topic, expected_dlq_count, timeout_sec=self.default_timeout_sec * 2)
        assert len(dlq_records) == expected_dlq_count

        # With copy-record enabled, verify DLQ records carry the actual original values, not just
        # some non-null value: since offsets map 1:1 to VerifiableProducer's sequential int values on
        # this single-partition topic, cross-check against the real values the producer sent rather
        # than trusting the DLQ content blindly.
        expected_dlq_values = {value for value in producer.acked_values if value % 3 != 2}
        actual_dlq_values = {int(record["value"]) for record in dlq_records}
        assert actual_dlq_values == expected_dlq_values, \
            "DLQ record values did not match the original produced values (copy-record enabled)"

        consumer.stop_all()
