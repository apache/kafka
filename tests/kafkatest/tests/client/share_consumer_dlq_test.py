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
    TOPIC_MULTI = {"name": "dlq-source-multi", "partitions": 3, "replication_factor": 3}
    TOPIC_MULTI_A = {"name": "dlq-source-multi-a", "partitions": 3, "replication_factor": 3}
    TOPIC_MULTI_B = {"name": "dlq-source-multi-b", "partitions": 2, "replication_factor": 3}

    num_consumers = 1
    num_producers = 1
    num_brokers = 3

    share_group_id = "dlq-test-group"
    total_messages = 50

    default_timeout_sec = 180

    def __init__(self, test_context):
        topics = {}
        for topic in (self.TOPIC, self.TOPIC_MULTI, self.TOPIC_MULTI_A, self.TOPIC_MULTI_B):
            topics[topic["name"]] = {"partitions": topic["partitions"],
                                      "replication-factor": topic["replication_factor"]}

        super(ShareConsumerDLQTest, self).__init__(test_context, num_consumers=self.num_consumers,
            num_producers=self.num_producers, num_zk=0, num_brokers=self.num_brokers,
            topics=topics)

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

    def create_dlq_topic(self, name, partitions=1, replication_factor=1):
        self.kafka.create_topic({
            "topic": name,
            "partitions": partitions,
            "replication-factor": replication_factor,
            "configs": {"errors.deadletterqueue.group.enable": "true"}
        })

    def dlq_partition_counts(self, dlq_topic):
        """Return {partition: record_count} for dlq_topic, from each partition's latest offset
        (these DLQ topics are freshly created for each test, so offset == record count).
        """
        counts = {}
        for line in self.kafka.get_offset_shell(topic=dlq_topic, time="latest").strip().split("\n"):
            if not line:
                continue
            _, partition, offset = line.rsplit(":", 2)
            counts[int(partition)] = int(offset)
        return counts

    def expected_dlq_partition_counts(self, dlq_counts_by_source_partition, num_dlq_partitions):
        """Compute the expected number of DLQ records per DLQ partition from the number of records
        actually DLQ'd on each source partition and the source_partition % num_dlq_partitions
        routing rule.
        """
        counts = {}
        for source_partition, dlq_count in dlq_counts_by_source_partition.items():
            dlq_partition = source_partition % num_dlq_partitions
            counts[dlq_partition] = counts.get(dlq_partition, 0) + dlq_count
        return counts

    def assert_dlq_partition_counts(self, dlq_topic, expected_counts_by_dlq_partition):
        """Assert the number of records that actually landed in each DLQ partition matches
        expected_counts_by_dlq_partition, validating source_partition % num_dlq_partitions routing
        purely from observable per-partition record counts (no per-record header access needed).
        """
        actual_counts = self.dlq_partition_counts(dlq_topic)
        for partition in set(expected_counts_by_dlq_partition) | set(actual_counts):
            expected = expected_counts_by_dlq_partition.get(partition, 0)
            actual = actual_counts.get(partition, 0)
            assert actual == expected, \
                "DLQ topic %s partition %d has %d records, expected %d" % \
                (dlq_topic, partition, actual, expected)

    def merge_partition_counts(self, *count_dicts):
        """Sum multiple {partition: count} dicts (e.g. one per source topic sharing a DLQ topic)
        into a single combined {partition: count} dict."""
        merged = {}
        for counts in count_dicts:
            for partition, count in counts.items():
                merged[partition] = merged.get(partition, 0) + count
        return merged

    def expected_mixed_dlq_outcome(self, producer):
        """For a producer whose consumer uses the reject/release/accept-cycling ack pattern, compute
        the expected DLQ'd value set and accepted count from the producer's actual per-partition ack
        order: (offset % 3) is evaluated per-partition, not globally, since each partition has its
        own independent offset sequence starting at 0.
        """
        dlq_values = set()
        accepted_count = 0
        for values in producer.acked_values_by_partition.values():
            for offset, value in enumerate(values):
                if offset % 3 != 2:
                    dlq_values.add(value)
                else:
                    accepted_count += 1
        return dlq_values, accepted_count

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
    @matrix(metadata_quorum=[quorum.isolated_kraft, quorum.combined_kraft])
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
    @matrix(metadata_quorum=[quorum.isolated_kraft, quorum.combined_kraft])
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
    @matrix(metadata_quorum=[quorum.isolated_kraft, quorum.combined_kraft])
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

    @cluster(num_nodes=10)
    @matrix(metadata_quorum=[quorum.isolated_kraft, quorum.combined_kraft], num_dlq_partitions=[3, 1])
    def test_multi_partition_dlq_reject(self, metadata_quorum=quorum.isolated_kraft, num_dlq_partitions=3):
        """Every record on a 3-partition source topic is REJECTed. Verifies both DLQ topic content
        and per-record routing (source_partition % num_dlq_partitions) -- num_dlq_partitions=3 covers
        1:1 routing, num_dlq_partitions=1 covers the modulus-collapsing case."""
        dlq_topic = "dlq.reject-multi-partition-%d" % num_dlq_partitions
        self.create_dlq_topic(dlq_topic, partitions=num_dlq_partitions, replication_factor=3)
        self.setup_dlq_group_config(dlq_topic)

        producer = self.setup_producer(self.TOPIC_MULTI["name"], max_messages=self.total_messages)
        consumer = self.setup_share_group(self.TOPIC_MULTI["name"], group_id=self.share_group_id,
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

        dlq_counts_by_source_partition = {tp.partition: len(values) for tp, values in producer.acked_values_by_partition.items()}
        expected_counts = self.expected_dlq_partition_counts(dlq_counts_by_source_partition, num_dlq_partitions)
        self.assert_dlq_partition_counts(dlq_topic, expected_counts)

    @cluster(num_nodes=10)
    @matrix(metadata_quorum=[quorum.isolated_kraft, quorum.combined_kraft])
    def test_multi_partition_dlq_release(self, metadata_quorum=quorum.isolated_kraft):
        """Same as test_single_partition_dlq_release but on a 3-partition source topic."""
        dlq_topic = "dlq.release-multi-partition"
        num_dlq_partitions = 3
        delivery_count_limit = 2
        self.create_dlq_topic(dlq_topic, partitions=num_dlq_partitions, replication_factor=3)
        self.setup_dlq_group_config(dlq_topic)
        wait_until(lambda: self.kafka.set_share_group_delivery_count_limit(group=self.share_group_id, limit=delivery_count_limit),
                   timeout_sec=20, backoff_sec=2, err_msg="share.delivery.count.limit not set")

        producer = self.setup_producer(self.TOPIC_MULTI["name"], max_messages=self.total_messages)
        consumer = self.setup_share_group(self.TOPIC_MULTI["name"], group_id=self.share_group_id,
                                           acknowledgement_mode="sync", ack_pattern=["release"])

        producer.start()
        self.await_produced_messages(producer, min_messages=self.total_messages, timeout_sec=self.default_timeout_sec)

        consumer.start()
        self.await_all_members(consumer, timeout_sec=self.default_timeout_sec)

        producer.stop()

        dlq_records = self.read_dlq_topic(dlq_topic, self.total_messages, timeout_sec=self.default_timeout_sec * 2)
        assert len(dlq_records) == self.total_messages

        dlq_counts_by_source_partition = {tp.partition: len(values) for tp, values in producer.acked_values_by_partition.items()}
        expected_counts = self.expected_dlq_partition_counts(dlq_counts_by_source_partition, num_dlq_partitions)
        self.assert_dlq_partition_counts(dlq_topic, expected_counts)

        consumer.stop_all()

    @cluster(num_nodes=10)
    @matrix(metadata_quorum=[quorum.isolated_kraft, quorum.combined_kraft])
    def test_multi_partition_dlq_mixed(self, metadata_quorum=quorum.isolated_kraft):
        """Same as test_single_partition_dlq_mixed but on a 3-partition source topic. The expected
        DLQ set/count is computed per-partition from the producer's actual per-partition ack order
        (offset % 3 is evaluated per-partition, not globally, since each partition has its own
        independent offset sequence starting at 0)."""
        dlq_topic = "dlq.mixed-multi-partition"
        num_dlq_partitions = 3
        delivery_count_limit = 2
        self.create_dlq_topic(dlq_topic, partitions=num_dlq_partitions, replication_factor=3)
        self.setup_dlq_group_config(dlq_topic, copy_record_enable=True)
        wait_until(lambda: self.kafka.set_share_group_delivery_count_limit(group=self.share_group_id, limit=delivery_count_limit),
                   timeout_sec=20, backoff_sec=2, err_msg="share.delivery.count.limit not set")

        producer = self.setup_producer(self.TOPIC_MULTI["name"], max_messages=self.total_messages)
        consumer = self.setup_share_group(self.TOPIC_MULTI["name"], group_id=self.share_group_id,
                                           acknowledgement_mode="sync", ack_pattern=["reject", "release", "accept"])

        producer.start()
        self.await_produced_messages(producer, min_messages=self.total_messages, timeout_sec=self.default_timeout_sec)

        consumer.start()
        self.await_all_members(consumer, timeout_sec=self.default_timeout_sec)

        expected_dlq_values, expected_accepted_count = self.expected_mixed_dlq_outcome(producer)

        wait_until(lambda: consumer.total_accepted() >= expected_accepted_count, timeout_sec=self.default_timeout_sec,
                   err_msg="Timed out waiting for all accept-pattern records to be accepted")

        producer.stop()

        dlq_records = self.read_dlq_topic(dlq_topic, len(expected_dlq_values), timeout_sec=self.default_timeout_sec * 2)
        assert len(dlq_records) == len(expected_dlq_values)

        actual_dlq_values = {int(record["value"]) for record in dlq_records}
        assert actual_dlq_values == expected_dlq_values, \
            "DLQ record values did not match the original produced values (copy-record enabled)"

        dlq_counts_by_source_partition = {
            tp.partition: sum(1 for offset in range(len(values)) if offset % 3 != 2)
            for tp, values in producer.acked_values_by_partition.items()
        }
        expected_counts = self.expected_dlq_partition_counts(dlq_counts_by_source_partition, num_dlq_partitions)
        self.assert_dlq_partition_counts(dlq_topic, expected_counts)

        consumer.stop_all()

    @cluster(num_nodes=11)
    @matrix(metadata_quorum=[quorum.isolated_kraft, quorum.combined_kraft])
    def test_multi_topic_dlq_reject(self, metadata_quorum=quorum.isolated_kraft):
        """Two source topics share one share group and one DLQ topic; every record on both is
        REJECTed. Two separate VerifiableShareConsumer members join the group, each subscribed to
        just one of the two topics. Verifies DLQ content is correctly attributed to its source
        topic via the __dlq.errors.topic header."""
        dlq_topic = "dlq.reject-multi-topic"
        num_dlq_partitions = 3
        self.create_dlq_topic(dlq_topic, partitions=num_dlq_partitions, replication_factor=3)
        self.setup_dlq_group_config(dlq_topic)

        producer_a = self.setup_producer(self.TOPIC_MULTI_A["name"], max_messages=self.total_messages)
        producer_b = self.setup_producer(self.TOPIC_MULTI_B["name"], max_messages=self.total_messages)
        consumer_a = self.setup_share_group(self.TOPIC_MULTI_A["name"], group_id=self.share_group_id,
                                             acknowledgement_mode="sync", ack_pattern=["reject"])
        consumer_b = self.setup_share_group(self.TOPIC_MULTI_B["name"], group_id=self.share_group_id,
                                             acknowledgement_mode="sync", ack_pattern=["reject"])

        producer_a.start()
        producer_b.start()
        self.await_produced_messages(producer_a, min_messages=self.total_messages, timeout_sec=self.default_timeout_sec)
        self.await_produced_messages(producer_b, min_messages=self.total_messages, timeout_sec=self.default_timeout_sec)

        consumer_a.start()
        consumer_b.start()
        self.await_all_members(consumer_a, timeout_sec=self.default_timeout_sec)
        self.await_all_members(consumer_b, timeout_sec=self.default_timeout_sec)

        wait_until(lambda: consumer_a.total_rejected() >= self.total_messages,
                   timeout_sec=self.default_timeout_sec, err_msg="Timed out waiting for all records on topic A to be rejected")
        wait_until(lambda: consumer_b.total_rejected() >= self.total_messages,
                   timeout_sec=self.default_timeout_sec, err_msg="Timed out waiting for all records on topic B to be rejected")

        producer_a.stop()
        producer_b.stop()
        consumer_a.stop_all()
        consumer_b.stop_all()

        dlq_records = self.read_dlq_topic(dlq_topic, 2 * self.total_messages)
        assert len(dlq_records) == 2 * self.total_messages

        counts_a = {tp.partition: len(values) for tp, values in producer_a.acked_values_by_partition.items()}
        counts_b = {tp.partition: len(values) for tp, values in producer_b.acked_values_by_partition.items()}
        expected_counts = self.merge_partition_counts(
            self.expected_dlq_partition_counts(counts_a, num_dlq_partitions),
            self.expected_dlq_partition_counts(counts_b, num_dlq_partitions))
        self.assert_dlq_partition_counts(dlq_topic, expected_counts)

    @cluster(num_nodes=11)
    @matrix(metadata_quorum=[quorum.isolated_kraft, quorum.combined_kraft])
    def test_multi_topic_dlq_release(self, metadata_quorum=quorum.isolated_kraft):
        """Same as test_multi_topic_dlq_reject but with every record RELEASEd repeatedly on both
        topics until it exceeds the (lowered) delivery count limit."""
        dlq_topic = "dlq.release-multi-topic"
        num_dlq_partitions = 3
        delivery_count_limit = 2
        self.create_dlq_topic(dlq_topic, partitions=num_dlq_partitions, replication_factor=3)
        self.setup_dlq_group_config(dlq_topic)
        wait_until(lambda: self.kafka.set_share_group_delivery_count_limit(group=self.share_group_id, limit=delivery_count_limit),
                   timeout_sec=20, backoff_sec=2, err_msg="share.delivery.count.limit not set")

        producer_a = self.setup_producer(self.TOPIC_MULTI_A["name"], max_messages=self.total_messages)
        producer_b = self.setup_producer(self.TOPIC_MULTI_B["name"], max_messages=self.total_messages)
        consumer_a = self.setup_share_group(self.TOPIC_MULTI_A["name"], group_id=self.share_group_id,
                                             acknowledgement_mode="sync", ack_pattern=["release"])
        consumer_b = self.setup_share_group(self.TOPIC_MULTI_B["name"], group_id=self.share_group_id,
                                             acknowledgement_mode="sync", ack_pattern=["release"])

        producer_a.start()
        producer_b.start()
        self.await_produced_messages(producer_a, min_messages=self.total_messages, timeout_sec=self.default_timeout_sec)
        self.await_produced_messages(producer_b, min_messages=self.total_messages, timeout_sec=self.default_timeout_sec)

        consumer_a.start()
        consumer_b.start()
        self.await_all_members(consumer_a, timeout_sec=self.default_timeout_sec)
        self.await_all_members(consumer_b, timeout_sec=self.default_timeout_sec)

        producer_a.stop()
        producer_b.stop()

        # Both consumers must stay running while we wait: it's what drives the redelivery cycles
        # that eventually push each record over the delivery count limit and into the DLQ.
        dlq_records = self.read_dlq_topic(dlq_topic, 2 * self.total_messages, timeout_sec=self.default_timeout_sec * 2)
        assert len(dlq_records) == 2 * self.total_messages

        counts_a = {tp.partition: len(values) for tp, values in producer_a.acked_values_by_partition.items()}
        counts_b = {tp.partition: len(values) for tp, values in producer_b.acked_values_by_partition.items()}
        expected_counts = self.merge_partition_counts(
            self.expected_dlq_partition_counts(counts_a, num_dlq_partitions),
            self.expected_dlq_partition_counts(counts_b, num_dlq_partitions))
        self.assert_dlq_partition_counts(dlq_topic, expected_counts)

        consumer_a.stop_all()
        consumer_b.stop_all()
