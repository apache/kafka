/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This tests the {@link ShareFetch} functionality.
 */
public class ShareFetchTest {

    private static final int NODE_ID = 0;
    private static final String TOPIC = "topic-a";
    private final TopicIdPartition partition0 = new TopicIdPartition(Uuid.randomUuid(), 0, TOPIC);
    private final TopicIdPartition partition1 = new TopicIdPartition(partition0.topicId(), 1, TOPIC);

    @Test
    public void testEmptyFetch() {
        ShareFetch<String, String> fetch = ShareFetch.empty();

        assertTrue(fetch.isEmpty());
        assertEquals(0, fetch.numRecords());
        assertTrue(fetch.records().isEmpty());
        assertTrue(fetch.takeAcknowledgedRecords().isEmpty());
        assertEquals(Optional.empty(), fetch.acquisitionLockTimeoutMs());
    }

    @Test
    public void testRecordsGroupedByPartition() {
        ShareFetch<String, String> fetch = ShareFetch.empty();
        fetch.add(partition0, batchWithRecords(partition0, 0, 2));
        fetch.add(partition1, batchWithRecords(partition1, 10, 3));

        assertFalse(fetch.isEmpty());
        assertEquals(5, fetch.numRecords());

        Map<TopicPartition, List<ConsumerRecord<String, String>>> records = fetch.records();
        assertEquals(2, records.size());
        assertEquals(List.of(0L, 1L), offsets(records.get(partition0.topicPartition())));
        assertEquals(List.of(10L, 11L, 12L), offsets(records.get(partition1.topicPartition())));
    }

    @Test
    public void testRecordsCombinesMultipleBatchesForPartition() {
        ShareFetch<String, String> fetch = ShareFetch.empty();
        fetch.add(partition0, batchWithRecords(partition0, 0, 2));
        fetch.add(partition0, batchWithRecords(partition0, 2, 2));

        assertEquals(4, fetch.numRecords());

        Map<TopicPartition, List<ConsumerRecord<String, String>>> records = fetch.records();
        assertEquals(1, records.size());
        assertEquals(List.of(0L, 1L, 2L, 3L), offsets(records.get(partition0.topicPartition())));
    }

    @Test
    public void testRecordsOmitsPartitionWithOnlyGaps() {
        // Partition 0 delivered real records. Partition 1 had acquired offsets which were all control
        // records, so it has gap acknowledgements to send but nothing to deliver to the application.
        ShareFetch<String, String> fetch = ShareFetch.empty();
        fetch.add(partition0, batchWithRecords(partition0, 0, 2));
        fetch.add(partition1, batchWithGaps(partition1, 5, 6));

        assertFalse(fetch.isEmpty());
        assertEquals(2, fetch.numRecords());

        // The application must not see a partition with an empty list of records.
        Map<TopicPartition, List<ConsumerRecord<String, String>>> records = fetch.records();
        assertEquals(1, records.size());
        assertEquals(List.of(0L, 1L), offsets(records.get(partition0.topicPartition())));
        assertNull(records.get(partition1.topicPartition()));

        // But the gap acknowledgements for the omitted partition are still sent.
        fetch.acknowledgeAll(AcknowledgeType.ACCEPT);
        Map<TopicIdPartition, NodeAcknowledgements> acknowledgements = fetch.takeAcknowledgedRecords();
        assertEquals(2, acknowledgements.size());
        assertEquals(2, acknowledgements.get(partition0).acknowledgements().size());
        assertEquals(AcknowledgeType.ACCEPT, acknowledgements.get(partition0).acknowledgements().get(0L));
        assertEquals(AcknowledgeType.ACCEPT, acknowledgements.get(partition0).acknowledgements().get(1L));
        assertEquals(2, acknowledgements.get(partition1).acknowledgements().size());
        assertNull(acknowledgements.get(partition1).acknowledgements().get(5L));
        assertNull(acknowledgements.get(partition1).acknowledgements().get(6L));
    }

    @Test
    public void testRecordsIncludesPartitionWithGapsAndRecordsInSeparateBatches() {
        // A gap-only batch does not hide the records from another batch for the same partition.
        ShareFetch<String, String> fetch = ShareFetch.empty();
        fetch.add(partition0, batchWithGaps(partition0, 0));
        fetch.add(partition0, batchWithRecords(partition0, 1, 2));

        assertEquals(2, fetch.numRecords());

        Map<TopicPartition, List<ConsumerRecord<String, String>>> records = fetch.records();
        assertEquals(1, records.size());
        assertEquals(List.of(1L, 2L), offsets(records.get(partition0.topicPartition())));
    }

    @Test
    public void testRecordsEmptyAfterAllAcknowledged() {
        ShareFetch<String, String> fetch = ShareFetch.empty();
        fetch.add(partition0, batchWithRecords(partition0, 0, 2));
        fetch.add(partition1, batchWithRecords(partition1, 0, 1));

        fetch.acknowledgeAll(AcknowledgeType.ACCEPT);
        Map<TopicIdPartition, NodeAcknowledgements> acknowledgements = fetch.takeAcknowledgedRecords();
        assertEquals(2, acknowledgements.size());
        assertEquals(NODE_ID, acknowledgements.get(partition0).nodeId());

        assertTrue(fetch.records().isEmpty());
        assertTrue(fetch.isEmpty());
        assertTrue(fetch.takeAcknowledgedRecords().isEmpty());
    }

    @Test
    public void testRecordsOnlyIncludesUnacknowledgedRecords() {
        ShareFetch<String, String> fetch = ShareFetch.empty();
        ShareInFlightBatch<String, String> batch = batchWithRecords(partition0, 0, 3);
        fetch.add(partition0, batch);

        List<ConsumerRecord<String, String>> delivered = fetch.records().get(partition0.topicPartition());
        fetch.acknowledge(delivered.get(0), AcknowledgeType.ACCEPT);
        fetch.acknowledge(delivered.get(2), AcknowledgeType.RELEASE);
        assertFalse(fetch.checkAllInFlightAreAcknowledged());
        fetch.takeAcknowledgedRecords();

        assertEquals(1, fetch.numRecords());
        assertEquals(List.of(1L), offsets(fetch.records().get(partition0.topicPartition())));
    }

    @Test
    public void testRecordsIsUnmodifiable() {
        ShareFetch<String, String> fetch = ShareFetch.empty();
        fetch.add(partition0, batchWithRecords(partition0, 0, 1));

        Map<TopicPartition, List<ConsumerRecord<String, String>>> records = fetch.records();
        assertThrows(UnsupportedOperationException.class, () -> records.remove(partition0.topicPartition()));
        assertThrows(UnsupportedOperationException.class, () -> records.put(partition1.topicPartition(), List.of()));
    }

    @Test
    public void testAcquisitionLockTimeoutTakenFromLatestBatch() {
        ShareFetch<String, String> fetch = ShareFetch.empty();
        fetch.add(partition0, new ShareInFlightBatch<>(NODE_ID, partition0, Optional.of(1000)));
        assertEquals(Optional.of(1000), fetch.acquisitionLockTimeoutMs());

        fetch.add(partition1, new ShareInFlightBatch<>(NODE_ID, partition1, Optional.empty()));
        assertEquals(Optional.of(1000), fetch.acquisitionLockTimeoutMs());

        fetch.add(partition1, new ShareInFlightBatch<>(NODE_ID, partition1, Optional.of(2000)));
        assertEquals(Optional.of(2000), fetch.acquisitionLockTimeoutMs());
    }

    private ShareInFlightBatch<String, String> batchWithRecords(TopicIdPartition partition, long baseOffset, int count) {
        ShareInFlightBatch<String, String> batch = new ShareInFlightBatch<>(NODE_ID, partition, Optional.empty());
        for (int i = 0; i < count; i++) {
            long offset = baseOffset + i;
            batch.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), offset, "key-" + offset, "value-" + offset));
        }
        return batch;
    }

    private ShareInFlightBatch<String, String> batchWithGaps(TopicIdPartition partition, long... offsets) {
        ShareInFlightBatch<String, String> batch = new ShareInFlightBatch<>(NODE_ID, partition, Optional.empty());
        for (long offset : offsets) {
            batch.addGap(offset);
        }
        return batch;
    }

    private static List<Long> offsets(List<ConsumerRecord<String, String>> records) {
        return records.stream().map(ConsumerRecord::offset).collect(Collectors.toList());
    }
}
