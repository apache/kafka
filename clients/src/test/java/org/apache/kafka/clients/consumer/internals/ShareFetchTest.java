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
import org.apache.kafka.clients.consumer.ShareAcknowledgementBatch;
import org.apache.kafka.clients.consumer.ShareAcknowledgements;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShareFetchTest {

    @Test
    public void testTakeAcknowledgementsForTransactionDrainsNormalAcknowledgementPath() {
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", 0));
        ShareFetch<String, String> fetch = ShareFetch.empty();
        fetch.add(topicIdPartition, inFlightBatch(topicIdPartition, 10L, 11L));

        fetch.acknowledge(record(10L), AcknowledgeType.ACCEPT);
        fetch.acknowledge(record(11L), AcknowledgeType.REJECT);

        ShareAcknowledgements transactionAcknowledgements = fetch.takeAcknowledgementsForTransaction();

        assertEquals(
            Map.of(topicIdPartition, List.of(new ShareAcknowledgementBatch(
                10L,
                11L,
                List.of(AcknowledgeType.ACCEPT.id, AcknowledgeType.REJECT.id)))),
            transactionAcknowledgements.acknowledgements());
        assertTrue(fetch.takeAcknowledgementsForTransaction().isEmpty());
        assertTrue(fetch.takeAcknowledgedRecords().isEmpty());
        assertTrue(fetch.isEmpty());
    }

    @Test
    public void testTakeAcknowledgementsForTransactionLeavesUnacknowledgedRecordsInFlight() {
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", 0));
        ShareFetch<String, String> fetch = ShareFetch.empty();
        fetch.add(topicIdPartition, inFlightBatch(topicIdPartition, 10L, 12L));

        fetch.acknowledge(record(10L), AcknowledgeType.ACCEPT);

        ShareAcknowledgements transactionAcknowledgements = fetch.takeAcknowledgementsForTransaction();

        assertEquals(
            Map.of(topicIdPartition, List.of(new ShareAcknowledgementBatch(
                10L,
                10L,
                List.of(AcknowledgeType.ACCEPT.id)))),
            transactionAcknowledgements.acknowledgements());
        assertTrue(fetch.takeAcknowledgedRecords().isEmpty());
        assertFalse(fetch.isEmpty());
    }

    @Test
    public void testTakeAcknowledgementsForTransactionPassesGapMarkerThrough() {
        // The consumer emits a gap for an acquired offset holding no non-control record, which is
        // unavoidable on any topic written by a transactional producer. It must reach the broker so
        // the offset can be archived; the broker stages it like any other terminal disposition.
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", 0));
        ShareFetch<String, String> fetch = ShareFetch.empty();
        ShareInFlightBatch<String, String> batch = new ShareInFlightBatch<>(1, topicIdPartition, Optional.empty());
        batch.addRecord(record(10L));
        batch.addGap(11L);
        batch.addRecord(record(12L));
        fetch.add(topicIdPartition, batch);

        fetch.acknowledge(record(10L), AcknowledgeType.ACCEPT);
        fetch.acknowledge(record(12L), AcknowledgeType.ACCEPT);

        ShareAcknowledgements acknowledgements = fetch.takeAcknowledgementsForTransaction();

        assertEquals(
            Map.of(topicIdPartition, List.of(new ShareAcknowledgementBatch(
                10L,
                12L,
                List.of(AcknowledgeType.ACCEPT.id, (byte) 0, AcknowledgeType.ACCEPT.id)))),
            acknowledgements.acknowledgements());
    }

    @Test
    public void testTakeAcknowledgementsForTransactionRejectsRelease() {
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", 0));
        ShareFetch<String, String> fetch = ShareFetch.empty();
        fetch.add(topicIdPartition, inFlightBatch(topicIdPartition, 10L, 11L));

        fetch.acknowledge(record(10L), AcknowledgeType.ACCEPT);
        fetch.acknowledge(record(11L), AcknowledgeType.RELEASE);

        IllegalStateException e = assertThrows(IllegalStateException.class, fetch::takeAcknowledgementsForTransaction);
        assertTrue(e.getMessage().contains(AcknowledgeType.RELEASE.toString()), e.getMessage());
        assertTrue(e.getMessage().contains("11"), e.getMessage());

        // Nothing may be drained by a rejected call, otherwise the records are stranded.
        assertFalse(fetch.isEmpty());
    }

    @Test
    public void testTakeAcknowledgementsForTransactionRejectsRenewWithoutStrandingRecords() {
        // Guards the ordering bug: hasRenewals() only becomes true once takeAcknowledgedRecords()
        // has populated renewingRecords, so a RENEW recorded on this fetch used to pass the guard,
        // reach the broker, and be rejected there - after the drain had already stranded the offset.
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", 0));
        ShareFetch<String, String> fetch = ShareFetch.empty();
        fetch.add(topicIdPartition, inFlightBatch(topicIdPartition, 10L, 11L));

        fetch.acknowledge(record(10L), AcknowledgeType.ACCEPT);
        fetch.acknowledge(record(11L), AcknowledgeType.RENEW);

        IllegalStateException e = assertThrows(IllegalStateException.class, fetch::takeAcknowledgementsForTransaction);
        assertTrue(e.getMessage().contains(AcknowledgeType.RENEW.toString()), e.getMessage());

        assertFalse(fetch.hasRenewals());
        assertFalse(fetch.isEmpty());
    }

    private static ShareInFlightBatch<String, String> inFlightBatch(
        TopicIdPartition topicIdPartition,
        long firstOffset,
        long lastOffset
    ) {
        ShareInFlightBatch<String, String> batch = new ShareInFlightBatch<>(1, topicIdPartition, Optional.empty());
        for (long offset = firstOffset; offset <= lastOffset; offset++) {
            batch.addRecord(record(offset));
        }
        return batch;
    }

    private static ConsumerRecord<String, String> record(long offset) {
        return new ConsumerRecord<>("topic", 0, offset, "key-" + offset, "value-" + offset);
    }
}
