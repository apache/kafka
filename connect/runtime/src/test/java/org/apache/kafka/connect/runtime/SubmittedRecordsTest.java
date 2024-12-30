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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.runtime.SubmittedRecords.SubmittedRecord;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.connect.runtime.SubmittedRecords.CommittableOffsets;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SubmittedRecordsTest {

    private static final Map<String, Object> PARTITION1 = Collections.singletonMap("subreddit", "apachekafka");
    private static final Map<String, Object> PARTITION2 = Collections.singletonMap("subreddit", "adifferentvalue");
    private static final Map<String, Object> PARTITION3 = Collections.singletonMap("subreddit", "asdfqweoicus");

    private AtomicInteger offset;

    SubmittedRecords submittedRecords;

    @BeforeEach
    public void setup() {
        submittedRecords = new SubmittedRecords();
        offset = new AtomicInteger();
    }

    @Test
    public void testNoRecords() {
        CommittableOffsets committableOffsets = submittedRecords.committableOffsets();
        assertTrue(committableOffsets.isEmpty());

        committableOffsets = submittedRecords.committableOffsets();
        assertTrue(committableOffsets.isEmpty());

        committableOffsets = submittedRecords.committableOffsets();
        assertTrue(committableOffsets.isEmpty());

        assertNoRemainingDeques();
    }

    @Test
    public void testNoCommittedRecords() {
        for (int i = 0; i < 3; i++) {
            for (Map<String, Object> partition : Arrays.asList(PARTITION1, PARTITION2, PARTITION3)) {
                submittedRecords.submit(partition, newOffset());
            }
        }

        CommittableOffsets committableOffsets = submittedRecords.committableOffsets();
        assertMetadata(committableOffsets, 0, 9, 3, 3, PARTITION1, PARTITION2, PARTITION3);
        assertEquals(Collections.emptyMap(), committableOffsets.offsets());

        committableOffsets = submittedRecords.committableOffsets();
        assertMetadata(committableOffsets, 0, 9, 3, 3, PARTITION1, PARTITION2, PARTITION3);
        assertEquals(Collections.emptyMap(), committableOffsets.offsets());

        committableOffsets = submittedRecords.committableOffsets();
        assertMetadata(committableOffsets, 0, 9, 3, 3, PARTITION1, PARTITION2, PARTITION3);
        assertEquals(Collections.emptyMap(), committableOffsets.offsets());
    }

    @Test
    public void testSingleAck() {
        Map<String, Object> offset = newOffset();

        SubmittedRecord submittedRecord = submittedRecords.submit(PARTITION1, offset);
        CommittableOffsets committableOffsets = submittedRecords.committableOffsets();
        // Record has been submitted but not yet acked; cannot commit offsets for it yet
        assertFalse(committableOffsets.isEmpty());
        assertEquals(Collections.emptyMap(), committableOffsets.offsets());
        assertMetadata(committableOffsets, 0, 1, 1, 1, PARTITION1);
        assertNoEmptyDeques();

        submittedRecord.ack();
        committableOffsets = submittedRecords.committableOffsets();
        // Record has been acked; can commit offsets for it
        assertFalse(committableOffsets.isEmpty());
        assertEquals(Collections.singletonMap(PARTITION1, offset), committableOffsets.offsets());
        assertMetadataNoPending(committableOffsets, 1);

        // Everything has been ack'd and consumed; make sure that it's been cleaned up to avoid memory leaks
        assertNoRemainingDeques();

        committableOffsets = submittedRecords.committableOffsets();
        // Old offsets should be wiped
        assertEquals(Collections.emptyMap(), committableOffsets.offsets());
        assertTrue(committableOffsets.isEmpty());
    }

    @Test
    public void testMultipleAcksAcrossMultiplePartitions() {
        Map<String, Object> partition1Offset1 = newOffset();
        Map<String, Object> partition1Offset2 = newOffset();
        Map<String, Object> partition2Offset1 = newOffset();
        Map<String, Object> partition2Offset2 = newOffset();

        SubmittedRecord partition1Record1 = submittedRecords.submit(PARTITION1, partition1Offset1);
        SubmittedRecord partition1Record2 = submittedRecords.submit(PARTITION1, partition1Offset2);
        SubmittedRecord partition2Record1 = submittedRecords.submit(PARTITION2, partition2Offset1);
        SubmittedRecord partition2Record2 = submittedRecords.submit(PARTITION2, partition2Offset2);

        CommittableOffsets committableOffsets = submittedRecords.committableOffsets();
        // No records ack'd yet; can't commit any offsets
        assertEquals(Collections.emptyMap(), committableOffsets.offsets());
        assertMetadata(committableOffsets, 0, 4, 2, 2, PARTITION1, PARTITION2);
        assertNoEmptyDeques();

        partition1Record2.ack();
        committableOffsets = submittedRecords.committableOffsets();
        // One record has been ack'd, but a record that comes before it and corresponds to the same source partition hasn't been
        assertEquals(Collections.emptyMap(), committableOffsets.offsets());
        assertMetadata(committableOffsets, 0, 4, 2, 2, PARTITION1, PARTITION2);
        assertNoEmptyDeques();

        partition2Record1.ack();
        committableOffsets = submittedRecords.committableOffsets();
        // We can commit the first offset for the second partition
        assertEquals(Collections.singletonMap(PARTITION2, partition2Offset1), committableOffsets.offsets());
        assertMetadata(committableOffsets, 1, 3, 2, 2, PARTITION1);
        assertNoEmptyDeques();

        committableOffsets = submittedRecords.committableOffsets();
        // No new offsets to commit
        assertEquals(Collections.emptyMap(), committableOffsets.offsets());
        assertMetadata(committableOffsets, 0, 3, 2, 2, PARTITION1);
        assertNoEmptyDeques();

        partition1Record1.ack();
        partition2Record2.ack();

        committableOffsets = submittedRecords.committableOffsets();
        // We can commit new offsets for both partitions now
        Map<Map<String, Object>, Map<String, Object>> expectedOffsets = new HashMap<>();
        expectedOffsets.put(PARTITION1, partition1Offset2);
        expectedOffsets.put(PARTITION2, partition2Offset2);
        assertEquals(expectedOffsets, committableOffsets.offsets());
        assertMetadataNoPending(committableOffsets, 3);

        // Everything has been ack'd and consumed; make sure that it's been cleaned up to avoid memory leaks
        assertNoRemainingDeques();

        committableOffsets = submittedRecords.committableOffsets();
        // No new offsets to commit
        assertTrue(committableOffsets.isEmpty());
    }

    @Test
    public void testRemoveLastSubmittedRecord() {
        SubmittedRecord submittedRecord = submittedRecords.submit(PARTITION1, newOffset());

        CommittableOffsets committableOffsets = submittedRecords.committableOffsets();
        assertEquals(Collections.emptyMap(), committableOffsets.offsets());
        assertMetadata(committableOffsets, 0, 1, 1, 1, PARTITION1);

        assertTrue(submittedRecord.drop(), "First attempt to remove record from submitted queue should succeed");
        assertFalse(submittedRecord.drop(), "Attempt to remove already-removed record from submitted queue should fail");

        committableOffsets = submittedRecords.committableOffsets();
        // Even if SubmittedRecords::remove is broken, we haven't ack'd anything yet, so there should be no committable offsets
        assertTrue(committableOffsets.isEmpty());

        submittedRecord.ack();
        committableOffsets = submittedRecords.committableOffsets();
        // Even though the record has somehow been acknowledged, it should not be counted when collecting committable offsets
        assertTrue(committableOffsets.isEmpty());
    }

    @Test
    public void testRemoveNotLastSubmittedRecord() {
        Map<String, Object> partition1Offset = newOffset();
        Map<String, Object> partition2Offset = newOffset();

        SubmittedRecord recordToRemove = submittedRecords.submit(PARTITION1, partition1Offset);
        SubmittedRecord lastSubmittedRecord = submittedRecords.submit(PARTITION2, partition2Offset);

        CommittableOffsets committableOffsets = submittedRecords.committableOffsets();
        assertMetadata(committableOffsets, 0, 2, 2, 1, PARTITION1, PARTITION2);
        assertNoEmptyDeques();

        assertTrue(recordToRemove.drop(), "First attempt to remove record from submitted queue should succeed");

        committableOffsets = submittedRecords.committableOffsets();
        // Even if SubmittedRecords::remove is broken, we haven't ack'd anything yet, so there should be no committable offsets
        assertEquals(Collections.emptyMap(), committableOffsets.offsets());
        assertMetadata(committableOffsets, 0, 1, 1, 1, PARTITION2);
        assertNoEmptyDeques();
        // The only record for this partition has been removed; we shouldn't be tracking a deque for it anymore
        assertRemovedDeques(PARTITION1);

        recordToRemove.ack();
        committableOffsets = submittedRecords.committableOffsets();
        // Even though the record has somehow been acknowledged, it should not be counted when collecting committable offsets
        assertEquals(Collections.emptyMap(), committableOffsets.offsets());
        assertMetadata(committableOffsets, 0, 1, 1, 1, PARTITION2);
        assertNoEmptyDeques();

        lastSubmittedRecord.ack();
        committableOffsets = submittedRecords.committableOffsets();
        // Now that the last-submitted record has been ack'd, we should be able to commit its offset
        assertEquals(Collections.singletonMap(PARTITION2, partition2Offset), committableOffsets.offsets());
        assertMetadata(committableOffsets, 1, 0, 0, 0, (Map<String, Object>) null);
        assertFalse(committableOffsets.hasPending());

        // Everything has been ack'd and consumed; make sure that it's been cleaned up to avoid memory leaks
        assertNoRemainingDeques();
        committableOffsets = submittedRecords.committableOffsets();
        assertTrue(committableOffsets.isEmpty());
    }

    @Test
    public void testNullPartitionAndOffset() {
        SubmittedRecord submittedRecord = submittedRecords.submit(null, null);
        CommittableOffsets committableOffsets = submittedRecords.committableOffsets();
        assertMetadata(committableOffsets, 0, 1, 1, 1, (Map<String, Object>) null);

        submittedRecord.ack();
        committableOffsets = submittedRecords.committableOffsets();
        assertEquals(Collections.singletonMap(null, null), committableOffsets.offsets());
        assertMetadataNoPending(committableOffsets, 1);

        assertNoEmptyDeques();
    }

    @Test
    public void testAwaitMessagesNoneSubmitted() {
        assertTrue(submittedRecords.awaitAllMessages(0, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testAwaitMessagesAfterAllAcknowledged() {
        SubmittedRecord recordToAck = submittedRecords.submit(PARTITION1, newOffset());
        assertFalse(submittedRecords.awaitAllMessages(0, TimeUnit.MILLISECONDS));
        recordToAck.ack();
        assertTrue(submittedRecords.awaitAllMessages(0, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testAwaitMessagesAfterAllRemoved() {
        SubmittedRecord recordToRemove1 = submittedRecords.submit(PARTITION1, newOffset());
        SubmittedRecord recordToRemove2 = submittedRecords.submit(PARTITION1, newOffset());
        assertFalse(
                submittedRecords.awaitAllMessages(0, TimeUnit.MILLISECONDS),
                "Await should fail since neither of the in-flight records has been removed so far"
        );

        recordToRemove1.drop();
        assertFalse(
                submittedRecords.awaitAllMessages(0, TimeUnit.MILLISECONDS),
                "Await should fail since only one of the two submitted records has been removed so far"
        );

        recordToRemove1.drop();
        assertFalse(
                submittedRecords.awaitAllMessages(0, TimeUnit.MILLISECONDS),
                "Await should fail since only one of the two submitted records has been removed so far, "
                        + "even though that record has been removed twice"
        );

        recordToRemove2.drop();
        assertTrue(
                submittedRecords.awaitAllMessages(0, TimeUnit.MILLISECONDS),
                "Await should succeed since both submitted records have now been removed"
        );
    }

    @Test
    public void testAwaitMessagesTimesOut() {
        submittedRecords.submit(PARTITION1, newOffset());
        assertFalse(submittedRecords.awaitAllMessages(10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testAwaitMessagesReturnsAfterAsynchronousAck() throws Exception {
        SubmittedRecord inFlightRecord1 = submittedRecords.submit(PARTITION1, newOffset());
        SubmittedRecord inFlightRecord2 = submittedRecords.submit(PARTITION2, newOffset());

        AtomicBoolean awaitResult = new AtomicBoolean();
        CountDownLatch awaitComplete = new CountDownLatch(1);
        new Thread(() -> {
            awaitResult.set(submittedRecords.awaitAllMessages(5, TimeUnit.SECONDS));
            awaitComplete.countDown();
        }).start();

        assertTrue(
                awaitComplete.getCount() > 0,
                "Should not have finished awaiting message delivery before either in-flight record was acknowledged"
        );

        inFlightRecord1.ack();
        assertTrue(
                awaitComplete.getCount() > 0,
                "Should not have finished awaiting message delivery before one in-flight record was acknowledged"
        );

        inFlightRecord1.ack();
        assertTrue(
                awaitComplete.getCount() > 0,
                "Should not have finished awaiting message delivery before one in-flight record was acknowledged, "
                        + "even though the other record has been acknowledged twice"
        );

        inFlightRecord2.ack();
        assertTrue(
                awaitComplete.await(1, TimeUnit.SECONDS),
                "Should have finished awaiting message delivery after both in-flight records were acknowledged"
        );
        assertTrue(
                awaitResult.get(),
                "Await of in-flight messages should have succeeded"
        );
    }

    private void assertNoRemainingDeques() {
        assertEquals(Collections.emptyMap(), submittedRecords.records, "Internal records map should be completely empty");
    }

    @SafeVarargs
    private void assertRemovedDeques(Map<String, ?>... partitions) {
        for (Map<String, ?> partition : partitions) {
            assertFalse(submittedRecords.records.containsKey(partition), "Deque for partition " + partition + " should have been cleaned up from internal records map");
        }
    }

    private void assertNoEmptyDeques() {
        submittedRecords.records.forEach((partition, deque) ->
            assertFalse(deque.isEmpty(), "Empty deque for partition " + partition + " should have been cleaned up from internal records map")
        );
    }

    private Map<String, Object> newOffset() {
        return Collections.singletonMap("timestamp", offset.getAndIncrement());
    }

    private void assertMetadataNoPending(CommittableOffsets committableOffsets, int committableMessages) {
        assertEquals(committableMessages, committableOffsets.numCommittableMessages());
        assertFalse(committableOffsets.hasPending());
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private void assertMetadata(
            CommittableOffsets committableOffsets,
            int committableMessages,
            int uncommittableMessages,
            int numDeques,
            int largestDequeSize,
            Map<String, Object>... largestDequePartitions
    ) {
        assertEquals(committableMessages, committableOffsets.numCommittableMessages());
        assertEquals(uncommittableMessages, committableOffsets.numUncommittableMessages());
        assertEquals(numDeques, committableOffsets.numDeques());
        assertEquals(largestDequeSize, committableOffsets.largestDequeSize());
        assertTrue(Arrays.asList(largestDequePartitions).contains(committableOffsets.largestDequePartition()));
    }
}
