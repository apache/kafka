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

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Used to track source records that have been (or are about to be) dispatched to a producer and their accompanying
 * source offsets. Records are tracked in the order in which they are submitted, which should match the order they were
 * returned from {@link SourceTask#poll()}. The latest-eligible offsets for each source partition can be retrieved via
 * {@link #committableOffsets()}, where every record up to and including the record for each returned offset has been
 * either {@link SubmittedRecord#ack() acknowledged} or {@link #removeLastOccurrence(SubmittedRecord) removed}.
 * Note that this class is not thread-safe, though a {@link SubmittedRecord} can be
 * {@link SubmittedRecord#ack() acknowledged} from a different thread.
 */
class SubmittedRecords {

    private static final Logger log = LoggerFactory.getLogger(SubmittedRecords.class);

    // Visible for testing
    final Map<Map<String, Object>, Deque<SubmittedRecord>> records = new HashMap<>();
    private int numUnackedMessages = 0;
    private CountDownLatch messageDrainLatch;

    public SubmittedRecords() {
    }

    /**
     * Enqueue a new source record before dispatching it to a producer.
     * The returned {@link SubmittedRecord} should either be {@link SubmittedRecord#ack() acknowledged} in the
     * producer callback, or {@link #removeLastOccurrence(SubmittedRecord) removed} if the record could not be successfully
     * sent to the producer.
     * 
     * @param record the record about to be dispatched; may not be null but may have a null
     *               {@link SourceRecord#sourcePartition()} and/or {@link SourceRecord#sourceOffset()}
     * @return a {@link SubmittedRecord} that can be either {@link SubmittedRecord#ack() acknowledged} once ack'd by
     *         the producer, or {@link #removeLastOccurrence removed} if synchronously rejected by the producer
     */
    @SuppressWarnings("unchecked")
    public SubmittedRecord submit(SourceRecord record) {
        return submit((Map<String, Object>) record.sourcePartition(), (Map<String, Object>) record.sourceOffset());
    }

    // Convenience method for testing
    SubmittedRecord submit(Map<String, Object> partition, Map<String, Object> offset) {
        SubmittedRecord result = new SubmittedRecord(partition, offset);
        records.computeIfAbsent(result.partition(), p -> new LinkedList<>())
                .add(result);
        synchronized (this) {
            numUnackedMessages++;
        }
        return result;
    }

    /**
     * Remove a source record and do not take it into account any longer when tracking offsets.
     * Useful if the record has been synchronously rejected by the producer.
     * If multiple instances of the same {@link SubmittedRecord} have been submitted already, only the first one found
     * (traversing from the end of the deque backward) will be removed.
     * @param record the {@link #submit previously-submitted} record to stop tracking; may not be null
     * @return whether an instance of the record was removed
     */
    public boolean removeLastOccurrence(SubmittedRecord record) {
        Deque<SubmittedRecord> deque = records.get(record.partition());
        if (deque == null) {
            log.warn("Attempted to remove record from submitted queue for partition {}, but no records with that partition appear to have been submitted", record.partition());
            return false;
        }
        boolean result = deque.removeLastOccurrence(record);
        if (deque.isEmpty()) {
            records.remove(record.partition());
        }
        if (result) {
            messageAcked();
        } else {
            log.warn("Attempted to remove record from submitted queue for partition {}, but the record has not been submitted or has already been removed", record.partition());
        }
        return result;
    }

    /**
     * Clear out any acknowledged records at the head of the deques and return a {@link CommittableOffsets snapshot} of the offsets and offset metadata
     * accrued between the last time this method was invoked and now. This snapshot can be {@link CommittableOffsets#updatedWith(CommittableOffsets) combined}
     * with an existing snapshot if desired.
     * Note that this may take some time to complete if a large number of records has built up, which may occur if a
     * Kafka partition is offline and all records targeting that partition go unacknowledged while records targeting
     * other partitions continue to be dispatched to the producer and sent successfully
     * @return a fresh offset snapshot; never null
     */
    public CommittableOffsets committableOffsets() {
        Map<Map<String, Object>, Map<String, Object>> offsets = new HashMap<>();
        int totalCommittableMessages = 0;
        int totalUncommittableMessages = 0;
        int largestDequeSize = 0;
        Map<String, Object> largestDequePartition = null;
        for (Map.Entry<Map<String, Object>, Deque<SubmittedRecord>> entry : records.entrySet()) {
            Map<String, Object> partition = entry.getKey();
            Deque<SubmittedRecord> queuedRecords = entry.getValue();
            int initialDequeSize = queuedRecords.size();
            if (canCommitHead(queuedRecords)) {
                Map<String, Object> offset = committableOffset(queuedRecords);
                offsets.put(partition, offset);
            }
            int uncommittableMessages = queuedRecords.size();
            int committableMessages = initialDequeSize - uncommittableMessages;
            totalCommittableMessages += committableMessages;
            totalUncommittableMessages += uncommittableMessages;
            if (uncommittableMessages > largestDequeSize) {
                largestDequeSize = uncommittableMessages;
                largestDequePartition = partition;
            }
        }
        // Clear out all empty deques from the map to keep it from growing indefinitely
        records.values().removeIf(Deque::isEmpty);
        return new CommittableOffsets(offsets, totalCommittableMessages, totalUncommittableMessages, records.size(), largestDequeSize, largestDequePartition);
    }

    /**
     * Wait for all currently in-flight messages to be acknowledged, up to the requested timeout.
     * This method is expected to be called from the same thread that calls {@link #committableOffsets()}.
     * @param timeout the maximum time to wait
     * @param timeUnit the time unit of the timeout argument
     * @return whether all in-flight messages were acknowledged before the timeout elapsed
     */
    public boolean awaitAllMessages(long timeout, TimeUnit timeUnit) {
        // Create a new message drain latch as a local variable to avoid SpotBugs warnings about inconsistent synchronization
        // on an instance variable when invoking CountDownLatch::await outside a synchronized block
        CountDownLatch messageDrainLatch;
        synchronized (this) {
            messageDrainLatch = new CountDownLatch(numUnackedMessages);
            this.messageDrainLatch = messageDrainLatch;
        }
        try {
            return messageDrainLatch.await(timeout, timeUnit);
        } catch (InterruptedException e) {
            return false;
        }
    }

    // Note that this will return null if either there are no committable offsets for the given deque, or the latest
    // committable offset is itself null. The caller is responsible for distinguishing between the two cases.
    private Map<String, Object> committableOffset(Deque<SubmittedRecord> queuedRecords) {
        Map<String, Object> result = null;
        while (canCommitHead(queuedRecords)) {
            result = queuedRecords.poll().offset();
        }
        return result;
    }

    private boolean canCommitHead(Deque<SubmittedRecord> queuedRecords) {
        return queuedRecords.peek() != null && queuedRecords.peek().acked();
    }

    // Synchronize in order to ensure that the number of unacknowledged messages isn't modified in the middle of a call
    // to awaitAllMessages (which might cause us to decrement first, then create a new message drain latch, then count down
    // that latch here, effectively double-acking the message)
    private synchronized void messageAcked() {
        numUnackedMessages--;
        if (messageDrainLatch != null) {
            messageDrainLatch.countDown();
        }
    }

    class SubmittedRecord {
        private final Map<String, Object> partition;
        private final Map<String, Object> offset;
        private final AtomicBoolean acked;

        public SubmittedRecord(Map<String, Object> partition, Map<String, Object> offset) {
            this.partition = partition;
            this.offset = offset;
            this.acked = new AtomicBoolean(false);
        }

        /**
         * Acknowledge this record; signals that its offset may be safely committed.
         * This is safe to be called from a different thread than what called {@link SubmittedRecords#submit(SourceRecord)}.
         */
        public void ack() {
            if (this.acked.compareAndSet(false, true)) {
                messageAcked();
            }
        }

        private boolean acked() {
            return acked.get();
        }

        private Map<String, Object> partition() {
            return partition;
        }

        private Map<String, Object> offset() {
            return offset;
        }
    }

    /**
     * Contains a snapshot of offsets that can be committed for a source task and metadata for that offset commit
     * (such as the number of messages for which offsets can and cannot be committed).
     */
    static class CommittableOffsets {

        /**
         * An "empty" snapshot that contains no offsets to commit and whose metadata contains no committable or uncommitable messages.
         */
        public static final CommittableOffsets EMPTY = new CommittableOffsets(Collections.emptyMap(), 0, 0, 0, 0, null);

        private final Map<Map<String, Object>, Map<String, Object>> offsets;
        private final int numCommittableMessages;
        private final int numUncommittableMessages;
        private final int numDeques;
        private final int largestDequeSize;
        private final Map<String, Object> largestDequePartition;

        CommittableOffsets(
                Map<Map<String, Object>, Map<String, Object>> offsets,
                int numCommittableMessages,
                int numUncommittableMessages,
                int numDeques,
                int largestDequeSize,
                Map<String, Object> largestDequePartition
        ) {
            this.offsets = offsets != null ? new HashMap<>(offsets) : Collections.emptyMap();
            this.numCommittableMessages = numCommittableMessages;
            this.numUncommittableMessages = numUncommittableMessages;
            this.numDeques = numDeques;
            this.largestDequeSize = largestDequeSize;
            this.largestDequePartition = largestDequePartition;
        }

        /**
         * @return the offsets that can be committed at the time of the snapshot
         */
        public Map<Map<String, Object>, Map<String, Object>> offsets() {
            return Collections.unmodifiableMap(offsets);
        }

        /**
         * @return the number of committable messages at the time of the snapshot, where a committable message is both
         * acknowledged and not preceded by any unacknowledged messages in the deque for its source partition
         */
        public int numCommittableMessages() {
            return numCommittableMessages;
        }

        /**
         * @return the number of uncommittable messages at the time of the snapshot, where an uncommittable message
         * is either unacknowledged, or preceded in the deque for its source partition by an unacknowledged message
         */
        public int numUncommittableMessages() {
            return numUncommittableMessages;
        }

        /**
         * @return the number of non-empty deques tracking uncommittable messages at the time of the snapshot
         */
        public int numDeques() {
            return numDeques;
        }

        /**
         * @return the size of the largest deque at the time of the snapshot
         */
        public int largestDequeSize() {
            return largestDequeSize;
        }

        /**
         * Get the partition for the deque with the most uncommitted messages at the time of the snapshot.
         * @return the applicable partition, which may be null, or null if there are no uncommitted messages;
         * it is the caller's responsibility to distinguish between these two cases via {@link #hasPending()}
         */
        public Map<String, Object> largestDequePartition() {
            return largestDequePartition;
        }

        /**
         * @return whether there were any uncommittable messages at the time of the snapshot
         */
        public boolean hasPending() {
            return numUncommittableMessages > 0;
        }

        /**
         * @return whether there were any committable or uncommittable messages at the time of the snapshot
         */
        public boolean isEmpty() {
            return numCommittableMessages == 0 && numUncommittableMessages == 0 && offsets.isEmpty();
        }

        /**
         * Create a new snapshot by combining the data for this snapshot with newer data in a more recent snapshot.
         * Offsets are combined (giving precedence to the newer snapshot in case of conflict), the total number of
         * committable messages is summed across the two snapshots, and the newer snapshot's information on pending
         * messages (num deques, largest deque size, etc.) is used.
         * @param newerOffsets the newer snapshot to combine with this snapshot
         * @return the new offset snapshot containing information from this snapshot and the newer snapshot; never null
         */
        public CommittableOffsets updatedWith(CommittableOffsets newerOffsets) {
            Map<Map<String, Object>, Map<String, Object>> offsets = new HashMap<>(this.offsets);
            offsets.putAll(newerOffsets.offsets);

            return new CommittableOffsets(
                    offsets,
                    this.numCommittableMessages + newerOffsets.numCommittableMessages,
                    newerOffsets.numUncommittableMessages,
                    newerOffsets.numDeques,
                    newerOffsets.largestDequeSize,
                    newerOffsets.largestDequePartition
            );
        }
    }
}
