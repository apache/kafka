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

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Used to track source records that have been (or are about to be) dispatched to a producer and their accompanying
 * source offsets. Records are tracked in the order in which they are submitted, which should match the order they were
 * returned from {@link SourceTask#poll()}. The latest-eligible offsets for each source partition can be retrieved via
 * {@link #committableOffsets()}, the latest-eligible offsets for each source partition can be retrieved, where every
 * record up to and including the record for each returned offset has been either
 * {@link SubmittedRecord#ack() acknowledged} or {@link #remove(SubmittedRecord) removed}.
 * Note that this class is not thread-safe, though a {@link SubmittedRecord} can be
 * {@link SubmittedRecord#ack() acknowledged} from a different thread.
 */
class SubmittedRecords {

    private static final Logger log = LoggerFactory.getLogger(SubmittedRecords.class);

    // Visible for testing
    final Map<Map<String, Object>, Deque<SubmittedRecord>> records;

    public SubmittedRecords() {
        this.records = new HashMap<>();
    }

    /**
     * Enqueue a new source record before dispatching it to a producer.
     * The returned {@link SubmittedRecord} should either be {@link SubmittedRecord#ack() acknowledged} in the
     * producer callback, or {@link #remove(SubmittedRecord) removed} if the record could not be successfully
     * sent to the producer.
     * 
     * @param record the record about to be dispatched; may not be null but may have a null
     *               {@link SourceRecord#sourcePartition()} and/or {@link SourceRecord#sourceOffset()}
     * @return a {@link SubmittedRecord} that can be either {@link SubmittedRecord#ack() acknowledged} once ack'd by
     *         the producer, or {@link #remove removed} if synchronously rejected by the producer
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
        return result;
    }

    /**
     * Remove a source record and do not take it into account any longer when tracking offsets.
     * Useful if the record has been synchronously rejected by the producer.
     * @param record the {@link #submit previously-submitted} record to stop tracking; may not be null
     */
    public void remove(SubmittedRecord record) {
        Deque<SubmittedRecord> deque = records.get(record.partition());
        if (deque == null) {
            log.warn("Attempted to remove record from submitted queue for partition {}, but no records with that partition appear to have been submitted", record.partition());
            return;
        }
        deque.removeLastOccurrence(record);
        if (deque.isEmpty()) {
            records.remove(record.partition());
        }
    }

    /**
     * Clear out any acknowledged records at the head of the deques and return the latest offset for each source partition that can be committed.
     * Note that this may take some time to complete if a large number of records has built up, which may occur if a
     * Kafka partition is offline and all records targeting that partition go unacknowledged while records targeting
     * other partitions continue to be dispatched to the producer and sent successfully
     * @return the latest-possible offsets to commit for each source partition; may be empty but never null
     */
    public Map<Map<String, Object>, Map<String, Object>> committableOffsets() {
        Map<Map<String, Object>, Map<String, Object>> result = new HashMap<>();
        records.forEach((partition, queuedRecords) -> {
            if (canCommitHead(queuedRecords)) {
                Map<String, Object> offset = committableOffset(queuedRecords);
                result.put(partition, offset);
            }
        });
        // Clear out all empty deques from the map to keep it from growing indefinitely
        records.values().removeIf(Deque::isEmpty);
        return result;
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

    static class SubmittedRecord {
        private final Map<String, Object> partition;
        private final Map<String, Object> offset;
        private volatile boolean acked;

        public SubmittedRecord(Map<String, Object> partition, Map<String, Object> offset) {
            this.partition = partition;
            this.offset = offset;
            this.acked = false;
        }

        /**
         * Acknowledge this record; signals that its offset may be safely committed.
         * This is safe to be called from a different thread than what called {@link SubmittedRecords#submit(SourceRecord)}.
         */
        public void ack() {
            this.acked = true;
        }

        private boolean acked() {
            return acked;
        }

        private Map<String, Object> partition() {
            return partition;
        }

        private Map<String, Object> offset() {
            return offset;
        }
    }
}
