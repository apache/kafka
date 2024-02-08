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
package org.apache.kafka.coordinator.group.runtime;

import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Interface to implement a coordinator loader. A loader reads records
 * from a partition and replays them to the coordinator.
 *
 * @param <U> The type of the record.
 */
public interface CoordinatorLoader<U> extends AutoCloseable {

    /**
     * UnknownRecordTypeException is thrown when the Deserializer encounters
     * an unknown record type.
     */
    class UnknownRecordTypeException extends RuntimeException {
        private final short unknownType;

        public UnknownRecordTypeException(short unknownType) {
            super(String.format("Found an unknown record type %d", unknownType));
            this.unknownType = unknownType;
        }

        public short unknownType() {
            return unknownType;
        }
    }

    /**
     * Object that is returned as part of the future from load(). Holds the partition load time and the
     * end time.
     */
    class LoadSummary {
        private final long startTimeMs;
        private final long endTimeMs;
        private final long schedulerQueueTimeMs;
        private final long numRecords;
        private final long numBytes;

        public LoadSummary(long startTimeMs, long endTimeMs, long schedulerQueueTimeMs, long numRecords, long numBytes) {
            this.startTimeMs = startTimeMs;
            this.endTimeMs = endTimeMs;
            this.schedulerQueueTimeMs = schedulerQueueTimeMs;
            this.numRecords = numRecords;
            this.numBytes = numBytes;
        }

        public long startTimeMs() {
            return startTimeMs;
        }

        public long endTimeMs() {
            return endTimeMs;
        }

        public long schedulerQueueTimeMs() {
            return schedulerQueueTimeMs;
        }

        public long numRecords() {
            return numRecords;
        }

        public long numBytes() {
            return numBytes;
        }

        @Override
        public String toString() {
            return "LoadSummary(" +
                "startTimeMs=" + startTimeMs +
                ", endTimeMs=" + endTimeMs +
                ", schedulerQueueTimeMs=" + schedulerQueueTimeMs +
                ", numRecords=" + numRecords +
                ", numBytes=" + numBytes + ")";
        }
    }

    /**
     * Deserializer to translates bytes to T.
     *
     * @param <T> The record type.
     */
    interface Deserializer<T> {
        /**
         * Deserializes the key and the value.
         *
         * @param key   The key or null if not present.
         * @param value The value or null if not present.
         * @return The record.
         */
        T deserialize(ByteBuffer key, ByteBuffer value) throws RuntimeException;
    }

    /**
     * Loads the coordinator by reading all the records from the TopicPartition
     * and applying them to the Replayable object.
     *
     * @param tp            The TopicPartition to read from.
     * @param coordinator   The object to apply records to.
     */
    CompletableFuture<LoadSummary> load(
        TopicPartition tp,
        CoordinatorPlayback<U> coordinator
    );
}
