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
package org.apache.kafka.server.share.fetch.acquire;

import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.server.share.fetch.AcquisitionLockTimerTask;

/**
 * Context object containing all parameters needed for batch creation.
 */
public record BatchCreationContext(
    String memberId,
    Iterable<? extends RecordBatch> batches,
    long firstAcquiredOffset,
    long lastAcquiredOffset,
    int batchSize,
    int maxFetchRecords,
    BatchCacheOperations cacheOperations // Callback interface for operations that require SharePartition state
) {
    /**
     * Interface for operations that interact with SharePartition's cached state.
     * This allows the strategy to perform necessary state updates without
     * direct access to SharePartition internals.
     */
    public interface BatchCacheOperations {
        /**
         * Adds a new in-flight batch to the cache with acquisition lock scheduled.
         */
        void addBatchToCache(
            String memberId,
            long firstOffset,
            long lastOffset,
            AcquisitionLockTimerTask timerTask
        );

        /**
         * Adds a new in-flight batch with offset state initialized for partial acquisition.
         * Used in record_limit mode when batch exceeds max fetch records.
         */
        void addBatchWithOffsetState(
            String memberId,
            long firstOffset,
            long lastOffset,
            long acquiredLastOffset,
            int delayMs
        );

        /**
         * Schedules an acquisition lock timeout for the given offset range.
         */
        AcquisitionLockTimerTask scheduleAcquisitionLockTimeout(
            String memberId,
            long firstOffset,
            long lastOffset
        );

        /**
         * Gets the record lock duration in milliseconds.
         */
        int recordLockDurationMs();

        /**
         * Updates the findNextFetchOffset flag.
         */
        void updateFindNextFetchOffset(boolean value);

        /**
         * Records metrics for in-flight batch message count.
         */
        void recordInFlightBatchMessageCount(long count);
    }
}
