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

import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.server.share.fetch.AcquisitionLockTimerTask;
import org.apache.kafka.server.share.fetch.InFlightBatch;

import java.util.List;

/**
 * Strategy for record-limit acquisition mode.
 * <p>
 * This mode provides precise control over the number of records acquired by:
 * <ul>
 *   <li>Creating a single batch containing all records</li>
 *   <li>Acquiring only up to maxFetchRecords, leaving remaining offsets AVAILABLE</li>
 *   <li>Initializing offset state when batch exceeds the limit</li>
 * </ul>
 */
public class RecordLimitStrategy implements AcquireStrategy {

    public static final RecordLimitStrategy INSTANCE = new RecordLimitStrategy();

    private RecordLimitStrategy() {
        // Singleton class.
    }

    @Override
    public boolean requiresSubsetMatch(InFlightBatch inFlightBatch, int maxRecordsToAcquire, int acquiredCount) {
        // In record_limit mode, check if batch has more records than can be acquired
        long numRecordsInBatch = inFlightBatch.lastOffset() - inFlightBatch.firstOffset() + 1;
        int numRecordsRemaining = maxRecordsToAcquire - acquiredCount;
        return numRecordsInBatch > numRecordsRemaining;
    }

    @Override
    public BatchCreationResult createBatches(BatchCreationContext context) {
        // In record_limit mode, always create a single batch
        AcquiredRecords acquiredRecords = new AcquiredRecords()
            .setFirstOffset(context.firstAcquiredOffset())
            .setLastOffset(context.lastAcquiredOffset())
            .setDeliveryCount((short) 1);

        long recordsInBatch = acquiredRecords.lastOffset() - acquiredRecords.firstOffset() + 1;

        // When batch exceeds max fetch limit, only acquire up to the limit
        // and leave remaining offsets in AVAILABLE state i.e. acquired records are 10-19 (10 records) and max fetch
        // records is 5, then only 10-14 should be acquired and offset 15-19 should still in available state.
        if (recordsInBatch > context.maxFetchRecords()) {
            return createPartiallyAcquiredBatch(context, acquiredRecords);
        }

        // Batch fits within limit - add normally
        AcquisitionLockTimerTask timerTask = context.cacheOperations()
            .scheduleAcquisitionLockTimeout(
                context.memberId(),
                acquiredRecords.firstOffset(),
                acquiredRecords.lastOffset()
            );

        context.cacheOperations().addBatchToCache(
            context.memberId(),
            acquiredRecords.firstOffset(),
            acquiredRecords.lastOffset(),
            timerTask
        );

        context.cacheOperations().recordInFlightBatchMessageCount(recordsInBatch);

        return new BatchCreationResult(List.of(acquiredRecords), false);
    }

    /**
     * Creates a batch where only the first maxFetchRecords are acquired,
     * and the remaining offsets are left in AVAILABLE state.
     */
    private BatchCreationResult createPartiallyAcquiredBatch(
        BatchCreationContext context,
        AcquiredRecords acquiredRecords
    ) {
        int delayMs = context.cacheOperations().recordLockDurationMs();
        long acquiredLastOffset = acquiredRecords.firstOffset() + context.maxFetchRecords() - 1;

        // Add batch with offset state initialized - only first maxFetchRecords are ACQUIRED,
        // remaining are AVAILABLE
        context.cacheOperations().addBatchWithOffsetState(
            context.memberId(),
            acquiredRecords.firstOffset(),
            acquiredRecords.lastOffset(),
            acquiredLastOffset,
            delayMs
        );

        context.cacheOperations().recordInFlightBatchMessageCount(
            acquiredRecords.lastOffset() - acquiredRecords.firstOffset() + 1
        );

        // Update the acquired records to reflect only what was actually acquired
        acquiredRecords.setLastOffset(acquiredLastOffset);

        // Signal that findNextFetchOffset should be updated since we have AVAILABLE records
        return new BatchCreationResult(List.of(acquiredRecords), true);
    }

    /**
     * This function returns the actual last offset from the acquired
     * records (which may be less than the original last offset).
     */
    @Override
    public long effectiveLastOffset(List<AcquiredRecords> acquiredRecords, long originalLastOffset) {
        // In record_limit mode, return the actual last offset from acquired records.
        // This is needed to calculate the correct acquired count.
        return acquiredRecords.get(0).lastOffset();
    }

    @Override
    public boolean shouldStopSubsetAcquisition(int acquiredCount, long maxFetchRecords) {
        // In record_limit mode, stop when exactly maxFetchRecords have been acquired
        return acquiredCount >= maxFetchRecords;
    }
}
