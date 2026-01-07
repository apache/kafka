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
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.server.share.fetch.AcquisitionLockTimerTask;
import org.apache.kafka.server.share.fetch.InFlightBatch;

import java.util.ArrayList;
import java.util.List;

/**
 * Strategy for batch-optimized acquisition mode.
 * <p>
 * This mode optimizes for throughput by:
 * <ul>
 *   <li>Splitting large fetches into multiple batches based on configured batch size</li>
 *   <li>Acquiring complete batches without per-record limits</li>
 *   <li>All records in the range are acquired and scheduled with acquisition locks</li>
 * </ul>
 */
public class BatchOptimizedStrategy implements AcquireStrategy {

    public static final BatchOptimizedStrategy INSTANCE = new BatchOptimizedStrategy();

    private BatchOptimizedStrategy() {
        // Singleton class.
    }

    @Override
    public boolean requiresSubsetMatch(InFlightBatch inFlightBatch, int maxRecordsToAcquire, int acquiredCount) {
        // batch_optimized mode doesn't force subset matching based on record count alone.
        // Subset matching is determined by other factors (fullMatch, offsetState, throttling).
        return false;
    }

    @Override
    public BatchCreationResult createBatches(BatchCreationContext context) {
        List<AcquiredRecords> result = new ArrayList<>();
        long currentFirstOffset = context.firstAcquiredOffset();
        long recordCount = context.lastAcquiredOffset() - context.firstAcquiredOffset() + 1;

        // Split into multiple batches if record count exceeds batch size
        if (recordCount > context.batchSize()) {
            for (RecordBatch batch : context.batches()) {
                long batchBaseOffset = batch.baseOffset();

                // Check if the batch is already past the last acquired offset then break.
                if (batchBaseOffset > context.lastAcquiredOffset()) {
                    // Break the loop and the last batch will be processed outside the loop.
                    break;
                }

                // Create new batch once the batch size is reached
                if (batchBaseOffset - currentFirstOffset >= context.batchSize()) {
                    result.add(new AcquiredRecords()
                        .setFirstOffset(currentFirstOffset)
                        .setLastOffset(batchBaseOffset - 1)
                        .setDeliveryCount((short) 1));
                    currentFirstOffset = batchBaseOffset;
                }
            }
        }

        // Add the last batch or the only batch if the batch size is greater than the records which
        // can be acquired.
        result.add(new AcquiredRecords()
            .setFirstOffset(currentFirstOffset)
            .setLastOffset(context.lastAcquiredOffset())
            .setDeliveryCount((short) 1));

        // Add all batches to cache with acquisition locks
        addBatchesToCache(context, result);

        return new BatchCreationResult(result, false);
    }

    private void addBatchesToCache(BatchCreationContext context, List<AcquiredRecords> acquiredRecordsList) {
        for (AcquiredRecords acquiredRecords : acquiredRecordsList) {
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

            context.cacheOperations().recordInFlightBatchMessageCount(
                acquiredRecords.lastOffset() - acquiredRecords.firstOffset() + 1
            );
        }
    }

    /**
     * In {@code BATCH_OPTIMIZED} mode, this function returns the original last acquired offset.
     */
    @Override
    public long effectiveLastOffset(List<AcquiredRecords> acquiredRecords, long originalLastOffset) {
        // In batch_optimized mode, return the original last offset
        return originalLastOffset;
    }

    @Override
    public boolean shouldStopSubsetAcquisition(int acquiredCount, long maxFetchRecords) {
        // batch_optimized mode doesn't have per-record limits within subset acquisition
        return false;
    }
}
