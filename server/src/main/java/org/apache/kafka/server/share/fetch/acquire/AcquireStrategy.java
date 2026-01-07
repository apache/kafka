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
import org.apache.kafka.server.share.fetch.InFlightBatch;

import java.util.List;

/**
 * Strategy interface for acquiring records in different modes.
 * <p>
 * Two modes are supported:
 * <ul>
 *   <li>{@code BATCH_OPTIMIZED} - Acquires records in batches optimized for throughput,
 *       potentially splitting large fetches into multiple batches based on batch size.</li>
 *   <li>{@code RECORD_LIMIT} - Acquires up to the requested number of records,
 *       creating a single batch only.</li>
 * </ul>
 */
public interface AcquireStrategy {

    /**
     * Determines if the in-flight batch requires subset matching based on the acquisition mode
     * and remaining records to acquire.
     * <p>
     * In {@code RECORD_LIMIT} mode, this returns true if the batch contains more records
     * than can be acquired. In {@code BATCH_OPTIMIZED} mode, this always returns false
     * as the mode doesn't force subset matching based on record count alone.
     *
     * @param inFlightBatch      The in-flight batch to evaluate.
     * @param maxRecordsToAcquire The maximum total records that can be acquired in this request.
     * @param acquiredCount      The number of records already acquired.
     * @return True if subset matching is required, false otherwise.
     */
    boolean requiresSubsetMatch(InFlightBatch inFlightBatch, int maxRecordsToAcquire, int acquiredCount);

    /**
     * Creates acquired record batches from the given offset range.
     *
     * @param context The batch creation context.
     * @return The result containing the list of acquired records and any side effects.
     */
    BatchCreationResult createBatches(BatchCreationContext context);

    /**
     * Calculates the effective last offset to return to the caller after batch creation.
     *
     * @param acquiredRecords      The list of acquired records created.
     * @param originalLastOffset   The original last offset before batch creation.
     * @return The effective last offset to use for count calculations.
     */
    long effectiveLastOffset(List<AcquiredRecords> acquiredRecords, long originalLastOffset);

    /**
     * Checks if acquisition should stop after acquiring records from a subset batch.
     *
     * @param acquiredCount   The number of records acquired from the current batch.
     * @param maxFetchRecords The maximum number of records to acquire.
     * @return True if acquisition should stop, false otherwise.
     */
    boolean shouldStopSubsetAcquisition(int acquiredCount, long maxFetchRecords);
}
