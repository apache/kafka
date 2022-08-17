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

package org.apache.kafka.metadata.loader;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.fault.FaultHandler;
import org.slf4j.Logger;

import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;


/**
 * BatchLoader is an internal class used to apply a series of batches to a MetadataDelta object.
 * Its main function is to track information about the loading process, and give good error
 * diagnostics in the event of a failure.
 */
class BatchLoader implements Consumer<Batch<ApiMessageAndVersion>> {
    private final Logger log;
    private final Time time;
    private final FaultHandler faultHandler;
    private final MetadataDelta delta;
    private final String source;
    private final boolean fromSnapshot;
    private final long startTimeNs;
    private long numBytes;
    private Batch<ApiMessageAndVersion> firstBatch;
    private Batch<ApiMessageAndVersion> lastBatch;
    private int numBatches;
    private int numRecords;

    BatchLoader(
        Logger log,
        Time time,
        FaultHandler faultHandler,
        MetadataDelta delta,
        String source,
        boolean fromSnapshot
    ) {
        this.log = log;
        this.time = time;
        this.faultHandler = faultHandler;
        this.delta = delta;
        this.source = source;
        this.fromSnapshot = fromSnapshot;
        this.startTimeNs = time.nanoseconds();
        this.numBytes = 0L;
        this.firstBatch = null;
        this.lastBatch = null;
        this.numBatches = 0;
        this.numRecords = 0;
    }

    @Override
    public void accept(Batch<ApiMessageAndVersion> batch) {
        if (firstBatch == null) firstBatch = batch;
        lastBatch = batch;
        int recordIndex = 0;
        long baseOffset = batch.baseOffset();
        for (ApiMessageAndVersion record : batch.records()) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Loading %s batch with base offset %d, record [%d/%d]: %s",
                        source, baseOffset, recordIndex + 1, batch.records().size(), record.message()));
            }
            try {
                // All snapshot records have the same offset, whereas in the log, each record has an
                // offset which is one more than the previous one.
                long effectiveOffset = fromSnapshot ? baseOffset : baseOffset + recordIndex;
                delta.replay(effectiveOffset, batch.epoch(), record.message());
            } catch (Throwable e) {
                String message = String.format("Error loading %s batch starting at %d, " +
                        "batch %d, record [%d/%d], type %s", source, batch.baseOffset(),
                        numBatches + 1, recordIndex + 1, batch.records().size(),
                        record.message().getClass().getSimpleName());
                faultHandler.handleFault(message, e); // Attempt to continue; do not throw.
            }
            recordIndex++;
            numRecords++;
        }
        numBytes += batch.sizeInBytes();
        numBatches++;
    }

    LoadInformation finish() {
        if (firstBatch == null || lastBatch == null) {
            throw faultHandler.handleFault("You must always load at least one batch.");
        }
        long endTimeNs = time.nanoseconds();
        LoadInformation info = new LoadInformation(lastBatch.appendTimestamp(),
                firstBatch.baseOffset(),
                lastBatch.lastOffset(),
                numBatches,
                numRecords,
                MICROSECONDS.convert(endTimeNs - startTimeNs, NANOSECONDS),
                numBytes,
                fromSnapshot);
        if (log.isDebugEnabled()) {
            log.debug("Loaded some new data from {}: {}", source, info);
        }
        return info;
    }
}
