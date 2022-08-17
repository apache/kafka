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

package org.apache.kafka.metadata.publisher;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.loader.MetadataLoader;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.SnapshotWriter;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;


public class MetadataSnapshotter implements Consumer<SnapshotCreationRequest>, AutoCloseable {
    /**
     * The maximum number of records we will put in each batch.
     *
     * From the perspective of the Raft layer, the limit on batch size is specified in terms of
     * bytes, not number of records. @See {@link KafkaRaftClient#MAX_BATCH_SIZE_BYTES} for details.
     * However, it's more convenient to limit the batch size here in terms of number of records.
     * So we chose a low number that will not cause problems.
     */
    private static final int MAX_RECORDS_PER_BATCH = 1024;

    public static class Builder {
        private Time time = Time.SYSTEM;
        private LogContext logContext = null;
        private String threadNamePrefix = "";
        private Function<SnapshotCreationRequest, SnapshotWriter<ApiMessageAndVersion>> writerCreator = null;
        private Supplier<String> suppressionReasonSupplier = () -> "";

        public Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        public Builder setThreadNamePrefix(String threadNamePrefix) {
            this.threadNamePrefix = threadNamePrefix;
            return this;
        }

        public Builder setWriterCreator(Function<SnapshotCreationRequest, SnapshotWriter<ApiMessageAndVersion>> writerCreator) {
            this.writerCreator = writerCreator;
            return this;
        }

        public Builder setWriterCreator(RaftClient<ApiMessageAndVersion> raftClient) {
            this.writerCreator = request -> {
                Optional<SnapshotWriter<ApiMessageAndVersion>> writer = raftClient.createSnapshot(
                        request.offset(), request.epoch(), request.lastContainedLogTimeMs());
                if (!writer.isPresent()) {
                    throw new RuntimeException("the committed offset is greater than the " +
                            "high water mark or less than the log start offset.");
                }
                return writer.get();
            };
            return this;
        }

        public Builder setSuppressionReasonSupplier(Supplier<String> suppressionReasonSupplier) {
            this.suppressionReasonSupplier = suppressionReasonSupplier;
            return this;
        }

        public MetadataSnapshotter build() {
            if (logContext == null) {
                logContext = new LogContext("[Metadatanapshotter ]");
            }
            if (writerCreator == null) {
                throw new RuntimeException("You must set a writerCreator.");
            }
            return new MetadataSnapshotter(time,
                    logContext,
                    threadNamePrefix,
                    writerCreator,
                    suppressionReasonSupplier);
        }
    }

    /**
     * The log4j logger for this snapshotter.
     */
    private final Logger log;

    /**
     * The clock supplier for this snapshotter.
     */
    private final Time time;

    /**
     * Creates SnapshotWriter objects.
     */
    private final Function<SnapshotCreationRequest, SnapshotWriter<ApiMessageAndVersion>> writerCreator;

    /**
     * Returns an empty string if a snapshot can be created; otherwise, returns the reason why it
     * cannot be created.
     */
    private final Supplier<String> suppressionReasonSupplier;

    /**
     * The event queue which runs this snapshotter.
     */
    private final EventQueue eventQueue;

    /**
     * The offset of the snapshot we are currently writing, or -1 if we are not writing one.
     * Modified only by the event queue thread.
     */
    private volatile long currentSnapshotOffset;

    private MetadataSnapshotter(
        Time time,
        LogContext logContext,
        String threadNamePrefix,
        Function<SnapshotCreationRequest, SnapshotWriter<ApiMessageAndVersion>> writerCreator,
        Supplier<String> suppressionReasonSupplier
    ) {
        this.log = logContext.logger(MetadataLoader.class);
        this.time = time;
        this.writerCreator = writerCreator;
        this.suppressionReasonSupplier = suppressionReasonSupplier;
        this.eventQueue = new KafkaEventQueue(time, logContext, threadNamePrefix);
        this.currentSnapshotOffset = -1L;
    }

    @Override
    public void accept(SnapshotCreationRequest request) {
        long offset = currentSnapshotOffset;
        if (offset < 0L) {
            if (!request.force()) {
                log.warn("Ignoring request to create new snapshot at offset " +
                        request.offset() + " because we are already writing a " +
                        "snapshot at offset " + offset + ".");
            }
        }
        eventQueue.append(() -> {
            String suppressionReason = suppressionReasonSupplier.get();
            if (!suppressionReason.isEmpty()) {
                log.error("Unable to create snapshot at offset {}: snapshots have " +
                    "been supressed because {}.", request.offset(), suppressionReason);
                return;
            }
            String forcedString = request.force() ? "forced " : "";
            log.debug("Starting {}snapshot at offset {}.", forcedString, request.offset());
            currentSnapshotOffset = request.offset();
            long startNs = time.nanoseconds();
            try (SnapshotWriter<ApiMessageAndVersion> writer = writerCreator.apply(request)) {
                BatchSplitter batchSplitter = new BatchSplitter(MAX_RECORDS_PER_BATCH,
                        recordList -> writer.append(recordList));
                request.image().write(batchSplitter);
                writer.freeze();
            } catch (Throwable e) {
                log.error("Failed to create snapshot at offset {}", request.offset(), e);
                return;
            } finally {
                currentSnapshotOffset = -1L;
            }
            long endNs = time.nanoseconds();
            long elapsedUs = MICROSECONDS.convert(endNs - startNs, NANOSECONDS);
            log.info("Wrote {}snapshot at offset {} in {} microseconds.",
                    forcedString, request.offset(), elapsedUs);
        });
    }

    public void beginShutdown() {
        eventQueue.beginShutdown("MetadataSnapshotter#beginShutdown");
    }

    @Override
    public void close() throws InterruptedException {
        beginShutdown();
        eventQueue.close();
    }
}
