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

package org.apache.kafka.image.publisher;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.loader.LogDeltaManifest;
import org.apache.kafka.image.loader.SnapshotManifest;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.server.fault.FaultHandler;

import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


/**
 * A metadata publisher that generates snapshots when appropriate.
 */
public class SnapshotGenerator implements MetadataPublisher {
    public static class Builder {
        private final Emitter emitter;
        private int nodeId = 0;
        private Time time = Time.SYSTEM;
        private FaultHandler faultHandler = (m, e) -> null;
        private long maxBytesSinceLastSnapshot = 100 * 1024L * 1024L;
        private long maxTimeSinceLastSnapshotNs = TimeUnit.DAYS.toNanos(1);
        private AtomicReference<String> disabledReason = null;
        private String threadNamePrefix = "";

        public Builder(Emitter emitter) {
            this.emitter = emitter;
        }

        public Builder setNodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder setFaultHandler(FaultHandler faultHandler) {
            this.faultHandler = faultHandler;
            return this;
        }

        public Builder setMaxBytesSinceLastSnapshot(long maxBytesSinceLastSnapshot) {
            this.maxBytesSinceLastSnapshot = maxBytesSinceLastSnapshot;
            return this;
        }

        public Builder setMaxTimeSinceLastSnapshotNs(long maxTimeSinceLastSnapshotNs) {
            this.maxTimeSinceLastSnapshotNs = maxTimeSinceLastSnapshotNs;
            return this;
        }

        public Builder setDisabledReason(AtomicReference<String> disabledReason) {
            this.disabledReason = disabledReason;
            return this;
        }

        public Builder setThreadNamePrefix(String threadNamePrefix) {
            this.threadNamePrefix = threadNamePrefix;
            return this;
        }

        public SnapshotGenerator build() {
            if (disabledReason == null) {
                disabledReason = new AtomicReference<>();
            }
            return new SnapshotGenerator(
                nodeId,
                time,
                emitter,
                faultHandler,
                maxBytesSinceLastSnapshot,
                maxTimeSinceLastSnapshotNs,
                disabledReason,
                threadNamePrefix
            );
        }
    }

    /**
     * The callback which actually generates the snapshot.
     */
    public interface Emitter {
        /**
         * Emit a snapshot for the given image.
         *
         * Note: if a snapshot has already been emitted for the given offset and epoch pair, this
         * function will not recreate it.
         *
         * @param image     The metadata image to emit.
         */
        void maybeEmit(MetadataImage image);
    }

    /**
     * The node ID.
     */
    private final int nodeId;

    /**
     * The clock to use.
     */
    private final Time time;

    /**
     * The emitter callback, which actually generates the snapshot.
     */
    private final Emitter emitter;

    /**
     * The slf4j logger to use.
     */
    private final Logger log;

    /**
     * The fault handler to use.
     */
    private final FaultHandler faultHandler;

    /**
     * The maximum number of bytes we will wait to see before triggering a new snapshot.
     */
    private final long maxBytesSinceLastSnapshot;

    /**
     * The maximum amount of time we will wait before triggering a snapshot, or 0 to disable
     * time-based snapshotting.
     */
    private final long maxTimeSinceLastSnapshotNs;

    /**
     * If non-null, the reason why snapshots have been disabled.
     */
    private final AtomicReference<String> disabledReason;

    /**
     * The event queue used to schedule emitting snapshots.
     */
    private final EventQueue eventQueue;

    /**
     * The log bytes that we have read since the last snapshot.
     */
    private long bytesSinceLastSnapshot;

    /**
     * The time at which we created the last snapshot.
     */
    private long lastSnapshotTimeNs;

    private SnapshotGenerator(
        int nodeId,
        Time time,
        Emitter emitter,
        FaultHandler faultHandler,
        long maxBytesSinceLastSnapshot,
        long maxTimeSinceLastSnapshotNs,
        AtomicReference<String> disabledReason,
        String threadNamePrefix
    ) {
        this.nodeId = nodeId;
        this.time = time;
        this.emitter = emitter;
        this.faultHandler = faultHandler;
        this.maxBytesSinceLastSnapshot = maxBytesSinceLastSnapshot;
        this.maxTimeSinceLastSnapshotNs = maxTimeSinceLastSnapshotNs;
        LogContext logContext = new LogContext("[SnapshotGenerator id=" + nodeId + "] ");
        this.log = logContext.logger(SnapshotGenerator.class);
        this.disabledReason = disabledReason;
        this.eventQueue = new KafkaEventQueue(time, logContext, threadNamePrefix + "snapshot-generator-");
        resetSnapshotCounters();
        log.debug("Starting SnapshotGenerator.");
    }

    @Override
    public String name() {
        return "SnapshotGenerator";
    }

    void resetSnapshotCounters() {
        this.bytesSinceLastSnapshot = 0L;
        this.lastSnapshotTimeNs = time.nanoseconds();
    }

    @Override
    public void onMetadataUpdate(
        MetadataDelta delta,
        MetadataImage newImage,
        LoaderManifest manifest
    ) {
        switch (manifest.type()) {
            case LOG_DELTA:
                publishLogDelta(delta, newImage, (LogDeltaManifest) manifest);
                break;
            case SNAPSHOT:
                publishSnapshot(delta, newImage, (SnapshotManifest) manifest);
                break;
        }
    }

    void publishSnapshot(
        MetadataDelta delta,
        MetadataImage newImage,
        SnapshotManifest manifest
    ) {
        log.debug("Resetting the snapshot counters because we just read {}.", newImage.provenance().snapshotName());
        resetSnapshotCounters();
    }

    void publishLogDelta(
        MetadataDelta delta,
        MetadataImage newImage,
        LogDeltaManifest manifest
    ) {
        bytesSinceLastSnapshot += manifest.numBytes();
        if (bytesSinceLastSnapshot >= maxBytesSinceLastSnapshot) {
            if (eventQueue.isEmpty()) {
                maybeScheduleEmit("we have replayed at least " + maxBytesSinceLastSnapshot +
                    " bytes", newImage, manifest.provenance().isOffsetBatchAligned());
            } else if (log.isTraceEnabled()) {
                log.trace("Not scheduling bytes-based snapshot because event queue is not empty yet.");
            }
        } else if (maxTimeSinceLastSnapshotNs != 0 &&
                (time.nanoseconds() - lastSnapshotTimeNs >= maxTimeSinceLastSnapshotNs)) {
            if (eventQueue.isEmpty()) {
                maybeScheduleEmit("we have waited at least " +
                    TimeUnit.NANOSECONDS.toMinutes(maxTimeSinceLastSnapshotNs) +
                    " minute(s)", newImage, manifest.provenance().isOffsetBatchAligned());
            } else if (log.isTraceEnabled()) {
                log.trace("Not scheduling time-based snapshot because event queue is not empty yet.");
            }
        } else if (log.isTraceEnabled()) {
            log.trace("Neither time-based nor bytes-based criteria are met; not scheduling snapshot.");
        }
    }

    void maybeScheduleEmit(
        String reason,
        MetadataImage image,
        boolean isOffsetBatchAligned
    ) {
        String currentDisabledReason = disabledReason.get();
        if (currentDisabledReason != null) {
            log.error("Not emitting {} despite the fact that {} because snapshots are " +
                "disabled; {}", image.provenance().snapshotName(), reason, currentDisabledReason);
        } else if (!isOffsetBatchAligned) {
            log.debug("Not emitting {} despite the fact that {} because snapshots are " +
                "disabled; {}", image.provenance().snapshotName(), reason, "metadata image is not batch aligned");
        } else {
            eventQueue.append(() -> {
                resetSnapshotCounters();
                log.info("Creating new KRaft snapshot file {} because {}.",
                        image.provenance().snapshotName(), reason);
                try {
                    emitter.maybeEmit(image);
                } catch (Throwable e) {
                    faultHandler.handleFault("KRaft snapshot file generation error", e);
                }
            });
        }
    }

    public void beginShutdown() {
        log.debug("Beginning shutdown of SnapshotGenerator.");
        this.disabledReason.compareAndSet(null, "we are shutting down");
        eventQueue.beginShutdown("beginShutdown");
    }

    @Override
    public void close() throws InterruptedException {
        eventQueue.beginShutdown("close");
        log.debug("Closing SnapshotGenerator.");
        eventQueue.close();
    }
}
