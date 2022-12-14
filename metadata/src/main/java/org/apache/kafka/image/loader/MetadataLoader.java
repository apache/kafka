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

package org.apache.kafka.image.loader;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.image.writer.ImageReWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.server.fault.FaultHandlerException;
import org.apache.kafka.snapshot.SnapshotReader;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.NANOSECONDS;


/**
 * The MetadataLoader follows changes provided by a RaftClient, and packages them into metadata
 * deltas and images that can be consumed by publishers.
 *
 * The Loader maintains its own thread, which is used to make all callbacks into publishers. If a
 * publisher A is installed before B, A will receive all callbacks before B. This is also true if
 * A and B are installed as part of a list [A, B].
 *
 * Publishers should not modify any data structures passed to them.
 *
 * It is possible to change the list of publishers dynamically over time. Whenever a new publisher is
 * added, it receives a catch-up delta which contains the full state. Any publisher installed when the
 * loader is closed will itself be closed.
 */
public class MetadataLoader implements RaftClient.Listener<ApiMessageAndVersion>, AutoCloseable {
    public static class Builder {
        private int nodeId = -1;
        private Time time = Time.SYSTEM;
        private LogContext logContext = null;
        private String threadNamePrefix = "";
        private FaultHandler faultHandler = (m, e) -> new FaultHandlerException(m, e);
        private MetadataLoaderMetrics metrics = new MetadataLoaderMetrics() {
            private volatile long lastAppliedOffset = -1L;

            @Override
            public void updateBatchProcessingTime(long elapsedNs) { }

            @Override
            public void updateBatchSize(int size) { }

            @Override
            public void updateLastAppliedImageProvenance(MetadataProvenance provenance) {
                this.lastAppliedOffset = provenance.offset();
            }

            @Override
            public long lastAppliedOffset() {
                return lastAppliedOffset;
            }

            @Override
            public void close() throws Exception { }
        };
        private Supplier<OptionalLong> highWaterMarkAccessor = null;

        public Builder setNodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder setThreadNamePrefix(String threadNamePrefix) {
            this.threadNamePrefix = threadNamePrefix;
            return this;
        }

        public Builder setFaultHandler(FaultHandler faultHandler) {
            this.faultHandler = faultHandler;
            return this;
        }

        public Builder setMetadataLoaderMetrics(MetadataLoaderMetrics metrics) {
            this.metrics = metrics;
            return this;
        }

        public Builder setHighWaterMarkAccessor(Supplier<OptionalLong> highWaterMarkAccessor) {
            this.highWaterMarkAccessor = highWaterMarkAccessor;
            return this;
        }

        public MetadataLoader build() {
            if (logContext == null) {
                logContext = new LogContext("[MetadataLoader " + nodeId + "] ");
            }
            if (highWaterMarkAccessor == null) {
                throw new RuntimeException("You must set the high water mark accessor.");
            }
            return new MetadataLoader(
                time,
                logContext,
                threadNamePrefix,
                faultHandler,
                metrics,
                highWaterMarkAccessor);
        }
    }

    /**
     * The log4j logger for this loader.
     */
    private final Logger log;

    /**
     * The clock used by this loader.
     */
    private final Time time;

    /**
     * The fault handler to use if metadata loading fails.
     */
    private final FaultHandler faultHandler;

    /**
     * Callbacks for updating metrics.
     */
    private final MetadataLoaderMetrics metrics;

    /**
     * A function which supplies the current high water mark, or empty if it is not known.
     */
    private final Supplier<OptionalLong> highWaterMarkAccessor;

    /**
     * Publishers which haven't been initialized yet.
     */
    private final LinkedHashMap<String, MetadataPublisher> uninitializedPublishers;

    /**
     * Publishers which are receiving updates.
     */
    private final LinkedHashMap<String, MetadataPublisher> publishers;

    /**
     * True if we have caught up with the initial high water mark.
     */
    private boolean catchingUp = false;

    /**
     * The current leader and epoch.
     */
    private LeaderAndEpoch currentLeaderAndEpoch = LeaderAndEpoch.UNKNOWN;

    /**
     * The current metadata image. Accessed only from the event queue thread.
     */
    private MetadataImage image;

    /**
     * The event queue which runs this loader.
     */
    private final KafkaEventQueue eventQueue;

    private MetadataLoader(
        Time time,
        LogContext logContext,
        String threadNamePrefix,
        FaultHandler faultHandler,
        MetadataLoaderMetrics metrics,
        Supplier<OptionalLong> highWaterMarkAccessor
    ) {
        this.log = logContext.logger(MetadataLoader.class);
        this.time = time;
        this.faultHandler = faultHandler;
        this.metrics = metrics;
        this.highWaterMarkAccessor = highWaterMarkAccessor;
        this.uninitializedPublishers = new LinkedHashMap<>();
        this.publishers = new LinkedHashMap<>();
        this.image = MetadataImage.EMPTY;
        this.eventQueue = new KafkaEventQueue(time, logContext, threadNamePrefix);
    }

    private boolean stillNeedToCatchUp(long offset) {
        if (!catchingUp) {
            log.trace("We are not in the initial catching up state.");
            return false;
        }
        OptionalLong highWaterMark = highWaterMarkAccessor.get();
        if (!highWaterMark.isPresent()) {
            log.info("The loader is still catching up because we still don't know the high " +
                    "water mark yet.");
            return true;
        }
        if (highWaterMark.getAsLong() > offset) {
            log.info("The loader is still catching up because we have loaded up to offset " +
                    offset + ", but the high water mark is " + highWaterMark.getAsLong());
            return true;
        }
        log.info("The loader finished catch up to the current high water mark of " +
                highWaterMark.getAsLong());
        catchingUp = true;
        return false;
    }

    private void maybeInitializeNewPublishers() {
        if (uninitializedPublishers.isEmpty()) {
            log.trace("There are no uninitialized publishers to initialize.");
            return;
        }
        long startNs = time.nanoseconds();
        MetadataDelta delta = new MetadataDelta.Builder().
                setImage(image).
                build();
        ImageReWriter writer = new ImageReWriter(delta);
        image.write(writer, new ImageWriterOptions.Builder().
                setMetadataVersion(image.features().metadataVersion()).
                build());
        SnapshotManifest manifest = new SnapshotManifest(
                image.provenance(),
                time.nanoseconds() - startNs);
        for (Iterator<MetadataPublisher> iter = uninitializedPublishers.values().iterator();
                iter.hasNext(); ) {
            MetadataPublisher publisher = iter.next();
            iter.remove();
            try {
                log.info("Publishing initial snapshot at offset {} to {}",
                        image.highestOffsetAndEpoch().offset(), publisher.name());
                publisher.publishSnapshot(delta, image, manifest);
                publishers.put(publisher.name(), publisher);
            } catch (Throwable e) {
                faultHandler.handleFault("Unhandled error publishing the initial metadata " +
                        "image from snapshot at offset " + image.highestOffsetAndEpoch().offset() +
                        " with publisher " + publisher.name(), e);
            }
        }
    }

    @Override
    public void handleCommit(BatchReader<ApiMessageAndVersion> reader) {
        eventQueue.append(() -> {
            try {
                MetadataDelta delta = new MetadataDelta.Builder().
                        setImage(image).
                        build();
                LogDeltaManifest manifest = loadLogDelta(delta, reader);
                if (log.isDebugEnabled()) {
                    log.debug("Generated a metadata delta between {} and {} from {} batch(es) " +
                            "in {} us.", image.offset(), manifest.provenance().offset(),
                            manifest.numBatches(), NANOSECONDS.toMicros(manifest.elapsedNs()));
                }
                try {
                    image = delta.apply(manifest.provenance());
                } catch (Throwable e) {
                    faultHandler.handleFault("Error generating new metadata image from " +
                        "metadata delta between offset " + image.offset() +
                            " and " + manifest.provenance().offset(), e);
                    return;
                }
                if (stillNeedToCatchUp(manifest.provenance().offset())) {
                    return;
                }
                log.debug("Publishing new image with provenance {}.", image.provenance());
                for (MetadataPublisher publisher : publishers.values()) {
                    try {
                        publisher.publishLogDelta(delta, image, manifest);
                    } catch (Throwable e) {
                        faultHandler.handleFault("Unhandled error publishing the new metadata " +
                            "image ending at " + manifest.provenance().offset() +
                                " with publisher " + publisher.name(), e);
                    }
                }
                maybeInitializeNewPublishers();
                metrics.updateLastAppliedImageProvenance(image.provenance());
            } catch (Throwable e) {
                // This is a general catch-all block where we don't expect to end up;
                // failure-prone operations should have individual try/catch blocks around them.
                faultHandler.handleFault("Unhandled fault in MetadataLoader#handleCommit. " +
                    "Last image offset was " + image.offset(), e);
            } finally {
                reader.close();
            }
        });
    }

    /**
     * Load some  batches of records from the log. We have to do some bookkeeping here to
     * translate between batch offsets and record offsets, and track the number of bytes we
     * have read. Additionally, there is the chance that one of the records is a metadata
     * version change which needs to be handled differently.
     *
     * @param delta     The metadata delta we are preparing.
     * @param reader    The reader which yields the batches.
     * @return          A manifest of what was loaded.
     */
    LogDeltaManifest loadLogDelta(
        MetadataDelta delta,
        BatchReader<ApiMessageAndVersion> reader
    ) {
        long startNs = time.nanoseconds();
        int numBatches = 0;
        long numBytes = 0L;
        long lastOffset = image.provenance().offset();
        int lastEpoch = image.provenance().epoch();
        long lastContainedLogTimeMs = image.provenance().lastContainedLogTimeMs();

        while (reader.hasNext()) {
            Batch<ApiMessageAndVersion> batch = reader.next();
            int indexWithinBatch = 0;
            for (ApiMessageAndVersion record : batch.records()) {
                try {
                    delta.replay(record.message());
                } catch (Throwable e) {
                    faultHandler.handleFault("Error loading metadata log record from offset " +
                            batch.baseOffset() + indexWithinBatch, e);
                }
                indexWithinBatch++;
            }
            metrics.updateBatchSize(batch.records().size());
            lastOffset = batch.lastOffset();
            lastEpoch = batch.epoch();
            lastContainedLogTimeMs = batch.appendTimestamp();
            numBytes += batch.sizeInBytes();
            numBatches++;
        }
        MetadataProvenance provenance =
                new MetadataProvenance(lastOffset, lastEpoch, lastContainedLogTimeMs);
        long elapsedNs = time.nanoseconds() - startNs;
        metrics.updateBatchProcessingTime(elapsedNs);
        return new LogDeltaManifest(provenance,
                numBatches,
                elapsedNs,
                numBytes);
    }

    @Override
    public void handleSnapshot(SnapshotReader<ApiMessageAndVersion> reader) {
        eventQueue.append(() -> {
            try {
                MetadataDelta delta = new MetadataDelta.Builder().
                        setImage(image).
                        build();
                SnapshotManifest manifest = loadSnapshot(delta, reader);
                if (log.isDebugEnabled()) {
                    log.debug("Generated a metadata delta from a snapshot at offset {} " +
                            "in {} us.", manifest.provenance().offset(),
                            NANOSECONDS.toMicros(manifest.elapsedNs()));
                }
                try {
                    image = delta.apply(manifest.provenance());
                } catch (Throwable e) {
                    faultHandler.handleFault("Error generating new metadata image from " +
                            "snapshot at offset " + reader.lastContainedLogOffset(), e);
                    return;
                }
                if (stillNeedToCatchUp(manifest.provenance().offset())) {
                    return;
                }
                log.debug("Publishing new snapshot image with provenance {}.", image.provenance());
                for (MetadataPublisher publisher : publishers.values()) {
                    try {
                        publisher.publishSnapshot(delta, image, manifest);
                    } catch (Throwable e) {
                        faultHandler.handleFault("Unhandled error publishing the new metadata " +
                                "image from snapshot at offset " + reader.lastContainedLogOffset() +
                                    " with publisher " + publisher.name(), e);
                    }
                }
                maybeInitializeNewPublishers();
                metrics.updateLastAppliedImageProvenance(image.provenance());
            } catch (Throwable e) {
                // This is a general catch-all block where we don't expect to end up;
                // failure-prone operations should have individual try/catch blocks around them.
                faultHandler.handleFault("Unhandled fault in MetadataLoader#handleSnapshot. " +
                        "Snapshot offset was " + reader.lastContainedLogOffset(), e);
            } finally {
                reader.close();
            }
        });
    }

    /**
     * Load a snapshot. This is relatively straightforward since we don't track as many things as
     * we do in loadLogDelta. The main complication here is that we have to maintain an index
     * of what record we are processing so that we can give useful error messages.
     *
     * @param delta     The metadata delta we are preparing.
     * @param reader    The reader which yields the snapshot batches.
     * @return          A manifest of what was loaded.
     */
    SnapshotManifest loadSnapshot(
            MetadataDelta delta,
            SnapshotReader<ApiMessageAndVersion> reader
    ) {
        long startNs = time.nanoseconds();
        int snapshotIndex = 0;
        while (reader.hasNext()) {
            Batch<ApiMessageAndVersion> batch = reader.next();
            for (ApiMessageAndVersion record : batch.records()) {
                try {
                    delta.replay(record.message());
                } catch (Throwable e) {
                    faultHandler.handleFault("Error loading metadata log record " + snapshotIndex +
                            " in snapshot at offset " + reader.lastContainedLogOffset(), e);
                }
                snapshotIndex++;
            }
        }
        MetadataProvenance provenance = new MetadataProvenance(reader.lastContainedLogOffset(),
                reader.lastContainedLogEpoch(), reader.lastContainedLogTimestamp());
        return new SnapshotManifest(provenance,
                time.nanoseconds() - startNs);
    }

    @Override
    public void handleLeaderChange(LeaderAndEpoch leaderAndEpoch) {
        eventQueue.append(() -> {
            currentLeaderAndEpoch = leaderAndEpoch;
        });
    }

    /**
     * Install a list of publishers. When a publisher is installed, we will publish a MetadataDelta
     * to it which contains the entire current image.
     *
     * @param newPublishers     The publishers to install.
     *
     * @return                  A future which yields null when the publishers have been added, or
     *                          an exception if the installation failed.
     */
    public CompletableFuture<Void> installPublishers(List<? extends MetadataPublisher> newPublishers) {
        if (newPublishers.isEmpty()) return CompletableFuture.completedFuture(null);
        CompletableFuture<Void> future = new CompletableFuture<>();
        eventQueue.append(() -> {
            try {
                installNewPublishers(newPublishers);
                future.complete(null);
            } catch (Throwable e) {
                future.completeExceptionally(faultHandler.handleFault("Unhandled fault in " +
                        "MetadataLoader#installPublishers", e));
            }
        });
        return future;
    }

    void installNewPublishers(
        List<? extends MetadataPublisher> newPublishers
    ) {
        // Publishers can't be re-installed if they're already present.
        for (MetadataPublisher newPublisher : newPublishers) {
            MetadataPublisher prev = publishers.get(newPublisher.name());
            if (prev == null) {
                prev = uninitializedPublishers.get(newPublisher.name());
            }
            if (prev != null) {
                if (prev == newPublisher) {
                    throw faultHandler.handleFault("Attempted to install publisher " +
                            newPublisher.name() + ", which is already installed.");
                } else {
                    throw faultHandler.handleFault("Attempted to install a new publisher " +
                            "named " + newPublisher.name() + ", but there is already a publisher " +
                            "with that name.");
                }
            }
            uninitializedPublishers.put(newPublisher.name(), newPublisher);
        }
    }

    // VisibleForTesting
    void waitForAllEventsToBeHandled() throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        eventQueue.append(() -> future.complete(null));
        future.get();
    }

    /**
     * Remove a publisher and close it.
     *
     * @param publisher         The publisher to remove and close.
     *
     * @return                  A future which yields null when the publisher has been removed
     *                          and closed, or an exception if the removal failed.
     */
    public CompletableFuture<Void> removeAndClosePublisher(MetadataPublisher publisher) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        eventQueue.append(() -> {
            try {
                if (!publishers.remove(publisher.name(), publisher)) {
                    if (!uninitializedPublishers.remove(publisher.name(), publisher)) {
                        throw faultHandler.handleFault("Attempted to remove publisher " + publisher.name() +
                                ", which is not installed.");
                    }
                }
                closePublisher(publisher);
                future.complete(null);
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    public long lastAppliedOffset() {
        return metrics.lastAppliedOffset();
    }

    @Override
    public void beginShutdown() {
        eventQueue.beginShutdown("beginShutdown", () -> {
            for (Iterator<MetadataPublisher> iter = uninitializedPublishers.values().iterator();
                    iter.hasNext(); ) {
                closePublisher(iter.next());
                iter.remove();
            }
            for (Iterator<MetadataPublisher> iter = publishers.values().iterator();
                 iter.hasNext(); ) {
                closePublisher(iter.next());
                iter.remove();
            }
        });
    }

    Time time() {
        return time;
    }

    private void closePublisher(MetadataPublisher publisher) {
        try {
            publisher.close();
        } catch (Throwable e) {
            faultHandler.handleFault("Got unexpected exception while closing " +
                    "publisher " + publisher.name(), e);
        }
    }

    @Override
    public void close() throws Exception {
        beginShutdown();
        eventQueue.close();
    }
}
