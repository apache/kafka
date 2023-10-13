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
import org.apache.kafka.image.loader.metrics.MetadataLoaderMetrics;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.image.writer.ImageReWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.server.fault.FaultHandlerException;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.snapshot.Snapshots;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
        private String threadNamePrefix = "";
        private Time time = Time.SYSTEM;
        private LogContext logContext = null;
        private FaultHandler faultHandler = (m, e) -> new FaultHandlerException(m, e);
        private MetadataLoaderMetrics metrics = null;
        private Supplier<OptionalLong> highWaterMarkAccessor = null;

        public Builder setNodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder setThreadNamePrefix(String threadNamePrefix) {
            this.threadNamePrefix = threadNamePrefix;
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

        public Builder setHighWaterMarkAccessor(Supplier<OptionalLong> highWaterMarkAccessor) {
            this.highWaterMarkAccessor = highWaterMarkAccessor;
            return this;
        }

        public Builder setMetrics(MetadataLoaderMetrics metrics) {
            this.metrics = metrics;
            return this;
        }

        public MetadataLoader build() {
            if (logContext == null) {
                logContext = new LogContext("[MetadataLoader id=" + nodeId + "] ");
            }
            if (highWaterMarkAccessor == null) {
                throw new RuntimeException("You must set the high water mark accessor.");
            }
            if (metrics == null) {
                metrics = new MetadataLoaderMetrics(Optional.empty(),
                    __ -> { },
                    __ -> { },
                    new AtomicReference<>(MetadataProvenance.EMPTY));
            }
            return new MetadataLoader(
                time,
                logContext,
                nodeId,
                threadNamePrefix,
                faultHandler,
                metrics,
                highWaterMarkAccessor);
        }
    }

    private static final String INITIALIZE_NEW_PUBLISHERS = "InitializeNewPublishers";

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
     * Publishers which haven't received any metadata yet.
     */
    private final LinkedHashMap<String, MetadataPublisher> uninitializedPublishers;

    /**
     * Publishers which are receiving updates.
     */
    private final LinkedHashMap<String, MetadataPublisher> publishers;

    /**
     * True if we have not caught up with the initial high water mark.
     * We do not send out any metadata updates until this is true.
     */
    private boolean catchingUp = true;

    /**
     * The current leader and epoch.
     */
    private LeaderAndEpoch currentLeaderAndEpoch = LeaderAndEpoch.UNKNOWN;

    /**
     * The current metadata image. Accessed only from the event queue thread.
     */
    private MetadataImage image;

    private final MetadataBatchLoader batchLoader;

    /**
     * The event queue which runs this loader.
     */
    private final KafkaEventQueue eventQueue;

    private MetadataLoader(
        Time time,
        LogContext logContext,
        int nodeId,
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
        this.batchLoader = new MetadataBatchLoader(
            logContext,
            time,
            faultHandler,
            this::maybePublishMetadata);
        this.eventQueue = new KafkaEventQueue(
            Time.SYSTEM,
            logContext,
            threadNamePrefix + "metadata-loader-",
            new ShutdownEvent());
    }

    // VisibleForTesting
    MetadataLoaderMetrics metrics() {
        return metrics;
    }

    private boolean stillNeedToCatchUp(String where, long offset) {
        if (!catchingUp) {
            log.trace("{}: we are not in the initial catching up state.", where);
            return false;
        }
        OptionalLong highWaterMark = highWaterMarkAccessor.get();
        if (!highWaterMark.isPresent()) {
            log.info("{}: the loader is still catching up because we still don't know the high " +
                    "water mark yet.", where);
            return true;
        }
        if (highWaterMark.getAsLong() - 1 > offset) {
            log.info("{}: The loader is still catching up because we have loaded up to offset " +
                    offset + ", but the high water mark is {}", where, highWaterMark.getAsLong());
            return true;
        }
        if (!batchLoader.hasSeenRecord()) {
            log.info("{}: The loader is still catching up because we have not loaded a controller record as of offset " +
                    offset + " and high water mark is {}", where, highWaterMark.getAsLong());
            return true;
        }
        log.info("{}: The loader finished catching up to the current high water mark of {}",
                where, highWaterMark.getAsLong());
        catchingUp = false;
        return false;
    }

    /**
     * Schedule an event to initialize the new publishers that are present in the system.
     *
     * @param delayNs   The minimum time in nanoseconds we should wait. If there is already an
     *                  initialization event scheduled, we will either move its deadline forward
     *                  in time or leave it unchanged.
     */
    void scheduleInitializeNewPublishers(long delayNs) {
        eventQueue.scheduleDeferred(INITIALIZE_NEW_PUBLISHERS,
            new EventQueue.EarliestDeadlineFunction(eventQueue.time().nanoseconds() + delayNs),
            () -> {
                try {
                    initializeNewPublishers();
                } catch (Throwable e) {
                    faultHandler.handleFault("Unhandled error initializing new publishers", e);
                }
            });
    }

    void initializeNewPublishers() {
        if (uninitializedPublishers.isEmpty()) {
            log.debug("InitializeNewPublishers: nothing to do.");
            return;
        }
        if (stillNeedToCatchUp("initializeNewPublishers", image.highestOffsetAndEpoch().offset())) {
            // Reschedule the initialization for later.
            log.debug("InitializeNewPublishers: unable to initialize new publisher(s) {} " +
                            "because we are still catching up with quorum metadata. Rescheduling.",
                    uninitializedPublisherNames());
            scheduleInitializeNewPublishers(TimeUnit.MILLISECONDS.toNanos(100));
            return;
        }
        log.debug("InitializeNewPublishers: setting up snapshot image for new publisher(s): {}",
                uninitializedPublisherNames());
        long startNs = time.nanoseconds();
        // We base this delta off of the empty image, reflecting the fact that these publishers
        // haven't seen anything previously.
        MetadataDelta delta = new MetadataDelta.Builder().
                setImage(MetadataImage.EMPTY).
                build();
        ImageReWriter writer = new ImageReWriter(delta);
        image.write(writer, new ImageWriterOptions.Builder().
                setMetadataVersion(image.features().metadataVersion()).
                build());
        // ImageReWriter#close invokes finishSnapshot, so we don't need to invoke it here.
        SnapshotManifest manifest = new SnapshotManifest(
                image.provenance(),
                time.nanoseconds() - startNs);
        for (Iterator<MetadataPublisher> iter = uninitializedPublishers.values().iterator();
                iter.hasNext(); ) {
            MetadataPublisher publisher = iter.next();
            iter.remove();
            try {
                log.info("InitializeNewPublishers: initializing {} with a snapshot at offset {}",
                        publisher.name(), image.highestOffsetAndEpoch().offset());
                publisher.onMetadataUpdate(delta, image, manifest);
                publisher.onControllerChange(currentLeaderAndEpoch);
                publishers.put(publisher.name(), publisher);
            } catch (Throwable e) {
                faultHandler.handleFault("Unhandled error initializing " + publisher.name() +
                        " with a snapshot at offset " + image.highestOffsetAndEpoch().offset(), e);
            }
        }
    }

    private String uninitializedPublisherNames() {
        return String.join(", ", uninitializedPublishers.keySet());
    }

    /**
     * Callback used by MetadataBatchLoader and handleLoadSnapshot to update the active metadata publishers.
     */
    private void maybePublishMetadata(MetadataDelta delta, MetadataImage image, LoaderManifest manifest) {
        this.image = image;

        if (stillNeedToCatchUp(
            "maybePublishMetadata(" + manifest.type().toString() + ")",
            manifest.provenance().lastContainedOffset())
        ) {
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("handleCommit: publishing new image with provenance {}.", image.provenance());
        }
        for (MetadataPublisher publisher : publishers.values()) {
            try {
                publisher.onMetadataUpdate(delta, image, manifest);
            } catch (Throwable e) {
                faultHandler.handleFault("Unhandled error publishing the new metadata " +
                    "image ending at " + manifest.provenance().lastContainedOffset() +
                    " with publisher " + publisher.name(), e);
            }
        }
        metrics.updateLastAppliedImageProvenance(image.provenance());
        metrics.setCurrentMetadataVersion(image.features().metadataVersion());
        if (uninitializedPublishers.isEmpty()) {
            scheduleInitializeNewPublishers(0);
        }
    }

    @Override
    public void handleCommit(BatchReader<ApiMessageAndVersion> reader) {
        eventQueue.append(() -> {
            try {
                while (reader.hasNext()) {
                    Batch<ApiMessageAndVersion> batch = reader.next();
                    long elapsedNs = batchLoader.loadBatch(batch, currentLeaderAndEpoch);
                    metrics.updateBatchSize(batch.records().size());
                    metrics.updateBatchProcessingTimeNs(elapsedNs);
                }
                batchLoader.maybeFlushBatches(currentLeaderAndEpoch);
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

    @Override
    public void handleLoadSnapshot(SnapshotReader<ApiMessageAndVersion> reader) {
        eventQueue.append(() -> {
            try {
                long numLoaded = metrics.incrementHandleLoadSnapshotCount();
                String snapshotName = Snapshots.filenameFromSnapshotId(reader.snapshotId());
                log.info("handleLoadSnapshot({}): incrementing HandleLoadSnapshotCount to {}.",
                    snapshotName, numLoaded);
                MetadataDelta delta = new MetadataDelta.Builder().
                    setImage(image).
                    build();
                SnapshotManifest manifest = loadSnapshot(delta, reader);
                log.info("handleLoadSnapshot({}): generated a metadata delta between offset {} " +
                        "and this snapshot in {} us.", snapshotName,
                        image.provenance().lastContainedOffset(),
                        NANOSECONDS.toMicros(manifest.elapsedNs()));
                MetadataImage image = delta.apply(manifest.provenance());
                batchLoader.resetToImage(image);
                maybePublishMetadata(delta, image, manifest);
            } catch (Throwable e) {
                // This is a general catch-all block where we don't expect to end up;
                // failure-prone operations should have individual try/catch blocks around them.
                faultHandler.handleFault("Unhandled fault in MetadataLoader#handleLoadSnapshot. " +
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
        delta.finishSnapshot();
        MetadataProvenance provenance = new MetadataProvenance(reader.lastContainedLogOffset(),
                reader.lastContainedLogEpoch(), reader.lastContainedLogTimestamp());
        return new SnapshotManifest(provenance,
                time.nanoseconds() - startNs);
    }

    @Override
    public void handleLeaderChange(LeaderAndEpoch leaderAndEpoch) {
        eventQueue.append(() -> {
            currentLeaderAndEpoch = leaderAndEpoch;
            for (MetadataPublisher publisher : publishers.values()) {
                try {
                    publisher.onControllerChange(currentLeaderAndEpoch);
                } catch (Throwable e) {
                    faultHandler.handleFault("Unhandled error publishing the new leader " +
                        "change to " + currentLeaderAndEpoch + " with publisher " +
                        publisher.name(), e);
                }
            }
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
                // Check that none of the publishers we are trying to install are already present.
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
                }
                // After installation, new publishers must be initialized by sending them a full
                // snapshot of the current state. However, we cannot necessarily do that immediately,
                // because the loader itself might not be ready. Therefore, we schedule a background
                // task.
                newPublishers.forEach(p -> uninitializedPublishers.put(p.name(), p));
                scheduleInitializeNewPublishers(0);
                future.complete(null);
            } catch (Throwable e) {
                future.completeExceptionally(faultHandler.handleFault("Unhandled fault in " +
                        "MetadataLoader#installPublishers", e));
            }
        });
        return future;
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
        eventQueue.beginShutdown("beginShutdown");
    }

    class ShutdownEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
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
        }
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
