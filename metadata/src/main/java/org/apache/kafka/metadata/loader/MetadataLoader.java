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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.metadata.publisher.MetadataPublisher;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;


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
        private FaultHandler faultHandler = null;

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

        public MetadataLoader build() {
            if (faultHandler == null) throw new RuntimeException("You must set a fault handler.");
            if (logContext == null) {
                logContext = new LogContext("[MetadataLoader id=" + nodeId + "] ");
            }
            return new MetadataLoader(nodeId,
                time,
                logContext,
                threadNamePrefix,
                faultHandler);
        }
    }

    /**
     * The log4j logger for this loader.
     */
    private final Logger log;

    /**
     * The node id.
     */
    private final int nodeId;

    /**
     * The clock used by this loader.
     */
    private final Time time;

    /**
     * The fault handler to use if metadata loading fails.
     */
    private final FaultHandler faultHandler;

    /**
     * The publishers who should receive cluster metadata updates.
     */
    private final List<MetadataPublisher> publishers;

    /**
     * The current metadata image. Accessed only from the event queue thread.
     */
    private MetadataImage image;

    /**
     * The event queue which runs this loader.
     */
    private final KafkaEventQueue eventQueue;

    private MetadataLoader(
        int nodeId,
        Time time,
        LogContext logContext,
        String threadNamePrefix,
        FaultHandler faultHandler
    ) {
        this.log = logContext.logger(MetadataLoader.class);
        this.nodeId = nodeId;
        this.time = time;
        this.faultHandler = faultHandler;
        this.publishers = new ArrayList<>();
        this.image = MetadataImage.EMPTY;
        this.eventQueue = new KafkaEventQueue(time, logContext, threadNamePrefix);
    }

    @Override
    public void handleCommit(BatchReader<ApiMessageAndVersion> reader) {
        eventQueue.append(() -> {
            long lastImageOffset = image.highestOffsetAndEpoch().offset;
            try {
                MetadataDelta delta = new MetadataDelta(image);
                BatchLoader loader = new BatchLoader(log, time, faultHandler, delta, "log", false);
                reader.forEachRemaining(loader);
                LoadInformation info = loader.finish();
                apply("log commits", delta, info);
                publish("log commits", publishers, delta, info);
            } catch (FaultHandlerException e) {
                // Ignore these because they have already been handled.
            } catch (Throwable e) {
                // This is a general catch-all block where we don't expect to end up;
                // failure-prone operations should have individual try/catch blocks around them.
                faultHandler.handleFault("Unhandled fault in MetadataLoader#handleCommit " +
                    "after last image offset " + lastImageOffset, e);
            }
        });
    }

    void apply(String what, MetadataDelta delta, LoadInformation info) {
        try {
            MetadataImage newImage = delta.apply();
            image = newImage;
        } catch (Throwable e) {
            throw faultHandler.handleFault("Error generating new metadata image from the " + what +
                    " ending at offset " + info.lastContainedOffset(), e);
        }
    }

    void publish(
        String what,
        List<MetadataPublisher> targetPublishers,
        MetadataDelta delta,
        LoadInformation info
    ) {
        for (MetadataPublisher publisher : targetPublishers) {
            try {
                publisher.publish(delta, image, info);
            } catch (Throwable e) {
                faultHandler.handleFault("Error publishing the " + what + " ending at " +
                        info.lastContainedOffset() + " with publisher " + publisher.name(), e);
            }
        }
    }

    @Override
    public void handleSnapshot(SnapshotReader<ApiMessageAndVersion> reader) {
        eventQueue.append(() -> {
            String snapName = String.format("snapshot %020d-%010d",
                    reader.lastContainedLogOffset(), reader.lastContainedLogEpoch());
            try {
                MetadataDelta delta = new MetadataDelta(image);
                BatchLoader loader = new BatchLoader(log, time, faultHandler, delta, snapName, true);
                reader.forEachRemaining(loader);
                LoadInformation info = loader.finish();
                apply("snapshot", delta, info);
                publish("snapshot", publishers, delta, info);
            } catch (FaultHandlerException e) {
                // Ignore these because they have already been handled.
            } catch (Throwable e) {
                // This is a general catch-all block where we don't expect to end up;
                // failure-prone operations should have individual try/catch blocks around them.
                faultHandler.handleFault("Unhandled fault in MetadataLoader#handleSnapshot " +
                        "for snapshot " + snapName, e);
            }
        });
    }

    @Override
    public void handleLeaderChange(LeaderAndEpoch leader) {
        // Nothing to do.
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
    public CompletableFuture<Void> installPublishers(List<MetadataPublisher> newPublishers) {
        if (!newPublishers.isEmpty()) return CompletableFuture.completedFuture(null);
        CompletableFuture<Void> future = new CompletableFuture<>();
        eventQueue.beginShutdown("installPublishers", () -> {
            try {
                // Publishers can't be re-installed if they're already present.
                for (MetadataPublisher publisher : newPublishers) {
                    if (publishers.contains(publisher)) {
                        throw faultHandler.handleFault("Attempted to install publisher " + publisher.name() +
                                ", which is already installed.");
                    }
                }
                String source = "installPublishers image at offset " + image.highestOffsetAndEpoch().offset;
                MetadataDelta delta = new MetadataDelta(image);
                BatchLoader loader = new BatchLoader(log, time, faultHandler, delta, source, true);
                long now = time.milliseconds();
                // Use the records in the current metadata image to build up a delta that contains
                // everything. The new publishers will need it because everything is new to them.
                image.write(recordList -> {
                    Batch<ApiMessageAndVersion> batch = Batch.data(image.highestOffsetAndEpoch().offset,
                            image.highestOffsetAndEpoch().epoch,
                            now,
                            0, // We don't populate the size field here.
                            recordList);
                    loader.accept(batch);
                });
                LoadInformation info = loader.finish();
                // Publish just to the new publishers.
                publish(source, newPublishers, delta, info);
                publishers.addAll(newPublishers);
                future.complete(null);
            } catch (Throwable e) {
                future.completeExceptionally(faultHandler.handleFault("Unhandled fault in " +
                        "MetadataLoader#installPublishers", e));
            }
        });
        return future;
    }

    /**
     * Remove a publisher.
     *
     * @param publisher         The publisher to remove.
     *
     * @return                  A future which yields null when the publisher has been removed,
     *                          or an exception if the removal failed.
     */
    public CompletableFuture<Void> removePublisher(MetadataPublisher publisher) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        eventQueue.beginShutdown("removePublisher", () -> {
            if (!publishers.remove(publisher)) {
                throw faultHandler.handleFault("Attempted to remove publisher " + publisher.name() +
                        ", which is not installed.");
            }
            future.complete(null);
        });
        return future;
    }

    @Override
    public void beginShutdown() {
        eventQueue.beginShutdown("beginShutdown", () -> {
            for (MetadataPublisher publisher : publishers) {
                try {
                    publisher.close();
                } catch (Throwable e) {
                    faultHandler.handleFault("Got unexpected exception while closing " +
                        "publisher " + publisher.name(), e);
                }
            }
        });
    }

    @Override
    public void close() throws Exception {
        beginShutdown();
        eventQueue.close();
    }
}
