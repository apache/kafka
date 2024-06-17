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
package org.apache.kafka.coordinator.group.runtime;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.slf4j.Logger;

/**
 * SnapshottableCoordinator is a wrapper on top of the coordinator state machine. This object is not accessed concurrently
 * but multiple threads access it while loading the coordinator partition and therefore requires all methods to be
 * synchronized.
 */
class SnapshottableCoordinator<S extends CoordinatorShard<U>, U> implements CoordinatorPlayback<U> {
    /**
     * The logger.
     */
    private final Logger log;

    /**
     * The snapshot registry backing the coordinator.
     */
    private final SnapshotRegistry snapshotRegistry;

    /**
     * The actual state machine.
     */
    private final S coordinator;

    /**
     * The topic partition.
     */
    private final TopicPartition tp;

    /**
     * The last offset written to the partition.
     */
    private long lastWrittenOffset;

    /**
     * The last offset committed. This represents the high
     * watermark of the partition.
     */
    private long lastCommittedOffset;

    SnapshottableCoordinator(
        LogContext logContext,
        SnapshotRegistry snapshotRegistry,
        S coordinator,
        TopicPartition tp
    ) {
        this.log = logContext.logger(SnapshottableCoordinator.class);
        this.coordinator = coordinator;
        this.snapshotRegistry = snapshotRegistry;
        this.tp = tp;
        this.lastWrittenOffset = 0;
        this.lastCommittedOffset = 0;
        snapshotRegistry.getOrCreateSnapshot(0);
    }

    /**
     * Reverts the last written offset. This also reverts the snapshot
     * registry to this offset. All the changes applied after the offset
     * are lost.
     *
     * @param offset The offset to revert to.
     */
    synchronized void revertLastWrittenOffset(
        long offset
    ) {
        if (offset > lastWrittenOffset) {
            throw new IllegalStateException("New offset " + offset + " of " + tp +
                " must be smaller than " + lastWrittenOffset + ".");
        }

        log.debug("Revert last written offset of {} to {}.", tp, offset);
        lastWrittenOffset = offset;
        snapshotRegistry.revertToSnapshot(offset);
    }

    /**
     * Replays the record onto the state machine.
     *
     * @param offset        The offset of the record in the log.
     * @param producerId    The producer id.
     * @param producerEpoch The producer epoch.
     * @param record        A record.
     */
    @Override
    public synchronized void replay(
        long offset,
        long producerId,
        short producerEpoch,
        U record
    ) {
        coordinator.replay(offset, producerId, producerEpoch, record);
    }

    /**
     * Applies the end transaction marker.
     *
     * @param producerId    The producer id.
     * @param producerEpoch The producer epoch.
     * @param result        The result of the transaction.
     */
    @Override
    public synchronized void replayEndTransactionMarker(
        long producerId,
        short producerEpoch,
        TransactionResult result
    ) {
        coordinator.replayEndTransactionMarker(producerId, producerEpoch, result);
    }

    /**
     * Updates the last written offset. This also create a new snapshot
     * in the snapshot registry.
     *
     * @param offset The new last written offset.
     */
    @Override
    public synchronized void updateLastWrittenOffset(Long offset) {
        if (offset <= lastWrittenOffset) {
            throw new IllegalStateException("New last written offset " + offset + " of " + tp +
                " must be greater than " + lastWrittenOffset + ".");
        }

        lastWrittenOffset = offset;
        snapshotRegistry.getOrCreateSnapshot(offset);
        log.debug("Updated last written offset of {} to {}.", tp, offset);
    }

    /**
     * Updates the last committed offset. This completes all the deferred
     * events waiting on this offset. This also cleanups all the snapshots
     * prior to this offset.
     *
     * @param offset The new last committed offset.
     */
    @Override
    public synchronized void updateLastCommittedOffset(Long offset) {
        if (offset < lastCommittedOffset) {
            throw new IllegalStateException("New committed offset " + offset + " of " + tp +
                " must be greater than or equal to " + lastCommittedOffset + ".");
        }

        if (offset > lastWrittenOffset) {
            throw new IllegalStateException("New committed offset " + offset + " of " + tp +
                "must be less than or equal to " + lastWrittenOffset + ".");
        }

        lastCommittedOffset = offset;
        snapshotRegistry.deleteSnapshotsUpTo(offset);
        log.debug("Updated committed offset of {} to {}.", tp, offset);
    }

    /**
     * The coordinator has been loaded. This is used to apply any
     * post loading operations.
     *
     * @param newImage  The metadata image.
     */
    synchronized void onLoaded(MetadataImage newImage) {
        this.coordinator.onLoaded(newImage);
    }

    /**
     * The coordinator has been unloaded. This is used to apply
     * any post unloading operations.
     */
    synchronized void onUnloaded() {
        if (this.coordinator != null) {
            this.coordinator.onUnloaded();
        }
    }

    /**
     * @return The last written offset.
     */
    synchronized long lastWrittenOffset() {
        return this.lastWrittenOffset;
    }

    /**
     * A new metadata image is available. This is only called after {@link SnapshottableCoordinator#onLoaded(MetadataImage)}
     * is called to signal that the coordinator has been fully loaded.
     *
     * @param newImage  The new metadata image.
     * @param delta     The delta image.
     */
    synchronized void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta) {
        this.coordinator.onNewMetadataImage(newImage, delta);
    }

    /**
     * @return The last committed offset.
     */
    synchronized long lastCommittedOffset() {
        return this.lastCommittedOffset;
    }

    /**
     * @return The coordinator.
     */
    synchronized S coordinator() {
        return this.coordinator;
    }

    /**
     * @return The snapshot registry.
     *
     * Only used for testing.
     */
    synchronized SnapshotRegistry snapshotRegistry() {
        return this.snapshotRegistry;
    }
}
