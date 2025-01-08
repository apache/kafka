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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class UnifiedLog {

    private static final Logger LOG = LoggerFactory.getLogger(UnifiedLog.class);

    /**
     * Rebuilds producer state until the provided lastOffset. This function may be called from the
     * recovery code path, and thus must be free of all side effects, i.e. it must not update any
     * log-specific state.
     *
     * @param producerStateManager    The {@link ProducerStateManager} instance to be rebuilt.
     * @param segments                The segments of the log whose producer state is being rebuilt
     * @param logStartOffset          The log start offset
     * @param lastOffset              The last offset upto which the producer state needs to be rebuilt
     * @param recordVersion           The record version
     * @param time                    The time instance used for checking the clock
     * @param reloadFromCleanShutdown True if the producer state is being built after a clean shutdown, false otherwise.
     * @param logPrefix               The logging prefix
     */
    public static void rebuildProducerState(ProducerStateManager producerStateManager,
                                            LogSegments segments,
                                            long logStartOffset,
                                            long lastOffset,
                                            Time time,
                                            boolean reloadFromCleanShutdown,
                                            String logPrefix) throws IOException {
        List<Optional<Long>> offsetsToSnapshot = new ArrayList<>();
        if (segments.nonEmpty()) {
            long lastSegmentBaseOffset = segments.lastSegment().get().baseOffset();
            Optional<LogSegment> lowerSegment = segments.lowerSegment(lastSegmentBaseOffset);
            Optional<Long> nextLatestSegmentBaseOffset = lowerSegment.map(LogSegment::baseOffset);
            offsetsToSnapshot.add(nextLatestSegmentBaseOffset);
            offsetsToSnapshot.add(Optional.of(lastSegmentBaseOffset));
        }
        offsetsToSnapshot.add(Optional.of(lastOffset));

        LOG.info("{}Loading producer state till offset {}", logPrefix, lastOffset);

        // We want to avoid unnecessary scanning of the log to build the producer state when the broker is being
        // upgraded. The basic idea is to use the absence of producer snapshot files to detect the upgrade case,
        // but we have to be careful not to assume too much in the presence of broker failures. The most common
        // upgrade case in which we expect to find no snapshots is the following:
        //
        // * The broker has been upgraded, and we had a clean shutdown.
        //
        // If we hit this case, we skip producer state loading and write a new snapshot at the log end
        // offset (see below). The next time the log is reloaded, we will load producer state using this snapshot
        // (or later snapshots). Otherwise, if there is no snapshot file, then we have to rebuild producer state
        // from the first segment.
        if (producerStateManager.latestSnapshotOffset().isEmpty() && reloadFromCleanShutdown) {
            // To avoid an expensive scan through all the segments, we take empty snapshots from the start of the
            // last two segments and the last offset. This should avoid the full scan in the case that the log needs
            // truncation.
            for (Optional<Long> offset : offsetsToSnapshot) {
                if (offset.isPresent()) {
                    producerStateManager.updateMapEndOffset(offset.get());
                    producerStateManager.takeSnapshot();
                }
            }
        } else {
            LOG.info("{}Reloading from producer snapshot and rebuilding producer state from offset {}", logPrefix, lastOffset);
            boolean isEmptyBeforeTruncation = producerStateManager.isEmpty() && producerStateManager.mapEndOffset() >= lastOffset;
            long producerStateLoadStart = time.milliseconds();
            producerStateManager.truncateAndReload(logStartOffset, lastOffset, time.milliseconds());
            long segmentRecoveryStart = time.milliseconds();

            // Only do the potentially expensive reloading if the last snapshot offset is lower than the log end
            // offset (which would be the case on first startup) and there were active producers prior to truncation
            // (which could be the case if truncating after initial loading). If there weren't, then truncating
            // shouldn't change that fact (although it could cause a producerId to expire earlier than expected),
            // and we can skip the loading. This is an optimization for users which are not yet using
            // idempotent/transactional features yet.
            if (lastOffset > producerStateManager.mapEndOffset() && !isEmptyBeforeTruncation) {
                Optional<LogSegment> segmentOfLastOffset = segments.floorSegment(lastOffset);

                for (LogSegment segment : segments.values(producerStateManager.mapEndOffset(), lastOffset)) {
                    long startOffset = Utils.max(segment.baseOffset(), producerStateManager.mapEndOffset(), logStartOffset);
                    producerStateManager.updateMapEndOffset(startOffset);

                    if (offsetsToSnapshot.contains(Optional.of(segment.baseOffset()))) {
                        producerStateManager.takeSnapshot();
                    }
                    int maxPosition = segment.size();
                    if (segmentOfLastOffset.isPresent() && segmentOfLastOffset.get() == segment) {
                        FileRecords.LogOffsetPosition lop = segment.translateOffset(lastOffset);
                        maxPosition = lop != null ? lop.position : segment.size();
                    }

                    FetchDataInfo fetchDataInfo = segment.read(startOffset, Integer.MAX_VALUE, maxPosition);
                    if (fetchDataInfo != null) {
                        loadProducersFromRecords(producerStateManager, fetchDataInfo.records);
                    }
                }
            }
            producerStateManager.updateMapEndOffset(lastOffset);
            producerStateManager.takeSnapshot();
            LOG.info(logPrefix + "Producer state recovery took " + (segmentRecoveryStart - producerStateLoadStart) + "ms for snapshot load " +
                    "and " + (time.milliseconds() - segmentRecoveryStart) + "ms for segment recovery from offset " + lastOffset);
        }
    }

    public static void deleteProducerSnapshots(Collection<LogSegment> segments,
                                               ProducerStateManager producerStateManager,
                                               boolean asyncDelete,
                                               Scheduler scheduler,
                                               LogConfig config,
                                               LogDirFailureChannel logDirFailureChannel,
                                               String parentDir,
                                               TopicPartition topicPartition) throws IOException {
        List<SnapshotFile> snapshotsToDelete = new ArrayList<>();
        for (LogSegment segment : segments) {
            Optional<SnapshotFile> snapshotFile = producerStateManager.removeAndMarkSnapshotForDeletion(segment.baseOffset());
            snapshotFile.ifPresent(snapshotsToDelete::add);
        }

        Runnable deleteProducerSnapshots = () -> deleteProducerSnapshots(snapshotsToDelete, logDirFailureChannel, parentDir, topicPartition);
        if (asyncDelete) {
            scheduler.scheduleOnce("delete-producer-snapshot", deleteProducerSnapshots, config.fileDeleteDelayMs);
        } else {
            deleteProducerSnapshots.run();
        }
    }

    private static void deleteProducerSnapshots(List<SnapshotFile> snapshotsToDelete, LogDirFailureChannel logDirFailureChannel, String parentDir, TopicPartition topicPartition) {
        LocalLog.maybeHandleIOException(
                logDirFailureChannel,
                parentDir,
                () -> "Error while deleting producer state snapshots for " + topicPartition + " in dir " + parentDir,
                () -> {
                    for (SnapshotFile snapshotFile : snapshotsToDelete) {
                        snapshotFile.deleteIfExists();
                    }
                    return null;
                });
    }

    private static void loadProducersFromRecords(ProducerStateManager producerStateManager, Records records) {
        Map<Long, ProducerAppendInfo> loadedProducers = new HashMap<>();
        final List<CompletedTxn> completedTxns = new ArrayList<>();
        records.batches().forEach(batch -> {
            if (batch.hasProducerId()) {
                Optional<CompletedTxn> maybeCompletedTxn = updateProducers(
                        producerStateManager,
                        batch,
                        loadedProducers,
                        Optional.empty(),
                        AppendOrigin.REPLICATION);
                maybeCompletedTxn.ifPresent(completedTxns::add);
            }
        });
        loadedProducers.values().forEach(producerStateManager::update);
        completedTxns.forEach(producerStateManager::completeTxn);
    }

    public static Optional<CompletedTxn> updateProducers(ProducerStateManager producerStateManager,
                                                         RecordBatch batch,
                                                         Map<Long, ProducerAppendInfo> producers,
                                                         Optional<LogOffsetMetadata> firstOffsetMetadata,
                                                         AppendOrigin origin) {
        long producerId = batch.producerId();
        ProducerAppendInfo appendInfo = producers.computeIfAbsent(producerId, __ -> producerStateManager.prepareUpdate(producerId, origin));
        Optional<CompletedTxn> completedTxn = appendInfo.append(batch, firstOffsetMetadata);
        // Whether we wrote a control marker or a data batch, we can remove VerificationGuard since either the transaction is complete or we have a first offset.
        if (batch.isTransactional()) {
            producerStateManager.clearVerificationStateEntry(producerId);
        }
        return completedTxn;
    }
}
