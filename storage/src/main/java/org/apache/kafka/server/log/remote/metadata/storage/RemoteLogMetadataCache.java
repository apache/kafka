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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

/**
 * This class provides an in-memory cache of remote log segment metadata. This maintains the lineage of segments
 * with respect to leader epochs.
 * <p>
 * Remote log segment can go through the state transitions as mentioned in {@link RemoteLogSegmentState}.
 * <p>
 * This class will have all the segments which did not reach terminal state viz DELETE_SEGMENT_FINISHED. That means,any
 * segment reaching the terminal state will get cleared from this instance.
 * This class provides different methods to fetch segment metadata like {@link #remoteLogSegmentMetadata(int, long)},
 * {@link #highestOffsetForEpoch(int)}, {@link #listRemoteLogSegments(int)}, {@link #listAllRemoteLogSegments()}. Those
 * methods have different semantics to fetch the segment based on its state.
 * <p>
 * <ul>
 * <li>
 * {@link RemoteLogSegmentState#COPY_SEGMENT_STARTED}:
 * <br>
 * Segment in this state indicates it is not yet copied successfully. So, these segments will not be
 * accessible for reads but these are considered for cleanups when a partition is deleted.
 * </li>
 * <li>
 * {@link RemoteLogSegmentState#COPY_SEGMENT_FINISHED}:
 * <br>
 * Segment in this state indicates it is successfully copied and it is available for reads. So, these segments
 * will be accessible for reads. But this should be available for any cleanup activity like deleting segments by the
 * caller of this class.
 * </li>
 * <li>
 * {@link RemoteLogSegmentState#DELETE_SEGMENT_STARTED}:
 * Segment in this state indicates it is getting deleted. That means, it is not available for reads. But it should be
 * available for any cleanup activity like deleting segments by the caller of this class.
 * </li>
 * <li>
 * {@link RemoteLogSegmentState#DELETE_SEGMENT_FINISHED}:
 * Segment in this state indicate it is already deleted. That means, it is not available for any activity including
 * reads or cleanup activity. This cache will clear entries containing this state.
 * </li>
 * </ul>
 *
 * <p>
 *  The below table summarizes whether the segment with the respective state are available for the given methods.
 * <pre>
 * +---------------------------------+----------------------+------------------------+-------------------------+-------------------------+
 * |  Method / SegmentState          | COPY_SEGMENT_STARTED | COPY_SEGMENT_FINISHED  | DELETE_SEGMENT_STARTED  | DELETE_SEGMENT_FINISHED |
 * |---------------------------------+----------------------+------------------------+-------------------------+-------------------------|
 * | remoteLogSegmentMetadata        |        No            |           Yes          |          No             |           No            |
 * | (int leaderEpoch, long offset)  |                      |                        |                         |                         |
 * |---------------------------------+----------------------+------------------------+-------------------------+-------------------------|
 * | listRemoteLogSegments           |        Yes           |           Yes          |          Yes            |           No            |
 * | (int leaderEpoch)               |                      |                        |                         |                         |
 * |---------------------------------+----------------------+------------------------+-------------------------+-------------------------|
 * | listAllRemoteLogSegments()      |        Yes           |           Yes          |          Yes            |           No            |
 * |                                 |                      |                        |                         |                         |
 * +---------------------------------+----------------------+------------------------+-------------------------+-------------------------+
 * </pre>
 * </p>
 * <p></p>
 */
public class RemoteLogMetadataCache {

    private static final Logger log = LoggerFactory.getLogger(RemoteLogMetadataCache.class);

    // It contains all the segment-id to metadata mappings which did not reach the terminal state viz DELETE_SEGMENT_FINISHED.
    protected final ConcurrentMap<RemoteLogSegmentId, RemoteLogSegmentMetadata> idToSegmentMetadata
            = new ConcurrentHashMap<>();

    // It contains leader epoch to the respective entry containing the state.
    // TODO We are not clearing the entry for epoch when RemoteLogLeaderEpochState becomes empty. This will be addressed
    // later. We will look into it when we integrate these APIs along with RemoteLogManager changes.
    // https://issues.apache.org/jira/browse/KAFKA-12641
    protected final ConcurrentMap<Integer, RemoteLogLeaderEpochState> leaderEpochEntries = new ConcurrentHashMap<>();

    private final CountDownLatch initializedLatch = new CountDownLatch(1);

    public void markInitialized() {
        initializedLatch.countDown();
    }

    public boolean isInitialized() {
        return initializedLatch.getCount() == 0;
    }

    /**
     * Returns {@link RemoteLogSegmentMetadata} if it exists for the given leader-epoch containing the offset and with
     * {@link RemoteLogSegmentState#COPY_SEGMENT_FINISHED} state, else returns {@link Optional#empty()}.
     *
     * @param leaderEpoch leader epoch for the given offset
     * @param offset      offset
     * @return the requested remote log segment metadata if it exists.
     */
    public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(int leaderEpoch, long offset) {
        RemoteLogSegmentMetadata metadata = getSegmentMetadata(leaderEpoch, offset);
        long epochEndOffset = -1L;
        if (metadata != null) {
            // Check whether the given offset with leaderEpoch exists in this segment.
            // Check for epoch's offset boundaries with in this segment.
            //   1. Get the next epoch's start offset -1 if exists
            //   2. If no next epoch exists, then segment end offset can be considered as epoch's relative end offset.
            Map.Entry<Integer, Long> nextEntry = metadata.segmentLeaderEpochs().higherEntry(leaderEpoch);
            epochEndOffset = (nextEntry != null) ? nextEntry.getValue() - 1 : metadata.endOffset();
        }
        // Return empty when target offset > epoch's end offset.
        return offset > epochEndOffset ? Optional.empty() : Optional.ofNullable(metadata);
    }

    public Optional<RemoteLogSegmentMetadata> nextSegmentWithTxnIndex(int leaderEpoch, long offset) {
        boolean txnIdxEmpty = true;
        Optional<RemoteLogSegmentMetadata> metadataOpt = remoteLogSegmentMetadata(leaderEpoch, offset);
        while (metadataOpt.isPresent() && txnIdxEmpty) {
            txnIdxEmpty = metadataOpt.get().isTxnIdxEmpty();
            if (txnIdxEmpty) { // If txn index is empty, then look for next segment.
                metadataOpt = remoteLogSegmentMetadata(leaderEpoch, metadataOpt.get().endOffset() + 1);
            }
        }
        return txnIdxEmpty ? Optional.empty() : metadataOpt;
    }

    private RemoteLogSegmentMetadata getSegmentMetadata(int leaderEpoch, long offset) {
        RemoteLogLeaderEpochState remoteLogLeaderEpochState = leaderEpochEntries.get(leaderEpoch);
        if (remoteLogLeaderEpochState != null) {
            // Look for floor entry as the given offset may exist in this entry.
            RemoteLogSegmentId remoteLogSegmentId = remoteLogLeaderEpochState.floorEntry(offset);
            if (remoteLogSegmentId != null) {
                return idToSegmentMetadata.get(remoteLogSegmentId);
            } else {
                log.warn("No remote segment found for leaderEpoch: {}, offset: {}", leaderEpoch, offset);
            }
        }
        // If the offset is lower than the minimum offset available in metadata then return null.
        return null;
    }

    public void updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate metadataUpdate)
            throws RemoteResourceNotFoundException {
        log.debug("Updating remote log segment metadata: [{}]", metadataUpdate);
        Objects.requireNonNull(metadataUpdate, "metadataUpdate can not be null");

        RemoteLogSegmentState targetState = metadataUpdate.state();
        RemoteLogSegmentId remoteLogSegmentId = metadataUpdate.remoteLogSegmentId();
        RemoteLogSegmentMetadata existingMetadata = idToSegmentMetadata.get(remoteLogSegmentId);
        if (!isInitialized() && existingMetadata == null) {
            log.debug("Dropping the event: {} as the base-metadata about the segment was already removed", metadataUpdate);
            return;
        }
        if (existingMetadata == null) {
            throw new RemoteResourceNotFoundException("No remote log segment metadata found for :" +
                                                      remoteLogSegmentId);
        }

        // Check the state transition.
        boolean isValid = checkStateTransition(existingMetadata.state(), targetState, metadataUpdate.remoteLogSegmentId());
        if (!isValid) {
            return;
        }

        switch (targetState) {
            case COPY_SEGMENT_STARTED:
                // Callers should use addCopyInProgressSegment to add RemoteLogSegmentMetadata with state as
                // RemoteLogSegmentState.COPY_SEGMENT_STARTED.
                throw new IllegalArgumentException("metadataUpdate: " + metadataUpdate + " with state " + RemoteLogSegmentState.COPY_SEGMENT_STARTED +
                                                   " can not be updated");
            case COPY_SEGMENT_FINISHED:
                handleSegmentWithCopySegmentFinishedState(existingMetadata.createWithUpdates(metadataUpdate));
                break;
            case DELETE_SEGMENT_STARTED:
                handleSegmentWithDeleteSegmentStartedState(existingMetadata.createWithUpdates(metadataUpdate));
                break;
            case DELETE_SEGMENT_FINISHED:
                handleSegmentWithDeleteSegmentFinishedState(existingMetadata.createWithUpdates(metadataUpdate));
                break;
            default:
                throw new IllegalArgumentException("Metadata with the state " + targetState + " is not supported");
        }
    }

    protected final void handleSegmentWithCopySegmentFinishedState(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        doHandleSegmentStateTransitionForLeaderEpochs(remoteLogSegmentMetadata,
            (leaderEpoch, remoteLogLeaderEpochState, startOffset, segmentId) -> {
                long leaderEpochEndOffset = highestOffsetForEpoch(leaderEpoch, remoteLogSegmentMetadata);
                remoteLogLeaderEpochState
                        .handleSegmentWithCopySegmentFinishedState(startOffset, segmentId, leaderEpochEndOffset);
            });

        // Put the entry with the updated metadata.
        idToSegmentMetadata.put(remoteLogSegmentMetadata.remoteLogSegmentId(), remoteLogSegmentMetadata);
    }

    protected final void handleSegmentWithDeleteSegmentStartedState(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        log.debug("Cleaning up the state for : [{}]", remoteLogSegmentMetadata);

        doHandleSegmentStateTransitionForLeaderEpochs(remoteLogSegmentMetadata,
            (leaderEpoch, remoteLogLeaderEpochState, startOffset, segmentId) ->
                    remoteLogLeaderEpochState.handleSegmentWithDeleteSegmentStartedState(startOffset, segmentId));

        // Put the entry with the updated metadata.
        idToSegmentMetadata.put(remoteLogSegmentMetadata.remoteLogSegmentId(), remoteLogSegmentMetadata);
    }

    private void handleSegmentWithDeleteSegmentFinishedState(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        log.debug("Removing the entry as it reached the terminal state: [{}]", remoteLogSegmentMetadata);

        doHandleSegmentStateTransitionForLeaderEpochs(remoteLogSegmentMetadata,
            (leaderEpoch, remoteLogLeaderEpochState, startOffset, segmentId) ->
                    remoteLogLeaderEpochState.handleSegmentWithDeleteSegmentFinishedState(segmentId));

        // Remove the segment's id to metadata mapping because this segment is considered as deleted and it cleared all
        // the state of this segment in the cache.
        idToSegmentMetadata.remove(remoteLogSegmentMetadata.remoteLogSegmentId());
    }

    private void doHandleSegmentStateTransitionForLeaderEpochs(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                                               RemoteLogLeaderEpochState.Action action) {
        RemoteLogSegmentId remoteLogSegmentId = remoteLogSegmentMetadata.remoteLogSegmentId();
        Map<Integer, Long> leaderEpochToOffset = remoteLogSegmentMetadata.segmentLeaderEpochs();

        // Go through all the leader epochs and apply the given action.
        for (Map.Entry<Integer, Long> entry : leaderEpochToOffset.entrySet()) {
            Integer leaderEpoch = entry.getKey();
            Long startOffset = entry.getValue();
            // leaderEpochEntries will be empty when resorting the metadata from snapshot.
            RemoteLogLeaderEpochState remoteLogLeaderEpochState = leaderEpochEntries.computeIfAbsent(
                    leaderEpoch, x -> new RemoteLogLeaderEpochState());
            action.accept(leaderEpoch, remoteLogLeaderEpochState, startOffset, remoteLogSegmentId);
        }
    }

    private static long highestOffsetForEpoch(Integer leaderEpoch, RemoteLogSegmentMetadata segmentMetadata) {
        // Compute the highest offset for the leader epoch with in the segment
        NavigableMap<Integer, Long> epochToOffset = segmentMetadata.segmentLeaderEpochs();
        Map.Entry<Integer, Long> nextEntry = epochToOffset.higherEntry(leaderEpoch);

        return nextEntry != null ? nextEntry.getValue() - 1 : segmentMetadata.endOffset();
    }

    /**
     * Returns all the segments stored in this cache.
     *
     * @return
     */
    public Iterator<RemoteLogSegmentMetadata> listAllRemoteLogSegments() {
        // Return all the segments including unreferenced metadata.
        return Collections.unmodifiableCollection(idToSegmentMetadata.values()).iterator();
    }

    /**
     * Returns all the segments mapped to the leader epoch that exist in this cache sorted by {@link RemoteLogSegmentMetadata#startOffset()}.
     *
     * @param leaderEpoch leader epoch.
     */
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(int leaderEpoch)
            throws RemoteResourceNotFoundException {
        RemoteLogLeaderEpochState remoteLogLeaderEpochState = leaderEpochEntries.get(leaderEpoch);
        if (remoteLogLeaderEpochState == null) {
            return Collections.emptyIterator();
        }

        return remoteLogLeaderEpochState.listAllRemoteLogSegments(idToSegmentMetadata);
    }

    /**
     * Returns the highest offset of a segment for the given leader epoch if exists, else it returns empty. The segments
     * that have reached the {@link RemoteLogSegmentState#COPY_SEGMENT_FINISHED} or later states are considered here.
     *
     * @param leaderEpoch leader epoch
     */
    public Optional<Long> highestOffsetForEpoch(int leaderEpoch) {
        RemoteLogLeaderEpochState entry = leaderEpochEntries.get(leaderEpoch);
        return entry != null ? Optional.ofNullable(entry.highestLogOffset()) : Optional.empty();
    }

    /**
     * This method tracks the given remote segment as not yet available for reads. It does not add the segment
     * leader epoch offset mapping until this segment reaches COPY_SEGMENT_FINISHED state.
     *
     * @param remoteLogSegmentMetadata RemoteLogSegmentMetadata instance
     */
    public void addCopyInProgressSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        log.debug("Adding to in-progress state: [{}]", remoteLogSegmentMetadata);
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");

        // This method is allowed only to add remote log segment with the initial state(which is RemoteLogSegmentState.COPY_SEGMENT_STARTED)
        // but not to update the existing remote log segment metadata.
        if (remoteLogSegmentMetadata.state() != RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
            throw new IllegalArgumentException(
                    "Given remoteLogSegmentMetadata:" + remoteLogSegmentMetadata + " should have state as " + RemoteLogSegmentState.COPY_SEGMENT_STARTED
                    + " but it contains state as: " + remoteLogSegmentMetadata.state());
        }

        RemoteLogSegmentId remoteLogSegmentId = remoteLogSegmentMetadata.remoteLogSegmentId();
        RemoteLogSegmentMetadata existingMetadata = idToSegmentMetadata.get(remoteLogSegmentId);
        boolean isValid = checkStateTransition(existingMetadata != null ? existingMetadata.state() : null,
                remoteLogSegmentMetadata.state(), remoteLogSegmentMetadata.remoteLogSegmentId());
        if (!isValid) {
            return;
        }
        for (Integer epoch : remoteLogSegmentMetadata.segmentLeaderEpochs().keySet()) {
            leaderEpochEntries.computeIfAbsent(epoch, leaderEpoch -> new RemoteLogLeaderEpochState())
                    .handleSegmentWithCopySegmentStartedState(remoteLogSegmentId);
        }
        idToSegmentMetadata.put(remoteLogSegmentId, remoteLogSegmentMetadata);
    }

    private boolean checkStateTransition(RemoteLogSegmentState existingState,
                                         RemoteLogSegmentState targetState,
                                         RemoteLogSegmentId segmentId) {
        boolean isValid = RemoteLogSegmentState.isValidTransition(existingState, targetState);
        if (!isValid) {
            log.error("Current state: {} can not be transitioned to target state: {}, segmentId: {}. Dropping the event",
                    existingState, targetState, segmentId);
        }
        return isValid;
    }
}
