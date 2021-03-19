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
package org.apache.kafka.server.log.remote.storage;

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
 * Segment in this state indicate it is not yet copied successfully. So, these segments will not be
 * accessible for reads but these are considered for cleanups when a partition is deleted.
 * </li>
 * <li>
 * {@link RemoteLogSegmentState#COPY_SEGMENT_FINISHED}:
 * <br>
 * Segment in this state indicate it is successfully copied and it is available for reads. So, these segments
 * will be accessible for reads. But this should be available for any cleanup activity like deleting segments by the
 * caller of this class.
 * </li>
 * <li>
 * {@link RemoteLogSegmentState#DELETE_SEGMENT_STARTED}:
 * Segment in this state indicate it is getting deleted. That means, it is not available for reads. But it should be
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
 * <table border="4">
 * <thead border="4">
 * <tr>
 * <th></th>
 * <th>COPY_SEGMENT_STARTED</th>
 * <th>COPY_SEGMENT_FINISHED</th>
 * <th>DELETE_SEGMENT_STARTED</th>
 * <th>DELETE_SEGMENT_FINISHED</th>
 * </tr>
 * </thead>
 * <tbody>
 * <tr>
 * <td>remoteLogSegmentMetadata<br>(int leaderEpoch, long offset)</td>
 * <td>No</td>
 * <td>Yes</td>
 * <td>No</td>
 * <td>No</td>
 * </tr>
 * <tr>
 * <td>listRemoteLogSegments <br>(int leaderEpoch)</td>
 * <td>Yes</td>
 * <td>Yes</td>
 * <td>Yes</td>
 * <td>No</td>
 * </tr>
 * <tr>
 * <td>listAllRemoteLogSegments()</td>
 * <td>Yes</td>
 * <td>Yes</td>
 * <td>Yes</td>
 * <td>No</td>
 * </tr>
 * </tbody>
 * </table>
 * </p>
 * <p></p>
 */
public class RemoteLogMetadataCache {

    private static final Logger log = LoggerFactory.getLogger(RemoteLogMetadataCache.class);

    // It contains all the segment-id to metadata mappings which did not reach the terminal state viz DELETE_SEGMENT_FINISHED.
    private final ConcurrentMap<RemoteLogSegmentId, RemoteLogSegmentMetadata> idToSegmentMetadata
            = new ConcurrentHashMap<>();

    // It contains leader epoch to the respective entry containing the state.
    private final ConcurrentMap<Integer, RemoteLogLeaderEpochState> leaderEpochEntries = new ConcurrentHashMap<>();

    /**
     * Returns {@link RemoteLogSegmentMetadata} if it exists for the given leader-epoch containing the offset and with
     * {@link RemoteLogSegmentState#COPY_SEGMENT_FINISHED} state, else returns {@link Optional#empty()}.
     *
     * @param leaderEpoch leader epoch for the given offset
     * @param offset      offset
     * @return the requested remote log segment metadata if it exists.
     */
    public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(int leaderEpoch, long offset) {
        RemoteLogLeaderEpochState remoteLogLeaderEpochState = leaderEpochEntries.get(leaderEpoch);

        if (remoteLogLeaderEpochState == null) {
            return Optional.empty();
        }

        // Look for floor entry as the given offset may exist in this entry.
        RemoteLogSegmentId remoteLogSegmentId = remoteLogLeaderEpochState.floorEntry(offset);
        if (remoteLogSegmentId == null) {
            // If the offset is lower than the minimum offset available in metadata then return empty.
            return Optional.empty();
        }

        RemoteLogSegmentMetadata metadata = idToSegmentMetadata.get(remoteLogSegmentId);
        // Check whether the given offset with leaderEpoch exists in this segment.
        // Check for epoch's offset boundaries with in this segment.
        //      1. Get the next epoch's start offset -1 if exists
        //      2. If no next epoch exists, then segment end offset can be considered as epoch's relative end offset.
        Map.Entry<Integer, Long> nextEntry = metadata.segmentLeaderEpochs().higherEntry(leaderEpoch);
        long epochEndOffset = (nextEntry != null) ? nextEntry.getValue() - 1 : metadata.endOffset();

        // Return empty when target offset > epoch's end offset or segment is not in COPY_SEGMENT_FINISHED state.
        // This segment will not be available in offsetToId when it reaches the DELETE_SEGMENT_FINISHED state. So, no
        // need to add for that state here.
        return (offset > epochEndOffset || metadata.state() != RemoteLogSegmentState.COPY_SEGMENT_FINISHED)
               ? Optional.empty() : Optional.of(metadata);
    }

    public void updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate metadataUpdate)
            throws RemoteResourceNotFoundException {
        log.debug("Updating remote log segment metadata: [{}]", metadataUpdate);
        Objects.requireNonNull(metadataUpdate, "metadataUpdate can not be null");

        RemoteLogSegmentState targetState = metadataUpdate.state();
        RemoteLogSegmentId remoteLogSegmentId = metadataUpdate.remoteLogSegmentId();
        RemoteLogSegmentMetadata existingMetadata = idToSegmentMetadata.get(remoteLogSegmentId);
        if (existingMetadata == null) {
            throw new RemoteResourceNotFoundException("No remote log segment metadata found for :" +
                                                      remoteLogSegmentId);
        }

        // Check the state transition.
        checkStateTransition(existingMetadata.state(), targetState);

        switch (targetState) {
            case COPY_SEGMENT_STARTED:
                // Callers should use addCopyInProgressSegment to add RemoteLogSegmentMetadata with state as
                // RemoteLogSegmentState.COPY_SEGMENT_STARTED.
                throw new IllegalArgumentException("Metadata with " + RemoteLogSegmentState.COPY_SEGMENT_STARTED +
                                                   " can not be updated");
            case COPY_SEGMENT_FINISHED:
                handleSegmentWithCopySegmentFinishedState(metadataUpdate, existingMetadata);
                break;
            case DELETE_SEGMENT_STARTED:
                handleSegmentWithDeleteSegmentStartedState(metadataUpdate, existingMetadata);
                break;
            case DELETE_SEGMENT_FINISHED:
                handleSegmentWithDeleteSegmentFinishedState(metadataUpdate, existingMetadata);
                break;
            default:
                throw new IllegalArgumentException("Metadata with the state" + targetState + " is not supported");
        }
    }

    private void handleSegmentWithCopySegmentFinishedState(RemoteLogSegmentMetadataUpdate metadataUpdate,
                                                           RemoteLogSegmentMetadata existingMetadata) {
        log.debug("Adding remote log segment metadata to leader epoch mappings with update: [{}]", metadataUpdate);

        doHandleSegmentStateTransitionForLeaderEpochs(existingMetadata,
                RemoteLogLeaderEpochState::handleSegmentWithCopySegmentFinishedState);

        // Put the entry with the updated metadata.
        idToSegmentMetadata.put(existingMetadata.remoteLogSegmentId(),
                existingMetadata.createWithUpdates(metadataUpdate));
    }

    private void handleSegmentWithDeleteSegmentStartedState(RemoteLogSegmentMetadataUpdate metadataUpdate,
                                                            RemoteLogSegmentMetadata existingMetadata) {
        log.debug("Cleaning up the state for : [{}]", metadataUpdate);

        doHandleSegmentStateTransitionForLeaderEpochs(existingMetadata,
                RemoteLogLeaderEpochState::handleSegmentWithDeleteSegmentStartedState);

        // Put the entry with the updated metadata.
        idToSegmentMetadata.put(existingMetadata.remoteLogSegmentId(),
                existingMetadata.createWithUpdates(metadataUpdate));
    }

    private void handleSegmentWithDeleteSegmentFinishedState(RemoteLogSegmentMetadataUpdate metadataUpdate,
                                                             RemoteLogSegmentMetadata existingMetadata) {
        log.debug("Removing the entry as it reached the terminal state: [{}]", metadataUpdate);

        doHandleSegmentStateTransitionForLeaderEpochs(existingMetadata,
                RemoteLogLeaderEpochState::handleSegmentWithDeleteSegmentFinishedState);

        // Remove the segment's id to metadata mapping because this segment is considered as deleted and it cleared all
        // the state of this segment in the cache.
        idToSegmentMetadata.remove(existingMetadata.remoteLogSegmentId());
    }

    private void doHandleSegmentStateTransitionForLeaderEpochs(RemoteLogSegmentMetadata existingMetadata,
                                                               RemoteLogLeaderEpochState.Action action) {
        RemoteLogSegmentId remoteLogSegmentId = existingMetadata.remoteLogSegmentId();
        Map<Integer, Long> leaderEpochToOffset = existingMetadata.segmentLeaderEpochs();

        // Go through all the leader epochs and apply the given action.
        for (Map.Entry<Integer, Long> entry : leaderEpochToOffset.entrySet()) {
            Integer leaderEpoch = entry.getKey();
            Long startOffset = entry.getValue();
            RemoteLogLeaderEpochState remoteLogLeaderEpochState = leaderEpochEntries.get(leaderEpoch);

            if (remoteLogLeaderEpochState == null) {
                throw new IllegalStateException("RemoteLogLeaderEpochState does not exist for the leader epoch: "
                                                + leaderEpoch);
            } else {
                long leaderEpochEndOffset = highestOffsetForEpoch(leaderEpoch, existingMetadata);
                action.accept(remoteLogLeaderEpochState, startOffset, remoteLogSegmentId, leaderEpochEndOffset);
            }
        }
    }

    private long highestOffsetForEpoch(Integer leaderEpoch, RemoteLogSegmentMetadata segmentMetadata) {
        //compute the highest offset for the leader epoch with in the segment
        NavigableMap<Integer, Long> epochToOffset = segmentMetadata.segmentLeaderEpochs();
        Map.Entry<Integer, Long> nextEntry = epochToOffset.higherEntry(leaderEpoch);

        // Update with the given metadata for leader epoch
        //  - If there is no highest entry  OR
        //  - If the existing entry's endOffset is lower than the given metadata's endOffset.
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
     * @return
     */
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(int leaderEpoch) {
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
     * @return
     */
    public Optional<Long> highestOffsetForEpoch(int leaderEpoch) {
        RemoteLogLeaderEpochState entry = leaderEpochEntries.get(leaderEpoch);
        return entry != null ? Optional.ofNullable(entry.highestLogOffset()) : Optional.empty();
    }

    /**
     * This method tracks the given remote segment as not yet available for reads. It does not add the segment
     * leader epoch offset mapping until this segment reaches COPY_SEGMENT_FINISHED state.
     *
     * @param remoteLogSegmentMetadata
     */
    public void addCopyInProgressSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        log.debug("Adding to in-progress state: [{}]", remoteLogSegmentMetadata);
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");

        // This method is allowed only to add remote log segment with the initial state(which is RemoteLogSegmentState.COPY_SEGMENT_STARTED)
        // but not to update the existing remote log segment metadata.
        if (remoteLogSegmentMetadata.state() != RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
            throw new IllegalArgumentException(
                    "Given remoteLogSegmentMetadata should have state as " + RemoteLogSegmentState.COPY_SEGMENT_STARTED
                    + " but it contains state as: " + remoteLogSegmentMetadata.state());
        }

        checkStateTransition(null, remoteLogSegmentMetadata.state());

        for (Integer epoch : remoteLogSegmentMetadata.segmentLeaderEpochs().keySet()) {
            leaderEpochEntries.computeIfAbsent(epoch, leaderEpoch -> new RemoteLogLeaderEpochState())
                    .handleSegmentWithCopySegmentStartedState(remoteLogSegmentMetadata.remoteLogSegmentId());
        }

        idToSegmentMetadata.put(remoteLogSegmentMetadata.remoteLogSegmentId(), remoteLogSegmentMetadata);
    }

    private void checkStateTransition(RemoteLogSegmentState existingState, RemoteLogSegmentState targetState) {
        if (!RemoteLogSegmentState.isValidTransition(existingState, targetState)) {
            throw new IllegalStateException(
                    "Current state: " + existingState + " can not be transitioned to target state: " + targetState);
        }
    }

}
