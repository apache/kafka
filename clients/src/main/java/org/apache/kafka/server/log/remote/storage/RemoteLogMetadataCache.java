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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

/**
 * This class provides an imemory cache of remote log segment metadata. This maintains the lineage of segments
 * with respect to epoch evolution. It also keeps track of segments which are not considered to be copied to remote
 * storage.
 */
public class RemoteLogMetadataCache {
    private static final Logger log = LoggerFactory.getLogger(RemoteLogMetadataCache.class);

    private final ConcurrentMap<RemoteLogSegmentId, RemoteLogSegmentMetadata> idToSegmentMetadata
            = new ConcurrentHashMap<>();

    private final Set<RemoteLogSegmentId> remoteLogSegmentIdInProgress = new HashSet<>();

    private final ConcurrentMap<Integer, NavigableMap<Long, RemoteLogSegmentId>> leaderEpochToOffsetToId
            = new ConcurrentHashMap<>();

    public RemoteLogMetadataCache() {
    }

    private void addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        log.debug("Adding remote log segment metadata: [{}]", remoteLogSegmentMetadata);
        idToSegmentMetadata.put(remoteLogSegmentMetadata.remoteLogSegmentId(), remoteLogSegmentMetadata);
        Map<Integer, Long> leaderEpochToOffset = remoteLogSegmentMetadata.segmentLeaderEpochs();
        for (Map.Entry<Integer, Long> entry : leaderEpochToOffset.entrySet()) {
            leaderEpochToOffsetToId.computeIfAbsent(entry.getKey(), k -> new ConcurrentSkipListMap<>())
                    .put(entry.getValue(), remoteLogSegmentMetadata.remoteLogSegmentId());
        }
    }

    public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(int leaderEpoch, long offset) {
        NavigableMap<Long, RemoteLogSegmentId> offsetToId = leaderEpochToOffsetToId.get(leaderEpoch);
        if (offsetToId == null || offsetToId.isEmpty()) {
            return Optional.empty();
        }

        // look for floor entry as the given offset may exist in this entry.
        Map.Entry<Long, RemoteLogSegmentId> entry = offsetToId.floorEntry(offset);
        if (entry == null) {
            // if the offset is lower than the minimum offset available in metadata then return null.
            return Optional.empty();
        }

        RemoteLogSegmentMetadata remoteLogSegmentMetadata = idToSegmentMetadata.get(entry.getValue());
        // check whether the given offset with leaderEpoch exists in this segment.
        // check for epoch's offset boundaries with in this segment.
        //      1. get the next epoch's start offset -1 if exists
        //      2. if no next epoch exists, then segment end offset can be considered as epoch's relative end offset.
        Map.Entry<Integer, Long> nextEntry = remoteLogSegmentMetadata.segmentLeaderEpochs()
                .higherEntry(leaderEpoch);
        long epochEndOffset = (nextEntry != null) ? nextEntry.getValue() - 1 : remoteLogSegmentMetadata.endOffset();

        // seek offset should be <= epoch's end offset.
        return (offset > epochEndOffset) ? Optional.empty() : Optional.of(remoteLogSegmentMetadata);
    }

    public void updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate rlsmUpdate)
            throws RemoteResourceNotFoundException {
        RemoteLogSegmentId remoteLogSegmentId = rlsmUpdate.remoteLogSegmentId();
        RemoteLogSegmentMetadata rlsm = idToSegmentMetadata.get(remoteLogSegmentId);
        if (rlsm == null) {
            throw new RemoteResourceNotFoundException("No remote log segment metadata found for : "
                                                      + remoteLogSegmentId);
        }

        log.debug("Updating remote log segment metadata: [{}]", rlsmUpdate);
        RemoteLogSegmentMetadata updatedRlsm = rlsm.createRemoteLogSegmentWithUpdates(rlsmUpdate);
        idToSegmentMetadata.put(remoteLogSegmentId, updatedRlsm);
        if (rlsmUpdate.state() != RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
            remoteLogSegmentIdInProgress.remove(remoteLogSegmentId);
            addRemoteLogSegmentMetadata(updatedRlsm);
        }

        if (rlsmUpdate.state() == RemoteLogSegmentState.DELETE_SEGMENT_FINISHED) {
            log.debug("Cleaning up the state for : [{}]", rlsmUpdate);
            // remove this entry when the state is moved to delete_segment_finished
            Map<Integer, Long> leaderEpochs = rlsm.segmentLeaderEpochs();
            for (Map.Entry<Integer, Long> entry : leaderEpochs.entrySet()) {
                NavigableMap<Long, RemoteLogSegmentId> offsetToIds = leaderEpochToOffsetToId.get(entry.getKey());
                // remove the mappings where this segment is deleted.
                offsetToIds.values().remove(remoteLogSegmentId);
            }

            // remove the segment-id mapping.
            idToSegmentMetadata.remove(remoteLogSegmentId);
        }
    }

    public Iterator<RemoteLogSegmentMetadata> listAllRemoteLogSegments() {
        ArrayList<RemoteLogSegmentMetadata> list = new ArrayList<>(idToSegmentMetadata.values());
        list.addAll(remoteLogSegmentIdInProgress.stream().map(id -> idToSegmentMetadata.get(id))
                .collect(Collectors.toList()));
        list.sort(Comparator.comparingLong(RemoteLogSegmentMetadata::startOffset));
        return list.iterator();
    }

    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(int leaderEpoch) {
        NavigableMap<Long, RemoteLogSegmentId> map = leaderEpochToOffsetToId.get(leaderEpoch);
        return map != null ? map.values().stream().map(id -> idToSegmentMetadata.get(id)).iterator()
                           : Collections.emptyIterator();
    }

    public Long highestLogOffset(int leaderEpoch) {
        return leaderEpochToOffsetToId.get(leaderEpoch).lastKey();
    }

    /**
     * This will be added to copy_in_progress metadata list. This will be removed from that list once it is moved to the
     * next state which can be COPY_SEGMENT_FINISHED or DELETE_SEGMENT_STARTED.
     *
     * @param remoteLogSegmentMetadata
     */
    public void addToInProgress(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        log.debug("Adding to inprogress state: [{}]", remoteLogSegmentMetadata);
        idToSegmentMetadata.put(remoteLogSegmentMetadata.remoteLogSegmentId(), remoteLogSegmentMetadata);
        remoteLogSegmentIdInProgress.add(remoteLogSegmentMetadata.remoteLogSegmentId());
    }
}
