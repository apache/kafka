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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.server.log.remote.metadata.storage.RemoteLogMetadataCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is an implementation of {@link RemoteLogMetadataManager} backed by in-memory store.
 * This class is not completely thread safe.
 */
public class InmemoryRemoteLogMetadataManager implements RemoteLogMetadataManager {
    private static final Logger log = LoggerFactory.getLogger(InmemoryRemoteLogMetadataManager.class);

    private Map<TopicIdPartition, RemotePartitionDeleteMetadata> idToPartitionDeleteMetadata =
            new ConcurrentHashMap<>();

    private Map<TopicIdPartition, RemoteLogMetadataCache> idToRemoteLogMetadataCache = new ConcurrentHashMap<>();

    @Override
    public void addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata)
            throws RemoteStorageException {
        log.debug("Adding remote log segment : [{}]", remoteLogSegmentMetadata);
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");

        RemoteLogSegmentId remoteLogSegmentId = remoteLogSegmentMetadata.remoteLogSegmentId();

        idToRemoteLogMetadataCache
                .computeIfAbsent(remoteLogSegmentId.topicIdPartition(), id -> new RemoteLogMetadataCache())
                .addCopyInProgressSegment(remoteLogSegmentMetadata);
    }

    @Override
    public void updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate metadataUpdate)
            throws RemoteStorageException {
        log.debug("Updating remote log segment: [{}]", metadataUpdate);
        Objects.requireNonNull(metadataUpdate, "metadataUpdate can not be null");

        getRemoteLogMetadataCache(metadataUpdate.remoteLogSegmentId().topicIdPartition())
                .updateRemoteLogSegmentMetadata(metadataUpdate);
    }

    private RemoteLogMetadataCache getRemoteLogMetadataCache(TopicIdPartition topicIdPartition)
            throws RemoteResourceNotFoundException {
        RemoteLogMetadataCache remoteLogMetadataCache = idToRemoteLogMetadataCache.get(topicIdPartition);
        if (remoteLogMetadataCache == null) {
            throw new RemoteResourceNotFoundException("No existing metadata found for partition: " + topicIdPartition);
        }

        return remoteLogMetadataCache;
    }

    @Override
    public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(TopicIdPartition topicIdPartition,
                                                                       int epochForOffset,
                                                                       long offset)
            throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");

        return getRemoteLogMetadataCache(topicIdPartition).remoteLogSegmentMetadata(epochForOffset, offset);
    }

    @Override
    public Optional<Long> highestOffsetForEpoch(TopicIdPartition topicIdPartition,
                                                int leaderEpoch) throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");

        return getRemoteLogMetadataCache(topicIdPartition).highestOffsetForEpoch(leaderEpoch);
    }

    @Override
    public void putRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata)
            throws RemoteStorageException {
        log.debug("Adding delete state with: [{}]", remotePartitionDeleteMetadata);
        Objects.requireNonNull(remotePartitionDeleteMetadata, "remotePartitionDeleteMetadata can not be null");

        TopicIdPartition topicIdPartition = remotePartitionDeleteMetadata.topicIdPartition();

        RemotePartitionDeleteState targetState = remotePartitionDeleteMetadata.state();
        RemotePartitionDeleteMetadata existingMetadata = idToPartitionDeleteMetadata.get(topicIdPartition);
        RemotePartitionDeleteState existingState = existingMetadata != null ? existingMetadata.state() : null;
        if (!RemotePartitionDeleteState.isValidTransition(existingState, targetState)) {
            throw new IllegalStateException("Current state: " + existingState + ", target state: " + targetState);
        }

        idToPartitionDeleteMetadata.put(topicIdPartition, remotePartitionDeleteMetadata);

        if (targetState == RemotePartitionDeleteState.DELETE_PARTITION_FINISHED) {
            // Remove the association for the partition.
            idToRemoteLogMetadataCache.remove(topicIdPartition);
            idToPartitionDeleteMetadata.remove(topicIdPartition);
        }
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition)
            throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");

        return getRemoteLogMetadataCache(topicIdPartition).listAllRemoteLogSegments();
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition, int leaderEpoch)
            throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");

        return getRemoteLogMetadataCache(topicIdPartition).listRemoteLogSegments(leaderEpoch);
    }

    @Override
    public void onPartitionLeadershipChanges(Set<TopicIdPartition> leaderPartitions,
                                             Set<TopicIdPartition> followerPartitions) {
        // It is not applicable for this implementation. This will track the segments that are added/updated as part of
        // this instance. It does not depend upon any leader or follower transitions.
    }

    @Override
    public void onStopPartitions(Set<TopicIdPartition> partitions) {
        // It is not applicable for this implementation. This will track the segments that are added/updated as part of
        // this instance. It does not depend upon stopped partitions.
    }

    @Override
    public void close() throws IOException {
        // Clearing the references to the map and assigning empty immutable maps.
        // Practically, this instance will not be used once it is closed.
        idToPartitionDeleteMetadata = Collections.emptyMap();
        idToRemoteLogMetadataCache = Collections.emptyMap();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Intentionally left blank here as nothing to be initialized here.
    }
}
