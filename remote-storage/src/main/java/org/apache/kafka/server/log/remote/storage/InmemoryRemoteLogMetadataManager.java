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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class is an implementation of {@link RemoteLogMetadataManager} backed by inmemory store.
 */
public class InmemoryRemoteLogMetadataManager implements RemoteLogMetadataManager {
    private static final Logger log = LoggerFactory.getLogger(InmemoryRemoteLogMetadataManager.class);

    private final ConcurrentMap<TopicIdPartition, RemotePartitionDeleteMetadata> idToPartitionDeleteMetadata =
            new ConcurrentHashMap<>();

    private final ConcurrentMap<TopicIdPartition, RemoteLogMetadataCache> partitionToRemoteLogMetadataCache =
            new ConcurrentHashMap<>();

    @Override
    public void addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata)
            throws RemoteStorageException {
        log.debug("Adding remote log segment : [{}]", remoteLogSegmentMetadata);
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");

        // this method is allowed only to add remote log segment with the initial state(which is RemoteLogSegmentState.COPY_SEGMENT_STARTED)
        // but not to update the existing remote log segment metadata.
        if (remoteLogSegmentMetadata.state() != RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
            throw new IllegalArgumentException("Given remoteLogSegmentMetadata should have state as " + RemoteLogSegmentState.COPY_SEGMENT_STARTED
                    + " but it contains state as: " + remoteLogSegmentMetadata.state());
        }

        RemoteLogSegmentId remoteLogSegmentId = remoteLogSegmentMetadata.remoteLogSegmentId();

        RemoteLogMetadataCache remoteLogMetadataCache = partitionToRemoteLogMetadataCache
                .computeIfAbsent(remoteLogSegmentId.topicIdPartition(), id -> new RemoteLogMetadataCache());

        remoteLogMetadataCache.addToInProgress(remoteLogSegmentMetadata);
    }

    @Override
    public void updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate metadataUpdate)
            throws RemoteStorageException {
        log.debug("Updating remote log segment: [{}]", metadataUpdate);
        Objects.requireNonNull(metadataUpdate, "metadataUpdate can not be null");

        RemoteLogSegmentState targetState = metadataUpdate.state();
        // Callers should use putRemoteLogSegmentMetadata to add RemoteLogSegmentMetadata with state as
        // RemoteLogSegmentState.COPY_SEGMENT_STARTED.
        if (targetState == RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
            throw new IllegalArgumentException("Given remoteLogSegmentMetadata should not have the state as: "
                                               + RemoteLogSegmentState.COPY_SEGMENT_STARTED);
        }

        RemoteLogSegmentId remoteLogSegmentId = metadataUpdate.remoteLogSegmentId();
        TopicIdPartition topicIdPartition = remoteLogSegmentId.topicIdPartition();
        RemoteLogMetadataCache remoteLogMetadataCache = partitionToRemoteLogMetadataCache.get(topicIdPartition);
        if (remoteLogMetadataCache == null) {
            throw new RemoteResourceNotFoundException("No metadata found for partition: " + topicIdPartition);
        }

        remoteLogMetadataCache.updateRemoteLogSegmentMetadata(metadataUpdate);
    }

    @Override
    public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(TopicIdPartition topicIdPartition,
                                                                       long offset,
                                                                       int epochForOffset)
            throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");

        RemoteLogMetadataCache remoteLogMetadataCache = partitionToRemoteLogMetadataCache.get(topicIdPartition);
        if (remoteLogMetadataCache == null) {
            throw new RemoteResourceNotFoundException("No metadata found for the given partition: " + topicIdPartition);
        }

        return remoteLogMetadataCache.remoteLogSegmentMetadata(epochForOffset, offset);
    }

    @Override
    public Optional<Long> highestLogOffset(TopicIdPartition topicIdPartition,
                                           int leaderEpoch) throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");

        RemoteLogMetadataCache remoteLogMetadataCache = partitionToRemoteLogMetadataCache.get(topicIdPartition);
        if (remoteLogMetadataCache == null) {
            throw new RemoteResourceNotFoundException("No metadata found for partition: " + topicIdPartition);
        }

        return remoteLogMetadataCache.highestLogOffset(leaderEpoch);    
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
            // remove the association for the partition.
            partitionToRemoteLogMetadataCache.remove(topicIdPartition);
            idToPartitionDeleteMetadata.remove(topicIdPartition);
        }
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition)
            throws RemoteStorageException {
        RemoteLogMetadataCache remoteLogMetadataCache = partitionToRemoteLogMetadataCache.get(topicIdPartition);
        if (remoteLogMetadataCache == null) {
            throw new RemoteResourceNotFoundException("No metadata found for partition: " + topicIdPartition);
        }

        return remoteLogMetadataCache.listAllRemoteLogSegments();
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition, int leaderEpoch)
            throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");

        RemoteLogMetadataCache remoteLogMetadataCache = partitionToRemoteLogMetadataCache.get(topicIdPartition);
        if (remoteLogMetadataCache == null) {
            throw new RemoteResourceNotFoundException("No metadata found for partition: " + topicIdPartition);
        }

        return remoteLogMetadataCache.listRemoteLogSegments(leaderEpoch);
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
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
