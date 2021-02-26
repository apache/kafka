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

public class InmemoryRemoteLogMetadataManager implements RemoteLogMetadataManager {
    private static final Logger log = LoggerFactory.getLogger(InmemoryRemoteLogMetadataManager.class);

    private final ConcurrentMap<TopicIdPartition, RemotePartitionDeleteMetadata> idToPartitionDeleteMetadata =
            new ConcurrentHashMap<>();

    private final ConcurrentMap<TopicIdPartition, RemoteLogMetadataCache> partitionToRemoteLogMetadataCache =
            new ConcurrentHashMap<>();

    @Override
    public void addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata)
            throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");

        // this method is allowed only to add remote log segment with the initial state(which is RemoteLogSegmentState.COPY_SEGMENT_STARTED)
        // but not to update the existing remote log segment metadata.
        if (remoteLogSegmentMetadata.state() != RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
            throw new IllegalArgumentException("Given remoteLogSegmentMetadata should have state as " + RemoteLogSegmentState.COPY_SEGMENT_STARTED
                    + " but it contains state as: " + remoteLogSegmentMetadata.state());
        }

        log.debug("Adding remote log segment : [{}]", remoteLogSegmentMetadata);

        RemoteLogSegmentId remoteLogSegmentId = remoteLogSegmentMetadata.remoteLogSegmentId();

        RemoteLogMetadataCache remoteLogMetadataCache = partitionToRemoteLogMetadataCache
                .computeIfAbsent(remoteLogSegmentId.topicIdPartition(), id -> new RemoteLogMetadataCache());

        remoteLogMetadataCache.addToInProgress(remoteLogSegmentMetadata);
    }

    @Override
    public void updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate rlsmUpdate)
            throws RemoteStorageException {
        Objects.requireNonNull(rlsmUpdate, "rlsmUpdate can not be null");

        // Callers should use putRemoteLogSegmentMetadata to add RemoteLogSegmentMetadata with state as
        // RemoteLogSegmentState.COPY_SEGMENT_STARTED.
        if (rlsmUpdate.state() == RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
            throw new IllegalArgumentException("Given remoteLogSegmentMetadata should not have the state as: "
                                               + RemoteLogSegmentState.COPY_SEGMENT_STARTED);
        }
        log.debug("Updating remote log segment: [{}]", rlsmUpdate);
        RemoteLogSegmentId remoteLogSegmentId = rlsmUpdate.remoteLogSegmentId();
        TopicIdPartition topicIdPartition = remoteLogSegmentId.topicIdPartition();
        RemoteLogMetadataCache remoteLogMetadataCache = partitionToRemoteLogMetadataCache.get(topicIdPartition);
        if (remoteLogMetadataCache == null) {
            throw new RemoteResourceNotFoundException("No partition metadata found for : " + topicIdPartition);
        }

        remoteLogMetadataCache.updateRemoteLogSegmentMetadata(rlsmUpdate);
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
            throw new RemoteResourceNotFoundException("No resource found for partition: " + topicIdPartition);
        }

        Long highestKey = remoteLogMetadataCache.highestLogOffset(leaderEpoch);
        return Optional.ofNullable(highestKey);
    }

    @Override
    public void putRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata)
            throws RemoteStorageException {
        Objects.requireNonNull(remotePartitionDeleteMetadata, "remotePartitionDeleteMetadata can not be null");
        log.debug("Adding delete state with: [{}]", remotePartitionDeleteMetadata);
        TopicIdPartition topicIdPartition = remotePartitionDeleteMetadata.topicIdPartition();
        idToPartitionDeleteMetadata.put(topicIdPartition, remotePartitionDeleteMetadata);
        // there will be a trigger to receive delete partition marker and act on that to delete all the segments.

        if (remotePartitionDeleteMetadata.state() == RemotePartitionDeleteState.DELETE_PARTITION_FINISHED) {
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
            throw new RemoteResourceNotFoundException("No resource found for partition: " + topicIdPartition);
        }

        return remoteLogMetadataCache.listAllRemoteLogSegments();
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition, int leaderEpoch)
            throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");

        RemoteLogMetadataCache remoteLogMetadataCache = partitionToRemoteLogMetadataCache.get(topicIdPartition);
        if (remoteLogMetadataCache == null) {
            throw new RemoteResourceNotFoundException("No resource found for partition: " + topicIdPartition);
        }

        return remoteLogMetadataCache.listRemoteLogSegments(leaderEpoch);
    }

    @Override
    public void onPartitionLeadershipChanges(Set<TopicIdPartition> leaderPartitions,
                                             Set<TopicIdPartition> followerPartitions) {
    }

    @Override
    public void onStopPartitions(Set<TopicIdPartition> partitions) {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
