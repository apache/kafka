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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteState;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class represents a store to maintain the {@link RemotePartitionDeleteMetadata} and {@link RemoteLogMetadataCache} for each topic partition.
 */
public class RemotePartitionMetadataStore extends RemotePartitionMetadataEventHandler implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(RemotePartitionMetadataStore.class);

    private Map<TopicIdPartition, RemotePartitionDeleteMetadata> idToPartitionDeleteMetadata =
            new ConcurrentHashMap<>();

    private Map<TopicIdPartition, RemoteLogMetadataCache> idToRemoteLogMetadataCache =
            new ConcurrentHashMap<>();

    public RemotePartitionMetadataStore() {
    }

    @Override
    public void handleRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        log.debug("Adding remote log segment: {}", remoteLogSegmentMetadata);

        final RemoteLogSegmentId remoteLogSegmentId = remoteLogSegmentMetadata.remoteLogSegmentId();
        TopicIdPartition topicIdPartition = remoteLogSegmentId.topicIdPartition();

        // This should have been already existing as it is loaded when the partitions are assigned.
        RemoteLogMetadataCache remoteLogMetadataCache = idToRemoteLogMetadataCache.get(topicIdPartition);
        if (remoteLogMetadataCache != null) {
            remoteLogMetadataCache.addCopyInProgressSegment(remoteLogSegmentMetadata);
        } else {
            throw new IllegalStateException("No partition metadata found for : " + topicIdPartition);
        }
    }

    @Override
    public void handleRemoteLogSegmentMetadataUpdate(RemoteLogSegmentMetadataUpdate rlsmUpdate) {
        log.debug("Updating remote log segment: {}", rlsmUpdate);
        RemoteLogSegmentId remoteLogSegmentId = rlsmUpdate.remoteLogSegmentId();
        TopicIdPartition topicIdPartition = remoteLogSegmentId.topicIdPartition();
        RemoteLogMetadataCache remoteLogMetadataCache = idToRemoteLogMetadataCache.get(topicIdPartition);
        if (remoteLogMetadataCache != null) {
            try {
                remoteLogMetadataCache.updateRemoteLogSegmentMetadata(rlsmUpdate);
            } catch (RemoteResourceNotFoundException e) {
                log.warn("Error occurred while updating the remote log segment.", e);
            }
        } else {
            throw new IllegalStateException("No partition metadata found for : " + topicIdPartition);
        }
    }

    @Override
    public void handleRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata) {
        log.debug("Received partition delete state with: {}", remotePartitionDeleteMetadata);

        TopicIdPartition topicIdPartition = remotePartitionDeleteMetadata.topicIdPartition();
        idToPartitionDeleteMetadata.put(topicIdPartition, remotePartitionDeleteMetadata);
        // there will be a trigger to receive delete partition marker and act on that to delete all the segments.

        if (remotePartitionDeleteMetadata.state() == RemotePartitionDeleteState.DELETE_PARTITION_FINISHED) {
            // remove the association for the partition.
            idToRemoteLogMetadataCache.remove(topicIdPartition);
            idToPartitionDeleteMetadata.remove(topicIdPartition);
        }
    }

    @Override
    public void clearTopicPartition(TopicIdPartition topicIdPartition) {
        idToRemoteLogMetadataCache.remove(topicIdPartition);
    }

    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition)
            throws RemoteStorageException {
        return getRemoteLogMetadataCache(topicIdPartition).listAllRemoteLogSegments();
    }

    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition, int leaderEpoch)
            throws RemoteStorageException {
        return getRemoteLogMetadataCache(topicIdPartition).listRemoteLogSegments(leaderEpoch);
    }

    private RemoteLogMetadataCache getRemoteLogMetadataCache(TopicIdPartition topicIdPartition)
            throws RemoteResourceNotFoundException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");
        RemoteLogMetadataCache remoteLogMetadataCache = idToRemoteLogMetadataCache.get(topicIdPartition);
        if (remoteLogMetadataCache == null) {
            throw new RemoteResourceNotFoundException("No resource found for partition: " + topicIdPartition);
        }
        if (!remoteLogMetadataCache.isInitialized()) {
            // Throwing a retriable ReplicaNotAvailableException here for clients retry.
            throw new ReplicaNotAvailableException("Remote log metadata cache is not initialized for partition: " + topicIdPartition);
        }
        return remoteLogMetadataCache;
    }

    public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(TopicIdPartition topicIdPartition,
                                                                       long offset,
                                                                       int epochForOffset)
            throws RemoteStorageException {
        return getRemoteLogMetadataCache(topicIdPartition).remoteLogSegmentMetadata(epochForOffset, offset);
    }

    public Optional<RemoteLogSegmentMetadata> nextSegmentWithTxnIndex(TopicIdPartition topicIdPartition,
                                                                      int epoch,
                                                                      long offset) throws RemoteStorageException {
        return getRemoteLogMetadataCache(topicIdPartition).nextSegmentWithTxnIndex(epoch, offset);
    }

    public Optional<Long> highestLogOffset(TopicIdPartition topicIdPartition,
                                           int leaderEpoch) throws RemoteStorageException {
        return getRemoteLogMetadataCache(topicIdPartition).highestOffsetForEpoch(leaderEpoch);
    }

    @Override
    public void close() throws IOException {
        log.info("Clearing the entries from the store.");

        // Clear the entries by creating unmodifiable empty maps.
        // Practically, we do not use the same instances that are closed.
        idToPartitionDeleteMetadata = Collections.emptyMap();
        idToRemoteLogMetadataCache = Collections.emptyMap();
    }

    @Override
    public void maybeLoadPartition(TopicIdPartition partition) {
        idToRemoteLogMetadataCache.computeIfAbsent(partition, idPartition -> new RemoteLogMetadataCache());
    }

    @Override
    public void markInitialized(TopicIdPartition partition) {
        idToRemoteLogMetadataCache.get(partition).markInitialized();
        log.trace("Remote log components are initialized for user-partition: {}", partition);
    }

    @Override
    public boolean isInitialized(TopicIdPartition topicIdPartition) {
        RemoteLogMetadataCache metadataCache = idToRemoteLogMetadataCache.get(topicIdPartition);
        return metadataCache != null && metadataCache.isInitialized();
    }
}
