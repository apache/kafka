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

    @Override
    public void handleRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        log.debug("Adding remote log segment : [{}]", remoteLogSegmentMetadata);

        RemoteLogSegmentId remoteLogSegmentId = remoteLogSegmentMetadata.remoteLogSegmentId();

        idToRemoteLogMetadataCache
                .computeIfAbsent(remoteLogSegmentId.topicIdPartition(), id -> new RemoteLogMetadataCache())
                .addCopyInProgressSegment(remoteLogSegmentMetadata);
    }

    @Override
    public void handleRemoteLogSegmentMetadataUpdate(RemoteLogSegmentMetadataUpdate rlsmUpdate) {
        log.debug("Updating remote log segment: [{}]", rlsmUpdate);
        RemoteLogSegmentId remoteLogSegmentId = rlsmUpdate.remoteLogSegmentId();
        TopicIdPartition topicIdPartition = remoteLogSegmentId.topicIdPartition();
        RemoteLogMetadataCache remoteLogMetadataCache = idToRemoteLogMetadataCache.get(topicIdPartition);
        if (remoteLogMetadataCache != null) {
            try {
                remoteLogMetadataCache.updateRemoteLogSegmentMetadata(rlsmUpdate);
            } catch (RemoteResourceNotFoundException e) {
                log.error("Error occurred while updating the remote log segment.");
            }
        } else {
            log.error("No partition metadata found for : " + topicIdPartition);
        }
    }

    @Override
    public void handleRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata) {
        log.debug("Received partition delete state with: [{}]", remotePartitionDeleteMetadata);

        TopicIdPartition topicIdPartition = remotePartitionDeleteMetadata.topicIdPartition();
        idToPartitionDeleteMetadata.put(topicIdPartition, remotePartitionDeleteMetadata);
        // there will be a trigger to receive delete partition marker and act on that to delete all the segments.

        if (remotePartitionDeleteMetadata.state() == RemotePartitionDeleteState.DELETE_PARTITION_FINISHED) {
            // remove the association for the partition.
            idToRemoteLogMetadataCache.remove(topicIdPartition);
            idToPartitionDeleteMetadata.remove(topicIdPartition);
        }
    }

    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition)
            throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");

        return getRemoteLogMetadataCache(topicIdPartition).listAllRemoteLogSegments();
    }

    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition, int leaderEpoch)
            throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");

        return getRemoteLogMetadataCache(topicIdPartition).listRemoteLogSegments(leaderEpoch);
    }

    private RemoteLogMetadataCache getRemoteLogMetadataCache(TopicIdPartition topicIdPartition)
            throws RemoteResourceNotFoundException {
        RemoteLogMetadataCache remoteLogMetadataCache = idToRemoteLogMetadataCache.get(topicIdPartition);
        if (remoteLogMetadataCache == null) {
            throw new RemoteResourceNotFoundException("No resource found for partition: " + topicIdPartition);
        }

        return remoteLogMetadataCache;
    }

    public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(TopicIdPartition topicIdPartition,
                                                                       long offset,
                                                                       int epochForOffset)
            throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");

        return getRemoteLogMetadataCache(topicIdPartition).remoteLogSegmentMetadata(epochForOffset, offset);
    }

    public Optional<Long> highestLogOffset(TopicIdPartition topicIdPartition,
                                           int leaderEpoch) throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");

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
}
