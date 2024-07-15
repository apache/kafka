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
package kafka.log.remote.quota;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.server.log.remote.metadata.storage.RemotePartitionMetadataStore;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class InMemoryRemoteLogMetadataManager implements RemoteLogMetadataManager {
    RemotePartitionMetadataStore cache = new RemotePartitionMetadataStore();

    public void initialise(TopicIdPartition tp) {
        cache.maybeLoadPartition(tp);
        cache.markInitialized(tp);
    }

    @Override
    public CompletableFuture<Void> addRemoteLogSegmentMetadata(final RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        cache.handleRemoteLogSegmentMetadata(remoteLogSegmentMetadata);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> updateRemoteLogSegmentMetadata(final RemoteLogSegmentMetadataUpdate remoteLogSegmentMetadataUpdate) throws RemoteStorageException {
        cache.handleRemoteLogSegmentMetadataUpdate(remoteLogSegmentMetadataUpdate);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(final TopicIdPartition topicIdPartition, final int epochForOffset, final long offset) throws RemoteStorageException {
        return cache.remoteLogSegmentMetadata(topicIdPartition, offset, epochForOffset);
    }

    @Override
    public Optional<Long> highestOffsetForEpoch(final TopicIdPartition topicIdPartition, final int leaderEpoch) throws RemoteStorageException {
        try {
            return cache.highestLogOffset(topicIdPartition, leaderEpoch);
        } catch (RemoteResourceNotFoundException e) {
            return Optional.empty();
        }
    }

    @Override
    public CompletableFuture<Void> putRemotePartitionDeleteMetadata(final RemotePartitionDeleteMetadata remotePartitionDeleteMetadata) throws RemoteStorageException {
        cache.handleRemotePartitionDeleteMetadata(remotePartitionDeleteMetadata);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(final TopicIdPartition topicIdPartition) throws RemoteStorageException {
        return cache.listRemoteLogSegments(topicIdPartition);
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(final TopicIdPartition topicIdPartition, final int leaderEpoch) throws RemoteStorageException {
        return cache.listRemoteLogSegments(topicIdPartition, leaderEpoch);
    }

    @Override
    public void onPartitionLeadershipChanges(final Set<TopicIdPartition> leaderPartitions, final Set<TopicIdPartition> followerPartitions) {
    }

    @Override
    public void onStopPartitions(final Set<TopicIdPartition> partitions) {
    }

    @Override
    public long remoteLogSize(final TopicIdPartition topicIdPartition, final int leaderEpoch) throws RemoteStorageException {
        long size = 0L;
        for (Iterator<RemoteLogSegmentMetadata> it = listRemoteLogSegments(topicIdPartition, leaderEpoch); it.hasNext(); ) {
            final RemoteLogSegmentMetadata metadata = it.next();
            size += metadata.segmentSizeInBytes();
        }
        return size;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(final Map<String, ?> configs) {

    }
}
