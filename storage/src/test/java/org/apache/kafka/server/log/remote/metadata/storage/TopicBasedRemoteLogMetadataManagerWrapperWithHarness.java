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
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class TopicBasedRemoteLogMetadataManagerWrapperWithHarness implements RemoteLogMetadataManager {

    private final TopicBasedRemoteLogMetadataManagerHarness remoteLogMetadataManagerHarness = new TopicBasedRemoteLogMetadataManagerHarness();

    @Override
    public CompletableFuture<Void> addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        return remoteLogMetadataManagerHarness.remoteLogMetadataManager().addRemoteLogSegmentMetadata(remoteLogSegmentMetadata);
    }

    @Override
    public CompletableFuture<Void> updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate remoteLogSegmentMetadataUpdate) throws RemoteStorageException {
        return remoteLogMetadataManagerHarness.remoteLogMetadataManager().updateRemoteLogSegmentMetadata(remoteLogSegmentMetadataUpdate);
    }

    @Override
    public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(TopicIdPartition topicIdPartition,
                                                                       int epochForOffset,
                                                                       long offset) throws RemoteStorageException {
        return remoteLogMetadataManagerHarness.remoteLogMetadataManager().remoteLogSegmentMetadata(topicIdPartition, epochForOffset, offset);
    }

    @Override
    public Optional<Long> highestOffsetForEpoch(TopicIdPartition topicIdPartition,
                                                int leaderEpoch) throws RemoteStorageException {
        return remoteLogMetadataManagerHarness.remoteLogMetadataManager().highestOffsetForEpoch(topicIdPartition, leaderEpoch);
    }

    @Override
    public CompletableFuture<Void> putRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata) throws RemoteStorageException {
        return remoteLogMetadataManagerHarness.remoteLogMetadataManager().putRemotePartitionDeleteMetadata(remotePartitionDeleteMetadata);
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition) throws RemoteStorageException {
        return remoteLogMetadataManagerHarness.remoteLogMetadataManager().listRemoteLogSegments(topicIdPartition);
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition,
                                                                    int leaderEpoch) throws RemoteStorageException {
        return remoteLogMetadataManagerHarness.remoteLogMetadataManager().listRemoteLogSegments(topicIdPartition, leaderEpoch);
    }

    @Override
    public void onPartitionLeadershipChanges(Set<TopicIdPartition> leaderPartitions,
                                             Set<TopicIdPartition> followerPartitions) {

        remoteLogMetadataManagerHarness.remoteLogMetadataManager().onPartitionLeadershipChanges(leaderPartitions, followerPartitions);
    }

    @Override
    public void onStopPartitions(Set<TopicIdPartition> partitions) {
        remoteLogMetadataManagerHarness.remoteLogMetadataManager().onStopPartitions(partitions);
    }

    @Override
    public long remoteLogSize(TopicIdPartition topicIdPartition, int leaderEpoch) throws RemoteStorageException {
        return remoteLogMetadataManagerHarness.remoteLogMetadataManager().remoteLogSize(topicIdPartition, leaderEpoch);
    }

    @Override
    public void close() throws IOException {
        remoteLogMetadataManagerHarness.close();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // This will make sure the cluster is up and TopicBasedRemoteLogMetadataManager is initialized.
        remoteLogMetadataManagerHarness.initialize(Collections.emptySet(), true);
        remoteLogMetadataManagerHarness.remoteLogMetadataManager().configure(configs);
    }
}
