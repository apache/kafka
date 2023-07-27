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
import org.apache.kafka.storage.internals.log.StorageAction;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A wrapper class of {@link RemoteLogMetadataManager} that sets the context class loader when calling the respective
 * methods.
 */
public class ClassLoaderAwareRemoteLogMetadataManager implements RemoteLogMetadataManager {
    private final RemoteLogMetadataManager delegate;
    private final ClassLoader loader;

    public ClassLoaderAwareRemoteLogMetadataManager(RemoteLogMetadataManager delegate,
                                                    ClassLoader loader) {
        this.delegate = delegate;
        this.loader = loader;
    }

    @Override
    public CompletableFuture<Void> addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        return withClassLoader(() -> delegate.addRemoteLogSegmentMetadata(remoteLogSegmentMetadata));
    }

    @Override
    public CompletableFuture<Void> updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate remoteLogSegmentMetadataUpdate) throws RemoteStorageException {
        return withClassLoader(() -> delegate.updateRemoteLogSegmentMetadata(remoteLogSegmentMetadataUpdate));
    }

    @Override
    public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(TopicIdPartition topicIdPartition,
                                                                       int epochForOffset,
                                                                       long offset) throws RemoteStorageException {
        return withClassLoader(() -> delegate.remoteLogSegmentMetadata(topicIdPartition, epochForOffset, offset));
    }

    @Override
    public Optional<Long> highestOffsetForEpoch(TopicIdPartition topicIdPartition,
                                                int leaderEpoch) throws RemoteStorageException {
        return withClassLoader(() -> delegate.highestOffsetForEpoch(topicIdPartition, leaderEpoch));
    }

    @Override
    public CompletableFuture<Void> putRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata) throws RemoteStorageException {
        return withClassLoader(() -> delegate.putRemotePartitionDeleteMetadata(remotePartitionDeleteMetadata));
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition) throws RemoteStorageException {
        return withClassLoader(() -> delegate.listRemoteLogSegments(topicIdPartition));
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition,
                                                                    int leaderEpoch) throws RemoteStorageException {
        return withClassLoader(() -> delegate.listRemoteLogSegments(topicIdPartition, leaderEpoch));
    }

    @Override
    public void onPartitionLeadershipChanges(Set<TopicIdPartition> leaderPartitions,
                                             Set<TopicIdPartition> followerPartitions) {
        withClassLoader(() -> {
            delegate.onPartitionLeadershipChanges(leaderPartitions, followerPartitions);
            return null;
        });
    }

    @Override
    public void onStopPartitions(Set<TopicIdPartition> partitions) {
        withClassLoader(() -> {
            delegate.onStopPartitions(partitions);
            return null;
        });
    }

    @Override
    public long remoteLogSize(TopicIdPartition topicIdPartition, int leaderEpoch) throws RemoteStorageException {
        return withClassLoader(() -> delegate.remoteLogSize(topicIdPartition, leaderEpoch));
    }

    @Override
    public void configure(Map<String, ?> configs) {
        withClassLoader(() -> {
            delegate.configure(configs);
            return null;
        });
    }

    @Override
    public void close() throws IOException {
        withClassLoader(() -> {
            delegate.close();
            return null;
        });
    }

    private <T, E extends Exception> T withClassLoader(StorageAction<T, E> action) throws E {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(loader);
        try {
            return action.execute();
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

}
