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
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

/**
 * This interface defines the lifecycle methods for {@code RemoteLogSegmentMetadata}. {@link RemoteLogSegmentLifecycleTest} tests
 * different implementations of this interface. This is responsible for managing all the segments for a given {@code topicIdPartition}
 * registered with {@link #initialize(TopicIdPartition)}.
 *
 * @see org.apache.kafka.server.log.remote.metadata.storage.RemoteLogSegmentLifecycleTest.RemoteLogMetadataCacheWrapper
 * @see org.apache.kafka.server.log.remote.metadata.storage.RemoteLogSegmentLifecycleTest.TopicBasedRemoteLogMetadataManagerWrapper
 */
public interface RemoteLogSegmentLifecycleManager extends Closeable {

    /**
     * Initialize the resources for this instance and register the given {@code topicIdPartition}.
     *
     * @param topicIdPartition topic partition to be registered with this instance.
     */
    default void initialize(TopicIdPartition topicIdPartition) {
    }

    @Override
    default void close() throws IOException {
    }

    void addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata segmentMetadata) throws RemoteStorageException;

    void updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate segmentMetadataUpdate) throws RemoteStorageException;

    Optional<Long> highestOffsetForEpoch(int epoch) throws RemoteStorageException;

    Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(int leaderEpoch,
                                                                long offset) throws RemoteStorageException;

    Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(int leaderEpoch) throws RemoteStorageException;

    Iterator<RemoteLogSegmentMetadata> listAllRemoteLogSegments() throws RemoteStorageException;
}
