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
package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ClassLoaderAwareRemoteLogMetadataManager implements RemoteLogMetadataManager {
    private final ClassLoader loader;
    private final RemoteLogMetadataManager delegate;

    public ClassLoaderAwareRemoteLogMetadataManager(RemoteLogMetadataManager delegate, ClassLoader loader) {
        this.loader = loader;
        this.delegate = delegate;
    }

    @Override
    public void putRemoteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(loader);
        try {
            delegate.putRemoteLogSegmentData(remoteLogSegmentMetadata);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    @Override
    public RemoteLogSegmentMetadata remoteLogSegmentMetadata(TopicPartition topicPartition, long offset)
            throws RemoteStorageException {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(loader);
        try {
            return delegate.remoteLogSegmentMetadata(topicPartition, offset);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    @Override
    public Optional<Long> earliestLogOffset(TopicPartition tp) throws RemoteStorageException {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(loader);
        try {
            return delegate.earliestLogOffset(tp);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    @Override
    public Optional<Long> highestLogOffset(TopicPartition tp) throws RemoteStorageException {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(loader);
        try {
            return delegate.highestLogOffset(tp);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    @Override
    public void deleteRemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId) throws RemoteStorageException {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(loader);
        try {
            delegate.deleteRemoteLogSegmentMetadata(remoteLogSegmentId);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    @Override
    public List<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicPartition topicPartition, long minOffset) {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(loader);
        try {
            return delegate.listRemoteLogSegments(topicPartition, minOffset);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    @Override
    public void onPartitionLeadershipChanges(Set<TopicPartition> leaderPartitions,
                                             Set<TopicPartition> followerPartitions) {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(loader);
        try {
            delegate.onPartitionLeadershipChanges(leaderPartitions, followerPartitions);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    @Override
    public void onStopPartitions(Set<TopicPartition> partitions) {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(loader);
        try {
            delegate.onStopPartitions(partitions);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    @Override
    public void onServerStarted(final String serverEndpoint) {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(loader);
        try {
            delegate.onServerStarted(serverEndpoint);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    @Override
    public void close() throws IOException {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(loader);
        try {
            delegate.close();
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(loader);
        try {
            delegate.configure(configs);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }
}
