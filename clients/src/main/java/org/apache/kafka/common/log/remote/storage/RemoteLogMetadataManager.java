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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * This interface provides storing and fetching remote log segment metadata with strongly consistent semantics.
 * <p>
 * This class can be plugged in to Kafka cluster by adding the implementation class as
 * <code>remote.log.metadata.manager.class.name</code> property value. There is an inbuilt implementation backed by
 * topic storage in the local cluster. This is used as the default implementation if
 * remote.log.metadata.manager.class.name is not configured.
 * </p>
 * <p>
 * <code>remote.log.metadata.manager.class.path</code> property is about the class path of the RemoteLogStorageManager
 * implementation. If specified, the RemoteLogStorageManager implementation and its dependent libraries will be loaded
 * by a dedicated classloader which searches this class path before the Kafka broker class path. The syntax of this
 * parameter is same with the standard Java class path string.
 * </p>
 * <p>
 * <code>remote.log.metadata.manager.listener.name</code> property is about listener name of the local broker to which
 * it should get connected if needed by RemoteLogMetadataManager implementation. When this is configured all other
 * required properties can be passed as properties with prefix of 'remote.log.metadata.manager.listener.
 * </p>
 * "cluster.id", "broker.id" and all the properties prefixed with "remote.log.metadata." are passed when
 * {@link #configure(Map)} is invoked on this instance.
 * <p>
 * <p>
 * <p>
 * All these APIs are still evolving.
 * <p>
 * We may refactor TopicPartition in the below APIs to an abstraction that contains a unique identifier
 * and TopicPartition. This will be done once unique identifier for a topic is introduced with
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-516%3A+Topic+Identifiers">KIP-516</a>
 */
@InterfaceStability.Unstable
public interface RemoteLogMetadataManager extends Configurable, Closeable {

    /**
     * Stores RemoteLogSegmentMetadata with the containing RemoteLogSegmentId into RemoteLogMetadataManager.
     * <p>
     * RemoteLogSegmentMetadata is identified by RemoteLogSegmentId.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment to be deleted.
     * @throws RemoteStorageException if there are any storage related errors occurred.
     */
    void putRemoteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException;

    /**
     * Fetches RemoteLogSegmentMetadata for the given topic partition containing offset and leader-epoch for the offset.
     * <p>
     *
     * @param topicPartition topic partition
     * @param offset         offset
     * @param epochForOffset leader epoch for the given offset
     * @return the requested remote log segment metadata.
     * @throws RemoteStorageException if there are any storage related errors occurred.
     */
    RemoteLogSegmentMetadata remoteLogSegmentMetadata(TopicPartition topicPartition, long offset, int epochForOffset)
            throws RemoteStorageException;

    /**
     * Returns earliest log offset if there are segments in the remote storage for the given topic partition and
     * leader epoch else returns {@link Optional#empty()}.
     *
     * @param topicPartition topic partition
     * @param leaderEpoch    leader epoch
     * @return the earliest log offset if exists.
     */
    Optional<Long> earliestLogOffset(TopicPartition topicPartition, int leaderEpoch) throws RemoteStorageException;

    /**
     * Returns highest log offset of topic partition for the given leader epoch in remote storage. This is used by
     * remote log management subsystem to know upto which offset the segments have been copied to remote storage  for
     * a given leader epoch.
     *
     * @param topicPartition topic partition
     * @param leaderEpoch    leader epoch
     * @return the requested highest log offset if exists.
     * @throws RemoteStorageException if there are any storage related errors occurred.
     */
    Optional<Long> highestLogOffset(TopicPartition topicPartition, int leaderEpoch) throws RemoteStorageException;

    /**
     * Deletes the log segment metadata for the given remoteLogSegmentMetadata.
     *
     * @param remoteLogSegmentMetadata remote log segment metadata to be deleted.
     * @throws RemoteStorageException if there are any storage related errors occurred.
     */
    void deleteRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException;

    /**
     * List the remote log segment metadata of the given topicPartition.
     * <p>
     * This is used when a topic partition is deleted, to fetch all the remote log segments for the given topic
     * partition and delete them .
     *
     * @return Iterator of remote segments, sorted by baseOffset in ascending order.
     */
    default Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicPartition topicPartition) {
        return listRemoteLogSegments(topicPartition, 0);
    }

    /**
     * Returns iterator of remote log segment metadata, sorted by {@link RemoteLogSegmentMetadata#startOffset()} in
     * ascending order which contains the given leader epoch. This is used by remote log retention management subsystem
     * to fetch the segment metadata for a given leader epoch and cleansup based on retention policies.
     *
     * @param topicPartition topic partition
     * @param leaderEpoch    leader epoch
     * @return Iterator of remote segments, sorted by baseOffset in ascending order.
     */
    Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicPartition topicPartition, long leaderEpoch);

    /**
     * This method is invoked only when there are changes in leadership of the topic partitions that this broker is
     * responsible for.
     *
     * @param leaderPartitions   partitions that have become leaders on this broker.
     * @param followerPartitions partitions that have become followers on this broker.
     */
    void onPartitionLeadershipChanges(Set<TopicPartition> leaderPartitions, Set<TopicPartition> followerPartitions);

    /**
     * This method is invoked only when the given topic partitions are stopped on this broker. This can happen when a
     * partition is emigrated to other broker or a partition is deleted.
     *
     * @param partitions topic partitions which have been stopped.
     */
    void onStopPartitions(Set<TopicPartition> partitions);
}
