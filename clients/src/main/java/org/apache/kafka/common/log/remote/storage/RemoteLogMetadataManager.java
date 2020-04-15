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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * This interface provides storing and fetching remote log segment metadata with strongly consistent semantics.
 *
 * When {@link #configure(Map)} is invoked on this instance, {@link #BROKER_ID}, {@link #CLUSTER_ID} properties are
 * passed which can be used by this instance if needed. These props can be used if there is a single storage used for
 * different clusters. For ex: MySQL storage can be used as metadata store for all the clusters across the org.
 *
 * todo-tier cleanup the abstractions in this interface.
 */
@InterfaceStability.Unstable
public interface RemoteLogMetadataManager extends Configurable, Closeable {

    /**
     *
     */
    String BROKER_ID = "broker.id";

    /**
     *
     */
    String CLUSTER_ID = "cluster.id";

    /**
     * Stores RemoteLogSegmentMetadata with the given RemoteLogSegmentId.
     *
     * @param remoteLogSegmentMetadata
     * @throws IOException
     */
    void putRemoteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws IOException;

    /**
     * Fetches RemoteLogSegmentId for the given topic partition which contains the given offset.
     *
     * @param topicPartition
     * @param offset
     * @return
     * @throws IOException
     */
    RemoteLogSegmentId getRemoteLogSegmentId(TopicPartition topicPartition, long offset) throws IOException;

    /**
     * Fetches RemoteLogSegmentMetadata for the given RemoteLogSegmentId.
     *
     * @param remoteLogSegmentId
     * @return
     * @throws IOException
     */
    RemoteLogSegmentMetadata getRemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId) throws IOException;

    /**
     * Earliest log offset if exists for the given topic partition in the remote storage. Return {@link Optional#empty()}
     * if there are no segments in the remote storage.
     *
     * @param tp
     * @return
     */
    Optional<Long> earliestLogOffset(TopicPartition tp) throws IOException;

    /**
     *
     * @param tp
     * @return
     * @throws IOException
     */
    Optional<Long> highestLogOffset(TopicPartition tp) throws IOException;

    /**
     * Deletes the log segment metadata for the given remoteLogSegmentId.
     *
     * @param remoteLogSegmentId
     * @throws IOException
     */
    void deleteRemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId) throws IOException;

    /**
     * List the remote log segment files of the given topicPartition.
     * The RemoteLogManager of a follower uses this method to find out the remote data for the given topic partition.
     *
     * @return List of remote segments, sorted by baseOffset in ascending order.
     */
    default List<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicPartition topicPartition) {
        return listRemoteLogSegments(topicPartition, 0);
    }

    /**
     * @param topicPartition
     * @param minOffset
     * @return List of remote segments, sorted by baseOffset in ascending order.
     */
    List<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicPartition topicPartition, long minOffset);

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
     * @param partitions
     */
    void onStopPartitions(Set<TopicPartition> partitions);

    /**
     * Callback to receive once server is started so that this class can run tasks which should be run only when the
     * server is started.
     */
    void onServerStarted(final String serverEndpoint);
}
