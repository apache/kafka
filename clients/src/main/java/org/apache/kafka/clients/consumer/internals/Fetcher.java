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

package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.utils.Timer;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface Fetcher<K, V> extends Closeable {

    void close();

    Fetch<K, V> collectFetch();

    Map<String, List<PartitionInfo>> getTopicMetadata(MetadataRequest.Builder request, Timer timer);

    Map<String, List<PartitionInfo>> getAllTopicMetadata(Timer timer);

    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Timer timer);

    Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Timer timer);

    Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Timer timer);

    boolean hasCompletedFetches();

    boolean hasAvailableFetches();

    int sendFetches();

    void resetOffsetsIfNeeded();

    void resetOffsetIfNeeded(TopicPartition partition, OffsetResetStrategy requestedResetStrategy, ListOffsetData offsetData);

    Node selectReadReplica(TopicPartition partition, Node leaderReplica, long currentTimeMs);

    void clearBufferedDataForUnassignedPartitions(Collection<TopicPartition> assignedPartitions);

    void validateOffsetsIfNeeded();


    /**
     * Represents data about an offset returned by a broker.
     */
    class ListOffsetData {
        final long offset;
        final Long timestamp; //  null if the broker does not support returning timestamps
        final Optional<Integer> leaderEpoch; // empty if the leader epoch is not known

        ListOffsetData(long offset, Long timestamp, Optional<Integer> leaderEpoch) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.leaderEpoch = leaderEpoch;
        }
    }

    static boolean hasUsableOffsetForLeaderEpochVersion(NodeApiVersions nodeApiVersions) {
        ApiVersionsResponseData.ApiVersion apiVersion = nodeApiVersions.apiVersion(ApiKeys.OFFSET_FOR_LEADER_EPOCH);
        if (apiVersion == null)
            return false;

        return OffsetsForLeaderEpochRequest.supportsTopicPermission(apiVersion.maxVersion());
    }

}