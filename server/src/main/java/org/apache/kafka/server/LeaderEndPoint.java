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

package org.apache.kafka.server;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.network.BrokerEndPoint;

import java.util.Map;
import java.util.Optional;

/**
 * Interface that defines APIs for accessing a broker that is a leader.
 */
public interface LeaderEndPoint {

    /**
     * A boolean specifying if truncation when fetching from the leader is supported
     */
    boolean isTruncationOnFetchSupported();

    /**
     * Initiate closing access to fetches from leader.
     */
    void initiateClose();

    /**
     * Closes access to fetches from leader.
     * `initiateClose` must be called prior to invoking `close`.
     */
    void close();

    /**
     * The specific broker (host:port) we want to connect to.
     */
    BrokerEndPoint brokerEndPoint();

    /**
     * Given a fetchRequest, carries out the expected request and returns
     * the results from fetching from the leader.
     *
     * @param fetchRequest The fetch request we want to carry out
     *
     * @return A map of topic partition -> fetch data
     */
    Map<TopicPartition, FetchResponseData.PartitionData> fetch(FetchRequest.Builder fetchRequest);

    /**
     * Fetches the epoch and log start offset of the given topic partition from the leader.
     *
     * @param topicPartition The topic partition that we want to fetch from
     * @param currentLeaderEpoch An int representing the current leader epoch of the requester
     *
     * @return An OffsetAndEpoch object representing the earliest offset and epoch in the leader's topic partition.
     */
    OffsetAndEpoch fetchEarliestOffset(TopicPartition topicPartition, int currentLeaderEpoch);

    /**
     * Fetches the epoch and log end offset of the given topic partition from the leader.
     *
     * @param topicPartition The topic partition that we want to fetch from
     * @param currentLeaderEpoch An int representing the current leader epoch of the requester
     *
     * @return An OffsetAndEpoch object representing the latest offset and epoch in the leader's topic partition.
     */
    OffsetAndEpoch fetchLatestOffset(TopicPartition topicPartition, int currentLeaderEpoch);

    /**
     * Fetches offset for leader epoch from the leader for each given topic partition
     *
     * @param partitions A map of topic partition -> leader epoch of the replica
     *
     * @return A map of topic partition -> end offset for a requested leader epoch
     */
    Map<TopicPartition, EpochEndOffset> fetchEpochEndOffsets(Map<TopicPartition, OffsetForLeaderPartition> partitions);

    /**
     * Fetches the epoch and local log start offset from the leader for the given partition and the current leader-epoch
     *
     * @param topicPartition  The topic partition that we want to fetch from
     * @param currentLeaderEpoch An int representing the current leader epoch of the requester
     *
     * @return An OffsetAndEpoch object representing the earliest local offset and epoch in the leader's topic partition.
     */
    OffsetAndEpoch fetchEarliestLocalOffset(TopicPartition topicPartition, int currentLeaderEpoch);


    /**
     * Fetches the earliest offset and epoch that is pending upload for the given topic partition from the leader.
     *
     * @param topicPartition     The topic partition for which the earliest pending upload offset is to be fetched.
     * @param currentLeaderEpoch The current leader epoch of the requesting replica.
     * @return An OffsetAndEpoch object representing the earliest pending upload offset and its associated epoch
     * in the leader's topic partition.
     */
    OffsetAndEpoch fetchEarliestPendingUploadOffset(TopicPartition topicPartition, int currentLeaderEpoch);

    /**
     * Builds a fetch request, given a partition map.
     *
     * @param partitions A map of topic partitions to their respective partition fetch state
     *
     * @return A ResultWithPartitions, used to create the fetchRequest for fetch.
     */
    ResultWithPartitions<Optional<ReplicaFetch>> buildFetch(Map<TopicPartition, PartitionFetchState> partitions);
}
