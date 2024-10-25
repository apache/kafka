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
package org.apache.kafka.raft.utils;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.raft.Endpoints;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.ReplicaKey;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Optional;
import java.util.function.UnaryOperator;

public class FetchSnapshotRpc {
    public static FetchSnapshotRequestData singletonFetchSnapshotRequest(
            String clusterId,
            ReplicaKey replicaKey,
            TopicPartition topicPartition,
            int epoch,
            OffsetAndEpoch offsetAndEpoch,
            int maxBytes,
            long position
    ) {
        FetchSnapshotRequestData.SnapshotId snapshotId = new FetchSnapshotRequestData.SnapshotId()
                .setEndOffset(offsetAndEpoch.offset())
                .setEpoch(offsetAndEpoch.epoch());

        FetchSnapshotRequestData.PartitionSnapshot partitionSnapshot = new FetchSnapshotRequestData.PartitionSnapshot()
                .setPartition(topicPartition.partition())
                .setCurrentLeaderEpoch(epoch)
                .setSnapshotId(snapshotId)
                .setPosition(position)
                .setReplicaDirectoryId(replicaKey.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID));

        return new FetchSnapshotRequestData()
                .setClusterId(clusterId)
                .setReplicaId(replicaKey.id())
                .setMaxBytes(maxBytes)
                .setTopics(
                        Collections.singletonList(
                                new FetchSnapshotRequestData.TopicSnapshot()
                                        .setName(topicPartition.topic())
                                        .setPartitions(Collections.singletonList(partitionSnapshot))
                        )
                );
    }

    /**
     * Creates a FetchSnapshotResponseData with a single PartitionSnapshot for the topic partition.
     *
     * The partition index will already be populated when calling operator.
     *
     * @param listenerName the listener used to accept the request
     * @param apiVersion the api version of the request
     * @param topicPartition the topic partition to include
     * @param leaderId the id of the leader
     * @param endpoints the endpoints of the leader
     * @param operator unary operator responsible for populating all of the appropriate fields
     * @return the created fetch snapshot response data
     */
    public static FetchSnapshotResponseData singletonFetchSnapshotResponse(
            ListenerName listenerName,
            short apiVersion,
            TopicPartition topicPartition,
            int leaderId,
            Endpoints endpoints,
            UnaryOperator<FetchSnapshotResponseData.PartitionSnapshot> operator
    ) {
        FetchSnapshotResponseData.PartitionSnapshot partitionSnapshot = operator.apply(
                new FetchSnapshotResponseData.PartitionSnapshot().setIndex(topicPartition.partition())
        );

        FetchSnapshotResponseData response = new FetchSnapshotResponseData()
                .setTopics(
                        Collections.singletonList(
                                new FetchSnapshotResponseData.TopicSnapshot()
                                        .setName(topicPartition.topic())
                                        .setPartitions(Collections.singletonList(partitionSnapshot))
                        )
                );

        if (apiVersion >= 1) {
            Optional<InetSocketAddress> address = endpoints.address(listenerName);
            if (address.isPresent() && leaderId >= 0) {
                // Populate the node endpoints
                FetchSnapshotResponseData.NodeEndpointCollection nodeEndpoints =
                        new FetchSnapshotResponseData.NodeEndpointCollection(1);
                nodeEndpoints.add(
                        new FetchSnapshotResponseData.NodeEndpoint()
                                .setNodeId(leaderId)
                                .setHost(address.get().getHostString())
                                .setPort(address.get().getPort())
                );
                response.setNodeEndpoints(nodeEndpoints);
            }
        }

        return response;
    }
}
