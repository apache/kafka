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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.raft.Endpoints;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;

public class FetchRpc {
    public static FetchRequestData singletonFetchRequest(
            TopicPartition topicPartition,
            Uuid topicId,
            Consumer<FetchRequestData.FetchPartition> partitionConsumer
    ) {
        FetchRequestData.FetchPartition fetchPartition =
                new FetchRequestData.FetchPartition()
                        .setPartition(topicPartition.partition());
        partitionConsumer.accept(fetchPartition);

        FetchRequestData.FetchTopic fetchTopic =
                new FetchRequestData.FetchTopic()
                        .setTopic(topicPartition.topic())
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(fetchPartition));

        return new FetchRequestData()
                .setTopics(Collections.singletonList(fetchTopic));
    }

    public static FetchResponseData singletonFetchResponse(
            ListenerName listenerName,
            short apiVersion,
            TopicPartition topicPartition,
            Uuid topicId,
            Errors topLevelError,
            int leaderId,
            Endpoints endpoints,
            Consumer<FetchResponseData.PartitionData> partitionConsumer
    ) {
        FetchResponseData.PartitionData fetchablePartition =
                new FetchResponseData.PartitionData();

        fetchablePartition.setPartitionIndex(topicPartition.partition());

        partitionConsumer.accept(fetchablePartition);

        FetchResponseData.FetchableTopicResponse fetchableTopic =
                new FetchResponseData.FetchableTopicResponse()
                        .setTopic(topicPartition.topic())
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(fetchablePartition));

        FetchResponseData response = new FetchResponseData();

        if (apiVersion >= 17) {
            Optional<InetSocketAddress> address = endpoints.address(listenerName);
            if (address.isPresent() && leaderId >= 0) {
                // Populate the node endpoints
                FetchResponseData.NodeEndpointCollection nodeEndpoints = new FetchResponseData.NodeEndpointCollection(1);
                nodeEndpoints.add(
                        new FetchResponseData.NodeEndpoint()
                                .setNodeId(leaderId)
                                .setHost(address.get().getHostString())
                                .setPort(address.get().getPort())
                );
                response.setNodeEndpoints(nodeEndpoints);
            }
        }

        return response
                .setErrorCode(topLevelError.code())
                .setResponses(Collections.singletonList(fetchableTopic));
    }

    public static boolean hasValidTopicPartition(FetchRequestData data, TopicPartition topicPartition, Uuid topicId) {
        return data.topics().size() == 1 &&
                data.topics().get(0).topicId().equals(topicId) &&
                data.topics().get(0).partitions().size() == 1 &&
                data.topics().get(0).partitions().get(0).partition() == topicPartition.partition();
    }

    public static boolean hasValidTopicPartition(FetchResponseData data, TopicPartition topicPartition, Uuid topicId) {
        return data.responses().size() == 1 &&
                data.responses().get(0).topicId().equals(topicId) &&
                data.responses().get(0).partitions().size() == 1 &&
                data.responses().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }
}
