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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Possible error codes.
 * - {@link Errors#GROUP_AUTHORIZATION_FAILED}
 * - {@link Errors#TOPIC_AUTHORIZATION_FAILED}
 * - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
 * - {@link Errors#NOT_LEADER_OR_FOLLOWER}
 * - {@link Errors#UNKNOWN_TOPIC_ID}
 * - {@link Errors#INVALID_RECORD_STATE}
 * - {@link Errors#KAFKA_STORAGE_ERROR}
 * - {@link Errors#INVALID_REQUEST}
 * - {@link Errors#UNKNOWN_SERVER_ERROR}
 */
public class ShareAcknowledgeResponse extends AbstractResponse {

    private final ShareAcknowledgeResponseData data;

    public ShareAcknowledgeResponse(ShareAcknowledgeResponseData data) {
        super(ApiKeys.SHARE_ACKNOWLEDGE);
        this.data = data;
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public ShareAcknowledgeResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new EnumMap<>(Errors.class);
        updateErrorCounts(counts, Errors.forCode(data.errorCode()));
        data.responses().forEach(
                topic -> topic.partitions().forEach(
                        partition -> updateErrorCounts(counts, Errors.forCode(partition.errorCode()))
                )
        );
        return counts;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public static ShareAcknowledgeResponse parse(Readable readable, short version) {
        return new ShareAcknowledgeResponse(
                new ShareAcknowledgeResponseData(readable, version)
        );
    }

    private static boolean matchingTopic(ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse previousTopic, TopicIdPartition currentTopic) {
        if (previousTopic == null)
            return false;
        return previousTopic.topicId().equals(currentTopic.topicId());
    }

    public static ShareAcknowledgeResponseData.PartitionData partitionResponse(TopicIdPartition topicIdPartition, Errors error) {
        return partitionResponse(topicIdPartition.topicPartition().partition(), error);
    }

    public static ShareAcknowledgeResponseData.PartitionData partitionResponse(int partition, Errors error) {
        return new ShareAcknowledgeResponseData.PartitionData()
                .setPartitionIndex(partition)
                .setErrorCode(error.code());
    }

    public static ShareAcknowledgeResponse of(Errors error,
                                              int throttleTimeMs,
                                              LinkedHashMap<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> responseData,
                                              List<Node> nodeEndpoints, int acquisitionLockTimeout) {
        return new ShareAcknowledgeResponse(toMessage(error, throttleTimeMs, responseData.entrySet().iterator(), nodeEndpoints, acquisitionLockTimeout));
    }

    public static ShareAcknowledgeResponseData toMessage(Errors error, int throttleTimeMs,
                                                         Iterator<Map.Entry<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> partIterator,
                                                         List<Node> nodeEndpoints, int acquisitionLockTimeout) {
        ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponseCollection topicResponses = new ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponseCollection();
        while (partIterator.hasNext()) {
            Map.Entry<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> entry = partIterator.next();
            ShareAcknowledgeResponseData.PartitionData partitionData = entry.getValue();
            // Since PartitionData alone doesn't know the partition ID, we set it here
            partitionData.setPartitionIndex(entry.getKey().topicPartition().partition());
            // Checking if the topic is already present in the map
            ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse topicResponse = topicResponses.find(entry.getKey().topicId());
            if (topicResponse == null) {
                topicResponse = new ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse()
                        .setTopicId(entry.getKey().topicId())
                        .setPartitions(new ArrayList<>());
                topicResponses.add(topicResponse);
            }
            topicResponse.partitions().add(partitionData);
        }
        ShareAcknowledgeResponseData data = new ShareAcknowledgeResponseData();
        // KafkaApis should only pass in node endpoints on error, otherwise this should be an empty list
        nodeEndpoints.forEach(endpoint -> data.nodeEndpoints().add(
                new ShareAcknowledgeResponseData.NodeEndpoint()
                        .setNodeId(endpoint.id())
                        .setHost(endpoint.host())
                        .setPort(endpoint.port())
                        .setRack(endpoint.rack())));
        return data.setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code())
                .setAcquisitionLockTimeoutMs(acquisitionLockTimeout)
                .setResponses(topicResponses);
    }
}
