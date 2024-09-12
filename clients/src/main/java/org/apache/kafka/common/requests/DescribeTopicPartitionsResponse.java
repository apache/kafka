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
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DescribeTopicPartitionsResponse extends AbstractResponse {
    private final DescribeTopicPartitionsResponseData data;

    public DescribeTopicPartitionsResponse(DescribeTopicPartitionsResponseData data) {
        super(ApiKeys.DESCRIBE_TOPIC_PARTITIONS);
        this.data = data;
    }

    @Override
    public DescribeTopicPartitionsResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return true;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        data.topics().forEach(topicResponse -> {
            topicResponse.partitions().forEach(p -> updateErrorCounts(errorCounts, Errors.forCode(p.errorCode())));
            updateErrorCounts(errorCounts, Errors.forCode(topicResponse.errorCode()));
        });
        return errorCounts;
    }

    public static DescribeTopicPartitionsResponse prepareResponse(
        int throttleTimeMs,
        List<DescribeTopicPartitionsResponseTopic> topics
    ) {
        DescribeTopicPartitionsResponseData responseData = new DescribeTopicPartitionsResponseData();
        responseData.setThrottleTimeMs(throttleTimeMs);
        topics.forEach(topicResponse -> responseData.topics().add(topicResponse));
        return new DescribeTopicPartitionsResponse(responseData);
    }

    public static DescribeTopicPartitionsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeTopicPartitionsResponse(
            new DescribeTopicPartitionsResponseData(new ByteBufferAccessor(buffer), version));
    }

    public static TopicPartitionInfo partitionToTopicPartitionInfo(
        DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition partition,
        Map<Integer, Node> nodes) {
        return new TopicPartitionInfo(
            partition.partitionIndex(),
            nodes.get(partition.leaderId()),
            partition.replicaNodes().stream().map(id -> nodes.getOrDefault(id, new Node(id, "", -1))).collect(Collectors.toList()),
            partition.isrNodes().stream().map(id -> nodes.getOrDefault(id, new Node(id, "", -1))).collect(Collectors.toList()),
            partition.eligibleLeaderReplicas().stream().map(id -> nodes.getOrDefault(id, new Node(id, "", -1))).collect(Collectors.toList()),
            partition.lastKnownElr().stream().map(id -> nodes.getOrDefault(id, new Node(id, "", -1))).collect(Collectors.toList()));
    }
}
