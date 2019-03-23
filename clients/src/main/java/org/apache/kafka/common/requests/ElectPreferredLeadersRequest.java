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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ElectPreferredLeadersRequestData;
import org.apache.kafka.common.message.ElectPreferredLeadersRequestData.TopicPartitions;
import org.apache.kafka.common.message.ElectPreferredLeadersResponseData;
import org.apache.kafka.common.message.ElectPreferredLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElectPreferredLeadersRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ElectPreferredLeadersRequest> {
        private final ElectPreferredLeadersRequestData data;

        public Builder(ElectPreferredLeadersRequestData data) {
            super(ApiKeys.ELECT_PREFERRED_LEADERS);
            this.data = data;
        }

        @Override
        public ElectPreferredLeadersRequest build(short version) {
            return new ElectPreferredLeadersRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public static ElectPreferredLeadersRequestData toRequestData(Collection<TopicPartition> partitions, int timeoutMs) {
        ElectPreferredLeadersRequestData d = new ElectPreferredLeadersRequestData()
                .setTimeoutMs(timeoutMs);
        if (partitions != null) {
            for (Map.Entry<String, List<Integer>> tp : CollectionUtils.groupPartitionsByTopic(partitions).entrySet()) {
                d.topicPartitions().add(new ElectPreferredLeadersRequestData.TopicPartitions().setTopic(tp.getKey()).setPartitionId(tp.getValue()));
            }
        } else {
            d.setTopicPartitions(null);
        }
        return d;
    }

    public static Map<TopicPartition, ApiError> fromResponseData(ElectPreferredLeadersResponseData data) {
        Map<TopicPartition, ApiError> map = new HashMap<>();
        for (ElectPreferredLeadersResponseData.ReplicaElectionResult topicResults : data.replicaElectionResults()) {
            for (ElectPreferredLeadersResponseData.PartitionResult partitionResult : topicResults.partitionResult()) {
                map.put(new TopicPartition(topicResults.topic(), partitionResult.partitionId()),
                        new ApiError(Errors.forCode(partitionResult.errorCode()),
                                partitionResult.errorMessage()));
            }
        }
        return map;
    }

    private final ElectPreferredLeadersRequestData data;
    private final short version;

    private ElectPreferredLeadersRequest(ElectPreferredLeadersRequestData data, short version) {
        super(ApiKeys.ELECT_PREFERRED_LEADERS, version);
        this.data = data;
        this.version = version;
    }

    public ElectPreferredLeadersRequest(Struct struct, short version) {
        super(ApiKeys.ELECT_PREFERRED_LEADERS, version);
        this.data = new ElectPreferredLeadersRequestData(struct, version);
        this.version = version;
    }

    public ElectPreferredLeadersRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ElectPreferredLeadersResponseData response = new ElectPreferredLeadersResponseData();
        response.setThrottleTimeMs(throttleTimeMs);
        ApiError apiError = ApiError.fromThrowable(e);
        for (TopicPartitions topic : data.topicPartitions()) {
            ReplicaElectionResult electionResult = new ReplicaElectionResult().setTopic(topic.topic());
            for (Integer partitionId : topic.partitionId()) {
                electionResult.partitionResult().add(new ElectPreferredLeadersResponseData.PartitionResult()
                        .setPartitionId(partitionId)
                        .setErrorCode(apiError.error().code())
                        .setErrorMessage(apiError.message()));
            }
            response.replicaElectionResults().add(electionResult);
        }
        return new ElectPreferredLeadersResponse(response);
    }

    public static ElectPreferredLeadersRequest parse(ByteBuffer buffer, short version) {
        return new ElectPreferredLeadersRequest(ApiKeys.ELECT_PREFERRED_LEADERS.parseRequest(version, buffer), version);
    }

    /**
     * Visible for testing.
     */
    @Override
    public Struct toStruct() {
        return data.toStruct(version);
    }
}