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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ElectLeadersRequestData.TopicPartitions;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

public class ElectLeadersRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ElectLeadersRequest> {
        private final ElectionType electionType;
        private final Collection<TopicPartition> topicPartitions;
        private final int timeoutMs;

        public Builder(ElectionType electionType, Collection<TopicPartition> topicPartitions, int timeoutMs) {
            super(ApiKeys.ELECT_LEADERS);
            this.electionType = electionType;
            this.topicPartitions = topicPartitions;
            this.timeoutMs = timeoutMs;
        }

        @Override
        public ElectLeadersRequest build(short version) {
            return new ElectLeadersRequest(toRequestData(version), version);
        }

        @Override
        public String toString() {
            return "ElectLeadersRequest("
                + "electionType=" + electionType
                + ", topicPartitions=" + ((topicPartitions == null) ? "null" : MessageUtil.deepToString(topicPartitions.iterator()))
                + ", timeoutMs=" + timeoutMs
                + ")";
        }

        private ElectLeadersRequestData toRequestData(short version) {
            if (electionType != ElectionType.PREFERRED && version == 0) {
                throw new UnsupportedVersionException("API Version 0 only supports PREFERRED election type");
            }

            ElectLeadersRequestData data = new ElectLeadersRequestData()
                .setTimeoutMs(timeoutMs);

            if (topicPartitions != null) {
                for (Map.Entry<String, List<Integer>> tp : CollectionUtils.groupPartitionsByTopic(topicPartitions).entrySet()) {
                    data.topicPartitions().add(new ElectLeadersRequestData.TopicPartitions().setTopic(tp.getKey()).setPartitionId(tp.getValue()));
                }
            } else {
                data.setTopicPartitions(null);
            }

            data.setElectionType(electionType.value);

            return data;
        }
    }

    private final ElectLeadersRequestData data;
    private final short version;

    private ElectLeadersRequest(ElectLeadersRequestData data, short version) {
        super(ApiKeys.ELECT_LEADERS, version);
        this.data = data;
        this.version = version;
    }

    public ElectLeadersRequest(Struct struct, short version) {
        super(ApiKeys.ELECT_LEADERS, version);
        this.data = new ElectLeadersRequestData(struct, version);
        this.version = version;
    }

    public ElectLeadersRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        Map<String, Map<Integer, ApiError>> electionResult = new HashMap<>();
        if (version == 0) {
            for (TopicPartitions topic : data.topicPartitions()) {
                Map<Integer, ApiError> partitionResult = new HashMap<>();
                for (Integer partitionId : topic.partitionId()) {
                    partitionResult.put(partitionId, apiError);
                }
                electionResult.put(topic.topic(), partitionResult);
            }
        }

        return new ElectLeadersResponse(throttleTimeMs, apiError.error().code(), electionResult, version);
    }

    public static ElectLeadersRequest parse(ByteBuffer buffer, short version) {
        return new ElectLeadersRequest(ApiKeys.ELECT_LEADERS.parseRequest(version, buffer), version);
    }

    /**
     * Visible for testing.
     */
    @Override
    public Struct toStruct() {
        return data.toStruct(version);
    }
}
