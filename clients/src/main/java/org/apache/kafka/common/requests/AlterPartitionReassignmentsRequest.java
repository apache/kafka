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

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignableTopicResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Readable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class AlterPartitionReassignmentsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<AlterPartitionReassignmentsRequest> {
        private final AlterPartitionReassignmentsRequestData data;

        public Builder(AlterPartitionReassignmentsRequestData data) {
            super(ApiKeys.ALTER_PARTITION_REASSIGNMENTS);
            this.data = data;
        }

        @Override
        public AlterPartitionReassignmentsRequest build(short version) {
            if (!data.allowReplicationFactorChange() && version < 1) {
                throw new UnsupportedVersionException("The broker does not support the AllowReplicationFactorChange " +
                        "option for the AlterPartitionReassignments API. Consider re-sending the request without the " +
                        "option or updating the server version");
            }
            return new AlterPartitionReassignmentsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final AlterPartitionReassignmentsRequestData data;

    private AlterPartitionReassignmentsRequest(AlterPartitionReassignmentsRequestData data, short version) {
        super(ApiKeys.ALTER_PARTITION_REASSIGNMENTS, version);
        this.data = data;
    }

    public static AlterPartitionReassignmentsRequest parse(Readable readable, short version) {
        return new AlterPartitionReassignmentsRequest(new AlterPartitionReassignmentsRequestData(
            readable, version), version);
    }

    public AlterPartitionReassignmentsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        List<ReassignableTopicResponse> topicResponses = new ArrayList<>();

        for (ReassignableTopic topic : data.topics()) {
            List<ReassignablePartitionResponse> partitionResponses = topic.partitions().stream().map(partition ->
                    new ReassignablePartitionResponse()
                            .setPartitionIndex(partition.partitionIndex())
                            .setErrorCode(apiError.error().code())
                            .setErrorMessage(apiError.message())
            ).collect(Collectors.toList());
            topicResponses.add(
                    new ReassignableTopicResponse()
                            .setName(topic.name())
                            .setPartitions(partitionResponses)
            );
        }

        AlterPartitionReassignmentsResponseData responseData = new AlterPartitionReassignmentsResponseData()
                .setResponses(topicResponses)
                .setErrorCode(apiError.error().code())
                .setErrorMessage(apiError.message())
                .setThrottleTimeMs(throttleTimeMs);
        return new AlterPartitionReassignmentsResponse(responseData);
    }
}
