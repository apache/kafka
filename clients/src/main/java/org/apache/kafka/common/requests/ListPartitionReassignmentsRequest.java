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

import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingPartitionReassignment;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingTopicReassignment;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.kafka.common.message.ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics;

public class ListPartitionReassignmentsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ListPartitionReassignmentsRequest> {
        private final ListPartitionReassignmentsRequestData data;

        public Builder(ListPartitionReassignmentsRequestData data) {
            super(ApiKeys.LIST_PARTITION_REASSIGNMENTS);
            this.data = data;
        }

        @Override
        public ListPartitionReassignmentsRequest build(short version) {
            return new ListPartitionReassignmentsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ListPartitionReassignmentsRequestData data;

    private ListPartitionReassignmentsRequest(ListPartitionReassignmentsRequestData data, short version) {
        super(ApiKeys.LIST_PARTITION_REASSIGNMENTS, version);
        this.data = data;
    }

    public static ListPartitionReassignmentsRequest parse(ByteBuffer buffer, short version) {
        return new ListPartitionReassignmentsRequest(new ListPartitionReassignmentsRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public ListPartitionReassignmentsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);

        List<OngoingTopicReassignment> ongoingTopicReassignments = new ArrayList<>();
        if (data.topics() != null) {
            for (ListPartitionReassignmentsTopics topic : data.topics()) {
                ongoingTopicReassignments.add(
                        new OngoingTopicReassignment()
                                .setName(topic.name())
                                .setPartitions(topic.partitionIndexes().stream().map(partitionIndex ->
                                        new OngoingPartitionReassignment().setPartitionIndex(partitionIndex)).collect(Collectors.toList()))
                );
            }
        }
        ListPartitionReassignmentsResponseData responseData = new ListPartitionReassignmentsResponseData()
                .setTopics(ongoingTopicReassignments)
                .setErrorCode(apiError.error().code())
                .setErrorMessage(apiError.message())
                .setThrottleTimeMs(throttleTimeMs);
        return new ListPartitionReassignmentsResponse(responseData);
    }
}
