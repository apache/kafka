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

import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignablePartition;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class AlterPartitionReassignmentsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<AlterPartitionReassignmentsRequest> {
        private final AlterPartitionReassignmentsRequestData data;

        public Builder(AlterPartitionReassignmentsRequestData data) {
            super(ApiKeys.ALTER_PARTITION_REASSIGNMENTS);
            this.data = data;
        }

        @Override
        public AlterPartitionReassignmentsRequest build(short version) {
            return new AlterPartitionReassignmentsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final AlterPartitionReassignmentsRequestData data;
    private final short version;

    private AlterPartitionReassignmentsRequest(AlterPartitionReassignmentsRequestData data, short version) {
        super(ApiKeys.ALTER_PARTITION_REASSIGNMENTS, version);
        this.data = data;
        this.version = version;
    }

    public AlterPartitionReassignmentsRequest(Struct struct, short version) {
        super(ApiKeys.ALTER_PARTITION_REASSIGNMENTS, version);
        this.data = new AlterPartitionReassignmentsRequestData(struct, version);
        this.version = version;
    }

    public AlterPartitionReassignmentsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        AlterPartitionReassignmentsResponseData response = new AlterPartitionReassignmentsResponseData();
        response.setThrottleTimeMs(throttleTimeMs);
        ApiError apiError = ApiError.fromThrowable(e);
        for (ReassignableTopic topicRequest : data.topics()) {
            for (ReassignablePartition partitionRequest : topicRequest.partitions()) {
                response.responses().add(new ReassignablePartitionResponse().
                        setErrorCode(apiError.error().code()).
                        setErrorString(apiError.error().message()));
            }
        }
        return new AlterPartitionReassignmentsResponse(response);
    }

    public static AlterPartitionReassignmentsRequest parse(ByteBuffer buffer, short version) {
        return new AlterPartitionReassignmentsRequest(ApiKeys.ALTER_PARTITION_REASSIGNMENTS.
                parseRequest(version, buffer), version);
    }

    /**
     * Visible for testing.
     */
    @Override
    public Struct toStruct() {
        return data.toStruct(version);
    }
}
