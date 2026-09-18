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

import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Readable;

/**
 * Sent by a Streams client to push its topology description to the broker, in response
 * to {@code TopologyDescriptionRequired=true} on a {@code StreamsGroupHeartbeatResponse}.
 * The broker validates that {@code MemberId} still belongs to the group, checks the
 * {@code TopologyEpoch} against the group's current epoch, and persists the description.
 * See KIP-1331.
 *
 * <p>Legal error codes are documented on {@link StreamsGroupTopologyDescriptionUpdateResponse}.
 */
public class StreamsGroupTopologyDescriptionUpdateRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<StreamsGroupTopologyDescriptionUpdateRequest> {
        private final StreamsGroupTopologyDescriptionUpdateRequestData data;

        public Builder(StreamsGroupTopologyDescriptionUpdateRequestData data) {
            super(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE);
            this.data = data;
        }

        @Override
        public StreamsGroupTopologyDescriptionUpdateRequest build(short version) {
            return new StreamsGroupTopologyDescriptionUpdateRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final StreamsGroupTopologyDescriptionUpdateRequestData data;

    public StreamsGroupTopologyDescriptionUpdateRequest(StreamsGroupTopologyDescriptionUpdateRequestData data, short version) {
        super(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        return new StreamsGroupTopologyDescriptionUpdateResponse(
            new StreamsGroupTopologyDescriptionUpdateResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(apiError.error().code())
                .setErrorMessage(apiError.message())
        );
    }

    @Override
    public StreamsGroupTopologyDescriptionUpdateRequestData data() {
        return data;
    }

    public static StreamsGroupTopologyDescriptionUpdateRequest parse(Readable readable, short version) {
        return new StreamsGroupTopologyDescriptionUpdateRequest(
            new StreamsGroupTopologyDescriptionUpdateRequestData(readable, version), version);
    }
}
