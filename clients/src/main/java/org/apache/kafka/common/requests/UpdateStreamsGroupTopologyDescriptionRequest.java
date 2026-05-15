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

import org.apache.kafka.common.message.UpdateStreamsGroupTopologyDescriptionRequestData;
import org.apache.kafka.common.message.UpdateStreamsGroupTopologyDescriptionResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class UpdateStreamsGroupTopologyDescriptionRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<UpdateStreamsGroupTopologyDescriptionRequest> {
        private final UpdateStreamsGroupTopologyDescriptionRequestData data;

        public Builder(UpdateStreamsGroupTopologyDescriptionRequestData data) {
            super(ApiKeys.UPDATE_STREAMS_GROUP_TOPOLOGY_DESCRIPTION);
            this.data = data;
        }

        @Override
        public UpdateStreamsGroupTopologyDescriptionRequest build(short version) {
            return new UpdateStreamsGroupTopologyDescriptionRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final UpdateStreamsGroupTopologyDescriptionRequestData data;

    public UpdateStreamsGroupTopologyDescriptionRequest(UpdateStreamsGroupTopologyDescriptionRequestData data, short version) {
        super(ApiKeys.UPDATE_STREAMS_GROUP_TOPOLOGY_DESCRIPTION, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new UpdateStreamsGroupTopologyDescriptionResponse(
            new UpdateStreamsGroupTopologyDescriptionResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(Errors.forException(e).code())
        );
    }

    @Override
    public UpdateStreamsGroupTopologyDescriptionRequestData data() {
        return data;
    }

    public static UpdateStreamsGroupTopologyDescriptionRequest parse(Readable readable, short version) {
        return new UpdateStreamsGroupTopologyDescriptionRequest(
            new UpdateStreamsGroupTopologyDescriptionRequestData(readable, version), version);
    }
}
