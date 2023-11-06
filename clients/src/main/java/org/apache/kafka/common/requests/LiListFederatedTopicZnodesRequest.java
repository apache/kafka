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
import java.util.Collections;
import org.apache.kafka.common.message.LiListFederatedTopicZnodesRequestData;
import org.apache.kafka.common.message.LiListFederatedTopicZnodesResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;


public class LiListFederatedTopicZnodesRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<LiListFederatedTopicZnodesRequest> {

        private final LiListFederatedTopicZnodesRequestData data;

        public Builder(LiListFederatedTopicZnodesRequestData data, short allowedVersion) {
            super(ApiKeys.LI_LIST_FEDERATED_TOPIC_ZNODES, allowedVersion);
            this.data = data;
        }

        @Override
        public LiListFederatedTopicZnodesRequest build(short version) {
            return new LiListFederatedTopicZnodesRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final LiListFederatedTopicZnodesRequestData data;

    public LiListFederatedTopicZnodesRequest(LiListFederatedTopicZnodesRequestData data, short version) {
        super(ApiKeys.LI_LIST_FEDERATED_TOPIC_ZNODES, version);
        this.data = data;
    }

    @Override
    public LiListFederatedTopicZnodesResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        LiListFederatedTopicZnodesResponseData data = new LiListFederatedTopicZnodesResponseData().
            setTopics(Collections.emptyList()).
            setThrottleTimeMs(throttleTimeMs).
            setErrorCode(Errors.forException(e).code());
        return new LiListFederatedTopicZnodesResponse(data, version());
    }

    public static LiListFederatedTopicZnodesRequest parse(ByteBuffer buffer, short version) {
        return new LiListFederatedTopicZnodesRequest(
            new LiListFederatedTopicZnodesRequestData(new ByteBufferAccessor(buffer), version), version
        );
    }

    @Override
    public LiListFederatedTopicZnodesRequestData data() {
        return data;
    }
}
