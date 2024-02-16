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

import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class ShareAcknowledgeRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ShareAcknowledgeRequest> {

        private final ShareAcknowledgeRequestData data;

        public Builder(ShareAcknowledgeRequestData data) {
            this(data, false);
        }

        public Builder(ShareAcknowledgeRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.SHARE_ACKNOWLEDGE, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public ShareAcknowledgeRequest build(short version) {
            return new ShareAcknowledgeRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ShareAcknowledgeRequestData data;

    public ShareAcknowledgeRequest(ShareAcknowledgeRequestData data, short version) {
        super(ApiKeys.SHARE_ACKNOWLEDGE, version);
        this.data = data;
    }

    @Override
    public ShareAcknowledgeRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new ShareAcknowledgeResponse(new ShareAcknowledgeResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.code()));
    }

    public static ShareAcknowledgeRequest parse(ByteBuffer buffer, short version) {
        return new ShareAcknowledgeRequest(
                new ShareAcknowledgeRequestData(new ByteBufferAccessor(buffer), version),
                version
        );
    }
}