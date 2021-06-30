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

import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class OffsetDeleteRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<OffsetDeleteRequest> {

        private final OffsetDeleteRequestData data;

        public Builder(OffsetDeleteRequestData data) {
            super(ApiKeys.OFFSET_DELETE);
            this.data = data;
        }

        @Override
        public OffsetDeleteRequest build(short version) {
            return new OffsetDeleteRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final OffsetDeleteRequestData data;

    public OffsetDeleteRequest(OffsetDeleteRequestData data, short version) {
        super(ApiKeys.OFFSET_DELETE, version);
        this.data = data;
    }

    public AbstractResponse getErrorResponse(int throttleTimeMs, Errors error) {
        return new OffsetDeleteResponse(
            new OffsetDeleteResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code())
        );
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return getErrorResponse(throttleTimeMs, Errors.forException(e));
    }

    public static OffsetDeleteRequest parse(ByteBuffer buffer, short version) {
        return new OffsetDeleteRequest(new OffsetDeleteRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public OffsetDeleteRequestData data() {
        return data;
    }
}
