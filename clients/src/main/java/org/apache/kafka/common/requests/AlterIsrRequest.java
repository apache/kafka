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

import org.apache.kafka.common.message.AlterIsrRequestData;
import org.apache.kafka.common.message.AlterIsrResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class AlterIsrRequest extends AbstractRequest {

    private final AlterIsrRequestData data;

    public AlterIsrRequest(AlterIsrRequestData data, short apiVersion) {
        super(ApiKeys.ALTER_ISR, apiVersion);
        this.data = data;
    }

    @Override
    public AlterIsrRequestData data() {
        return data;
    }

    /**
     * Get an error response for a request with specified throttle time in the response if applicable
     */
    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new AlterIsrResponse(new AlterIsrResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(Errors.forException(e).code()));
    }

    public static AlterIsrRequest parse(ByteBuffer buffer, short version) {
        return new AlterIsrRequest(new AlterIsrRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public static class Builder extends AbstractRequest.Builder<AlterIsrRequest> {

        private final AlterIsrRequestData data;

        public Builder(AlterIsrRequestData data) {
            super(ApiKeys.ALTER_ISR);
            this.data = data;
        }

        @Override
        public AlterIsrRequest build(short version) {
            return new AlterIsrRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
