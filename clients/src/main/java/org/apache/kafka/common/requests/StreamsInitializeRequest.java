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

import org.apache.kafka.common.message.StreamsInitializeRequestData;
import org.apache.kafka.common.message.StreamsInitializeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class StreamsInitializeRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<StreamsInitializeRequest> {
        private final StreamsInitializeRequestData data;

        public Builder(StreamsInitializeRequestData data) {
            this(data, false);
        }

        public Builder(StreamsInitializeRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.STREAMS_INITIALIZE, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public StreamsInitializeRequest build(short version) {
            return new StreamsInitializeRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final StreamsInitializeRequestData data;

    public StreamsInitializeRequest(StreamsInitializeRequestData data, short version) {
        super(ApiKeys.STREAMS_HEARTBEAT, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new StreamsInitializeResponse(
            new StreamsInitializeResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(Errors.forException(e).code())
        );
    }

    @Override
    public StreamsInitializeRequestData data() {
        return data;
    }

    public static StreamsInitializeRequest parse(ByteBuffer buffer, short version) {
        return new StreamsInitializeRequest(new StreamsInitializeRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
