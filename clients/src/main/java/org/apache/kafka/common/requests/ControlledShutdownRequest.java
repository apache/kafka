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

import org.apache.kafka.common.message.ControlledShutdownRequestData;
import org.apache.kafka.common.message.ControlledShutdownResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class ControlledShutdownRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ControlledShutdownRequest> {

        private final ControlledShutdownRequestData data;

        public Builder(ControlledShutdownRequestData data, short desiredVersion) {
            super(ApiKeys.CONTROLLED_SHUTDOWN, desiredVersion);
            this.data = data;
        }

        @Override
        public ControlledShutdownRequest build(short version) {
            return new ControlledShutdownRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ControlledShutdownRequestData data;

    private ControlledShutdownRequest(ControlledShutdownRequestData data, short version) {
        super(ApiKeys.CONTROLLED_SHUTDOWN, version);
        this.data = data;
    }

    @Override
    public ControlledShutdownResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ControlledShutdownResponseData data = new ControlledShutdownResponseData()
                .setErrorCode(Errors.forException(e).code());
        return new ControlledShutdownResponse(data);
    }

    public static ControlledShutdownRequest parse(ByteBuffer buffer, short version) {
        return new ControlledShutdownRequest(new ControlledShutdownRequestData(new ByteBufferAccessor(buffer), version),
            version);
    }

    @Override
    public ControlledShutdownRequestData data() {
        return data;
    }
}
