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
import org.apache.kafka.common.message.LiControlledShutdownSkipSafetyCheckRequestData;
import org.apache.kafka.common.message.LiControlledShutdownSkipSafetyCheckResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

public class LiControlledShutdownSkipSafetyCheckRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<LiControlledShutdownSkipSafetyCheckRequest> {
        private final LiControlledShutdownSkipSafetyCheckRequestData data;

        public Builder(LiControlledShutdownSkipSafetyCheckRequestData data, short allowedVersion) {
            super(ApiKeys.LI_CONTROLLED_SHUTDOWN_SKIP_SAFETY_CHECK, allowedVersion);
            this.data = data;
        }

        @Override
        public LiControlledShutdownSkipSafetyCheckRequest build(short version) {
            return new LiControlledShutdownSkipSafetyCheckRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final LiControlledShutdownSkipSafetyCheckRequestData data;

    public LiControlledShutdownSkipSafetyCheckRequest(LiControlledShutdownSkipSafetyCheckRequestData data, short version) {
        super(ApiKeys.LI_CONTROLLED_SHUTDOWN_SKIP_SAFETY_CHECK, version);
        this.data = data;
    }

    public static LiControlledShutdownSkipSafetyCheckRequest parse(ByteBuffer buffer, short version) {
        return new LiControlledShutdownSkipSafetyCheckRequest(new LiControlledShutdownSkipSafetyCheckRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        LiControlledShutdownSkipSafetyCheckResponseData data = new LiControlledShutdownSkipSafetyCheckResponseData()
            .setErrorCode(Errors.forException(e).code());
        return new LiControlledShutdownSkipSafetyCheckResponse(data);
    }

    @Override
    public LiControlledShutdownSkipSafetyCheckRequestData data() {
        return data;
    }
}
