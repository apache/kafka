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

import org.apache.kafka.common.message.PushConfigRequestData;
import org.apache.kafka.common.message.PushConfigResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class PushConfigRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<PushConfigRequest> {
        private final PushConfigRequestData data;

        public Builder(PushConfigRequestData data) {
            this(data, false);
        }

        public Builder(PushConfigRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.PUSH_CONFIG, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public PushConfigRequest build(short version) {
            return new PushConfigRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final PushConfigRequestData data;

    public PushConfigRequest(PushConfigRequestData data, short version) {
        super(ApiKeys.PUSH_CONFIG, version);
        this.data = data;
    }

    @Override
    public PushConfigResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        PushConfigResponseData responseData = new PushConfigResponseData()
            .setErrorCode(Errors.forException(e).code())
            .setThrottleTimeMs(throttleTimeMs);
        return new PushConfigResponse(responseData);
    }

    @Override
    public PushConfigRequestData data() {
        return data;
    }

    public static PushConfigRequest parse(Readable readable, short version) {
        return new PushConfigRequest(new PushConfigRequestData(readable, version), version);
    }
}
