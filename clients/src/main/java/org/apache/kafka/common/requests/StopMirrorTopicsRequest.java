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

import org.apache.kafka.common.message.StopMirrorTopicsRequestData;
import org.apache.kafka.common.message.StopMirrorTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Readable;

public class StopMirrorTopicsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<StopMirrorTopicsRequest> {
        private final StopMirrorTopicsRequestData data;

        public Builder(StopMirrorTopicsRequestData data) {
            this(data, false);
        }

        public Builder(StopMirrorTopicsRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.STOP_MIRROR_TOPICS, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public StopMirrorTopicsRequest build(short version) {
            return new StopMirrorTopicsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final StopMirrorTopicsRequestData data;

    public StopMirrorTopicsRequest(StopMirrorTopicsRequestData data, short version) {
        super(ApiKeys.STOP_MIRROR_TOPICS, version);
        this.data = data;
    }

    @Override
    public StopMirrorTopicsRequestData data() {
        return data;
    }

    @Override
    public StopMirrorTopicsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError error = ApiError.fromThrowable(e);
        return new StopMirrorTopicsResponse(new StopMirrorTopicsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.error().code())
            .setErrorMessage(error.message()));
    }

    public static StopMirrorTopicsRequest parse(Readable readable, short version) {
        return new StopMirrorTopicsRequest(new StopMirrorTopicsRequestData(readable, version), version);
    }
}
