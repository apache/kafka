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

import org.apache.kafka.common.message.UnregisterControllerRequestData;
import org.apache.kafka.common.message.UnregisterControllerResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class UnregisterControllerRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<UnregisterControllerRequest> {
        private final UnregisterControllerRequestData data;

        public Builder(UnregisterControllerRequestData data) {
            super(ApiKeys.UNREGISTER_CONTROLLER);
            this.data = data;
        }

        @Override
        public UnregisterControllerRequest build(short version) {
            return new UnregisterControllerRequest(data, version);
        }
    }

    private final UnregisterControllerRequestData data;

    public UnregisterControllerRequest(UnregisterControllerRequestData data, short version) {
        super(ApiKeys.UNREGISTER_CONTROLLER, version);
        this.data = data;
    }

    @Override
    public UnregisterControllerRequestData data() {
        return data;
    }

    @Override
    public UnregisterControllerResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new UnregisterControllerResponse(new UnregisterControllerResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code())
                .setErrorMessage(e.getMessage()));
    }

    public static UnregisterControllerRequest parse(Readable readable, short version) {
        return new UnregisterControllerRequest(new UnregisterControllerRequestData(readable, version),
                version);
    }
}
