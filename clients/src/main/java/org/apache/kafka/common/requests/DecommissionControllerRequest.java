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

import org.apache.kafka.common.message.DecommissionControllerRequestData;
import org.apache.kafka.common.message.DecommissionControllerResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class DecommissionControllerRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<DecommissionControllerRequest> {
        private final DecommissionControllerRequestData data;

        public Builder(DecommissionControllerRequestData data) {
            super(ApiKeys.DECOMMISSION_CONTROLLER);
            this.data = data;
        }

        @Override
        public DecommissionControllerRequest build(short version) {
            return new DecommissionControllerRequest(data, version);
        }
    }

    private final DecommissionControllerRequestData data;

    public DecommissionControllerRequest(DecommissionControllerRequestData data, short version) {
        super(ApiKeys.DECOMMISSION_CONTROLLER, version);
        this.data = data;
    }

    @Override
    public DecommissionControllerRequestData data() {
        return data;
    }

    @Override
    public DecommissionControllerResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new DecommissionControllerResponse(new DecommissionControllerResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code())
                .setErrorMessage(error.message()));
    }

    public static DecommissionControllerRequest parse(Readable readable, short version) {
        return new DecommissionControllerRequest(new DecommissionControllerRequestData(readable, version),
                version);
    }
}
