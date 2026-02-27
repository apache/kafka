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

import org.apache.kafka.common.message.GetConfigSubscriptionRequestData;
import org.apache.kafka.common.message.GetConfigSubscriptionResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.utils.AppInfoParser;

public class GetConfigSubscriptionRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<GetConfigSubscriptionRequest> {
        private static final String DEFAULT_CLIENT_SOFTWARE_NAME = "apache-kafka-java";
        private static final String DEFAULT_CLIENT_SOFTWARE_ROLE_CONSUMER = "consumer";

        public static GetConfigSubscriptionRequest.Builder forConsumer(short maxVersion) {
            GetConfigSubscriptionRequestData requestData = new GetConfigSubscriptionRequestData()
                .setClientSoftwareName(DEFAULT_CLIENT_SOFTWARE_NAME)
                .setClientSoftwareVersion(AppInfoParser.getVersion())
                .setClientSoftwareRole(DEFAULT_CLIENT_SOFTWARE_ROLE_CONSUMER);
            return new GetConfigSubscriptionRequest.Builder(requestData);
        }

        private final GetConfigSubscriptionRequestData data;

        public Builder(GetConfigSubscriptionRequestData data) {
            this(data, false);
        }

        public Builder(GetConfigSubscriptionRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.GET_CONFIG_SUBSCRIPTION, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public GetConfigSubscriptionRequest build(short version) {
            return new GetConfigSubscriptionRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final GetConfigSubscriptionRequestData data;

    public GetConfigSubscriptionRequest(GetConfigSubscriptionRequestData data, short version) {
        super(ApiKeys.GET_CONFIG_SUBSCRIPTION, version);
        this.data = data;
    }

    @Override
    public GetConfigSubscriptionResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        GetConfigSubscriptionResponseData responseData = new GetConfigSubscriptionResponseData()
            .setErrorCode(Errors.forException(e).code())
            .setThrottleTimeMs(throttleTimeMs);
        return new GetConfigSubscriptionResponse(responseData);
    }

    @Override
    public GetConfigSubscriptionRequestData data() {
        return data;
    }

    public static GetConfigSubscriptionRequest parse(Readable readable, short version) {
        return new GetConfigSubscriptionRequest(new GetConfigSubscriptionRequestData(
            readable, version), version);
    }
}
