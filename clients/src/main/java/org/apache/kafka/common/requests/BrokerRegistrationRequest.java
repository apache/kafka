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

import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.BrokerRegistrationResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class BrokerRegistrationRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<BrokerRegistrationRequest> {
        private final BrokerRegistrationRequestData data;

        public Builder(BrokerRegistrationRequestData data) {
            super(ApiKeys.BROKER_REGISTRATION);
            this.data = data;
        }

        @Override
        public short oldestAllowedVersion() {
            if (data.isMigratingZkBroker()) {
                return (short) 1;
            } else {
                return (short) 0;
            }
        }

        @Override
        public BrokerRegistrationRequest build(short version) {
            if (version < 4) {
                // Workaround for KAFKA-17492: for BrokerRegistrationRequest versions older than 4,
                // remove features with minSupportedVersion = 0.
                BrokerRegistrationRequestData newData = data.duplicate();
                newData.features().removeIf(feature -> feature.minSupportedVersion() == 0);
                return new BrokerRegistrationRequest(newData, version);
            } else {
                return new BrokerRegistrationRequest(data, version);
            }
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final BrokerRegistrationRequestData data;

    public BrokerRegistrationRequest(BrokerRegistrationRequestData data, short version) {
        super(ApiKeys.BROKER_REGISTRATION, version);
        this.data = data;
    }

    @Override
    public BrokerRegistrationRequestData data() {
        return data;
    }

    @Override
    public BrokerRegistrationResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new BrokerRegistrationResponse(new BrokerRegistrationResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code()));
    }

    public static BrokerRegistrationRequest parse(ByteBuffer buffer, short version) {
        return new BrokerRegistrationRequest(new BrokerRegistrationRequestData(new ByteBufferAccessor(buffer), version),
                version);
    }
}
