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

import org.apache.kafka.common.message.ConsumerGroupInstallAssignmentRequestData;
import org.apache.kafka.common.message.ConsumerGroupInstallAssignmentResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class ConsumerGroupInstallAssignmentRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ConsumerGroupInstallAssignmentRequest> {
        private final ConsumerGroupInstallAssignmentRequestData data;

        public Builder(ConsumerGroupInstallAssignmentRequestData data) {
            this(data, false);
        }

        public Builder(ConsumerGroupInstallAssignmentRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.CONSUMER_GROUP_INSTALL_ASSIGNMENT, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public ConsumerGroupInstallAssignmentRequest build(short version) {
            return new ConsumerGroupInstallAssignmentRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ConsumerGroupInstallAssignmentRequestData data;

    public ConsumerGroupInstallAssignmentRequest(ConsumerGroupInstallAssignmentRequestData data, short version) {
        super(ApiKeys.CONSUMER_GROUP_INSTALL_ASSIGNMENT, version);
        this.data = data;
    }

    @Override
    public ConsumerGroupInstallAssignmentResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new ConsumerGroupInstallAssignmentResponse(
                new ConsumerGroupInstallAssignmentResponseData()
                        .setThrottleTimeMs(throttleTimeMs)
                        .setErrorCode(Errors.forException(e).code())
        );
    }

    @Override
    public ConsumerGroupInstallAssignmentRequestData data() {
        return data;
    }

    public static ConsumerGroupInstallAssignmentRequest parse(ByteBuffer buffer, short version) {
        return new ConsumerGroupInstallAssignmentRequest(
                new ConsumerGroupInstallAssignmentRequestData(new ByteBufferAccessor(buffer), version),
                version
        );
    }

}

