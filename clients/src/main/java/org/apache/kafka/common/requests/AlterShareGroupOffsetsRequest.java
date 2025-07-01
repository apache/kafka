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

import org.apache.kafka.common.message.AlterShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.AlterShareGroupOffsetsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class AlterShareGroupOffsetsRequest extends AbstractRequest {

    private final AlterShareGroupOffsetsRequestData data;

    private AlterShareGroupOffsetsRequest(AlterShareGroupOffsetsRequestData data, short version) {
        super(ApiKeys.ALTER_SHARE_GROUP_OFFSETS, version);
        this.data = data;
    }

    public static class Builder extends AbstractRequest.Builder<AlterShareGroupOffsetsRequest> {

        private final AlterShareGroupOffsetsRequestData data;

        public Builder(AlterShareGroupOffsetsRequestData data) {
            super(ApiKeys.ALTER_SHARE_GROUP_OFFSETS);
            this.data = data;
        }

        @Override
        public AlterShareGroupOffsetsRequest build(short version) {
            return new AlterShareGroupOffsetsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    @Override
    public AlterShareGroupOffsetsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return getErrorResponse(throttleTimeMs, Errors.forException(e));
    }

    public AlterShareGroupOffsetsResponse getErrorResponse(int throttleTimeMs, Errors error) {
        return getErrorResponse(throttleTimeMs, error.code(), error.message());
    }

    public AlterShareGroupOffsetsResponse getErrorResponse(int throttleTimeMs, short errorCode, String message) {
        return new AlterShareGroupOffsetsResponse(
            new AlterShareGroupOffsetsResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(errorCode)
                .setErrorMessage(message)
        );
    }

    public static AlterShareGroupOffsetsResponseData getErrorResponseData(Errors error) {
        return getErrorResponseData(error, null);
    }

    public static AlterShareGroupOffsetsResponseData getErrorResponseData(Errors error, String errorMessage) {
        return new AlterShareGroupOffsetsResponseData()
            .setErrorCode(error.code())
            .setErrorMessage(errorMessage == null ? error.message() : errorMessage);
    }

    public static AlterShareGroupOffsetsRequest parse(Readable readable, short version) {
        return new AlterShareGroupOffsetsRequest(
            new AlterShareGroupOffsetsRequestData(readable, version),
            version
        );
    }

    @Override
    public AlterShareGroupOffsetsRequestData data() {
        return data;
    }
}
