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

import org.apache.kafka.common.message.DeleteShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class DeleteShareGroupOffsetsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DeleteShareGroupOffsetsRequest> {

        private final DeleteShareGroupOffsetsRequestData data;

        public Builder(DeleteShareGroupOffsetsRequestData data) {
            super(ApiKeys.DELETE_SHARE_GROUP_OFFSETS);
            this.data = data;
        }

        @Override
        public DeleteShareGroupOffsetsRequest build(short version) {
            return new DeleteShareGroupOffsetsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DeleteShareGroupOffsetsRequestData data;

    public DeleteShareGroupOffsetsRequest(DeleteShareGroupOffsetsRequestData data, short version) {
        super(ApiKeys.DELETE_SHARE_GROUP_OFFSETS, version);
        this.data = data;
    }

    DeleteShareGroupOffsetsResponse getErrorResponse(int throttleTimeMs, Errors error) {
        return getErrorResponse(throttleTimeMs, error.code(), error.message());
    }

    public DeleteShareGroupOffsetsResponse getErrorResponse(int throttleTimeMs, short errorCode, String errorMessage) {
        return new DeleteShareGroupOffsetsResponse(new DeleteShareGroupOffsetsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorMessage(errorMessage)
            .setErrorCode(errorCode));
    }

    @Override
    public DeleteShareGroupOffsetsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return getErrorResponse(throttleTimeMs, Errors.forException(e));
    }

    @Override
    public DeleteShareGroupOffsetsRequestData data() {
        return data;
    }

    public static DeleteShareGroupOffsetsRequest parse(Readable readable, short version) {
        return new DeleteShareGroupOffsetsRequest(
            new DeleteShareGroupOffsetsRequestData(readable, version),
            version
        );
    }

    public static DeleteShareGroupOffsetsResponseData getErrorDeleteResponseData(Errors error) {
        return getErrorDeleteResponseData(error, null);
    }

    public static DeleteShareGroupOffsetsResponseData getErrorDeleteResponseData(short errorCode, String errorMessage) {
        return new DeleteShareGroupOffsetsResponseData()
            .setErrorCode(errorCode)
            .setErrorMessage(errorMessage == null ? Errors.forCode(errorCode).message() : errorMessage);
    }

    public static DeleteShareGroupOffsetsResponseData getErrorDeleteResponseData(Errors error, String errorMessage) {
        return new DeleteShareGroupOffsetsResponseData()
            .setErrorCode(error.code())
            .setErrorMessage(errorMessage == null ? error.message() : errorMessage);
    }
}