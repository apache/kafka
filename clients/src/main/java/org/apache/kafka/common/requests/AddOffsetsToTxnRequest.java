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

import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class AddOffsetsToTxnRequest extends AbstractRequest {

    private final AddOffsetsToTxnRequestData data;

    public static class Builder extends AbstractRequest.Builder<AddOffsetsToTxnRequest> {
        public AddOffsetsToTxnRequestData data;

        public Builder(AddOffsetsToTxnRequestData data) {
            super(ApiKeys.ADD_OFFSETS_TO_TXN);
            this.data = data;
        }

        @Override
        public AddOffsetsToTxnRequest build(short version) {
            return new AddOffsetsToTxnRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public AddOffsetsToTxnRequest(AddOffsetsToTxnRequestData data, short version) {
        super(ApiKeys.ADD_OFFSETS_TO_TXN, version);
        this.data = data;
    }

    @Override
    public AddOffsetsToTxnRequestData data() {
        return data;
    }

    @Override
    public AddOffsetsToTxnResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new AddOffsetsToTxnResponse(new AddOffsetsToTxnResponseData()
                                               .setErrorCode(Errors.forException(e).code())
                                               .setThrottleTimeMs(throttleTimeMs));
    }

    public static AddOffsetsToTxnRequest parse(ByteBuffer buffer, short version) {
        return new AddOffsetsToTxnRequest(new AddOffsetsToTxnRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
