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

import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;

public class InitProducerIdRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<InitProducerIdRequest> {
        public final InitProducerIdRequestData data;

        public Builder(InitProducerIdRequestData data) {
            super(ApiKeys.INIT_PRODUCER_ID);
            this.data = data;
        }

        @Override
        public InitProducerIdRequest build(short version) {
            if (data.transactionTimeoutMs() <= 0)
                throw new IllegalArgumentException("transaction timeout value is not positive: " + data.transactionTimeoutMs());

            if (data.transactionalId() != null && data.transactionalId().isEmpty())
                throw new IllegalArgumentException("Must set either a null or a non-empty transactional id.");

            return new InitProducerIdRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final InitProducerIdRequestData data;

    private InitProducerIdRequest(InitProducerIdRequestData data, short version) {
        super(ApiKeys.INIT_PRODUCER_ID, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        InitProducerIdResponseData response = new InitProducerIdResponseData()
                .setErrorCode(Errors.forException(e).code())
                .setProducerId(RecordBatch.NO_PRODUCER_ID)
                .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
                .setThrottleTimeMs(0);
        return new InitProducerIdResponse(response);
    }

    public static InitProducerIdRequest parse(ByteBuffer buffer, short version) {
        return new InitProducerIdRequest(new InitProducerIdRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public InitProducerIdRequestData data() {
        return data;
    }

}
