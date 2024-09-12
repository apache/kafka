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

import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class DescribeTransactionsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DescribeTransactionsRequest> {
        public final DescribeTransactionsRequestData data;

        public Builder(DescribeTransactionsRequestData data) {
            super(ApiKeys.DESCRIBE_TRANSACTIONS);
            this.data = data;
        }

        @Override
        public DescribeTransactionsRequest build(short version) {
            return new DescribeTransactionsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DescribeTransactionsRequestData data;

    private DescribeTransactionsRequest(DescribeTransactionsRequestData data, short version) {
        super(ApiKeys.DESCRIBE_TRANSACTIONS, version);
        this.data = data;
    }

    @Override
    public DescribeTransactionsRequestData data() {
        return data;
    }

    @Override
    public DescribeTransactionsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        DescribeTransactionsResponseData response = new DescribeTransactionsResponseData()
            .setThrottleTimeMs(throttleTimeMs);

        for (String transactionalId : data.transactionalIds()) {
            DescribeTransactionsResponseData.TransactionState transactionState =
                new DescribeTransactionsResponseData.TransactionState()
                    .setTransactionalId(transactionalId)
                    .setErrorCode(error.code());
            response.transactionStates().add(transactionState);
        }
        return new DescribeTransactionsResponse(response);
    }

    public static DescribeTransactionsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeTransactionsRequest(new DescribeTransactionsRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public String toString(boolean verbose) {
        return data.toString();
    }

}
