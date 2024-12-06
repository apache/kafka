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

import org.apache.kafka.common.errors.UnsupportedProtocolFieldException;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class ListTransactionsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ListTransactionsRequest> {
        public final ListTransactionsRequestData data;

        public Builder(ListTransactionsRequestData data) {
            super(ApiKeys.LIST_TRANSACTIONS);
            this.data = data;
        }

        @Override
        public ListTransactionsRequest build(short version) {
            if (data.durationFilter() >= 0 && version < 1) {
                throw new UnsupportedProtocolFieldException("DurationFilter", apiKey().name(), version, 1);
            }
            return new ListTransactionsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ListTransactionsRequestData data;

    private ListTransactionsRequest(ListTransactionsRequestData data, short version) {
        super(ApiKeys.LIST_TRANSACTIONS, version);
        this.data = data;
    }

    public ListTransactionsRequestData data() {
        return data;
    }

    @Override
    public ListTransactionsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        ListTransactionsResponseData response = new ListTransactionsResponseData()
            .setErrorCode(error.code())
            .setThrottleTimeMs(throttleTimeMs);
        return new ListTransactionsResponse(response);
    }

    public static ListTransactionsRequest parse(ByteBuffer buffer, short version) {
        return new ListTransactionsRequest(new ListTransactionsRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public String toString(boolean verbose) {
        return data.toString();
    }

}
