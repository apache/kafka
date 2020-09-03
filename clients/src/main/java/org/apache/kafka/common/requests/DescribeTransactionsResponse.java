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

import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData.TransactionState;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class DescribeTransactionsResponse extends AbstractResponse {
    private final DescribeTransactionsResponseData data;

    public DescribeTransactionsResponse(Struct struct, short version) {
        this(new DescribeTransactionsResponseData(struct, version));
    }

    public DescribeTransactionsResponse(DescribeTransactionsResponseData data) {
        this.data = data;
    }

    public DescribeTransactionsResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (TransactionState transactionState : data.transactionStates()) {
            Errors error = Errors.forCode(transactionState.errorCode());
            updateErrorCounts(errorCounts, error);
        }
        return errorCounts;
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    public static DescribeTransactionsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeTransactionsResponse(
            ApiKeys.DESCRIBE_TRANSACTIONS.responseSchema(version).read(buffer), version);
    }

}
