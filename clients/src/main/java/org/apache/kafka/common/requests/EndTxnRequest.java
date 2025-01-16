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

import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class EndTxnRequest extends AbstractRequest {
    public static final short LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2 = 4;
    private final EndTxnRequestData data;

    public static class Builder extends AbstractRequest.Builder<EndTxnRequest> {
        public final EndTxnRequestData data;
        public final boolean isTransactionV2Enabled;

        public Builder(EndTxnRequestData data, boolean isTransactionV2Enabled) {
            this(data, false, isTransactionV2Enabled);
        }

        public Builder(EndTxnRequestData data, boolean enableUnstableLastVersion, boolean isTransactionV2Enabled) {
            super(ApiKeys.END_TXN, enableUnstableLastVersion);
            this.data = data;
            this.isTransactionV2Enabled = isTransactionV2Enabled;
        }

        @Override
        public EndTxnRequest build(short version) {
            if (!isTransactionV2Enabled) {
                version = (short) Math.min(version, LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2);
            }
            return new EndTxnRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private EndTxnRequest(EndTxnRequestData data, short version) {
        super(ApiKeys.END_TXN, version);
        this.data = data;
    }

    public TransactionResult result() {
        if (data.committed())
            return TransactionResult.COMMIT;
        else
            return TransactionResult.ABORT;
    }

    @Override
    public EndTxnRequestData data() {
        return data;
    }

    @Override
    public EndTxnResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new EndTxnResponse(new EndTxnResponseData()
                                      .setErrorCode(Errors.forException(e).code())
                                      .setThrottleTimeMs(throttleTimeMs)
        );
    }

    public static EndTxnRequest parse(ByteBuffer buffer, short version) {
        return new EndTxnRequest(new EndTxnRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
