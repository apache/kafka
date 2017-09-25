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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

import static org.apache.kafka.common.protocol.types.Type.BOOLEAN;
import static org.apache.kafka.common.protocol.types.Type.INT16;
import static org.apache.kafka.common.protocol.types.Type.INT64;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class EndTxnRequest extends AbstractRequest {
    private static final String TRANSACTIONAL_ID_KEY_NAME = "transactional_id";
    private static final String PRODUCER_ID_KEY_NAME = "producer_id";
    private static final String PRODUCER_EPOCH_KEY_NAME = "producer_epoch";
    private static final String TRANSACTION_RESULT_KEY_NAME = "transaction_result";

    private static final Schema END_TXN_REQUEST_V0 = new Schema(
            new Field(TRANSACTIONAL_ID_KEY_NAME, STRING, "The transactional id corresponding to the transaction."),
            new Field(PRODUCER_ID_KEY_NAME, INT64, "Current producer id in use by the transactional id."),
            new Field(PRODUCER_EPOCH_KEY_NAME, INT16, "Current epoch associated with the producer id."),
            new Field(TRANSACTION_RESULT_KEY_NAME, BOOLEAN, "The result of the transaction (0 = ABORT, 1 = COMMIT)"));

    public static Schema[] schemaVersions() {
        return new Schema[]{END_TXN_REQUEST_V0};
    }

    public static class Builder extends AbstractRequest.Builder<EndTxnRequest> {
        private final String transactionalId;
        private final long producerId;
        private final short producerEpoch;
        private final TransactionResult result;

        public Builder(String transactionalId, long producerId, short producerEpoch, TransactionResult result) {
            super(ApiKeys.END_TXN);
            this.transactionalId = transactionalId;
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.result = result;
        }

        public TransactionResult result() {
            return result;
        }

        @Override
        public EndTxnRequest build(short version) {
            return new EndTxnRequest(version, transactionalId, producerId, producerEpoch, result);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=EndTxnRequest").
                    append(", transactionalId=").append(transactionalId).
                    append(", producerId=").append(producerId).
                    append(", producerEpoch=").append(producerEpoch).
                    append(", result=").append(result).
                    append(")");
            return bld.toString();
        }
    }

    private final String transactionalId;
    private final long producerId;
    private final short producerEpoch;
    private final TransactionResult result;

    private EndTxnRequest(short version, String transactionalId, long producerId, short producerEpoch, TransactionResult result) {
        super(version);
        this.transactionalId = transactionalId;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.result = result;
    }

    public EndTxnRequest(Struct struct, short version) {
        super(version);
        this.transactionalId = struct.getString(TRANSACTIONAL_ID_KEY_NAME);
        this.producerId = struct.getLong(PRODUCER_ID_KEY_NAME);
        this.producerEpoch = struct.getShort(PRODUCER_EPOCH_KEY_NAME);
        this.result = TransactionResult.forId(struct.getBoolean(TRANSACTION_RESULT_KEY_NAME));
    }

    public String transactionalId() {
        return transactionalId;
    }

    public long producerId() {
        return producerId;
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    public TransactionResult command() {
        return result;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.END_TXN.requestSchema(version()));
        struct.set(TRANSACTIONAL_ID_KEY_NAME, transactionalId);
        struct.set(PRODUCER_ID_KEY_NAME, producerId);
        struct.set(PRODUCER_EPOCH_KEY_NAME, producerEpoch);
        struct.set(TRANSACTION_RESULT_KEY_NAME, result.id);
        return struct;
    }

    @Override
    public EndTxnResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new EndTxnResponse(throttleTimeMs, Errors.forException(e));
    }

    public static EndTxnRequest parse(ByteBuffer buffer, short version) {
        return new EndTxnRequest(ApiKeys.END_TXN.parseRequest(version, buffer), version);
    }

}
