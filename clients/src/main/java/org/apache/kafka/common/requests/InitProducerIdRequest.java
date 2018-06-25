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

import static org.apache.kafka.common.protocol.CommonFields.NULLABLE_TRANSACTIONAL_ID;
import static org.apache.kafka.common.protocol.types.Type.INT32;

public class InitProducerIdRequest extends AbstractRequest {
    public static final int NO_TRANSACTION_TIMEOUT_MS = Integer.MAX_VALUE;

    private static final String TRANSACTION_TIMEOUT_KEY_NAME = "transaction_timeout_ms";

    private static final Schema INIT_PRODUCER_ID_REQUEST_V0 = new Schema(
            NULLABLE_TRANSACTIONAL_ID,
            new Field(TRANSACTION_TIMEOUT_KEY_NAME, INT32, "The time in ms to wait for before aborting idle transactions sent by this producer."));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema INIT_PRODUCER_ID_REQUEST_V1 = INIT_PRODUCER_ID_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{INIT_PRODUCER_ID_REQUEST_V0, INIT_PRODUCER_ID_REQUEST_V1};
    }

    private final String transactionalId;
    private final int transactionTimeoutMs;

    public static class Builder extends AbstractRequest.Builder<InitProducerIdRequest> {
        private final String transactionalId;
        private final int transactionTimeoutMs;

        public Builder(String transactionalId) {
            this(transactionalId, NO_TRANSACTION_TIMEOUT_MS);
        }

        public Builder(String transactionalId, int transactionTimeoutMs) {
            super(ApiKeys.INIT_PRODUCER_ID);

            if (transactionTimeoutMs <= 0)
                throw new IllegalArgumentException("transaction timeout value is not positive: " + transactionTimeoutMs);

            if (transactionalId != null && transactionalId.isEmpty())
                throw new IllegalArgumentException("Must set either a null or a non-empty transactional id.");

            this.transactionalId = transactionalId;
            this.transactionTimeoutMs = transactionTimeoutMs;
        }

        @Override
        public InitProducerIdRequest build(short version) {
            return new InitProducerIdRequest(version, transactionalId, transactionTimeoutMs);
        }

        @Override
        public String toString() {
            return "(type=InitProducerIdRequest, transactionalId=" + transactionalId + ", transactionTimeoutMs=" +
                    transactionTimeoutMs + ")";
        }
    }

    public InitProducerIdRequest(Struct struct, short version) {
        super(version);
        this.transactionalId = struct.get(NULLABLE_TRANSACTIONAL_ID);
        this.transactionTimeoutMs = struct.getInt(TRANSACTION_TIMEOUT_KEY_NAME);
    }

    private InitProducerIdRequest(short version, String transactionalId, int transactionTimeoutMs) {
        super(version);
        this.transactionalId = transactionalId;
        this.transactionTimeoutMs = transactionTimeoutMs;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new InitProducerIdResponse(throttleTimeMs, Errors.forException(e));
    }

    public static InitProducerIdRequest parse(ByteBuffer buffer, short version) {
        return new InitProducerIdRequest(ApiKeys.INIT_PRODUCER_ID.parseRequest(version, buffer), version);
    }

    public String transactionalId() {
        return transactionalId;
    }

    public int transactionTimeoutMs() {
        return transactionTimeoutMs;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.INIT_PRODUCER_ID.requestSchema(version()));
        struct.set(NULLABLE_TRANSACTIONAL_ID, transactionalId);
        struct.set(TRANSACTION_TIMEOUT_KEY_NAME, transactionTimeoutMs);
        return struct;
    }

}
