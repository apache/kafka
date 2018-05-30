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
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

import static org.apache.kafka.common.protocol.CommonFields.GROUP_ID;
import static org.apache.kafka.common.protocol.CommonFields.PRODUCER_EPOCH;
import static org.apache.kafka.common.protocol.CommonFields.PRODUCER_ID;
import static org.apache.kafka.common.protocol.CommonFields.TRANSACTIONAL_ID;

public class AddOffsetsToTxnRequest extends AbstractRequest {
    private static final Schema ADD_OFFSETS_TO_TXN_REQUEST_V0 = new Schema(
            TRANSACTIONAL_ID,
            PRODUCER_ID,
            PRODUCER_EPOCH,
            GROUP_ID);

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema ADD_OFFSETS_TO_TXN_REQUEST_V1 = ADD_OFFSETS_TO_TXN_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{ADD_OFFSETS_TO_TXN_REQUEST_V0, ADD_OFFSETS_TO_TXN_REQUEST_V1};
    }

    public static class Builder extends AbstractRequest.Builder<AddOffsetsToTxnRequest> {
        private final String transactionalId;
        private final long producerId;
        private final short producerEpoch;
        private final String consumerGroupId;

        public Builder(String transactionalId, long producerId, short producerEpoch, String consumerGroupId) {
            super(ApiKeys.ADD_OFFSETS_TO_TXN);
            this.transactionalId = transactionalId;
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.consumerGroupId = consumerGroupId;
        }

        public String consumerGroupId() {
            return consumerGroupId;
        }

        @Override
        public AddOffsetsToTxnRequest build(short version) {
            return new AddOffsetsToTxnRequest(version, transactionalId, producerId, producerEpoch, consumerGroupId);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=AddOffsetsToTxnRequest").
                    append(", transactionalId=").append(transactionalId).
                    append(", producerId=").append(producerId).
                    append(", producerEpoch=").append(producerEpoch).
                    append(", consumerGroupId=").append(consumerGroupId).
                    append(")");
            return bld.toString();
        }
    }

    private final String transactionalId;
    private final long producerId;
    private final short producerEpoch;
    private final String consumerGroupId;

    private AddOffsetsToTxnRequest(short version, String transactionalId, long producerId, short producerEpoch, String consumerGroupId) {
        super(version);
        this.transactionalId = transactionalId;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.consumerGroupId = consumerGroupId;
    }

    public AddOffsetsToTxnRequest(Struct struct, short version) {
        super(version);
        this.transactionalId = struct.get(TRANSACTIONAL_ID);
        this.producerId = struct.get(PRODUCER_ID);
        this.producerEpoch = struct.get(PRODUCER_EPOCH);
        this.consumerGroupId = struct.get(GROUP_ID);
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

    public String consumerGroupId() {
        return consumerGroupId;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.ADD_OFFSETS_TO_TXN.requestSchema(version()));
        struct.set(TRANSACTIONAL_ID, transactionalId);
        struct.set(PRODUCER_ID, producerId);
        struct.set(PRODUCER_EPOCH, producerEpoch);
        struct.set(GROUP_ID, consumerGroupId);
        return struct;
    }

    @Override
    public AddOffsetsToTxnResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new AddOffsetsToTxnResponse(throttleTimeMs, Errors.forException(e));
    }

    public static AddOffsetsToTxnRequest parse(ByteBuffer buffer, short version) {
        return new AddOffsetsToTxnRequest(ApiKeys.ADD_OFFSETS_TO_TXN.parseRequest(version, buffer), version);
    }

}
