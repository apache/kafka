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
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class AddOffsetsToTxnRequest extends AbstractRequest {
    private static final String TRANSACTIONAL_ID_KEY_NAME = "transactional_id";
    private static final String PID_KEY_NAME = "producer_id";
    private static final String EPOCH_KEY_NAME = "producer_epoch";
    private static final String CONSUMER_GROUP_ID_KEY_NAME = "consumer_group_id";

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

        @Override
        public AddOffsetsToTxnRequest build(short version) {
            return new AddOffsetsToTxnRequest(version, transactionalId, producerId, producerEpoch, consumerGroupId);
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
        this.transactionalId = struct.getString(TRANSACTIONAL_ID_KEY_NAME);
        this.producerId = struct.getLong(PID_KEY_NAME);
        this.producerEpoch = struct.getShort(EPOCH_KEY_NAME);
        this.consumerGroupId = struct.getString(CONSUMER_GROUP_ID_KEY_NAME);
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
        struct.set(TRANSACTIONAL_ID_KEY_NAME, transactionalId);
        struct.set(PID_KEY_NAME, producerId);
        struct.set(EPOCH_KEY_NAME, producerEpoch);
        struct.set(CONSUMER_GROUP_ID_KEY_NAME, consumerGroupId);
        return struct;
    }

    @Override
    public AddOffsetsToTxnResponse getErrorResponse(Throwable e) {
        return new AddOffsetsToTxnResponse(Errors.forException(e));
    }

    public static AddOffsetsToTxnRequest parse(ByteBuffer buffer, short version) {
        return new AddOffsetsToTxnRequest(ApiKeys.ADD_OFFSETS_TO_TXN.parseRequest(version, buffer), version);
    }

}
