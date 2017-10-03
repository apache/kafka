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
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.types.Type.INT16;
import static org.apache.kafka.common.protocol.types.Type.INT64;

public class InitProducerIdResponse extends AbstractResponse {
    // Possible error codes:
    //   NotCoordinator
    //   CoordinatorNotAvailable
    //   CoordinatorLoadInProgress
    //   TransactionalIdAuthorizationFailed
    //   ClusterAuthorizationFailed

    private static final String PRODUCER_ID_KEY_NAME = "producer_id";
    private static final String EPOCH_KEY_NAME = "producer_epoch";

    private static final Schema INIT_PRODUCER_ID_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            ERROR_CODE,
            new Field(PRODUCER_ID_KEY_NAME, INT64, "The producer id for the input transactional id. If the input " +
                    "id was empty, then this is used only for ensuring idempotence of messages."),
            new Field(EPOCH_KEY_NAME, INT16, "The epoch for the producer id. Will always be 0 if no transactional " +
                    "id was specified in the request."));

    public static Schema[] schemaVersions() {
        return new Schema[]{INIT_PRODUCER_ID_RESPONSE_V0};
    }

    private final int throttleTimeMs;
    private final Errors error;
    private final long producerId;
    private final short epoch;

    public InitProducerIdResponse(int throttleTimeMs, Errors error, long producerId, short epoch) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
        this.producerId = producerId;
        this.epoch = epoch;
    }

    public InitProducerIdResponse(Struct struct) {
        this.throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        this.error = Errors.forCode(struct.get(ERROR_CODE));
        this.producerId = struct.getLong(PRODUCER_ID_KEY_NAME);
        this.epoch = struct.getShort(EPOCH_KEY_NAME);
    }

    public InitProducerIdResponse(int throttleTimeMs, Errors errors) {
        this(throttleTimeMs, errors, RecordBatch.NO_PRODUCER_ID, (short) 0);
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public long producerId() {
        return producerId;
    }

    public Errors error() {
        return error;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    public short epoch() {
        return epoch;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.INIT_PRODUCER_ID.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        struct.set(PRODUCER_ID_KEY_NAME, producerId);
        struct.set(EPOCH_KEY_NAME, epoch);
        struct.set(ERROR_CODE, error.code());
        return struct;
    }

    public static InitProducerIdResponse parse(ByteBuffer buffer, short version) {
        return new InitProducerIdResponse(ApiKeys.INIT_PRODUCER_ID.parseResponse(version, buffer));
    }

    @Override
    public String toString() {
        return "InitProducerIdResponse(" +
                "error=" + error +
                ", producerId=" + producerId +
                ", producerEpoch=" + epoch +
                ", throttleTimeMs=" + throttleTimeMs +
                ')';
    }
}
