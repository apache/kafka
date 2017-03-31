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
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;

public class InitPidResponse extends AbstractResponse {
    /**
     * Possible Error codes:
     * OK
     *
     */
    private static final String PRODUCER_ID_KEY_NAME = "pid";
    private static final String EPOCH_KEY_NAME = "epoch";
    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private final Errors error;
    private final long producerId;
    private final short epoch;

    public InitPidResponse(Errors error, long producerId, short epoch) {
        this.error = error;
        this.producerId = producerId;
        this.epoch = epoch;
    }

    public InitPidResponse(Struct struct) {
        this.error = Errors.forCode(struct.getShort(ERROR_CODE_KEY_NAME));
        this.producerId = struct.getLong(PRODUCER_ID_KEY_NAME);
        this.epoch = struct.getShort(EPOCH_KEY_NAME);
    }

    public InitPidResponse(Errors errors) {
        this(errors, RecordBatch.NO_PRODUCER_ID, (short) 0);
    }

    public long producerId() {
        return producerId;
    }

    public Errors error() {
        return error;
    }

    public short epoch() {
        return epoch;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.INIT_PRODUCER_ID.responseSchema(version));
        struct.set(PRODUCER_ID_KEY_NAME, producerId);
        struct.set(EPOCH_KEY_NAME, epoch);
        struct.set(ERROR_CODE_KEY_NAME, error.code());
        return struct;
    }

    public static InitPidResponse parse(ByteBuffer buffer, short version) {
        return new InitPidResponse(ApiKeys.INIT_PRODUCER_ID.parseResponse(version, buffer));
    }

}
