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
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.types.Type.INT64;

public class ExpireDelegationTokenResponse extends AbstractResponse {

    private static final String EXPIRY_TIMESTAMP_KEY_NAME = "expiry_timestamp";

    private final Errors error;
    private final long expiryTimestamp;
    private final int throttleTimeMs;

    private  static final Schema TOKEN_EXPIRE_RESPONSE_V0 = new Schema(
        ERROR_CODE,
        new Field(EXPIRY_TIMESTAMP_KEY_NAME, INT64, "timestamp (in msec) at which this token expires.."),
        THROTTLE_TIME_MS);

    public ExpireDelegationTokenResponse(int throttleTimeMs, Errors error, long expiryTimestamp) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
        this.expiryTimestamp = expiryTimestamp;
    }

    public ExpireDelegationTokenResponse(int throttleTimeMs, Errors error) {
        this(throttleTimeMs, error, -1);
    }

    public ExpireDelegationTokenResponse(Struct struct) {
        error = Errors.forCode(struct.get(ERROR_CODE));
        this.expiryTimestamp = struct.getLong(EXPIRY_TIMESTAMP_KEY_NAME);
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
    }

    public static ExpireDelegationTokenResponse parse(ByteBuffer buffer, short version) {
        return new ExpireDelegationTokenResponse(ApiKeys.EXPIRE_DELEGATION_TOKEN.responseSchema(version).read(buffer));
    }

    public static Schema[] schemaVersions() {
        return new Schema[] {TOKEN_EXPIRE_RESPONSE_V0};
    }

    public Errors error() {
        return error;
    }

    public long expiryTimestamp() {
        return expiryTimestamp;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.EXPIRE_DELEGATION_TOKEN.responseSchema(version));

        struct.set(ERROR_CODE, error.code());
        struct.set(EXPIRY_TIMESTAMP_KEY_NAME, expiryTimestamp);
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);

        return struct;
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public boolean hasError() {
        return this.error != Errors.NONE;
    }
}
