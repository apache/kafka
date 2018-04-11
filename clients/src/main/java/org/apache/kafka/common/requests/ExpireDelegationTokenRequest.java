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

import static org.apache.kafka.common.protocol.types.Type.BYTES;
import static org.apache.kafka.common.protocol.types.Type.INT64;

public class ExpireDelegationTokenRequest extends AbstractRequest {

    private static final String HMAC_KEY_NAME = "hmac";
    private static final String EXPIRY_TIME_PERIOD_KEY_NAME = "expiry_time_period";
    private final ByteBuffer hmac;
    private final long expiryTimePeriod;

    private static final Schema TOKEN_EXPIRE_REQUEST_V0 = new Schema(
        new Field(HMAC_KEY_NAME, BYTES, "HMAC of the delegation token to be expired."),
        new Field(EXPIRY_TIME_PERIOD_KEY_NAME, INT64, "expiry time period in milli seconds."));

    private ExpireDelegationTokenRequest(short version, ByteBuffer hmac, long renewTimePeriod) {
        super(version);

        this.hmac = hmac;
        this.expiryTimePeriod = renewTimePeriod;
    }

    public ExpireDelegationTokenRequest(Struct struct, short versionId) {
        super(versionId);

        hmac = struct.getBytes(HMAC_KEY_NAME);
        expiryTimePeriod = struct.getLong(EXPIRY_TIME_PERIOD_KEY_NAME);
    }

    public static ExpireDelegationTokenRequest parse(ByteBuffer buffer, short version) {
        return new ExpireDelegationTokenRequest(ApiKeys.EXPIRE_DELEGATION_TOKEN.parseRequest(version, buffer), version);
    }

    public static Schema[] schemaVersions() {
        return new Schema[] {TOKEN_EXPIRE_REQUEST_V0};
    }

    @Override
    protected Struct toStruct() {
        short version = version();
        Struct struct = new Struct(ApiKeys.EXPIRE_DELEGATION_TOKEN.requestSchema(version));

        struct.set(HMAC_KEY_NAME, hmac);
        struct.set(EXPIRY_TIME_PERIOD_KEY_NAME, expiryTimePeriod);

        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new ExpireDelegationTokenResponse(throttleTimeMs, Errors.forException(e));
    }

    public ByteBuffer hmac() {
        return hmac;
    }

    public long expiryTimePeriod() {
        return expiryTimePeriod;
    }

    public static class Builder extends AbstractRequest.Builder<ExpireDelegationTokenRequest> {
        private final ByteBuffer hmac;
        private final long expiryTimePeriod;

        public Builder(byte[] hmac, long expiryTimePeriod) {
            super(ApiKeys.EXPIRE_DELEGATION_TOKEN);
            this.hmac = ByteBuffer.wrap(hmac);
            this.expiryTimePeriod = expiryTimePeriod;
        }

        @Override
        public ExpireDelegationTokenRequest build(short version) {
            return new ExpireDelegationTokenRequest(version, hmac, expiryTimePeriod);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type: ExpireDelegationTokenRequest").
                append(", hmac=").append(hmac).
                append(", expiryTimePeriod=").append(expiryTimePeriod).
                append(")");
            return bld.toString();
        }
    }
}
