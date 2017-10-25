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

public class RenewTokenRequest extends AbstractRequest {

    private static final String HMAC_KEY_NAME = "hmac";
    private static final String RENEW_TIME_PERIOD_KEY_NAME = "renew_time_period";
    private final ByteBuffer hmac;
    private final long renewTimePeriod;

    public static final Schema TOKEN_RENEW_REQUEST_V0 = new Schema(
        new Field("hmac", BYTES, "HMAC of the delegation token to be renewed."),
        new Field("renew_time_period", INT64, "Renew time period in milli seconds."));

    private RenewTokenRequest(short version, ByteBuffer hmac, long renewTimePeriod) {
        super(version);

        this.hmac = hmac;
        this.renewTimePeriod = renewTimePeriod;
    }

    public RenewTokenRequest(Struct struct, short versionId) {
        super(versionId);

        hmac = struct.getBytes(HMAC_KEY_NAME);
        renewTimePeriod = struct.getLong(RENEW_TIME_PERIOD_KEY_NAME);
    }

    public static RenewTokenRequest parse(ByteBuffer buffer, short version) {
        return new RenewTokenRequest(ApiKeys.RENEW_TOKEN.parseRequest(version, buffer), version);
    }

    public static Schema[] schemaVersions() {
        return new Schema[] {TOKEN_RENEW_REQUEST_V0};
    }

    @Override
    protected Struct toStruct() {
        short version = version();
        Struct struct = new Struct(ApiKeys.RENEW_TOKEN.requestSchema(version));

        struct.set(HMAC_KEY_NAME, hmac);
        struct.set(RENEW_TIME_PERIOD_KEY_NAME, renewTimePeriod);

        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new RenewTokenResponse(throttleTimeMs, Errors.forException(e));
    }

    public ByteBuffer hmac() {
        return hmac;
    }

    public long renewTimePeriod() {
        return renewTimePeriod;
    }

    public static class Builder extends AbstractRequest.Builder<RenewTokenRequest> {
        private final ByteBuffer hmac;
        private final long renewTimePeriod;

        public Builder(ByteBuffer hmac, long renewTimePeriod) {
            super(ApiKeys.RENEW_TOKEN);
            this.hmac = hmac;
            this.renewTimePeriod = renewTimePeriod;
        }

        @Override
        public RenewTokenRequest build(short version) {
            return new RenewTokenRequest(version, hmac, renewTimePeriod);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type: RenewTokenRequest").
                append(", hmac=").append(hmac).
                append(", renewTimePeriod=").append(renewTimePeriod).
                append(")");
            return bld.toString();
        }
    }
}
