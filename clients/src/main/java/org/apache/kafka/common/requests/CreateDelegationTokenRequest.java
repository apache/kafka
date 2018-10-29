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
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.common.protocol.types.Type.INT64;
import static org.apache.kafka.common.protocol.CommonFields.PRINCIPAL_TYPE;
import static org.apache.kafka.common.protocol.CommonFields.PRINCIPAL_NAME;

public class CreateDelegationTokenRequest extends AbstractRequest {
    private static final String RENEWERS_KEY_NAME = "renewers";
    private static final String MAX_LIFE_TIME_KEY_NAME = "max_life_time";

    private static final Schema TOKEN_CREATE_REQUEST_V0 = new Schema(
            new Field(RENEWERS_KEY_NAME, new ArrayOf(new Schema(PRINCIPAL_TYPE, PRINCIPAL_NAME)),
                    "An array of token renewers. Renewer is an Kafka PrincipalType and name string," +
                        " who is allowed to renew this token before the max lifetime expires."),
            new Field(MAX_LIFE_TIME_KEY_NAME, INT64,
                    "Max lifetime period for token in milli seconds. if value is -1, then max lifetime" +
                        "  will default to a server side config value."));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema TOKEN_CREATE_REQUEST_V1 = TOKEN_CREATE_REQUEST_V0;

    private final List<KafkaPrincipal> renewers;
    private final long maxLifeTime;

    private CreateDelegationTokenRequest(short version, List<KafkaPrincipal> renewers, long maxLifeTime) {
        super(ApiKeys.CREATE_DELEGATION_TOKEN, version);
        this.maxLifeTime = maxLifeTime;
        this.renewers = renewers;
    }

    public CreateDelegationTokenRequest(Struct struct, short version) {
        super(ApiKeys.CREATE_DELEGATION_TOKEN, version);
        maxLifeTime = struct.getLong(MAX_LIFE_TIME_KEY_NAME);
        Object[] renewerArray = struct.getArray(RENEWERS_KEY_NAME);
        renewers = new ArrayList<>();
        if (renewerArray != null) {
            for (Object renewerObj : renewerArray) {
                Struct renewerObjStruct = (Struct) renewerObj;
                String principalType = renewerObjStruct.get(PRINCIPAL_TYPE);
                String principalName = renewerObjStruct.get(PRINCIPAL_NAME);
                renewers.add(new KafkaPrincipal(principalType, principalName));
            }
        }
    }

    public static CreateDelegationTokenRequest parse(ByteBuffer buffer, short version) {
        return new CreateDelegationTokenRequest(ApiKeys.CREATE_DELEGATION_TOKEN.parseRequest(version, buffer), version);
    }

    public static Schema[] schemaVersions() {
        return new Schema[]{TOKEN_CREATE_REQUEST_V0, TOKEN_CREATE_REQUEST_V1};
    }

    @Override
    protected Struct toStruct() {
        short version = version();
        Struct struct = new Struct(ApiKeys.CREATE_DELEGATION_TOKEN.requestSchema(version));
        Object[] renewersArray = new Object[renewers.size()];

        int i = 0;
        for (KafkaPrincipal principal: renewers) {
            Struct renewerStruct = struct.instance(RENEWERS_KEY_NAME);
            renewerStruct.set(PRINCIPAL_TYPE, principal.getPrincipalType());
            renewerStruct.set(PRINCIPAL_NAME, principal.getName());
            renewersArray[i++] = renewerStruct;
        }

        struct.set(RENEWERS_KEY_NAME, renewersArray);
        struct.set(MAX_LIFE_TIME_KEY_NAME, maxLifeTime);
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new CreateDelegationTokenResponse(throttleTimeMs, Errors.forException(e), KafkaPrincipal.ANONYMOUS);
    }

    public List<KafkaPrincipal> renewers() {
        return renewers;
    }

    public long maxLifeTime() {
        return maxLifeTime;
    }

    public static class Builder extends AbstractRequest.Builder<CreateDelegationTokenRequest> {
        private final List<KafkaPrincipal> renewers;
        private final long maxLifeTime;

        public Builder(List<KafkaPrincipal> renewers, long maxLifeTime) {
            super(ApiKeys.CREATE_DELEGATION_TOKEN);
            this.renewers = renewers;
            this.maxLifeTime = maxLifeTime;
        }

        @Override
        public CreateDelegationTokenRequest build(short version) {
            return new CreateDelegationTokenRequest(version, renewers, maxLifeTime);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type: CreateDelegationTokenRequest").
                append(", renewers=").append(renewers).
                append(", maxLifeTime=").append(maxLifeTime).
                append(")");
            return bld.toString();
        }
    }
}
