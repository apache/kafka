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

import static org.apache.kafka.common.protocol.CommonFields.PRINCIPAL_NAME;
import static org.apache.kafka.common.protocol.CommonFields.PRINCIPAL_TYPE;

public class DescribeDelegationTokenRequest extends AbstractRequest {
    private static final String OWNER_KEY_NAME = "owners";

    private final List<KafkaPrincipal> owners;

    public static final Schema TOKEN_DESCRIBE_REQUEST_V0 = new Schema(
        new Field(OWNER_KEY_NAME, ArrayOf.nullable(new Schema(PRINCIPAL_TYPE, PRINCIPAL_NAME)), "An array of token owners."));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    public static final Schema TOKEN_DESCRIBE_REQUEST_V1 = TOKEN_DESCRIBE_REQUEST_V0;

    public static class Builder extends AbstractRequest.Builder<DescribeDelegationTokenRequest> {
        // describe token for the given list of owners, or null if we want to describe all tokens.
        private final List<KafkaPrincipal> owners;

        public Builder(List<KafkaPrincipal> owners) {
            super(ApiKeys.DESCRIBE_DELEGATION_TOKEN);
            this.owners = owners;
        }

        @Override
        public DescribeDelegationTokenRequest build(short version) {
            return new DescribeDelegationTokenRequest(version, owners);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type: DescribeDelegationTokenRequest").
                append(", owners=").append(owners).
                append(")");
            return bld.toString();
        }
    }

    private DescribeDelegationTokenRequest(short version, List<KafkaPrincipal> owners) {
        super(ApiKeys.DESCRIBE_DELEGATION_TOKEN, version);
        this.owners = owners;
    }

    public DescribeDelegationTokenRequest(Struct struct, short versionId) {
        super(ApiKeys.DESCRIBE_DELEGATION_TOKEN, versionId);

        Object[] ownerArray = struct.getArray(OWNER_KEY_NAME);

        if (ownerArray != null) {
            owners = new ArrayList<>();
            for (Object ownerObj : ownerArray) {
                Struct ownerObjStruct = (Struct) ownerObj;
                String principalType = ownerObjStruct.get(PRINCIPAL_TYPE);
                String principalName = ownerObjStruct.get(PRINCIPAL_NAME);
                owners.add(new KafkaPrincipal(principalType, principalName));
            }
        } else
            owners = null;
    }

    public static Schema[] schemaVersions() {
        return new Schema[]{TOKEN_DESCRIBE_REQUEST_V0, TOKEN_DESCRIBE_REQUEST_V1};
    }

    @Override
    protected Struct toStruct() {
        short version = version();
        Struct struct = new Struct(ApiKeys.DESCRIBE_DELEGATION_TOKEN.requestSchema(version));

        if (owners == null) {
            struct.set(OWNER_KEY_NAME, null);
        } else {
            Object[] ownersArray = new Object[owners.size()];

            int i = 0;
            for (KafkaPrincipal principal: owners) {
                Struct ownerStruct = struct.instance(OWNER_KEY_NAME);
                ownerStruct.set(PRINCIPAL_TYPE, principal.getPrincipalType());
                ownerStruct.set(PRINCIPAL_NAME, principal.getName());
                ownersArray[i++] = ownerStruct;
            }

            struct.set(OWNER_KEY_NAME, ownersArray);
        }

        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new DescribeDelegationTokenResponse(throttleTimeMs, Errors.forException(e));
    }

    public List<KafkaPrincipal> owners() {
        return owners;
    }

    public boolean ownersListEmpty() {
        return owners != null && owners.isEmpty();
    }

    public static DescribeDelegationTokenRequest parse(ByteBuffer buffer, short version) {
        return new DescribeDelegationTokenRequest(ApiKeys.DESCRIBE_DELEGATION_TOKEN.parseRequest(version, buffer), version);
    }
}
