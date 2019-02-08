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
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.TokenInformation;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.PRINCIPAL_NAME;
import static org.apache.kafka.common.protocol.CommonFields.PRINCIPAL_TYPE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.types.Type.BYTES;
import static org.apache.kafka.common.protocol.types.Type.INT64;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class DescribeDelegationTokenResponse extends AbstractResponse {

    private static final String TOKEN_DETAILS_KEY_NAME = "token_details";
    private static final String ISSUE_TIMESTAMP_KEY_NAME = "issue_timestamp";
    private static final String EXPIRY_TIMESTAMP_NAME = "expiry_timestamp";
    private static final String MAX_TIMESTAMP_NAME = "max_timestamp";
    private static final String TOKEN_ID_KEY_NAME = "token_id";
    private static final String HMAC_KEY_NAME = "hmac";
    private static final String OWNER_KEY_NAME = "owner";
    private static final String RENEWERS_KEY_NAME = "renewers";

    private final Errors error;
    private final List<DelegationToken> tokens;
    private final int throttleTimeMs;

    private static final Schema TOKEN_DETAILS_V0 = new Schema(
        new Field(OWNER_KEY_NAME, new Schema(PRINCIPAL_TYPE, PRINCIPAL_NAME), "token owner."),
        new Field(ISSUE_TIMESTAMP_KEY_NAME, INT64, "timestamp (in msec) when this token was generated."),
        new Field(EXPIRY_TIMESTAMP_NAME, INT64, "timestamp (in msec) at which this token expires."),
        new Field(MAX_TIMESTAMP_NAME, INT64, "max life time of this token."),
        new Field(TOKEN_ID_KEY_NAME, STRING, "UUID to ensure uniqueness."),
        new Field(HMAC_KEY_NAME, BYTES, "HMAC of the delegation token to be expired."),
        new Field(RENEWERS_KEY_NAME, new ArrayOf(new Schema(PRINCIPAL_TYPE, PRINCIPAL_NAME)),
            "An array of token renewers. Renewer is an Kafka PrincipalType and name string," +
                " who is allowed to renew this token before the max lifetime expires."));

    private static final Schema TOKEN_DESCRIBE_RESPONSE_V0 = new Schema(
        ERROR_CODE,
        new Field(TOKEN_DETAILS_KEY_NAME, new ArrayOf(TOKEN_DETAILS_V0)),
        THROTTLE_TIME_MS);

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema TOKEN_DESCRIBE_RESPONSE_V1 = TOKEN_DESCRIBE_RESPONSE_V0;

    public DescribeDelegationTokenResponse(int throttleTimeMs, Errors error, List<DelegationToken> tokens) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
        this.tokens = tokens;
    }

    public DescribeDelegationTokenResponse(int throttleTimeMs, Errors error) {
        this(throttleTimeMs, error, new ArrayList<>());
    }

    public DescribeDelegationTokenResponse(Struct struct) {
        Object[] requestStructs = struct.getArray(TOKEN_DETAILS_KEY_NAME);
        List<DelegationToken> tokens = new ArrayList<>();

        for (Object requestStructObj : requestStructs) {
            Struct singleRequestStruct = (Struct) requestStructObj;

            Struct ownerStruct = (Struct) singleRequestStruct.get(OWNER_KEY_NAME);
            KafkaPrincipal owner = new KafkaPrincipal(ownerStruct.get(PRINCIPAL_TYPE), ownerStruct.get(PRINCIPAL_NAME));
            long issueTimestamp = singleRequestStruct.getLong(ISSUE_TIMESTAMP_KEY_NAME);
            long expiryTimestamp = singleRequestStruct.getLong(EXPIRY_TIMESTAMP_NAME);
            long maxTimestamp = singleRequestStruct.getLong(MAX_TIMESTAMP_NAME);
            String tokenId = singleRequestStruct.getString(TOKEN_ID_KEY_NAME);
            ByteBuffer hmac = singleRequestStruct.getBytes(HMAC_KEY_NAME);

            Object[] renewerArray = singleRequestStruct.getArray(RENEWERS_KEY_NAME);
            List<KafkaPrincipal>  renewers = new ArrayList<>();
            if (renewerArray != null) {
                for (Object renewerObj : renewerArray) {
                    Struct renewerObjStruct = (Struct) renewerObj;
                    String principalType = renewerObjStruct.get(PRINCIPAL_TYPE);
                    String principalName = renewerObjStruct.get(PRINCIPAL_NAME);
                    renewers.add(new KafkaPrincipal(principalType, principalName));
                }
            }

            TokenInformation tokenInfo = new TokenInformation(tokenId, owner, renewers, issueTimestamp, maxTimestamp, expiryTimestamp);

            byte[] hmacBytes = new byte[hmac.remaining()];
            hmac.get(hmacBytes);

            DelegationToken tokenDetails = new DelegationToken(tokenInfo, hmacBytes);
            tokens.add(tokenDetails);
        }

        this.tokens = tokens;
        error = Errors.forCode(struct.get(ERROR_CODE));
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
    }

    public static DescribeDelegationTokenResponse parse(ByteBuffer buffer, short version) {
        return new DescribeDelegationTokenResponse(ApiKeys.DESCRIBE_DELEGATION_TOKEN.responseSchema(version).read(buffer));
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.DESCRIBE_DELEGATION_TOKEN.responseSchema(version));
        List<Struct> tokenDetailsStructs = new ArrayList<>(tokens.size());

        struct.set(ERROR_CODE, error.code());

        for (DelegationToken token : tokens) {
            TokenInformation tokenInfo = token.tokenInfo();
            Struct singleRequestStruct = struct.instance(TOKEN_DETAILS_KEY_NAME);
            Struct ownerStruct = singleRequestStruct.instance(OWNER_KEY_NAME);
            ownerStruct.set(PRINCIPAL_TYPE, tokenInfo.owner().getPrincipalType());
            ownerStruct.set(PRINCIPAL_NAME, tokenInfo.owner().getName());
            singleRequestStruct.set(OWNER_KEY_NAME, ownerStruct);
            singleRequestStruct.set(ISSUE_TIMESTAMP_KEY_NAME, tokenInfo.issueTimestamp());
            singleRequestStruct.set(EXPIRY_TIMESTAMP_NAME, tokenInfo.expiryTimestamp());
            singleRequestStruct.set(MAX_TIMESTAMP_NAME, tokenInfo.maxTimestamp());
            singleRequestStruct.set(TOKEN_ID_KEY_NAME, tokenInfo.tokenId());
            singleRequestStruct.set(HMAC_KEY_NAME, ByteBuffer.wrap(token.hmac()));

            Object[] renewersArray = new Object[tokenInfo.renewers().size()];

            int i = 0;
            for (KafkaPrincipal principal: tokenInfo.renewers()) {
                Struct renewerStruct = singleRequestStruct.instance(RENEWERS_KEY_NAME);
                renewerStruct.set(PRINCIPAL_TYPE, principal.getPrincipalType());
                renewerStruct.set(PRINCIPAL_NAME, principal.getName());
                renewersArray[i++] = renewerStruct;
            }

            singleRequestStruct.set(RENEWERS_KEY_NAME, renewersArray);

            tokenDetailsStructs.add(singleRequestStruct);
        }
        struct.set(TOKEN_DETAILS_KEY_NAME, tokenDetailsStructs.toArray());
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);

        return struct;
    }

    public static Schema[] schemaVersions() {
        return new Schema[]{TOKEN_DESCRIBE_RESPONSE_V0, TOKEN_DESCRIBE_RESPONSE_V1};
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Errors error() {
        return error;
    }

    public List<DelegationToken> tokens() {
        return tokens;
    }

    public boolean hasError() {
        return this.error != Errors.NONE;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
