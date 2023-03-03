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

import org.apache.kafka.common.message.DescribeDelegationTokenResponseData;
import org.apache.kafka.common.message.DescribeDelegationTokenResponseData.DescribedDelegationToken;
import org.apache.kafka.common.message.DescribeDelegationTokenResponseData.DescribedDelegationTokenRenewer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.TokenInformation;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DescribeDelegationTokenResponse extends AbstractResponse {

    private final DescribeDelegationTokenResponseData data;

    public DescribeDelegationTokenResponse(int version, int throttleTimeMs, Errors error, List<DelegationToken> tokens) {
        super(ApiKeys.DESCRIBE_DELEGATION_TOKEN);
        List<DescribedDelegationToken> describedDelegationTokenList = tokens
            .stream()
            .map(dt -> {
                DescribedDelegationToken ddt = new DescribedDelegationToken()
                    .setTokenId(dt.tokenInfo().tokenId())
                    .setPrincipalType(dt.tokenInfo().owner().getPrincipalType())
                    .setPrincipalName(dt.tokenInfo().owner().getName())
                    .setIssueTimestamp(dt.tokenInfo().issueTimestamp())
                    .setMaxTimestamp(dt.tokenInfo().maxTimestamp())
                    .setExpiryTimestamp(dt.tokenInfo().expiryTimestamp())
                    .setHmac(dt.hmac())
                    .setRenewers(dt.tokenInfo().renewers()
                        .stream()
                        .map(r -> new DescribedDelegationTokenRenewer().setPrincipalName(r.getName()).setPrincipalType(r.getPrincipalType()))
                        .collect(Collectors.toList()));
                if (version > 2) {
                    ddt.setTokenRequesterPrincipalType(dt.tokenInfo().tokenRequester().getPrincipalType())
                        .setTokenRequesterPrincipalName(dt.tokenInfo().tokenRequester().getName());
                }
                return ddt;
            })
            .collect(Collectors.toList());

        this.data = new DescribeDelegationTokenResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.code())
            .setTokens(describedDelegationTokenList);
    }

    public DescribeDelegationTokenResponse(int version, int throttleTimeMs, Errors error) {
        this(version, throttleTimeMs, error, new ArrayList<>());
    }

    public DescribeDelegationTokenResponse(DescribeDelegationTokenResponseData data) {
        super(ApiKeys.DESCRIBE_DELEGATION_TOKEN);
        this.data = data;
    }

    public static DescribeDelegationTokenResponse parse(ByteBuffer buffer, short version) {
        return new DescribeDelegationTokenResponse(new DescribeDelegationTokenResponseData(
            new ByteBufferAccessor(buffer), version));
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error());
    }

    @Override
    public DescribeDelegationTokenResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    public List<DelegationToken> tokens() {
        return data.tokens()
            .stream()
            .map(ddt -> new DelegationToken(new TokenInformation(
                ddt.tokenId(),
                new KafkaPrincipal(ddt.principalType(), ddt.principalName()),
                new KafkaPrincipal(ddt.tokenRequesterPrincipalType(), ddt.tokenRequesterPrincipalName()),
                ddt.renewers()
                    .stream()
                    .map(ddtr -> new KafkaPrincipal(ddtr.principalType(), ddtr.principalName()))
                    .collect(Collectors.toList()), ddt.issueTimestamp(), ddt.maxTimestamp(), ddt.expiryTimestamp()),
                ddt.hmac()))
            .collect(Collectors.toList());
    }

    public boolean hasError() {
        return error() != Errors.NONE;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
