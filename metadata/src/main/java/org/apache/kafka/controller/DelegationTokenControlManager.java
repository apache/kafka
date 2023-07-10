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

package org.apache.kafka.controller;

import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData.CreatableRenewers;
import org.apache.kafka.common.message.CreateDelegationTokenResponseData;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.ExpireDelegationTokenResponseData;
import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.message.RenewDelegationTokenResponseData;
import org.apache.kafka.common.metadata.DelegationTokenRecord;
import org.apache.kafka.common.metadata.RemoveDelegationTokenRecord;
// import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
// XXX import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.DelegationTokenData;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
// import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.common.utils.Time;

import java.security.NoSuchAlgorithmException;
import java.security.InvalidKeyException;
import java.nio.charset.StandardCharsets;
import javax.crypto.spec.SecretKeySpec;
// import javax.crypto.SecretKey;
import javax.crypto.Mac;

import org.slf4j.Logger;

import java.util.ArrayList;
// import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static org.apache.kafka.common.protocol.Errors.DELEGATION_TOKEN_AUTH_DISABLED;
import static org.apache.kafka.common.protocol.Errors.DELEGATION_TOKEN_EXPIRED;
import static org.apache.kafka.common.protocol.Errors.DELEGATION_TOKEN_NOT_FOUND;
import static org.apache.kafka.common.protocol.Errors.INVALID_PRINCIPAL_TYPE;
import static org.apache.kafka.common.protocol.Errors.NONE;
import static org.apache.kafka.common.protocol.Errors.UNSUPPORTED_VERSION;


/**
 * Manages DelegationTokens.
 */
public class DelegationTokenControlManager {
    // XXX private static final long defaultTokenMaxTime = config.delegationTokenMaxLifeMs;
    // XXX private static final long defaultTokenRenewTime = config.delegationTokenExpiryTimeMs;

    private static final long DEFAULTTOKENMAXTIME = 0;
    private static final long DEFAULTTOKENRENEWTIME = 0;
    private Time time = Time.SYSTEM;

    static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        private DelegationTokenCache tokenCache = null;
        private String secretKeyString = null;

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder setTokenCache(DelegationTokenCache tokenCache) {
            this.tokenCache = tokenCache;
            return this;
        }

        Builder setTokenKeyString(String secretKeyString) {
            this.secretKeyString = secretKeyString;
            return this;
        }

        DelegationTokenControlManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            return new DelegationTokenControlManager(
              logContext,
              snapshotRegistry,
              tokenCache,
              secretKeyString);
        }
    }

    private final Logger log;
    private final DelegationTokenCache tokenCache;
    private final String secretKeyString;
    // private final Mac mac;
    // private final TimelineHashMap<ScramCredentialKey, ScramCredentialValue> credentials;

    private DelegationTokenControlManager(
        LogContext logContext,
        SnapshotRegistry snapshotRegistry,
        DelegationTokenCache tokenCache,
        String secretKeyString
    ) {
        this.log = logContext.logger(DelegationTokenControlManager.class);
        this.tokenCache = tokenCache;
        this.secretKeyString = secretKeyString;

        // this.credentials = new TimelineHashMap<>(snapshotRegistry, 0);
//        try {
//            this.mac = Mac.getInstance("HmacSHA512");
//            this.secretKey = new SecretKeySpec(toBytes("testKey"), mac.getAlgorithm());
//        } catch (NoSuchAlgorithmException e) {
//            System.out.println("Caught an exception");
//            this.secretKey = null;
//        }
    }

    public static byte[] toBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] createHmac(String tokenId) {
        byte[] result = {};

        try {
            Mac mac = Mac.getInstance("HmacSHA512");
            SecretKeySpec secretKey = new SecretKeySpec(toBytes(secretKeyString), mac.getAlgorithm());
            mac.init(secretKey);
            result = mac.doFinal(toBytes(tokenId));
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            System.out.println("Caught an exception 2");
        }
        return result;
    }

    private TokenInformation getToken(byte[] hmac) {
        String base64Pwd = Base64.getEncoder().encodeToString(hmac);
        System.out.println("tokencache has token count : " + tokenCache.tokens().size());
        return tokenCache.tokenForHmac(base64Pwd);
    }

    /*
     * Pass in the MetadataVersion so that we can return a response to the caller 
     * if the current metadataVersion is too low.
     */
    public ControllerResult<CreateDelegationTokenResponseData> createDelegationToken(
        ControllerRequestContext context,
        CreateDelegationTokenRequestData requestData,
        MetadataVersion metadataVersion
    ) {
        long now = time.milliseconds();
        // XXX Handle default vs requested
        long maxTimestamp = now + DEFAULTTOKENMAXTIME + 10000000;
        long expiryTimestamp = Math.min(maxTimestamp, now + DEFAULTTOKENRENEWTIME + 1000000);

        String tokenId = Uuid.randomUuid().toString();
        KafkaPrincipal owner = context.principal();
        List<KafkaPrincipal> renewers = new ArrayList<KafkaPrincipal>();

        List<ApiMessageAndVersion> records = new ArrayList<>();
        CreateDelegationTokenResponseData responseData = new CreateDelegationTokenResponseData()
                .setPrincipalName(owner.getName())
                .setPrincipalType(owner.getPrincipalType())
                .setTokenRequesterPrincipalName(owner.getName())
                .setTokenRequesterPrincipalType(owner.getPrincipalType());

        if (secretKeyString == null) {
            // DelegationTokens are not enabled
            return ControllerResult.atomicOf(records, responseData.setErrorCode(DELEGATION_TOKEN_AUTH_DISABLED.code()));
        }

        if (metadataVersion.isDelegationTokenSupported()) {
            // DelegationTokens are not supported in this metadata version
            return ControllerResult.atomicOf(records, responseData.setErrorCode(UNSUPPORTED_VERSION.code()));
        }


        for (CreatableRenewers renewer : requestData.renewers()) {
            if (renewer.principalType().equals(KafkaPrincipal.USER_TYPE)) {
                renewers.add(new KafkaPrincipal(renewer.principalType(), renewer.principalName()));
            } else {
                return ControllerResult.atomicOf(records, responseData.setErrorCode(INVALID_PRINCIPAL_TYPE.code()));
            }
        }

        TokenInformation newTokenInformation = new TokenInformation(tokenId, owner, owner, renewers,
            now, maxTimestamp, expiryTimestamp);

        DelegationTokenData newDelegationTokenData = new DelegationTokenData(newTokenInformation);

        responseData
                .setErrorCode(NONE.code())
                .setIssueTimestampMs(now)
                .setExpiryTimestampMs(expiryTimestamp)
                .setMaxTimestampMs(maxTimestamp)
                .setTokenId(tokenId)
                .setHmac(createHmac(tokenId));

        System.out.println("context owner is :" + context.principal().toString());

        records.add(new ApiMessageAndVersion(newDelegationTokenData.toRecord(), (short) 0));
        return ControllerResult.atomicOf(records, responseData);
    }

    public ControllerResult<RenewDelegationTokenResponseData> renewDelegationToken(
        ControllerRequestContext context,
        RenewDelegationTokenRequestData requestData,
        MetadataVersion metadataVersion
    ) {
        long now = time.milliseconds();

        List<ApiMessageAndVersion> records = new ArrayList<>();
        RenewDelegationTokenResponseData responseData = new RenewDelegationTokenResponseData();

//        if (!allowedToRenew(principal, tokenInfo)
//            renewCallback(Errors.DELEGATION_TOKEN_OWNER_MISMATCH, -1)
//            } else if (tokenInfo.maxTimestamp < now || tokenInfo.expiryTimestamp < now) {
//              renewCallback(Errors.DELEGATION_TOKEN_EXPIRED, -1)
//
//              renewCallback(Errors.DELEGATION_TOKEN_NOT_FOUND, -1)

//        byte[] hmac = requestData.hmac();

        TokenInformation myTokenInformation = getToken(requestData.hmac());

        if (myTokenInformation == null) {
            return ControllerResult.atomicOf(records, responseData.setErrorCode(DELEGATION_TOKEN_NOT_FOUND.code()));
        }

        long renewTimeStamp = now + 1000000;
        long expiryTimestamp = Math.min(myTokenInformation.maxTimestamp(), renewTimeStamp);

        myTokenInformation.setExpiryTimestamp(expiryTimestamp);

        DelegationTokenData newDelegationTokenData = new DelegationTokenData(myTokenInformation);

        responseData
            .setErrorCode(NONE.code())
            .setExpiryTimestampMs(expiryTimestamp);

        records.add(new ApiMessageAndVersion(newDelegationTokenData.toRecord(), (short) 0));
        return ControllerResult.atomicOf(records, responseData);
    }

    public ControllerResult<ExpireDelegationTokenResponseData> expireDelegationToken(
        ControllerRequestContext context,
        ExpireDelegationTokenRequestData requestData,
        MetadataVersion metadataVersion
    ) {
        long now = time.milliseconds();

        List<ApiMessageAndVersion> records = new ArrayList<>();
        ExpireDelegationTokenResponseData responseData = new ExpireDelegationTokenResponseData();

        if (secretKeyString == null) {
            // DelegationTokens are not enabled
            return ControllerResult.atomicOf(records, responseData.setErrorCode(DELEGATION_TOKEN_AUTH_DISABLED.code()));
        }

        TokenInformation myTokenInformation = getToken(requestData.hmac());

        if (myTokenInformation == null) {
            return ControllerResult.atomicOf(records, responseData.setErrorCode(DELEGATION_TOKEN_NOT_FOUND.code()));
        }

        // XXX if allowedToRenew

        if (myTokenInformation.maxTimestamp() < now || myTokenInformation.expiryTimestamp() < now) {
            return ControllerResult.atomicOf(records, responseData.setErrorCode(DELEGATION_TOKEN_EXPIRED.code()));
        }

        if (requestData.expiryTimePeriodMs() < 0) { // expire immediately
            responseData
                .setErrorCode(NONE.code())
                .setExpiryTimestampMs(requestData.expiryTimePeriodMs());
            records.add(new ApiMessageAndVersion(new RemoveDelegationTokenRecord().
                setTokenId(myTokenInformation.tokenId()), (short) 0));
        } else {
            long expiryTimestamp = Math.min(myTokenInformation.maxTimestamp(),
                now + requestData.expiryTimePeriodMs());

            responseData
                .setErrorCode(NONE.code())
                .setExpiryTimestampMs(expiryTimestamp);

            myTokenInformation.setExpiryTimestamp(expiryTimestamp);

            DelegationTokenData newDelegationTokenData = new DelegationTokenData(myTokenInformation);
            records.add(new ApiMessageAndVersion(newDelegationTokenData.toRecord(), (short) 0));
        }

        return ControllerResult.atomicOf(records, responseData);
    }

    public void replay(DelegationTokenRecord record) {
        // XXX Do nothing right now
    }

    public void replay(RemoveDelegationTokenRecord record) {
        // XXX Do nothing right now
    }
}
