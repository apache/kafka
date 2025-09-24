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
package org.apache.kafka.security;

import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.server.config.DelegationTokenManagerConfigs;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

public class DelegationTokenManager {
    private static final String DEFAULT_HMAC_ALGORITHM = "HmacSHA512";
    public static final long ERROR_TIMESTAMP = -1;

    private final DelegationTokenCache tokenCache;
    private final SecretKey secretKey;

    public DelegationTokenManager(DelegationTokenManagerConfigs config, DelegationTokenCache tokenCache) {
        this.tokenCache = tokenCache;

        byte[] keyBytes = config.tokenAuthEnabled() ? config.delegationTokenSecretKey().value().getBytes(StandardCharsets.UTF_8) : null;
        if (keyBytes == null || keyBytes.length == 0) {
            this.secretKey = null;
        } else {
            this.secretKey = createSecretKey(keyBytes);
        }
    }

    private static SecretKey createSecretKey(byte[] keyBytes) {
        return new SecretKeySpec(keyBytes, DEFAULT_HMAC_ALGORITHM);
    }

    public static byte[] createHmac(String tokenId, SecretKey secretKey) {
        try {
            Mac mac = Mac.getInstance(DEFAULT_HMAC_ALGORITHM);
            mac.init(secretKey);
            return mac.doFinal(tokenId.getBytes(StandardCharsets.UTF_8));
        } catch (InvalidKeyException e) {
            throw new IllegalArgumentException("Invalid key to HMAC computation", e);
        } catch (Exception e) {
            throw new RuntimeException("Error while creating HMAC", e);
        }
    }

    private Map<String, ScramCredential> prepareScramCredentials(String hmacString) throws NoSuchAlgorithmException {
        Map<String, ScramCredential> scramCredentialMap = new HashMap<>();
        for (ScramMechanism mechanism : ScramMechanism.values()) {
            ScramFormatter formatter = new ScramFormatter(mechanism);
            scramCredentialMap.put(mechanism.mechanismName(), formatter.generateCredential(hmacString, mechanism.minIterations()));
        }
        return scramCredentialMap;
    }

    public void updateToken(DelegationToken token) throws NoSuchAlgorithmException {
        String hmacString = token.hmacAsBase64String();
        Map<String, ScramCredential> scramCredentialMap = prepareScramCredentials(hmacString);
        tokenCache.updateCache(token, scramCredentialMap);
    }

    public DelegationToken getDelegationToken(TokenInformation tokenInfo) {
        byte[] hmac = createHmac(tokenInfo.tokenId(), secretKey);
        return new DelegationToken(tokenInfo, hmac);
    }

    public void removeToken(String tokenId) {
        tokenCache.removeCache(tokenId);
    }

    public List<DelegationToken> getTokens(Predicate<TokenInformation> filterToken) {
        return tokenCache.tokens().stream()
            .filter(filterToken)
            .map(this::getDelegationToken)
            .toList();
    }

    public boolean isEnabled() {
        return secretKey != null;
    }

    public static boolean filterToken(
        KafkaPrincipal requesterPrincipal,
        Optional<List<KafkaPrincipal>> owners,
        TokenInformation token,
        Function<String, Boolean> authorizeToken,
        Function<KafkaPrincipal, Boolean> authorizeRequester
    ) {
        if (owners.isPresent() && owners.get().stream().noneMatch(token::ownerOrRenewer)) {
            //exclude tokens which are not requested
            return false;
        } else if (token.ownerOrRenewer(requesterPrincipal)) {
            //Owners and the renewers can describe their own tokens
            return true;
        } else {
            // Check permission for non-owned tokens
            return authorizeToken.apply(token.tokenId()) || authorizeRequester.apply(token.owner());
        }
    }
}
