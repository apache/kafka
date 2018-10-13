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

package org.apache.kafka.common.security.token.delegation.internals;

import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.TokenInformation;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DelegationTokenCache {

    private CredentialCache credentialCache = new CredentialCache();
    //Cache to hold all the tokens
    private Map<String, TokenInformation> tokenCache = new ConcurrentHashMap<>();
    //Cache to hold hmac->tokenId mapping. This is required for renew, expire requests
    private Map<String, String> hmacIDCache = new ConcurrentHashMap<>();

    public DelegationTokenCache(Collection<String> scramMechanisms) {
        //Create caches for scramMechanisms
        ScramCredentialUtils.createCache(credentialCache, scramMechanisms);
    }

    public ScramCredential credential(String mechanism, String tokenId) {
        CredentialCache.Cache<ScramCredential> cache = credentialCache.cache(mechanism, ScramCredential.class);
        return cache == null ? null : cache.get(tokenId);
    }

    public String owner(String tokenId) {
        TokenInformation tokenInfo = tokenCache.get(tokenId);
        return tokenInfo == null ? null : tokenInfo.owner().getName();
    }

    public void updateCache(DelegationToken token, Map<String, ScramCredential> scramCredentialMap) {
        //Update TokenCache
        String tokenId =  token.tokenInfo().tokenId();
        addToken(tokenId, token.tokenInfo());
        String hmac = token.hmacAsBase64String();
        //Update Scram Credentials
        updateCredentials(tokenId, scramCredentialMap);
        //Update hmac-id cache
        hmacIDCache.put(hmac, tokenId);
    }


    public void removeCache(String tokenId) {
        removeToken(tokenId);
        updateCredentials(tokenId, new HashMap<String, ScramCredential>());
    }

    public TokenInformation tokenForHmac(String base64hmac) {
        String tokenId = hmacIDCache.get(base64hmac);
        return tokenId == null ? null : tokenCache.get(tokenId);
    }

    public TokenInformation addToken(String tokenId, TokenInformation tokenInfo) {
        return tokenCache.put(tokenId, tokenInfo);
    }

    public void removeToken(String tokenId) {
        TokenInformation tokenInfo = tokenCache.remove(tokenId);
        if (tokenInfo != null) {
            hmacIDCache.remove(tokenInfo.tokenId());
        }
    }

    public Collection<TokenInformation> tokens() {
        return tokenCache.values();
    }

    public TokenInformation token(String tokenId) {
        return tokenCache.get(tokenId);
    }

    public CredentialCache.Cache<ScramCredential> credentialCache(String mechanism) {
        return credentialCache.cache(mechanism, ScramCredential.class);
    }

    private void updateCredentials(String tokenId, Map<String, ScramCredential> scramCredentialMap) {
        for (String mechanism : ScramMechanism.mechanismNames()) {
            CredentialCache.Cache<ScramCredential> cache = credentialCache.cache(mechanism, ScramCredential.class);
            if (cache != null) {
                ScramCredential credential = scramCredentialMap.get(mechanism);
                if (credential == null) {
                    cache.remove(tokenId);
                } else {
                    cache.put(tokenId, credential);
                }
            }
        }
    }
}