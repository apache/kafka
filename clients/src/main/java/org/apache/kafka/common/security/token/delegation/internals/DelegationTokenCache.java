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
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.TokenInformation;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DelegationTokenCache {

    private final CredentialCache credentialCache;

    //Cache to hold all the tokens
    private final Map<String, DelegationToken> tokenCache = new ConcurrentHashMap<>();

    //Cache to hold hmac->tokenId mapping. This is required for renew, expire requests
    private final Map<String, String> hmacTokenIdCache = new ConcurrentHashMap<>();

    public DelegationTokenCache(Collection<String> scramMechanisms) {
        //Create caches for scramMechanisms
        this.credentialCache = new CredentialCache(scramMechanisms);
    }

    public ScramCredential credential(String mechanism, String tokenId) {
        CredentialCache.Cache cache = credentialCache.cache(mechanism);
        return cache == null ? null : cache.get(tokenId);
    }

    public String owner(String tokenId) {
        DelegationToken token = tokenCache.get(tokenId);
        return token == null ? null : token.tokenInfo().owner().getName();
    }

    public void updateCache(DelegationToken token, Map<String, ScramCredential> scramCredentialMap) {
        //Update TokenCache and hmac-id cache
        addToken(token);
        //Update Scram Credentials
        updateCredentials(token.tokenInfo().tokenId(), scramCredentialMap);
    }

    public void removeCache(String tokenId) {
        removeToken(tokenId);
        updateCredentials(tokenId, new HashMap<>());
    }

    public TokenInformation tokenForHmac(String base64hmac) {
        String tokenId = hmacTokenIdCache.get(base64hmac);
        DelegationToken token = tokenId == null ? null : tokenCache.get(tokenId);
        return token == null ? null : token.tokenInfo();
    }

    public void addToken(DelegationToken token) {
        String tokenId = token.tokenInfo().tokenId();
        String hmac = token.hmacAsBase64String();
        DelegationToken previous = tokenCache.put(tokenId, token);
        hmacTokenIdCache.put(hmac, tokenId);
        if (previous != null) {
            String previousHmac = previous.hmacAsBase64String();
            //Drop the hmac of the replaced token, unless the new token has the same hmac
            if (!previousHmac.equals(hmac)) {
                hmacTokenIdCache.remove(previousHmac);
            }
        }
    }

    public void removeToken(String tokenId) {
        DelegationToken token = tokenCache.remove(tokenId);
        if (token != null) {
            hmacTokenIdCache.remove(token.hmacAsBase64String());
        }
    }

    public Collection<DelegationToken> tokens() {
        return tokenCache.values();
    }

    public TokenInformation token(String tokenId) {
        DelegationToken token = tokenCache.get(tokenId);
        return token == null ? null : token.tokenInfo();
    }

    public CredentialCache.Cache credentialCache(String mechanism) {
        return credentialCache.cache(mechanism);
    }

    private void updateCredentials(String tokenId, Map<String, ScramCredential> scramCredentialMap) {
        for (String mechanism : ScramMechanism.mechanismNames()) {
            CredentialCache.Cache cache = credentialCache.cache(mechanism);
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
