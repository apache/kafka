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

import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.TokenInformation;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class DelegationTokenCacheTest {

    private static final KafkaPrincipal OWNER = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "owner");

    private static DelegationToken delegationToken(String tokenId, long expiryTimestamp) {
        TokenInformation tokenInfo = new TokenInformation(tokenId, OWNER, List.of(OWNER), 0L, Long.MAX_VALUE, expiryTimestamp);
        return new DelegationToken(tokenInfo, ("hmac-" + tokenId).getBytes(StandardCharsets.UTF_8));
    }

    private static Map<String, ScramCredential> scramCredentials(DelegationToken token) throws NoSuchAlgorithmException {
        Map<String, ScramCredential> credentials = new HashMap<>();
        for (ScramMechanism mechanism : ScramMechanism.values()) {
            ScramFormatter formatter = new ScramFormatter(mechanism);
            credentials.put(mechanism.mechanismName(), formatter.generateCredential(token.hmacAsBase64String(), mechanism.minIterations()));
        }
        return credentials;
    }

    @Test
    public void testUpdateCache() throws Exception {
        DelegationTokenCache cache = new DelegationTokenCache(ScramMechanism.mechanismNames());
        DelegationToken token = delegationToken("token1", 1000L);
        Map<String, ScramCredential> credentials = scramCredentials(token);

        cache.updateCache(token, credentials);

        assertEquals(token.tokenInfo(), cache.token("token1"));
        assertEquals(OWNER.getName(), cache.owner("token1"));
        assertEquals(List.of(token.tokenInfo()), List.copyOf(cache.tokens()));
        assertEquals(token.tokenInfo(), cache.tokenForHmac(token.hmacAsBase64String()));
        for (String mechanism : ScramMechanism.mechanismNames()) {
            assertEquals(credentials.get(mechanism), cache.credential(mechanism, "token1"));
        }
    }

    @Test
    public void testUpdateCacheWithRenewedToken() throws Exception {
        DelegationTokenCache cache = new DelegationTokenCache(ScramMechanism.mechanismNames());
        DelegationToken token = delegationToken("token1", 1000L);
        cache.updateCache(token, scramCredentials(token));

        DelegationToken renewed = delegationToken("token1", 2000L);
        cache.updateCache(renewed, scramCredentials(renewed));

        assertEquals(1, cache.tokens().size());
        assertEquals(2000L, cache.token("token1").expiryTimestamp());
        assertEquals(2000L, cache.tokenForHmac(renewed.hmacAsBase64String()).expiryTimestamp());

        cache.removeCache("token1");
        assertNull(cache.token("token1"));
        assertNull(cache.tokenForHmac(renewed.hmacAsBase64String()));
    }

    @Test
    public void testRemoveCache() throws Exception {
        DelegationTokenCache cache = new DelegationTokenCache(ScramMechanism.mechanismNames());
        DelegationToken token1 = delegationToken("token1", 1000L);
        DelegationToken token2 = delegationToken("token2", 1000L);
        cache.updateCache(token1, scramCredentials(token1));
        cache.updateCache(token2, scramCredentials(token2));

        cache.removeCache("token1");
        cache.removeCache("unknown");

        assertNull(cache.token("token1"));
        assertNull(cache.owner("token1"));
        assertNull(cache.tokenForHmac(token1.hmacAsBase64String()));
        for (String mechanism : ScramMechanism.mechanismNames()) {
            assertNull(cache.credential(mechanism, "token1"));
        }

        assertEquals(List.of(token2.tokenInfo()), List.copyOf(cache.tokens()));
        assertEquals(token2.tokenInfo(), cache.tokenForHmac(token2.hmacAsBase64String()));
        for (String mechanism : ScramMechanism.mechanismNames()) {
            assertNotNull(cache.credential(mechanism, "token2"));
        }
    }

    @Test
    public void testAddTokenDropsReplacedHmacMapping() {
        DelegationTokenCache cache = new DelegationTokenCache(ScramMechanism.mechanismNames());
        DelegationToken token = delegationToken("token1", 1000L);
        DelegationToken reissued = new DelegationToken(token.tokenInfo(), "another-hmac".getBytes(StandardCharsets.UTF_8));
        cache.addToken(token);
        cache.addToken(reissued);

        assertNull(cache.tokenForHmac(token.hmacAsBase64String()));
        assertEquals(token.tokenInfo(), cache.tokenForHmac(reissued.hmacAsBase64String()));
    }

    @Test
    public void testRemoveTokenDropsHmacMapping() {
        DelegationTokenCache cache = new DelegationTokenCache(ScramMechanism.mechanismNames());
        DelegationToken token = delegationToken("token1", 1000L);
        cache.addToken(token);
        cache.removeToken("token1");

        // Add the same token id back under another hmac. A stale entry for the old hmac would now resolve to it.
        DelegationToken reissued = new DelegationToken(token.tokenInfo(), "another-hmac".getBytes(StandardCharsets.UTF_8));
        cache.addToken(reissued);

        assertNull(cache.tokenForHmac(token.hmacAsBase64String()));
        assertEquals(token.tokenInfo(), cache.tokenForHmac(reissued.hmacAsBase64String()));
    }

    @Test
    public void testCredentialsOnlyCachedForEnabledMechanisms() throws Exception {
        String enabled = ScramMechanism.SCRAM_SHA_256.mechanismName();
        String disabled = ScramMechanism.SCRAM_SHA_512.mechanismName();
        DelegationTokenCache cache = new DelegationTokenCache(List.of(enabled));
        DelegationToken token = delegationToken("token1", 1000L);
        Map<String, ScramCredential> credentials = scramCredentials(token);

        cache.updateCache(token, credentials);

        assertEquals(credentials.get(enabled), cache.credential(enabled, "token1"));
        assertNotNull(cache.credentialCache(enabled));
        assertNull(cache.credential(disabled, "token1"));
        assertNull(cache.credentialCache(disabled));
    }
}
