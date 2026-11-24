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
package org.apache.kafka.common.security.token.delegation;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TokenInformationTest {

    private static final KafkaPrincipal OWNER = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "owner");
    private static final long ISSUE_TIMESTAMP = 1000L;
    private static final long MAX_TIMESTAMP = 9000L;

    private static TokenInformation tokenWithExpiry(long expiryTimestamp) {
        return new TokenInformation("tokenId", OWNER, OWNER, List.of(OWNER),
            ISSUE_TIMESTAMP, MAX_TIMESTAMP, expiryTimestamp);
    }

    @Test
    public void testEqualsAndHashCode() {
        TokenInformation tokenInformation = tokenWithExpiry(5000L);
        TokenInformation same = tokenWithExpiry(5000L);

        assertEquals(tokenInformation, same);
        assertEquals(tokenInformation.hashCode(), same.hashCode());
    }

    @Test
    public void testHashCodeIgnoresExpiryTimestampLikeEquals() {
        // equals() deliberately leaves expiryTimestamp out: renewing a token does not
        // make it a different token. hashCode() has to leave it out for the same reason,
        // otherwise two instances can be equal and still hash differently.
        TokenInformation tokenInformation = tokenWithExpiry(5000L);
        TokenInformation renewed = tokenWithExpiry(6000L);

        assertEquals(tokenInformation, renewed);
        assertEquals(tokenInformation.hashCode(), renewed.hashCode());
    }

    @Test
    public void testEqualInstancesCollapseInHashSet() {
        TokenInformation tokenInformation = tokenWithExpiry(5000L);
        TokenInformation renewed = tokenWithExpiry(6000L);

        Set<TokenInformation> tokens = new HashSet<>();
        tokens.add(tokenInformation);

        assertTrue(tokens.contains(renewed));
        tokens.add(renewed);
        assertEquals(1, tokens.size());
    }

    @Test
    public void testHashCodeIsStableAcrossRenewal() {
        // setExpiryTimestamp is what renewal calls, so it can run on an instance that
        // is already an element of a hash-based collection.
        TokenInformation tokenInformation = tokenWithExpiry(5000L);
        Set<TokenInformation> tokens = new HashSet<>();
        tokens.add(tokenInformation);

        int hashCodeBeforeRenewal = tokenInformation.hashCode();
        tokenInformation.setExpiryTimestamp(7000L);

        assertEquals(hashCodeBeforeRenewal, tokenInformation.hashCode());
        assertTrue(tokens.contains(tokenInformation));
    }
}
