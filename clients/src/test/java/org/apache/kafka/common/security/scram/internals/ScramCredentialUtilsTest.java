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
package org.apache.kafka.common.security.scram.internals;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.scram.ScramCredential;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ScramCredentialUtilsTest {

    private ScramFormatter formatter;

    @BeforeEach
    public void setUp() throws NoSuchAlgorithmException {
        formatter = new ScramFormatter(ScramMechanism.SCRAM_SHA_256);
    }

    @Test
    public void stringConversion() {
        ScramCredential credential = formatter.generateCredential("password", 1024);
        assertTrue(credential.salt().length > 0, "Salt must not be empty");
        assertTrue(credential.storedKey().length > 0, "Stored key must not be empty");
        assertTrue(credential.serverKey().length > 0, "Server key must not be empty");
        ScramCredential credential2 = ScramCredentialUtils.credentialFromString(ScramCredentialUtils.credentialToString(credential));
        assertArrayEquals(credential.salt(), credential2.salt());
        assertArrayEquals(credential.storedKey(), credential2.storedKey());
        assertArrayEquals(credential.serverKey(), credential2.serverKey());
        assertEquals(credential.iterations(), credential2.iterations());
    }

    @Test
    public void generateCredential() {
        ScramCredential credential1 = formatter.generateCredential("password", 4096);
        ScramCredential credential2 = formatter.generateCredential("password", 4096);
        // Random salt should ensure that the credentials persisted are different every time
        assertNotEquals(ScramCredentialUtils.credentialToString(credential1), ScramCredentialUtils.credentialToString(credential2));
    }

    @Test
    public void invalidCredential() {
        assertThrows(IllegalArgumentException.class, () -> ScramCredentialUtils.credentialFromString("abc"));
    }

    @Test
    public void missingFields() {
        String cred = ScramCredentialUtils.credentialToString(formatter.generateCredential("password", 2048));
        assertThrows(IllegalArgumentException.class, () -> ScramCredentialUtils.credentialFromString(cred.substring(cred.indexOf(','))));
    }

    @Test
    public void extraneousFields() {
        String cred = ScramCredentialUtils.credentialToString(formatter.generateCredential("password", 2048));
        assertThrows(IllegalArgumentException.class, () -> ScramCredentialUtils.credentialFromString(cred + ",a=test"));
    }

    @Test
    public void scramCredentialCache() throws Exception {
        CredentialCache cache = new CredentialCache();
        ScramCredentialUtils.createCache(cache, Arrays.asList("SCRAM-SHA-512", "PLAIN"));
        assertNotNull(cache.cache(ScramMechanism.SCRAM_SHA_512.mechanismName(), ScramCredential.class), "Cache not created for enabled mechanism");
        assertNull(cache.cache(ScramMechanism.SCRAM_SHA_256.mechanismName(), ScramCredential.class), "Cache created for disabled mechanism");

        CredentialCache.Cache<ScramCredential> sha512Cache = cache.cache(ScramMechanism.SCRAM_SHA_512.mechanismName(), ScramCredential.class);
        ScramFormatter formatter = new ScramFormatter(ScramMechanism.SCRAM_SHA_512);
        ScramCredential credentialA = formatter.generateCredential("password", 4096);
        sha512Cache.put("userA", credentialA);
        assertEquals(credentialA, sha512Cache.get("userA"));
        assertNull(sha512Cache.get("userB"), "Invalid user credential");
    }
}
