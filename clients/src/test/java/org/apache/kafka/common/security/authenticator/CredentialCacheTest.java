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
package org.apache.kafka.common.security.authenticator;

import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class CredentialCacheTest {

    @Test
    public void testScramCredentialCache() throws Exception {
        CredentialCache cache = new CredentialCache(List.of("SCRAM-SHA-512", "PLAIN"));
        assertNotNull(cache.cache(ScramMechanism.SCRAM_SHA_512.mechanismName()), "Cache not created for enabled mechanism");
        assertNull(cache.cache(ScramMechanism.SCRAM_SHA_256.mechanismName()), "Cache created for disabled mechanism");
        assertNull(cache.cache("PLAIN"), "Cache created for non-SCRAM mechanism");

        CredentialCache.Cache sha512Cache = cache.cache(ScramMechanism.SCRAM_SHA_512.mechanismName());
        ScramFormatter formatter = new ScramFormatter(ScramMechanism.SCRAM_SHA_512);
        ScramCredential credentialA = formatter.generateCredential("password", 4096);
        sha512Cache.put("userA", credentialA);
        assertEquals(credentialA, sha512Cache.get("userA"));
        assertNull(sha512Cache.get("userB"), "Invalid user credential");

        assertEquals(credentialA, sha512Cache.remove("userA"));
        assertNull(sha512Cache.get("userA"));
    }
}
