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

import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.scram.ScramCredential;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;


public class ScramCredentialUtilsTest {

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
