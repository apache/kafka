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

import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CredentialProviderTest {
    final String sha256 = ScramMechanism.SCRAM_SHA_256.mechanismName();
    final String sha512 = ScramMechanism.SCRAM_SHA_512.mechanismName();
    
    @Test
    void testConstructorKeepsSameCache() {
        DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(sha256, sha512));
        CredentialProvider credentialProvider = new CredentialProvider(List.of(sha256, sha512), delegationTokenCache);
        assertSame(delegationTokenCache, credentialProvider.tokenCache);
    }
    
    @Test
    void testEmptyCacheAtStartForConfiguredMechanism() {
        // Configure sha256
        DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(sha256));
        CredentialProvider credentialProvider = new CredentialProvider(List.of(sha256), delegationTokenCache);
        assertNotNull(credentialProvider.credentialCache.cache(sha256, ScramCredential.class));
        assertNull(credentialProvider.credentialCache.cache(sha512, ScramCredential.class));
        assertNull(credentialProvider.credentialCache.cache(sha256, ScramCredential.class).get("Name1"));
        
        // Configure sha512
        DelegationTokenCache delegationTokenCache1 = new DelegationTokenCache(List.of(sha512));
        CredentialProvider credentialProvider1 = new CredentialProvider(List.of(sha512), delegationTokenCache1);
        assertNotNull(credentialProvider1.credentialCache.cache(sha512, ScramCredential.class));
        assertNull(credentialProvider1.credentialCache.cache(sha256, ScramCredential.class));
        assertNull(credentialProvider1.credentialCache.cache(sha512, ScramCredential.class).get("Name1"));
        
        assertNotSame(credentialProvider, credentialProvider1);
    }
    
    @Test
    void testAddUser() {
        DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(sha256));
        CredentialProvider credentialProvider = new CredentialProvider(List.of(sha256), delegationTokenCache);
        ScramCredential scramCredential = new ScramCredential(
                new byte[] {1},
                new byte[] {2},
                new byte[] {3},
                4096
        );
        credentialProvider.updateCredential(ScramMechanism.SCRAM_SHA_256, "Name1", scramCredential);
        assertSame(credentialProvider.credentialCache.cache(sha256, ScramCredential.class).get("Name1"), scramCredential);
    }
    
    @Test
    void testExistingUser() {
        DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(sha256));
        CredentialProvider credentialProvider = new CredentialProvider(List.of(sha256), delegationTokenCache);
        ScramCredential scramCredentialOld = new ScramCredential(
                new byte[] {1},
                new byte[] {2},
                new byte[] {3},
                4096
        );
        credentialProvider.updateCredential(ScramMechanism.SCRAM_SHA_256, "Name1", scramCredentialOld);
        
        ScramCredential scramCredentialNew = new ScramCredential(
                new byte[] {1},
                new byte[] {2},
                new byte[] {3},
                4096
        );
        credentialProvider.updateCredential(ScramMechanism.SCRAM_SHA_256, "Name1", scramCredentialNew);
        assertSame(credentialProvider.credentialCache.cache(sha256, ScramCredential.class).get("Name1"), scramCredentialNew);
    }
    
    @Test
    void testUpdateKeepsUserIndependent() {
        DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(sha256, sha512));
        CredentialProvider credentialProvider = new CredentialProvider(List.of(sha256), delegationTokenCache);
        ScramCredential scramCredential = new ScramCredential(
                new byte[] {1},
                new byte[] {2},
                new byte[] {3},
                4096
        );
        credentialProvider.updateCredential(ScramMechanism.SCRAM_SHA_256, "Name1", scramCredential);
        credentialProvider.updateCredential(ScramMechanism.SCRAM_SHA_256, "Name12", scramCredential);
        assertSame(credentialProvider.credentialCache.cache(sha256, ScramCredential.class).get("Name1"), scramCredential);
        assertSame(credentialProvider.credentialCache.cache(sha256, ScramCredential.class).get("Name12"), scramCredential);
    }
    
    @Test
    void testUpdateKeepsMechanismIsolated() {
        DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(sha256));
        CredentialProvider credentialProvider = new CredentialProvider(List.of(sha256, sha512), delegationTokenCache);
        ScramCredential scramCredential = new ScramCredential(
                new byte[] {1},
                new byte[] {2},
                new byte[] {3},
                4096
        );
        credentialProvider.updateCredential(ScramMechanism.SCRAM_SHA_256, "Name1", scramCredential);
        assertSame(credentialProvider.credentialCache.cache(sha256, ScramCredential.class).get("Name1"), scramCredential);
        assertNull(credentialProvider.credentialCache.cache(sha512, ScramCredential.class).get("Name1"));
    }
    
    @Test
    void testNullPointerWhenNotConfiguredMechanism() {
        ScramCredential scramCredential = new ScramCredential(
                new byte[] {1},
                new byte[] {2},
                new byte[] {3},
                4096
        );
        DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(sha256));
        CredentialProvider credentialProvider = new CredentialProvider(List.of(sha256), delegationTokenCache);
        assertThrows(NullPointerException.class, () -> credentialProvider.updateCredential(ScramMechanism.SCRAM_SHA_512, "Name1", scramCredential));
    }
    
    @Test
    void testRemoveCredentials() {
        DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(sha256));
        CredentialProvider credentialProvider = new CredentialProvider(List.of(sha256), delegationTokenCache);
        ScramCredential scramCredential = new ScramCredential(
                new byte[] {1},
                new byte[] {2},
                new byte[] {3},
                4096
        );
        credentialProvider.updateCredential(ScramMechanism.SCRAM_SHA_256, "Name1", scramCredential);
        credentialProvider.removeCredentials(ScramMechanism.SCRAM_SHA_256, "Name1");
        assertNull(credentialProvider.credentialCache.cache(sha256, ScramCredential.class).get("Name1"));
    }
    
    @Test
    void testRemoveUserOnlyRemovesGivenUser() {
        DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(sha256));
        CredentialProvider credentialProvider = new CredentialProvider(List.of(sha256), delegationTokenCache);
        ScramCredential scramCredential = new ScramCredential(new byte[] {1}, new byte[] {2}, new byte[] {3}, 4096);
        ScramCredential scramCredential2 = new ScramCredential(new byte[] {1}, new byte[] {2}, new byte[] {3}, 4096);
        credentialProvider.updateCredential(ScramMechanism.SCRAM_SHA_256, "Name1", scramCredential);
        credentialProvider.updateCredential(ScramMechanism.SCRAM_SHA_256, "NameA", scramCredential2);
        credentialProvider.removeCredentials(ScramMechanism.SCRAM_SHA_256, "Name1");
        assertNull(credentialProvider.credentialCache.cache(sha256, ScramCredential.class).get("Name1"));
        assertNotNull(credentialProvider.credentialCache.cache(sha256, ScramCredential.class).get("NameA"));
    }
    
    @Test
    void testRemoveUnknownUserIsNoOp() {
        DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(sha256));
        CredentialProvider credentialProvider = new CredentialProvider(List.of(sha256), delegationTokenCache);
        
        credentialProvider.removeCredentials(ScramMechanism.SCRAM_SHA_256, "Name1");
        credentialProvider.removeCredentials(ScramMechanism.SCRAM_SHA_512, "Name1");
        assertNull(credentialProvider.credentialCache.cache(sha256, ScramCredential.class).get("Name1"));
        assertNull(credentialProvider.credentialCache.cache(sha512, ScramCredential.class));
    }
}
