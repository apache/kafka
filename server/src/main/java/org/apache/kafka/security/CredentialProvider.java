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

import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;

import java.util.Collection;
import java.util.Properties;

public class CredentialProvider {
    public final DelegationTokenCache tokenCache;
    public final CredentialCache credentialCache = new CredentialCache();

    public CredentialProvider(Collection<String> scramMechanisms, DelegationTokenCache tokenCache) {
        this.tokenCache = tokenCache;
        ScramCredentialUtils.createCache(credentialCache, scramMechanisms);
    }

    public void updateCredentials(String username, Properties config) {
        for (ScramMechanism mechanism : ScramMechanism.values()) {
            CredentialCache.Cache<ScramCredential> cache = credentialCache.cache(mechanism.mechanismName(), ScramCredential.class);
            if (cache != null) {
                String c = config.getProperty(mechanism.mechanismName());
                if (c == null) {
                    cache.remove(username);
                } else {
                    cache.put(username, ScramCredentialUtils.credentialFromString(c));
                }
            }
        }
    }

    public void updateCredential(
        org.apache.kafka.clients.admin.ScramMechanism mechanism,
        String name,
        ScramCredential credential
    ) {
        CredentialCache.Cache<ScramCredential> cache = credentialCache.cache(mechanism.mechanismName(), ScramCredential.class);
        cache.put(name, credential);
    }

    public void removeCredentials(
        org.apache.kafka.clients.admin.ScramMechanism mechanism,
        String name
    ) {
        CredentialCache.Cache<ScramCredential> cache = credentialCache.cache(mechanism.mechanismName(), ScramCredential.class);
        if (cache != null) {
            cache.remove(name);
        }
    }
}
