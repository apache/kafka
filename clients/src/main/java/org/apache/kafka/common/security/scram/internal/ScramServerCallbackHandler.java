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
package org.apache.kafka.common.security.scram.internal;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.ScramCredentialCallback;
import org.apache.kafka.common.security.token.delegation.internal.DelegationTokenCache;
import org.apache.kafka.common.security.token.delegation.internal.DelegationTokenCredentialCallback;

public class ScramServerCallbackHandler implements AuthenticateCallbackHandler {

    private final CredentialCache.Cache<ScramCredential> credentialCache;
    private final DelegationTokenCache tokenCache;
    private String saslMechanism;

    public ScramServerCallbackHandler(CredentialCache.Cache<ScramCredential> credentialCache,
                                      DelegationTokenCache tokenCache) {
        this.credentialCache = credentialCache;
        this.tokenCache = tokenCache;
    }

    @Override
    public void configure(Map<String, ?> configs, String mechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        this.saslMechanism = mechanism;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        String username = null;
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback)
                username = ((NameCallback) callback).getDefaultName();
            else if (callback instanceof DelegationTokenCredentialCallback) {
                DelegationTokenCredentialCallback tokenCallback = (DelegationTokenCredentialCallback) callback;
                tokenCallback.scramCredential(tokenCache.credential(saslMechanism, username));
                tokenCallback.tokenOwner(tokenCache.owner(username));
            } else if (callback instanceof ScramCredentialCallback) {
                ScramCredentialCallback sc = (ScramCredentialCallback) callback;
                sc.scramCredential(credentialCache.get(username));
            } else
                throw new UnsupportedCallbackException(callback);
        }
    }

    @Override
    public void close() {
    }
}
