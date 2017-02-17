/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.security.scram;

import java.io.IOException;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.auth.AuthCallbackHandler;
import org.apache.kafka.common.security.authenticator.CredentialCache;

public class ScramServerCallbackHandler implements AuthCallbackHandler {

    private final CredentialCache.Cache<ScramCredential> credentialCache;

    public ScramServerCallbackHandler(CredentialCache.Cache<ScramCredential> credentialCache) {
        this.credentialCache = credentialCache;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        String username = null;
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback)
                username = ((NameCallback) callback).getDefaultName();
            else if (callback instanceof ScramCredentialCallback)
                ((ScramCredentialCallback) callback).scramCredential(credentialCache.get(username));
            else
                throw new UnsupportedCallbackException(callback);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, Mode mode, Subject subject, String saslMechanism) {
    }

    @Override
    public void close() {
    }
}
