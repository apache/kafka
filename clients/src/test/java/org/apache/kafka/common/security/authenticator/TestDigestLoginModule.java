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

import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.plain.PlainLoginModule;

/**
 * Digest-MD5 login module for multi-mechanism tests.
 * This login module uses the same format as PlainLoginModule and hence simply reuses the same methods.
 *
 */
public class TestDigestLoginModule extends PlainLoginModule {

    public static class DigestServerCallbackHandler implements AuthenticateCallbackHandler {

        @Override
        public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        }

        @Override
        public void handle(Callback[] callbacks) {
            String username = null;
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    NameCallback nameCallback = (NameCallback) callback;
                    if (TestJaasConfig.USERNAME.equals(nameCallback.getDefaultName())) {
                        nameCallback.setName(nameCallback.getDefaultName());
                        username = TestJaasConfig.USERNAME;
                    }
                } else if (callback instanceof PasswordCallback) {
                    PasswordCallback passwordCallback = (PasswordCallback) callback;
                    if (TestJaasConfig.USERNAME.equals(username))
                        passwordCallback.setPassword(TestJaasConfig.PASSWORD.toCharArray());
                } else if (callback instanceof RealmCallback) {
                    RealmCallback realmCallback = (RealmCallback) callback;
                    realmCallback.setText(realmCallback.getDefaultText());
                } else if (callback instanceof AuthorizeCallback) {
                    AuthorizeCallback authCallback = (AuthorizeCallback) callback;
                    if (TestJaasConfig.USERNAME.equals(authCallback.getAuthenticationID())) {
                        authCallback.setAuthorized(true);
                        authCallback.setAuthorizedID(authCallback.getAuthenticationID());
                    }
                }
            }
        }

        @Override
        public void close() {
        }
    }
}
