/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.security.authenticator;

import java.io.IOException;
import java.security.Provider;
import java.security.Security;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import org.apache.kafka.common.security.plain.PlainLoginModule;

/**
 * Digest-MD5 login module for multi-mechanism tests. Since callback handlers are not configurable in Kafka
 * yet, this replaces the standard Digest-MD5 SASL server provider with one that invokes the test callback handler.
 * This login module uses the same format as PlainLoginModule and hence simply reuses the same methods.
 *
 */
public class TestDigestLoginModule extends PlainLoginModule {

    private static final SaslServerFactory STANDARD_DIGEST_SASL_SERVER_FACTORY;
    static {
        SaslServerFactory digestSaslServerFactory = null;
        Enumeration<SaslServerFactory> factories = Sasl.getSaslServerFactories();
        Map<String, Object> emptyProps = new HashMap<>();
        while (factories.hasMoreElements()) {
            SaslServerFactory factory = factories.nextElement();
            if (Arrays.asList(factory.getMechanismNames(emptyProps)).contains("DIGEST-MD5")) {
                digestSaslServerFactory = factory;
                break;
            }
        }
        STANDARD_DIGEST_SASL_SERVER_FACTORY = digestSaslServerFactory;
        Security.insertProviderAt(new DigestSaslServerProvider(), 1);
    }

    public static class DigestServerCallbackHandler implements CallbackHandler {

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    NameCallback nameCallback = (NameCallback) callback;
                    nameCallback.setName(nameCallback.getDefaultName());
                } else if (callback instanceof PasswordCallback) {
                    PasswordCallback passwordCallback = (PasswordCallback) callback;
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
    }

    public static class DigestSaslServerFactory implements SaslServerFactory {

        @Override
        public SaslServer createSaslServer(String mechanism, String protocol, String serverName, Map<String, ?> props, CallbackHandler cbh)
                throws SaslException {
            return STANDARD_DIGEST_SASL_SERVER_FACTORY.createSaslServer(mechanism, protocol, serverName, props, new DigestServerCallbackHandler());
        }

        @Override
        public String[] getMechanismNames(Map<String, ?> props) {
            return new String[] {"DIGEST-MD5"};
        }
    }

    public static class DigestSaslServerProvider extends Provider {

        private static final long serialVersionUID = 1L;

        protected DigestSaslServerProvider() {
            super("Test SASL/Digest-MD5 Server Provider", 1.0, "Test SASL/Digest-MD5 Server Provider for Kafka");
            super.put("SaslServerFactory.DIGEST-MD5", TestDigestLoginModule.DigestSaslServerFactory.class.getName());
        }
    }
}
