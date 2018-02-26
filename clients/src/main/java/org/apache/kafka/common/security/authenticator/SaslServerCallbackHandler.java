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

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.security.JaasContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

import org.apache.kafka.common.network.Mode;

/**
 * Callback handler for Sasl servers. The callbacks required for all the SASL
 * mechanisms enabled in the server should be supported by this callback handler. See
 * <a href="https://docs.oracle.com/javase/8/docs/technotes/guides/security/sasl/sasl-refguide.html">Java SASL API</a>
 * for the list of SASL callback handlers required for each SASL mechanism.
 */
public class SaslServerCallbackHandler implements AuthCallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SaslServerCallbackHandler.class);
    private final JaasContext jaasContext;

    public SaslServerCallbackHandler(JaasContext jaasContext) throws IOException {
        this.jaasContext = jaasContext;
    }

    @Override
    public void configure(Map<String, ?> configs, Mode mode, Subject subject, String saslMechanism) {
    }

    public JaasContext jaasContext() {
        return jaasContext;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof RealmCallback) {
                handleRealmCallback((RealmCallback) callback);
            } else if (callback instanceof AuthorizeCallback) {
                handleAuthorizeCallback((AuthorizeCallback) callback);
            }
        }
    }

    private void handleRealmCallback(RealmCallback rc) {
        LOG.trace("Client supplied realm: {} ", rc.getDefaultText());
        rc.setText(rc.getDefaultText());
    }

    private void handleAuthorizeCallback(AuthorizeCallback ac) {
        String authenticationID = ac.getAuthenticationID();
        String authorizationID = ac.getAuthorizationID();
        LOG.info("Successfully authenticated client: authenticationID={}; authorizationID={}.",
                authenticationID, authorizationID);
        ac.setAuthorized(true);
        ac.setAuthorizedID(authenticationID);
    }

    @Override
    public void close() {
    }
}
