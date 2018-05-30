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
package org.apache.kafka.common.security.oauthbearer.internal;

import java.io.IOException;
import java.security.AccessController;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;

/**
 * An implementation of {@code AuthenticateCallbackHandler} that recognizes
 * {@link OAuthBearerTokenCallback} and retrieves OAuth 2 Bearer Token that was
 * created when the {@code OAuthBearerLoginModule} logged in by looking for an
 * instance of {@link OAuthBearerToken} in the {@code Subject}'s private
 * credentials.
 * <p>
 * Use of this class is configured automatically and does not need to be
 * explicitly set via the {@code sasl.client.callback.handler.class}
 * configuration property.
 */
public class OAuthBearerSaslClientCallbackHandler implements AuthenticateCallbackHandler {
    private boolean configured = false;

    /**
     * Return true if this instance has been configured, otherwise false
     * 
     * @return true if this instance has been configured, otherwise false
     */
    public boolean configured() {
        return configured;
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism))
            throw new IllegalArgumentException(String.format("Unexpected SASL mechanism: %s", saslMechanism));
        configured = true;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        if (!configured())
            throw new IllegalStateException("Callback handler not configured");
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback)
                handleCallback((OAuthBearerTokenCallback) callback);
            else
                throw new UnsupportedCallbackException(callback);
        }
    }

    @Override
    public void close() {
        // empty
    }

    private void handleCallback(OAuthBearerTokenCallback callback) throws IOException {
        if (callback.token() != null)
            throw new IllegalArgumentException("Callback had a token already");
        Subject subject = Subject.getSubject(AccessController.getContext());
        Set<OAuthBearerToken> privateCredentials = subject != null
                ? subject.getPrivateCredentials(OAuthBearerToken.class)
                : Collections.<OAuthBearerToken>emptySet();
        if (privateCredentials.size() != 1)
            throw new IOException(
                    String.format("Unable to find OAuth Bearer token in Subject's private credentials (size=%d)",
                            privateCredentials.size()));
        callback.token(privateCredentials.iterator().next());
    }
}
