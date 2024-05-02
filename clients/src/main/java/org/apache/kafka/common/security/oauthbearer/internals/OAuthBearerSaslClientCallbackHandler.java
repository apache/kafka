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
package org.apache.kafka.common.security.oauthbearer.internals;

import java.io.IOException;
import java.security.AccessController;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.SaslExtensions;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@code AuthenticateCallbackHandler} that recognizes
 * {@link OAuthBearerTokenCallback} and retrieves OAuth 2 Bearer Token that was
 * created when the {@code OAuthBearerLoginModule} logged in by looking for an
 * instance of {@link OAuthBearerToken} in the {@code Subject}'s private
 * credentials. This class also recognizes {@link SaslExtensionsCallback} and retrieves any SASL extensions that were
 * created when the {@code OAuthBearerLoginModule} logged in by looking for an instance of {@link SaslExtensions}
 * in the {@code Subject}'s public credentials
 * <p>
 * Use of this class is configured automatically and does not need to be
 * explicitly set via the {@code sasl.client.callback.handler.class}
 * configuration property.
 */
public class OAuthBearerSaslClientCallbackHandler implements AuthenticateCallbackHandler {
    private static final Logger log = LoggerFactory.getLogger(OAuthBearerSaslClientCallbackHandler.class);
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
            else if (callback instanceof SaslExtensionsCallback)
                handleCallback((SaslExtensionsCallback) callback, Subject.getSubject(AccessController.getContext()));
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
            : Collections.emptySet();
        if (privateCredentials.isEmpty())
            throw new IOException("No OAuth Bearer tokens in Subject's private credentials");
        if (privateCredentials.size() == 1)
            callback.token(privateCredentials.iterator().next());
        else {
            /*
             * There a very small window of time upon token refresh (on the order of milliseconds)
             * where both an old and a new token appear on the Subject's private credentials.
             * Rather than implement a lock to eliminate this window, we will deal with it by
             * checking for the existence of multiple tokens and choosing the one that has the
             * longest lifetime.  It is also possible that a bug could cause multiple tokens to
             * exist (e.g. KAFKA-7902), so dealing with the unlikely possibility that occurs
             * during normal operation also allows us to deal more robustly with potential bugs.
             */
            SortedSet<OAuthBearerToken> sortedByLifetime =
                new TreeSet<>(
                        Comparator.comparingLong(OAuthBearerToken::lifetimeMs));
            sortedByLifetime.addAll(privateCredentials);
            log.warn("Found {} OAuth Bearer tokens in Subject's private credentials; the oldest expires at {}, will use the newest, which expires at {}",
                sortedByLifetime.size(),
                new Date(sortedByLifetime.first().lifetimeMs()),
                new Date(sortedByLifetime.last().lifetimeMs()));
            callback.token(sortedByLifetime.last());
        }
    }

    /**
     * Attaches the first {@link SaslExtensions} found in the public credentials of the Subject
     */
    private static void handleCallback(SaslExtensionsCallback extensionsCallback, Subject subject) {
        if (subject != null && !subject.getPublicCredentials(SaslExtensions.class).isEmpty()) {
            SaslExtensions extensions = subject.getPublicCredentials(SaslExtensions.class).iterator().next();
            extensionsCallback.extensions(extensions);
        }
    }
}
