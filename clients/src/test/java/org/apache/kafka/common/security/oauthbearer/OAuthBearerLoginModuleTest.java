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
package org.apache.kafka.common.security.oauthbearer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginException;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.auth.SaslExtensions;
import org.junit.jupiter.api.Test;

public class OAuthBearerLoginModuleTest {

    public static final SaslExtensions RAISE_UNSUPPORTED_CB_EXCEPTION_FLAG = null;

    private static class TestCallbackHandler implements AuthenticateCallbackHandler {
        private final OAuthBearerToken[] tokens;
        private int index = 0;
        private int extensionsIndex = 0;
        private final SaslExtensions[] extensions;

        public TestCallbackHandler(OAuthBearerToken[] tokens, SaslExtensions[] extensions) {
            this.tokens = Objects.requireNonNull(tokens);
            this.extensions = extensions;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof OAuthBearerTokenCallback)
                    try {
                        handleCallback((OAuthBearerTokenCallback) callback);
                    } catch (KafkaException e) {
                        throw new IOException(e.getMessage(), e);
                    }
                else if (callback instanceof SaslExtensionsCallback) {
                    try {
                        handleExtensionsCallback((SaslExtensionsCallback) callback);
                    } catch (KafkaException e) {
                        throw new IOException(e.getMessage(), e);
                    }
                } else
                    throw new UnsupportedCallbackException(callback);
            }
        }

        @Override
        public void configure(Map<String, ?> configs, String saslMechanism,
                List<AppConfigurationEntry> jaasConfigEntries) {
            // empty
        }

        @Override
        public void close() {
            // empty
        }

        private void handleCallback(OAuthBearerTokenCallback callback) throws IOException {
            if (callback.token() != null)
                throw new IllegalArgumentException("Callback had a token already");
            if (tokens.length > index)
                callback.token(tokens[index++]);
            else
                throw new IOException("no more tokens");
        }

        private void handleExtensionsCallback(SaslExtensionsCallback callback) throws IOException, UnsupportedCallbackException {
            if (extensions.length > extensionsIndex) {
                SaslExtensions extension = extensions[extensionsIndex++];

                if (extension == RAISE_UNSUPPORTED_CB_EXCEPTION_FLAG) {
                    throw new UnsupportedCallbackException(callback);
                }

                callback.extensions(extension);
            } else
                throw new IOException("no more extensions");
        }
    }

    @Test
    public void login1Commit1Login2Commit2Logout1Login3Commit3Logout2() throws LoginException {
        /*
         * Invoke login()/commit() on loginModule1; invoke login/commit() on
         * loginModule2; invoke logout() on loginModule1; invoke login()/commit() on
         * loginModule3; invoke logout() on loginModule2
         */
        Subject subject = new Subject();
        Set<Object> privateCredentials = subject.getPrivateCredentials();
        Set<Object> publicCredentials = subject.getPublicCredentials();

        // Create callback handler
        OAuthBearerToken[] tokens = new OAuthBearerToken[] {mock(OAuthBearerToken.class),
            mock(OAuthBearerToken.class), mock(OAuthBearerToken.class)};
        SaslExtensions[] extensions = new SaslExtensions[] {saslExtensions(),
            saslExtensions(), saslExtensions()};
        TestCallbackHandler testTokenCallbackHandler = new TestCallbackHandler(tokens, extensions);

        // Create login modules
        OAuthBearerLoginModule loginModule1 = new OAuthBearerLoginModule();
        loginModule1.initialize(subject, testTokenCallbackHandler, Collections.emptyMap(),
                Collections.emptyMap());
        OAuthBearerLoginModule loginModule2 = new OAuthBearerLoginModule();
        loginModule2.initialize(subject, testTokenCallbackHandler, Collections.emptyMap(),
                Collections.emptyMap());
        OAuthBearerLoginModule loginModule3 = new OAuthBearerLoginModule();
        loginModule3.initialize(subject, testTokenCallbackHandler, Collections.emptyMap(),
                Collections.emptyMap());

        // Should start with nothing
        assertEquals(0, privateCredentials.size());
        assertEquals(0, publicCredentials.size());
        loginModule1.login();
        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size());
        assertEquals(0, publicCredentials.size());
        loginModule1.commit();
        // Now we should have the first token and extensions
        assertEquals(1, privateCredentials.size());
        assertEquals(1, publicCredentials.size());
        assertSame(tokens[0], privateCredentials.iterator().next());
        assertSame(extensions[0], publicCredentials.iterator().next());

        // Now login on loginModule2 to get the second token
        // loginModule2 does not support the extensions callback and will raise UnsupportedCallbackException
        loginModule2.login();
        // Should still have just the first token and extensions
        assertEquals(1, privateCredentials.size());
        assertEquals(1, publicCredentials.size());
        assertSame(tokens[0], privateCredentials.iterator().next());
        assertSame(extensions[0], publicCredentials.iterator().next());
        loginModule2.commit();
        // Should have the first and second tokens at this point
        assertEquals(2, privateCredentials.size());
        assertEquals(2, publicCredentials.size());
        Iterator<Object> iterator = privateCredentials.iterator();
        Iterator<Object> publicIterator = publicCredentials.iterator();
        assertNotSame(tokens[2], iterator.next());
        assertNotSame(tokens[2], iterator.next());
        assertNotSame(extensions[2], publicIterator.next());
        assertNotSame(extensions[2], publicIterator.next());
        // finally logout() on loginModule1
        loginModule1.logout();
        // Now we should have just the second token and extension
        assertEquals(1, privateCredentials.size());
        assertEquals(1, publicCredentials.size());
        assertSame(tokens[1], privateCredentials.iterator().next());
        assertSame(extensions[1], publicCredentials.iterator().next());

        // Now login on loginModule3 to get the third token
        loginModule3.login();
        // Should still have just the second token and extensions
        assertEquals(1, privateCredentials.size());
        assertEquals(1, publicCredentials.size());
        assertSame(tokens[1], privateCredentials.iterator().next());
        assertSame(extensions[1], publicCredentials.iterator().next());
        loginModule3.commit();
        // Should have the second and third tokens at this point
        assertEquals(2, privateCredentials.size());
        assertEquals(2, publicCredentials.size());
        iterator = privateCredentials.iterator();
        publicIterator = publicCredentials.iterator();
        assertNotSame(tokens[0], iterator.next());
        assertNotSame(tokens[0], iterator.next());
        assertNotSame(extensions[0], publicIterator.next());
        assertNotSame(extensions[0], publicIterator.next());
        // finally logout() on loginModule2
        loginModule2.logout();
        // Now we should have just the third token
        assertEquals(1, privateCredentials.size());
        assertEquals(1, publicCredentials.size());
        assertSame(tokens[2], privateCredentials.iterator().next());
        assertSame(extensions[2], publicCredentials.iterator().next());

        verifyNoInteractions((Object[]) tokens);
    }

    @Test
    public void login1Commit1Logout1Login2Commit2Logout2() throws LoginException {
        /*
         * Invoke login()/commit() on loginModule1; invoke logout() on loginModule1;
         * invoke login()/commit() on loginModule2; invoke logout() on loginModule2
         */
        Subject subject = new Subject();
        Set<Object> privateCredentials = subject.getPrivateCredentials();
        Set<Object> publicCredentials = subject.getPublicCredentials();

        // Create callback handler
        OAuthBearerToken[] tokens = new OAuthBearerToken[] {mock(OAuthBearerToken.class),
            mock(OAuthBearerToken.class)};
        SaslExtensions[] extensions = new SaslExtensions[] {saslExtensions(),
            saslExtensions()};
        TestCallbackHandler testTokenCallbackHandler = new TestCallbackHandler(tokens, extensions);

        // Create login modules
        OAuthBearerLoginModule loginModule1 = new OAuthBearerLoginModule();
        loginModule1.initialize(subject, testTokenCallbackHandler, Collections.emptyMap(),
                Collections.emptyMap());
        OAuthBearerLoginModule loginModule2 = new OAuthBearerLoginModule();
        loginModule2.initialize(subject, testTokenCallbackHandler, Collections.emptyMap(),
                Collections.emptyMap());

        // Should start with nothing
        assertEquals(0, privateCredentials.size());
        assertEquals(0, publicCredentials.size());
        loginModule1.login();
        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size());
        assertEquals(0, publicCredentials.size());
        loginModule1.commit();
        // Now we should have the first token
        assertEquals(1, privateCredentials.size());
        assertEquals(1, publicCredentials.size());
        assertSame(tokens[0], privateCredentials.iterator().next());
        assertSame(extensions[0], publicCredentials.iterator().next());
        loginModule1.logout();
        // Should have nothing again
        assertEquals(0, privateCredentials.size());
        assertEquals(0, publicCredentials.size());

        loginModule2.login();
        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size());
        assertEquals(0, publicCredentials.size());
        loginModule2.commit();
        // Now we should have the second token
        assertEquals(1, privateCredentials.size());
        assertEquals(1, publicCredentials.size());
        assertSame(tokens[1], privateCredentials.iterator().next());
        assertSame(extensions[1], publicCredentials.iterator().next());
        loginModule2.logout();
        // Should have nothing again
        assertEquals(0, privateCredentials.size());
        assertEquals(0, publicCredentials.size());

        verifyNoInteractions((Object[]) tokens);
    }

    @Test
    public void loginAbortLoginCommitLogout() throws LoginException {
        /*
         * Invoke login(); invoke abort(); invoke login(); logout()
         */
        Subject subject = new Subject();
        Set<Object> privateCredentials = subject.getPrivateCredentials();
        Set<Object> publicCredentials = subject.getPublicCredentials();

        // Create callback handler
        OAuthBearerToken[] tokens = new OAuthBearerToken[] {mock(OAuthBearerToken.class),
            mock(OAuthBearerToken.class)};
        SaslExtensions[] extensions = new SaslExtensions[] {saslExtensions(),
            saslExtensions()};
        TestCallbackHandler testTokenCallbackHandler = new TestCallbackHandler(tokens, extensions);

        // Create login module
        OAuthBearerLoginModule loginModule = new OAuthBearerLoginModule();
        loginModule.initialize(subject, testTokenCallbackHandler, Collections.emptyMap(),
                Collections.emptyMap());

        // Should start with nothing
        assertEquals(0, privateCredentials.size());
        assertEquals(0, publicCredentials.size());
        loginModule.login();
        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size());
        assertEquals(0, publicCredentials.size());
        loginModule.abort();
        // Should still have nothing since we aborted
        assertEquals(0, privateCredentials.size());
        assertEquals(0, publicCredentials.size());

        loginModule.login();
        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size());
        assertEquals(0, publicCredentials.size());
        loginModule.commit();
        // Now we should have the second token
        assertEquals(1, privateCredentials.size());
        assertEquals(1, publicCredentials.size());
        assertSame(tokens[1], privateCredentials.iterator().next());
        assertSame(extensions[1], publicCredentials.iterator().next());
        loginModule.logout();
        // Should have nothing again
        assertEquals(0, privateCredentials.size());
        assertEquals(0, publicCredentials.size());

        verifyNoInteractions((Object[]) tokens);
    }

    @Test
    public void login1Commit1Login2Abort2Login3Commit3Logout3() throws LoginException {
        /*
         * Invoke login()/commit() on loginModule1; invoke login()/abort() on
         * loginModule2; invoke login()/commit()/logout() on loginModule3
         */
        Subject subject = new Subject();
        Set<Object> privateCredentials = subject.getPrivateCredentials();
        Set<Object> publicCredentials = subject.getPublicCredentials();

        // Create callback handler
        OAuthBearerToken[] tokens = new OAuthBearerToken[] {mock(OAuthBearerToken.class),
            mock(OAuthBearerToken.class), mock(OAuthBearerToken.class)};
        SaslExtensions[] extensions = new SaslExtensions[] {saslExtensions(), saslExtensions(),
            saslExtensions()};
        TestCallbackHandler testTokenCallbackHandler = new TestCallbackHandler(tokens, extensions);

        // Create login modules
        OAuthBearerLoginModule loginModule1 = new OAuthBearerLoginModule();
        loginModule1.initialize(subject, testTokenCallbackHandler, Collections.emptyMap(),
                Collections.emptyMap());
        OAuthBearerLoginModule loginModule2 = new OAuthBearerLoginModule();
        loginModule2.initialize(subject, testTokenCallbackHandler, Collections.emptyMap(),
                Collections.emptyMap());
        OAuthBearerLoginModule loginModule3 = new OAuthBearerLoginModule();
        loginModule3.initialize(subject, testTokenCallbackHandler, Collections.emptyMap(),
                Collections.emptyMap());

        // Should start with nothing
        assertEquals(0, privateCredentials.size());
        assertEquals(0, publicCredentials.size());
        loginModule1.login();
        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size());
        assertEquals(0, publicCredentials.size());
        loginModule1.commit();
        // Now we should have the first token
        assertEquals(1, privateCredentials.size());
        assertEquals(1, publicCredentials.size());
        assertSame(tokens[0], privateCredentials.iterator().next());
        assertSame(extensions[0], publicCredentials.iterator().next());

        // Now go get the second token
        loginModule2.login();
        // Should still have first token
        assertEquals(1, privateCredentials.size());
        assertEquals(1, publicCredentials.size());
        assertSame(tokens[0], privateCredentials.iterator().next());
        assertSame(extensions[0], publicCredentials.iterator().next());
        loginModule2.abort();
        // Should still have just the first token because we aborted
        assertEquals(1, privateCredentials.size());
        assertSame(tokens[0], privateCredentials.iterator().next());
        assertEquals(1, publicCredentials.size());
        assertSame(extensions[0], publicCredentials.iterator().next());

        // Now go get the third token
        loginModule2.login();
        // Should still have first token
        assertEquals(1, privateCredentials.size());
        assertSame(tokens[0], privateCredentials.iterator().next());
        assertEquals(1, publicCredentials.size());
        assertSame(extensions[0], publicCredentials.iterator().next());
        loginModule2.commit();
        // Should have first and third tokens at this point
        assertEquals(2, privateCredentials.size());
        Iterator<Object> iterator = privateCredentials.iterator();
        assertNotSame(tokens[1], iterator.next());
        assertNotSame(tokens[1], iterator.next());
        assertEquals(2, publicCredentials.size());
        Iterator<Object> publicIterator = publicCredentials.iterator();
        assertNotSame(extensions[1], publicIterator.next());
        assertNotSame(extensions[1], publicIterator.next());
        loginModule1.logout();
        // Now we should have just the third token
        assertEquals(1, privateCredentials.size());
        assertSame(tokens[2], privateCredentials.iterator().next());
        assertEquals(1, publicCredentials.size());
        assertSame(extensions[2], publicCredentials.iterator().next());

        verifyNoInteractions((Object[]) tokens);
    }

    /**
     * 2.1.0 added customizable SASL extensions and a new callback type.
     * Ensure that old, custom-written callbackHandlers that do not handle the callback work
     */
    @Test
    public void commitDoesNotThrowOnUnsupportedExtensionsCallback() throws LoginException {
        Subject subject = new Subject();

        // Create callback handler
        OAuthBearerToken[] tokens = new OAuthBearerToken[] {mock(OAuthBearerToken.class),
                mock(OAuthBearerToken.class), mock(OAuthBearerToken.class)};
        TestCallbackHandler testTokenCallbackHandler = new TestCallbackHandler(tokens, new SaslExtensions[] {RAISE_UNSUPPORTED_CB_EXCEPTION_FLAG});

        // Create login modules
        OAuthBearerLoginModule loginModule1 = new OAuthBearerLoginModule();
        loginModule1.initialize(subject, testTokenCallbackHandler, Collections.emptyMap(),
                Collections.emptyMap());

        loginModule1.login();
        // Should populate public credentials with SaslExtensions and not throw an exception
        loginModule1.commit();
        SaslExtensions extensions = subject.getPublicCredentials(SaslExtensions.class).iterator().next();
        assertNotNull(extensions);
        assertTrue(extensions.map().isEmpty());

        verifyNoInteractions((Object[]) tokens);
    }

    /**
     * We don't want to use mocks for our tests as we need to make sure to test
     * {@link SaslExtensions}' {@link SaslExtensions#equals(Object)} and
     * {@link SaslExtensions#hashCode()} methods.
     *
     * <p/>
     *
     * We need to make distinct calls to this method (vs. caching the result and reusing it
     * multiple times) because we need to ensure the {@link SaslExtensions} instances are unique.
     * This properly mimics the behavior that is used during the token refresh logic.
     *
     * @return Unique, newly-created {@link SaslExtensions} instance
     */
    private SaslExtensions saslExtensions() {
        return SaslExtensions.empty();
    }
}
