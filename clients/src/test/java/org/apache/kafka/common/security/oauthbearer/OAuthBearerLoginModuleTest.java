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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

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
import org.easymock.EasyMock;
import org.junit.Test;

public class OAuthBearerLoginModuleTest {
    private static class TestTokenCallbackHandler implements AuthenticateCallbackHandler {
        private final OAuthBearerToken[] tokens;
        private int index = 0;

        public TestTokenCallbackHandler(OAuthBearerToken[] tokens) {
            this.tokens = Objects.requireNonNull(tokens);
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
                else
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

        // Create callback handler
        OAuthBearerToken[] tokens = new OAuthBearerToken[] {EasyMock.mock(OAuthBearerToken.class),
            EasyMock.mock(OAuthBearerToken.class), EasyMock.mock(OAuthBearerToken.class)};
        EasyMock.replay(tokens[0], tokens[1], tokens[2]); // expect nothing
        TestTokenCallbackHandler testTokenCallbackHandler = new TestTokenCallbackHandler(tokens);

        // Create login modules
        OAuthBearerLoginModule loginModule1 = new OAuthBearerLoginModule();
        loginModule1.initialize(subject, testTokenCallbackHandler, Collections.<String, Object>emptyMap(),
                Collections.<String, Object>emptyMap());
        OAuthBearerLoginModule loginModule2 = new OAuthBearerLoginModule();
        loginModule2.initialize(subject, testTokenCallbackHandler, Collections.<String, Object>emptyMap(),
                Collections.<String, Object>emptyMap());
        OAuthBearerLoginModule loginModule3 = new OAuthBearerLoginModule();
        loginModule3.initialize(subject, testTokenCallbackHandler, Collections.<String, Object>emptyMap(),
                Collections.<String, Object>emptyMap());

        // Should start with nothing
        assertEquals(0, privateCredentials.size());
        loginModule1.login();
        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size());
        loginModule1.commit();
        // Now we should have the first token
        assertEquals(1, privateCredentials.size());
        assertSame(tokens[0], privateCredentials.iterator().next());

        // Now login on loginModule2 to get the second token
        loginModule2.login();
        // Should still have just the first token
        assertEquals(1, privateCredentials.size());
        assertSame(tokens[0], privateCredentials.iterator().next());
        loginModule2.commit();
        // Should have the first and second tokens at this point
        assertEquals(2, privateCredentials.size());
        Iterator<Object> iterator = privateCredentials.iterator();
        assertNotSame(tokens[2], iterator.next());
        assertNotSame(tokens[2], iterator.next());
        // finally logout() on loginModule1
        loginModule1.logout();
        // Now we should have just the second token
        assertEquals(1, privateCredentials.size());
        assertSame(tokens[1], privateCredentials.iterator().next());

        // Now login on loginModule3 to get the third token
        loginModule3.login();
        // Should still have just the second token
        assertEquals(1, privateCredentials.size());
        assertSame(tokens[1], privateCredentials.iterator().next());
        loginModule3.commit();
        // Should have the second and third tokens at this point
        assertEquals(2, privateCredentials.size());
        iterator = privateCredentials.iterator();
        assertNotSame(tokens[0], iterator.next());
        assertNotSame(tokens[0], iterator.next());
        // finally logout() on loginModule2
        loginModule2.logout();
        // Now we should have just the third token
        assertEquals(1, privateCredentials.size());
        assertSame(tokens[2], privateCredentials.iterator().next());
    }

    @Test
    public void login1Commit1Logout1Login2Commit2Logout2() throws LoginException {
        /*
         * Invoke login()/commit() on loginModule1; invoke logout() on loginModule1;
         * invoke login()/commit() on loginModule2; invoke logout() on loginModule2
         */
        Subject subject = new Subject();
        Set<Object> privateCredentials = subject.getPrivateCredentials();

        // Create callback handler
        OAuthBearerToken[] tokens = new OAuthBearerToken[] {EasyMock.mock(OAuthBearerToken.class),
            EasyMock.mock(OAuthBearerToken.class)};
        EasyMock.replay(tokens[0], tokens[1]); // expect nothing
        TestTokenCallbackHandler testTokenCallbackHandler = new TestTokenCallbackHandler(tokens);

        // Create login modules
        OAuthBearerLoginModule loginModule1 = new OAuthBearerLoginModule();
        loginModule1.initialize(subject, testTokenCallbackHandler, Collections.<String, Object>emptyMap(),
                Collections.<String, Object>emptyMap());
        OAuthBearerLoginModule loginModule2 = new OAuthBearerLoginModule();
        loginModule2.initialize(subject, testTokenCallbackHandler, Collections.<String, Object>emptyMap(),
                Collections.<String, Object>emptyMap());

        // Should start with nothing
        assertEquals(0, privateCredentials.size());
        loginModule1.login();
        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size());
        loginModule1.commit();
        // Now we should have the first token
        assertEquals(1, privateCredentials.size());
        assertSame(tokens[0], privateCredentials.iterator().next());
        loginModule1.logout();
        // Should have nothing again
        assertEquals(0, privateCredentials.size());

        loginModule2.login();
        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size());
        loginModule2.commit();
        // Now we should have the second token
        assertEquals(1, privateCredentials.size());
        assertSame(tokens[1], privateCredentials.iterator().next());
        loginModule2.logout();
        // Should have nothing again
        assertEquals(0, privateCredentials.size());
    }

    @Test
    public void loginAbortLoginCommitLogout() throws LoginException {
        /*
         * Invoke login(); invoke abort(); invoke login(); logout()
         */
        Subject subject = new Subject();
        Set<Object> privateCredentials = subject.getPrivateCredentials();

        // Create callback handler
        OAuthBearerToken[] tokens = new OAuthBearerToken[] {EasyMock.mock(OAuthBearerToken.class),
            EasyMock.mock(OAuthBearerToken.class)};
        EasyMock.replay(tokens[0], tokens[1]); // expect nothing
        TestTokenCallbackHandler testTokenCallbackHandler = new TestTokenCallbackHandler(tokens);

        // Create login module
        OAuthBearerLoginModule loginModule = new OAuthBearerLoginModule();
        loginModule.initialize(subject, testTokenCallbackHandler, Collections.<String, Object>emptyMap(),
                Collections.<String, Object>emptyMap());

        // Should start with nothing
        assertEquals(0, privateCredentials.size());
        loginModule.login();
        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size());
        loginModule.abort();
        // Should still have nothing since we aborted
        assertEquals(0, privateCredentials.size());

        loginModule.login();
        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size());
        loginModule.commit();
        // Now we should have the second token
        assertEquals(1, privateCredentials.size());
        assertSame(tokens[1], privateCredentials.iterator().next());
        loginModule.logout();
        // Should have nothing again
        assertEquals(0, privateCredentials.size());
    }

    @Test
    public void login1Commit1Login2Abort2Login3Commit3Logout3() throws LoginException {
        /*
         * Invoke login()/commit() on loginModule1; invoke login()/abort() on
         * loginModule2; invoke login()/commit()/logout() on loginModule3
         */
        Subject subject = new Subject();
        Set<Object> privateCredentials = subject.getPrivateCredentials();

        // Create callback handler
        OAuthBearerToken[] tokens = new OAuthBearerToken[] {EasyMock.mock(OAuthBearerToken.class),
            EasyMock.mock(OAuthBearerToken.class), EasyMock.mock(OAuthBearerToken.class)};
        EasyMock.replay(tokens[0], tokens[1], tokens[2]); // expect nothing
        TestTokenCallbackHandler testTokenCallbackHandler = new TestTokenCallbackHandler(tokens);

        // Create login modules
        OAuthBearerLoginModule loginModule1 = new OAuthBearerLoginModule();
        loginModule1.initialize(subject, testTokenCallbackHandler, Collections.<String, Object>emptyMap(),
                Collections.<String, Object>emptyMap());
        OAuthBearerLoginModule loginModule2 = new OAuthBearerLoginModule();
        loginModule2.initialize(subject, testTokenCallbackHandler, Collections.<String, Object>emptyMap(),
                Collections.<String, Object>emptyMap());
        OAuthBearerLoginModule loginModule3 = new OAuthBearerLoginModule();
        loginModule3.initialize(subject, testTokenCallbackHandler, Collections.<String, Object>emptyMap(),
                Collections.<String, Object>emptyMap());

        // Should start with nothing
        assertEquals(0, privateCredentials.size());
        loginModule1.login();
        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size());
        loginModule1.commit();
        // Now we should have the first token
        assertEquals(1, privateCredentials.size());
        assertSame(tokens[0], privateCredentials.iterator().next());

        // Now go get the second token
        loginModule2.login();
        // Should still have first token
        assertEquals(1, privateCredentials.size());
        assertSame(tokens[0], privateCredentials.iterator().next());
        loginModule2.abort();
        // Should still have just the first token because we aborted
        assertEquals(1, privateCredentials.size());
        assertSame(tokens[0], privateCredentials.iterator().next());

        // Now go get the third token
        loginModule2.login();
        // Should still have first token
        assertEquals(1, privateCredentials.size());
        assertSame(tokens[0], privateCredentials.iterator().next());
        loginModule2.commit();
        // Should have first and third tokens at this point
        assertEquals(2, privateCredentials.size());
        Iterator<Object> iterator = privateCredentials.iterator();
        assertNotSame(tokens[1], iterator.next());
        assertNotSame(tokens[1], iterator.next());
        loginModule1.logout();
        // Now we should have just the third token
        assertEquals(1, privateCredentials.size());
        assertSame(tokens[2], privateCredentials.iterator().next());
    }
}
