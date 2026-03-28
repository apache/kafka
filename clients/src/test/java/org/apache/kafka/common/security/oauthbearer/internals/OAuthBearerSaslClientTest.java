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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.SaslExtensions;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class OAuthBearerSaslClientTest {

    private static final Map<String, String> TEST_PROPERTIES = new LinkedHashMap<>() {
        {
            put("One", "1");
            put("Two", "2");
            put("Three", "3");
        }
    };
    private static final String ERROR_MESSAGE = "Error as expected!";

    private SaslExtensions testExtensions = new SaslExtensions(TEST_PROPERTIES);

    public class ExtensionsCallbackHandler implements AuthenticateCallbackHandler {
        private final boolean toThrow;
        private boolean configured = false;

        ExtensionsCallbackHandler(boolean toThrow) {
            this.toThrow = toThrow;
        }

        public boolean configured() {
            return configured;
        }

        @Override
        public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
            configured = true;
        }

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof OAuthBearerTokenCallback)
                    ((OAuthBearerTokenCallback) callback).token(new OAuthBearerToken() {
                        @Override
                        public String value() {
                            return "";
                        }

                        @Override
                        public Set<String> scope() {
                            return Collections.emptySet();
                        }

                        @Override
                        public long lifetimeMs() {
                            return 100;
                        }

                        @Override
                        public String principalName() {
                            return "principalName";
                        }

                        @Override
                        public Long startTimeMs() {
                            return null;
                        }
                    });
                else if (callback instanceof SaslExtensionsCallback) {
                    if (toThrow)
                        throw new ConfigException(ERROR_MESSAGE);
                    else
                        ((SaslExtensionsCallback) callback).extensions(testExtensions);
                } else
                    throw new UnsupportedCallbackException(callback);
            }
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void testAttachesExtensionsToFirstClientMessage() throws Exception {
        String expectedToken = new String(new OAuthBearerClientInitialResponse("", testExtensions).toBytes(), StandardCharsets.UTF_8);

        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));

        String message = new String(client.evaluateChallenge("".getBytes()), StandardCharsets.UTF_8);

        assertEquals(expectedToken, message);
    }

    @Test
    public void testNoExtensionsDoesNotAttachAnythingToFirstClientMessage() throws Exception {
        TEST_PROPERTIES.clear();
        testExtensions = new SaslExtensions(TEST_PROPERTIES);
        String expectedToken = new String(new OAuthBearerClientInitialResponse("", new SaslExtensions(TEST_PROPERTIES)).toBytes(), StandardCharsets.UTF_8);
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));

        String message = new String(client.evaluateChallenge("".getBytes()), StandardCharsets.UTF_8);

        assertEquals(expectedToken, message);
    }

    @Test
    public void testWrapsExtensionsCallbackHandlingErrorInSaslExceptionInFirstClientMessage() {
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(true));
        try {
            client.evaluateChallenge("".getBytes());
            fail("Should have failed with " + SaslException.class.getName());
        } catch (SaslException e) {
            // assert it has caught our expected exception
            assertEquals(ConfigException.class, e.getCause().getClass());
            assertEquals(ERROR_MESSAGE, e.getCause().getMessage());
        }
    }

    @Test
    public void testGetMechanismNameReturnsOAuthBearerMechanism() {
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));

        assertEquals(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, client.getMechanismName());
    }

    @Test
    public void testHasInitialResponseReturnsTrue() {
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));

        assertTrue(client.hasInitialResponse());
    }

    @Test
    public void testEvaluateChallengeThrowsOnFirstMessageWithChallenge() {
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));

        assertThrows(SaslException.class, () -> client.evaluateChallenge("unexpected".getBytes()));
    }

    @ParameterizedTest
    @EmptySource
    @NullSource
    public void testEvaluateChallengeReturnsTokenOnEmptyChallenge(String challenge) throws Exception {
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));
        byte[] challengeBytes = challenge == null ? null : challenge.getBytes();
        byte[] response = client.evaluateChallenge(challengeBytes);

        assertNotNull(response);
        assertFalse(client.isComplete());
    }

    @Test
    public void testEvaluateChallengeReturnsControlAOnServerError() throws Exception {
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));
        client.evaluateChallenge(new byte[0]);
        byte[] response = client.evaluateChallenge("error".getBytes());

        assertArrayEquals(new byte[]{OAuthBearerSaslClient.BYTE_CONTROL_A}, response);
        assertFalse(client.isComplete());
    }

    @ParameterizedTest
    @EmptySource
    @NullSource
    void testEvaluateChallengeCompletesOnEmptyServerChallenge(String challenge) throws Exception {
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));
        byte[] challengeBytes = challenge == null ? null : challenge.getBytes();
        client.evaluateChallenge(new byte[0]);
        byte[] response = client.evaluateChallenge(challengeBytes);

        assertNull(response);
        assertTrue(client.isComplete());
    }

    @Test
    void testEvaluateChallengeThrowsOnUnexpectedState() throws Exception {
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));
        client.evaluateChallenge(new byte[0]);
        client.evaluateChallenge(new byte[0]);

        assertThrows(IllegalSaslStateException.class,
                () -> client.evaluateChallenge(new byte[0]));
    }

    @Test
    void testEvaluateChallengeWrapsIOExceptionInSaslException() throws Exception {
        AuthenticateCallbackHandler callbackHandler = mock(AuthenticateCallbackHandler.class);
        doThrow(IOException.class).when(callbackHandler).handle(any());

        OAuthBearerSaslClient client = new OAuthBearerSaslClient(callbackHandler);
        SaslException ex = assertThrows(SaslException.class,
                () -> client.evaluateChallenge(new byte[0]));

        assertInstanceOf(IOException.class, ex.getCause());
        assertFalse(client.isComplete());
    }

    @Test
    public void testUnwrapThrowsIfNotComplete() {
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));

        assertThrows(IllegalStateException.class, () -> client.unwrap(null, 0, 0));
    }

    @Test
    public void testUnwrapThrowsIfComplete() throws SaslException {
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));
        client.evaluateChallenge(new byte[0]);
        client.evaluateChallenge(new byte[0]);

        assertThrows(IllegalStateException.class, () -> client.unwrap(null, 0, 0));
    }

    @Test
    public void testWrapThrowsIfNotComplete() {
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));

        assertThrows(IllegalStateException.class, () -> client.wrap(null, 0, 0));
    }

    @Test
    public void testWrapThrowsIfComplete() throws SaslException {
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));
        client.evaluateChallenge(new byte[0]);
        client.evaluateChallenge(new byte[0]);

        assertThrows(IllegalStateException.class, () -> client.wrap(null, 0, 0));
    }

    @Test
    public void testGetNegotiatedPropertyThrowsIfNotComplete() {
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));

        assertThrows(IllegalStateException.class, () -> client.getNegotiatedProperty("test"));
    }

    @Test
    public void testGetNegotiatedPropertyThrowsIfComplete() throws SaslException {
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));
        client.evaluateChallenge(new byte[0]);
        client.evaluateChallenge(new byte[0]);

        assertNull(client.getNegotiatedProperty("test"));
    }

    @Test
    public void testCreateSaslClientReturnsClientForSupportedMechanism() {
        OAuthBearerSaslClient.OAuthBearerSaslClientFactory factory = new OAuthBearerSaslClient.OAuthBearerSaslClientFactory();
        AuthenticateCallbackHandler callbackHandler = mock(AuthenticateCallbackHandler.class);
        SaslClient client = factory.createSaslClient(
                new String[]{OAuthBearerLoginModule.OAUTHBEARER_MECHANISM},
                null, "https", "localhost", Collections.emptyMap(),
                callbackHandler);

        assertNotNull(client);
        assertInstanceOf(OAuthBearerSaslClient.class, client);
    }

    @Test
    public void testCreateSaslClientReturnsNullForUnsupportedMechanism() {
        OAuthBearerSaslClient.OAuthBearerSaslClientFactory factory = new OAuthBearerSaslClient.OAuthBearerSaslClientFactory();
        AuthenticateCallbackHandler callbackHandler = mock(AuthenticateCallbackHandler.class);
        SaslClient client = factory.createSaslClient(
                new String[]{"PLAIN", "SCRAM-SHA-256"},
                null, "https", "localhost", Collections.emptyMap(),
                callbackHandler);

        assertNull(client);
    }

    @Test
    public void testCreateSaslClientThrowsIfCallbackHandlerIsNotAuthenticateCallbackHandler() {
        OAuthBearerSaslClient.OAuthBearerSaslClientFactory factory = new OAuthBearerSaslClient.OAuthBearerSaslClientFactory();
        CallbackHandler nonAuthHandler = callbacks -> { };

        assertThrows(IllegalArgumentException.class, () ->
                factory.createSaslClient(
                        new String[]{OAuthBearerLoginModule.OAUTHBEARER_MECHANISM},
                        null, "https", "localhost", Collections.emptyMap(),
                        nonAuthHandler));
    }

    @Test
    public void testCreateSaslClientThrowsIfCallbackHandlerIsNull() {
        OAuthBearerSaslClient.OAuthBearerSaslClientFactory factory = new OAuthBearerSaslClient.OAuthBearerSaslClientFactory();

        assertThrows(NullPointerException.class, () ->
                factory.createSaslClient(
                        new String[]{OAuthBearerLoginModule.OAUTHBEARER_MECHANISM},
                        null, "https", "localhost", Collections.emptyMap(),
                        null));
    }

    @Test
    public void testGetMechanismNamesReturnsOAuthBearer() {
        OAuthBearerSaslClient.OAuthBearerSaslClientFactory factory = new OAuthBearerSaslClient.OAuthBearerSaslClientFactory();
        String[] names = factory.getMechanismNames(Collections.emptyMap());

        assertNotNull(names);
        assertTrue(Arrays.asList(names).contains(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM));
    }
}
