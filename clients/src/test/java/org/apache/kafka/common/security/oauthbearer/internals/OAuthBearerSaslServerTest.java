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

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.SaslExtensions;
import org.apache.kafka.common.security.authenticator.SaslInternalConfigs;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerExtensionsValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenMock;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerConfigException;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredValidatorCallbackHandler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OAuthBearerSaslServerTest {
    private static final String USER = "user";
    private static final String JAAS_CONFIG_TEXT = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required"
                + " unsecuredLoginStringClaim_sub=\"" + USER + "\";";
    private static final Map<String, ?> CONFIGS = Map.of(SaslConfigs.SASL_JAAS_CONFIG, new Password(JAAS_CONFIG_TEXT));

    private static final AuthenticateCallbackHandler LOGIN_CALLBACK_HANDLER;
    static {
        LOGIN_CALLBACK_HANDLER = new OAuthBearerUnsecuredLoginCallbackHandler();
        LOGIN_CALLBACK_HANDLER.configure(CONFIGS, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                JaasContext.loadClientContext(CONFIGS).configurationEntries());
    }
    private static final AuthenticateCallbackHandler VALIDATOR_CALLBACK_HANDLER;
    private static final AuthenticateCallbackHandler EXTENSIONS_VALIDATOR_CALLBACK_HANDLER;
    static {
        VALIDATOR_CALLBACK_HANDLER = new OAuthBearerUnsecuredValidatorCallbackHandler();
        VALIDATOR_CALLBACK_HANDLER.configure(CONFIGS, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                JaasContext.loadClientContext(CONFIGS).configurationEntries());
        // only validate extensions "firstKey" and "secondKey"
        EXTENSIONS_VALIDATOR_CALLBACK_HANDLER = new OAuthBearerUnsecuredValidatorCallbackHandler() {
            @Override
            public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
                for (Callback callback : callbacks) {
                    if (callback instanceof OAuthBearerValidatorCallback) {
                        OAuthBearerValidatorCallback validationCallback = (OAuthBearerValidatorCallback) callback;
                        validationCallback.token(new OAuthBearerTokenMock());
                    } else if (callback instanceof OAuthBearerExtensionsValidatorCallback) {
                        OAuthBearerExtensionsValidatorCallback extensionsCallback = (OAuthBearerExtensionsValidatorCallback) callback;
                        extensionsCallback.valid("firstKey");
                        extensionsCallback.valid("secondKey");
                    } else
                        throw new UnsupportedCallbackException(callback);
                }
            }
        };
    }
    private OAuthBearerSaslServer saslServer;

    @BeforeEach
    public void setUp() {
        saslServer = new OAuthBearerSaslServer(VALIDATOR_CALLBACK_HANDLER);
    }

    @Test
    public void noAuthorizationIdSpecified() throws Exception {
        byte[] nextChallenge = saslServer
                .evaluateResponse(clientInitialResponse(null));
        // also asserts that no authentication error is thrown if OAuthBearerExtensionsValidatorCallback is not supported
        assertEquals(0, nextChallenge.length, "Next challenge is not empty");
    }

    @Test
    public void negotiatedProperty() throws Exception {
        saslServer.evaluateResponse(clientInitialResponse(USER));
        OAuthBearerToken token = (OAuthBearerToken) saslServer.getNegotiatedProperty("OAUTHBEARER.token");
        assertNotNull(token);
        assertEquals(token.lifetimeMs(),
                saslServer.getNegotiatedProperty(SaslInternalConfigs.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY));
    }

    /**
     * SASL Extensions that are validated by the callback handler should be accessible through the {@code #getNegotiatedProperty()} method
     */
    @Test
    public void savesCustomExtensionAsNegotiatedProperty() throws Exception {
        Map<String, String> customExtensions = new HashMap<>();
        customExtensions.put("firstKey", "value1");
        customExtensions.put("secondKey", "value2");

        byte[] nextChallenge = saslServer
                .evaluateResponse(clientInitialResponse(null, false, customExtensions));

        assertEquals(0, nextChallenge.length, "Next challenge is not empty");
        assertEquals("value1", saslServer.getNegotiatedProperty("firstKey"));
        assertEquals("value2", saslServer.getNegotiatedProperty("secondKey"));
    }

    /**
     * SASL Extensions that were not recognized (neither validated nor invalidated)
     * by the callback handler must not be accessible through the {@code #getNegotiatedProperty()} method
     */
    @Test
    public void unrecognizedExtensionsAreNotSaved() throws Exception {
        saslServer = new OAuthBearerSaslServer(EXTENSIONS_VALIDATOR_CALLBACK_HANDLER);
        Map<String, String> customExtensions = new HashMap<>();
        customExtensions.put("firstKey", "value1");
        customExtensions.put("secondKey", "value1");
        customExtensions.put("thirdKey", "value1");

        byte[] nextChallenge = saslServer
                .evaluateResponse(clientInitialResponse(null, false, customExtensions));

        assertEquals(0, nextChallenge.length, "Next challenge is not empty");
        assertNull(saslServer.getNegotiatedProperty("thirdKey"), "Extensions not recognized by the server must be ignored");
    }

    /**
     * If the callback handler handles the `OAuthBearerExtensionsValidatorCallback`
     *  and finds an invalid extension, SaslServer should throw an authentication exception
     */
    @Test
    public void throwsAuthenticationExceptionOnInvalidExtensions() {
        OAuthBearerUnsecuredValidatorCallbackHandler invalidHandler = new OAuthBearerUnsecuredValidatorCallbackHandler() {
            @Override
            public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
                for (Callback callback : callbacks) {
                    if (callback instanceof OAuthBearerValidatorCallback) {
                        OAuthBearerValidatorCallback validationCallback = (OAuthBearerValidatorCallback) callback;
                        validationCallback.token(new OAuthBearerTokenMock());
                    } else if (callback instanceof OAuthBearerExtensionsValidatorCallback) {
                        OAuthBearerExtensionsValidatorCallback extensionsCallback = (OAuthBearerExtensionsValidatorCallback) callback;
                        extensionsCallback.error("firstKey", "is not valid");
                        extensionsCallback.error("secondKey", "is not valid either");
                    } else
                        throw new UnsupportedCallbackException(callback);
                }
            }
        };
        saslServer = new OAuthBearerSaslServer(invalidHandler);
        Map<String, String> customExtensions = new HashMap<>();
        customExtensions.put("firstKey", "value");
        customExtensions.put("secondKey", "value");

        assertThrows(SaslAuthenticationException.class,
            () -> saslServer.evaluateResponse(clientInitialResponse(null, false, customExtensions)));
    }

    @Test
    public void authorizationIdEqualsAuthenticationId() throws Exception {
        byte[] nextChallenge = saslServer
                .evaluateResponse(clientInitialResponse(USER));
        assertEquals(0, nextChallenge.length, "Next challenge is not empty");
    }

    @Test
    public void authorizationIdNotEqualsAuthenticationId() {
        assertThrows(SaslAuthenticationException.class,
            () -> saslServer.evaluateResponse(clientInitialResponse(USER + "x")));
    }

    @Test
    public void illegalToken() throws Exception {
        byte[] bytes = saslServer.evaluateResponse(clientInitialResponse(null, true, Collections.emptyMap()));
        String challenge = new String(bytes, StandardCharsets.UTF_8);
        assertEquals("{\"status\":\"invalid_token\"}", challenge);
    }

    private byte[] clientInitialResponse(String authorizationId)
            throws OAuthBearerConfigException, IOException, UnsupportedCallbackException {
        return clientInitialResponse(authorizationId, false, Collections.emptyMap());
    }

    private byte[] clientInitialResponse(String authorizationId, boolean illegalToken, Map<String, String> customExtensions)
            throws OAuthBearerConfigException, IOException, UnsupportedCallbackException {
        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        LOGIN_CALLBACK_HANDLER.handle(new Callback[] {callback});
        OAuthBearerToken token = callback.token();
        String compactSerialization = token.value();

        String tokenValue = compactSerialization + (illegalToken ? "AB" : "");
        return new OAuthBearerClientInitialResponse(tokenValue, authorizationId, new SaslExtensions(customExtensions)).toBytes();
    }
}
