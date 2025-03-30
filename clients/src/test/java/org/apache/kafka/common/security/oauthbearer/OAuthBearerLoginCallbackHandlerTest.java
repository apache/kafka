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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JwtBuilder;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest;

import org.jose4j.jws.AlgorithmIdentifiers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OAuthBearerLoginCallbackHandlerTest extends OAuthBearerTest {

    @AfterEach
    public void tearDown() throws Exception {
        System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
    }

    @Test
    public void testHandleTokenCallback() throws Exception {
        JwtBuilder builder = new JwtBuilder()
            .jwk(createRsaJwk())
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256);
        String jwt = builder.build();
        JwtRetriever jwtRetriever = mock(JwtRetriever.class);
        when(jwtRetriever.retrieve()).thenReturn(jwt);

        try (OAuthBearerLoginCallbackHandler handler = createHandler(jwtRetriever)) {
            OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
            handler.handle(new Callback[]{callback});

            assertNotNull(callback.token());
            OAuthBearerToken token = callback.token();
            assertEquals(jwt, token.value());
            assertEquals(builder.subject(), token.principalName());
            assertEquals(builder.expirationSeconds() * 1000, token.lifetimeMs());
            assertEquals(builder.issuedAtSeconds() * 1000, token.startTimeMs());
        }
    }

    @Test
    public void testHandleSaslExtensionsCallback() throws Exception {
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, "http://www.example.com");
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, "http://www.example.com");
        Map<String, Object> jaasConfig = new HashMap<>();
        jaasConfig.put("clientId", "an ID");
        jaasConfig.put("clientSecret", "a secret");
        jaasConfig.put("extension_foo", "1");
        jaasConfig.put("extension_bar", 2);
        jaasConfig.put("EXTENSION_baz", "3");
        configureHandler(handler, configs, jaasConfig);

        try {
            SaslExtensionsCallback callback = new SaslExtensionsCallback();
            handler.handle(new Callback[]{callback});

            assertNotNull(callback.extensions());
            Map<String, String> extensions = callback.extensions().map();
            assertEquals("1", extensions.get("foo"));
            assertEquals("2", extensions.get("bar"));
            assertNull(extensions.get("baz"));
            assertEquals(2, extensions.size());
        } finally {
            handler.close();
        }
    }

    @Test
    public void testHandleSaslExtensionsCallbackWithInvalidExtension() {
        String illegalKey = "extension_" + OAuthBearerClientInitialResponse.AUTH_KEY;

        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, "http://www.example.com");
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, "http://www.example.com");
        Map<String, Object> jaasConfig = new HashMap<>();
        jaasConfig.put("clientId", "an ID");
        jaasConfig.put("clientSecret", "a secret");
        jaasConfig.put(illegalKey, "this key isn't allowed per OAuthBearerClientInitialResponse.validateExtensions");
        configureHandler(handler, configs, jaasConfig);

        try {
            SaslExtensionsCallback callback = new SaslExtensionsCallback();
            assertThrowsWithMessage(ConfigException.class,
                () -> handler.handle(new Callback[]{callback}),
                "Extension name " + OAuthBearerClientInitialResponse.AUTH_KEY + " is invalid");
        } finally {
            handler.close();
        }
    }

    @Test
    public void testConfigureThrowsExceptionOnJwtValidatorConfigure() {
        try (OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
             JwtRetriever jwtRetriever = mock(JwtRetriever.class);
             JwtValidator jwtValidator = mock(JwtValidator.class)) {

            doThrow(new KafkaException("Forced failure")).when(jwtValidator).configure(any(), any(), any());

            assertThrows(
                KafkaException.class,
                () -> handler.configure(jwtRetriever, jwtValidator, getSaslConfigs(), OAUTHBEARER_MECHANISM, List.of())
            );
        }
    }

    @Test
    public void testConfigureThrowsExceptionOnJwtValidatorClose() {
        try (OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
             JwtRetriever jwtRetriever = mock(JwtRetriever.class)) {
            JwtValidator jwtValidator = mock(JwtValidator.class);
            doThrow(new KafkaException("Forced failure")).when(jwtValidator).close();
            handler.configure(jwtRetriever, jwtValidator, getSaslConfigs(), OAUTHBEARER_MECHANISM, List.of());
            assertDoesNotThrow(handler::close);
        }
    }

    @Test
    public void testInvalidCallbackGeneratesUnsupportedCallbackException() throws IOException {
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        JwtRetriever jwtRetriever = mock(JwtRetriever.class);
        when(jwtRetriever.retrieve()).thenReturn("foo");
        JwtValidator jwtValidator = new ClientJwtValidator();
        handler.configure(jwtRetriever, jwtValidator, getSaslConfigs(), OAUTHBEARER_MECHANISM, List.of());

        try {
            Callback unsupportedCallback = new Callback() { };
            assertThrows(UnsupportedCallbackException.class, () -> handler.handle(new Callback[]{unsupportedCallback}));
        } finally {
            handler.close();
        }
    }

    @Test
    public void testInvalidJwt() throws Exception {
        testInvalidJwt("this isn't valid", "Malformed JWT provided");
        testInvalidJwt("this.isn't.valid", "malformed Base64 URL encoded value");
        testInvalidJwt(createAccessKey("this", "isn't", "valid"), "malformed JSON");
        testInvalidJwt(createAccessKey("{}", "{}", "{}"), "exp value must be non-null");
    }

    @Test
    public void testMissingJwt() throws IOException {
        JwtRetriever jwtRetriever = mock(JwtRetriever.class);
        when(jwtRetriever.retrieve()).thenThrow(new JwtRetrieverException("The token endpoint response id_token value must be non-null"));

        try (OAuthBearerLoginCallbackHandler handler = createHandler(jwtRetriever)) {
            OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
            assertThrowsWithMessage(
                JwtRetrieverException.class,
                () -> handler.handle(new Callback[]{callback}),
                "token endpoint response id_token value must be non-null"
            );
        }
    }

    @Test
    public void testNotConfigured() {
        try (OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler()) {
            assertThrowsWithMessage(IllegalStateException.class, () -> handler.handle(new Callback[] {}), "first call the configure method");
        }
    }

    @Test
    public void testConfigureWithJwtFile() throws Exception {
        String expected = "{}";

        File tmpDir = createTempDir("jwt");
        File jwtFile = createTempFile(tmpDir, "jwt-", ".json", expected);
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, jwtFile.toURI().toString());

        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, jwtFile.toURI().toString());
        Map<String, Object> jaasConfig = Collections.emptyMap();
        configureHandler(handler, configs, jaasConfig);
        assertInstanceOf(DefaultJwtRetriever.class, handler.jwtRetriever);
        assertInstanceOf(FileJwtRetriever.class, ((DefaultJwtRetriever) handler.jwtRetriever).delegate());
    }

    @Test
    public void testConfigureWithAccessClientCredentials() {
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, "http://www.example.com");
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, "http://www.example.com");
        Map<String, Object> jaasConfig = new HashMap<>();
        jaasConfig.put("clientId", "an ID");
        jaasConfig.put("clientSecret", "a secret");
        configureHandler(handler, configs, jaasConfig);
        assertInstanceOf(DefaultJwtRetriever.class, handler.jwtRetriever);
        assertInstanceOf(ClientCredentialsJwtRetriever.class, ((DefaultJwtRetriever) handler.jwtRetriever).delegate());
    }

    private void testInvalidJwt(String jwt, String expectedMessageSubstring) throws Exception {
        JwtRetriever jwtRetriever = mock(JwtRetriever.class);
        when(jwtRetriever.retrieve()).thenReturn(jwt);

        try (OAuthBearerLoginCallbackHandler handler = createHandler(jwtRetriever)) {
            OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
            handler.handle(new Callback[]{callback});

            assertNull(callback.token());
            String actualMessage = callback.errorDescription();
            assertNotNull(actualMessage);
            assertTrue(actualMessage.contains(expectedMessageSubstring), String.format(
                "The error message \"%s\" didn't contain the expected substring \"%s\"",
                actualMessage, expectedMessageSubstring));
        }
    }

    protected OAuthBearerLoginCallbackHandler createHandler(JwtRetriever jwtRetriever) {
        Map<String, ?> configs = getSaslConfigs();
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        JwtValidator jwtValidator = new ClientJwtValidator();
        handler.configure(jwtRetriever, jwtValidator, configs, OAUTHBEARER_MECHANISM, List.of());
        return handler;
    }
}
