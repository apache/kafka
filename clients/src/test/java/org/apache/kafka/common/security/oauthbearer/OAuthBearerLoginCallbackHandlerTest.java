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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenBuilder;
import org.apache.kafka.common.security.oauthbearer.internals.secured.DefaultJwtRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.DefaultJwtValidator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.FileJwtRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JwtRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JwtValidator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest;

import org.jose4j.jws.AlgorithmIdentifiers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class OAuthBearerLoginCallbackHandlerTest extends OAuthBearerTest {

    @AfterEach
    public void tearDown() throws Exception {
        System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
    }

    @Test
    public void testHandleTokenCallback() throws Exception {
        Map<String, ?> configs = getSaslConfigs();
        AccessTokenBuilder builder = new AccessTokenBuilder()
            .jwk(createRsaJwk())
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256);
        String accessToken = builder.build();
        JwtRetriever jwtRetriever = () -> accessToken;
        JwtValidator jwtValidator = createJwtValidator(configs);
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        handler.init(Map.of(), jwtRetriever, jwtValidator);

        try {
            OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
            handler.handle(new Callback[] {callback});

            assertNotNull(callback.token());
            OAuthBearerToken token = callback.token();
            assertEquals(accessToken, token.value());
            assertEquals(builder.subject(), token.principalName());
            assertEquals(builder.expirationSeconds() * 1000, token.lifetimeMs());
            assertEquals(builder.issuedAtSeconds() * 1000, token.startTimeMs());
        } finally {
            handler.close();
        }
    }

    @Test
    public void testHandleSaslExtensionsCallback() throws Exception {
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, "http://www.example.com");
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, "http://www.example.com");
        Map<String, Object> jaasConfig = new HashMap<>();
        jaasConfig.put(CLIENT_ID_CONFIG, "an ID");
        jaasConfig.put(CLIENT_SECRET_CONFIG, "a secret");
        jaasConfig.put("extension_foo", "1");
        jaasConfig.put("extension_bar", 2);
        jaasConfig.put("EXTENSION_baz", "3");

        JwtRetriever jwtRetriever = createJwtRetriever(configs, jaasConfig);
        JwtValidator jwtValidator = createJwtValidator(configs);
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        handler.init(jaasConfig, jwtRetriever, jwtValidator);

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

        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, "http://www.example.com");
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, "http://www.example.com");
        Map<String, Object> jaasConfig = new HashMap<>();
        jaasConfig.put(CLIENT_ID_CONFIG, "an ID");
        jaasConfig.put(CLIENT_SECRET_CONFIG, "a secret");
        jaasConfig.put(illegalKey, "this key isn't allowed per OAuthBearerClientInitialResponse.validateExtensions");

        JwtRetriever jwtRetriever = createJwtRetriever(configs, jaasConfig);
        JwtValidator jwtValidator = createJwtValidator(configs);
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        handler.init(jaasConfig, jwtRetriever, jwtValidator);

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
    public void testInvalidCallbackGeneratesUnsupportedCallbackException() {
        Map<String, ?> configs = getSaslConfigs();
        JwtRetriever jwtRetriever = () -> "test";
        JwtValidator jwtValidator = createJwtValidator(configs);
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        handler.init(Map.of(), jwtRetriever, jwtValidator);

        try {
            Callback unsupportedCallback = new Callback() { };
            assertThrows(UnsupportedCallbackException.class, () -> handler.handle(new Callback[]{unsupportedCallback}));
        } finally {
            handler.close();
        }
    }

    @Test
    public void testInvalidAccessToken() throws Exception {
        testInvalidAccessToken("this isn't valid", "Malformed JWT provided");
        testInvalidAccessToken("this.isn't.valid", "malformed Base64 URL encoded value");
        testInvalidAccessToken(createAccessKey("this", "isn't", "valid"), "malformed JSON");
        testInvalidAccessToken(createAccessKey("{}", "{}", "{}"), "exp value must be non-null");
    }

    @Test
    public void testMissingAccessToken() {
        Map<String, ?> configs = getSaslConfigs();
        JwtRetriever jwtRetriever = () -> {
            throw new IOException("The token endpoint response access_token value must be non-null");
        };
        JwtValidator jwtValidator = createJwtValidator(configs);
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        handler.init(Map.of(), jwtRetriever, jwtValidator);

        try {
            OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
            assertThrowsWithMessage(IOException.class,
                () -> handler.handle(new Callback[]{callback}),
                "token endpoint response access_token value must be non-null");
        } finally {
            handler.close();
        }
    }

    @Test
    public void testFileTokenRetrieverHandlesNewline() throws IOException {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        long cur = cal.getTimeInMillis() / 1000;
        String exp = "" + (cur + 60 * 60);  // 1 hour in future
        String iat = "" + cur;

        String expected = createAccessKey("{}", String.format("{\"exp\":%s, \"iat\":%s, \"sub\":\"subj\"}", exp, iat), "sign");
        String withNewline = expected + "\n";

        File tmpDir = createTempDir("access-token");
        File accessTokenFile = createTempFile(tmpDir, "access-token-", ".json", withNewline);

        Map<String, ?> configs = getSaslConfigs();
        JwtRetriever jwtRetriever = new FileJwtRetriever(accessTokenFile.toPath());
        JwtValidator jwtValidator = createJwtValidator(configs);
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        handler.init(Map.of(), jwtRetriever, jwtValidator);

        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        try {
            handler.handle(new Callback[]{callback});
            assertEquals(callback.token().value(), expected);
        } catch (Exception e) {
            fail(e);
        } finally {
            handler.close();
        }
    }

    @Test
    public void testNotConfigured() {
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        assertThrowsWithMessage(IllegalStateException.class, () -> handler.handle(new Callback[] {}), "first call the configure method");
    }

    private void testInvalidAccessToken(String accessToken, String expectedMessageSubstring) throws Exception {
        Map<String, ?> configs = getSaslConfigs();
        JwtRetriever jwtRetriever = () -> accessToken;
        JwtValidator jwtValidator = createJwtValidator(configs);
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        handler.init(Map.of(), jwtRetriever, jwtValidator);

        try {
            OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
            handler.handle(new Callback[]{callback});

            assertNull(callback.token());
            String actualMessage = callback.errorDescription();
            assertNotNull(actualMessage);
            assertTrue(actualMessage.contains(expectedMessageSubstring), String.format(
                "The error message \"%s\" didn't contain the expected substring \"%s\"",
                actualMessage, expectedMessageSubstring));
        } finally {
            handler.close();
        }
    }

    private static DefaultJwtRetriever createJwtRetriever(Map<String, ?> configs) {
        return createJwtRetriever(configs, Map.of());
    }

    private static DefaultJwtRetriever createJwtRetriever(Map<String, ?> configs, Map<String, Object> jaasConfigs) {
        return new DefaultJwtRetriever(configs, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, jaasConfigs);
    }

    private static DefaultJwtValidator createJwtValidator(Map<String, ?> configs) {
        return new DefaultJwtValidator(configs, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM);
    }
}
