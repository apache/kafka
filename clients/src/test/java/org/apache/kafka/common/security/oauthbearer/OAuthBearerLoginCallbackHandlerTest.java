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

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenBuilder;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidatorFactory;
import org.apache.kafka.common.security.oauthbearer.internals.secured.FileTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpAccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest;
import org.apache.kafka.common.utils.Utils;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.junit.jupiter.api.Test;

public class OAuthBearerLoginCallbackHandlerTest extends OAuthBearerTest {

    @Test
    public void testHandleTokenCallback() throws Exception {
        Map<String, ?> configs = getSaslConfigs();
        AccessTokenBuilder builder = new AccessTokenBuilder()
            .jwk(createRsaJwk())
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256);
        String accessToken = builder.build();
        AccessTokenRetriever accessTokenRetriever = () -> accessToken;

        OAuthBearerLoginCallbackHandler handler = createHandler(accessTokenRetriever, configs);

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
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, "http://www.example.com");
        Map<String, Object> jaasConfig = new HashMap<>();
        jaasConfig.put(CLIENT_ID_CONFIG, "an ID");
        jaasConfig.put(CLIENT_SECRET_CONFIG, "a secret");
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
        Map<String, Object> jaasConfig = new HashMap<>();
        jaasConfig.put(CLIENT_ID_CONFIG, "an ID");
        jaasConfig.put(CLIENT_SECRET_CONFIG, "a secret");
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
    public void testInvalidCallbackGeneratesUnsupportedCallbackException() {
        Map<String, ?> configs = getSaslConfigs();
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        AccessTokenRetriever accessTokenRetriever = () -> "foo";
        AccessTokenValidator accessTokenValidator = AccessTokenValidatorFactory.create(configs);
        handler.init(accessTokenRetriever, accessTokenValidator);

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
        AccessTokenRetriever accessTokenRetriever = () -> {
            throw new IOException("The token endpoint response access_token value must be non-null");
        };
        Map<String, ?> configs = getSaslConfigs();
        OAuthBearerLoginCallbackHandler handler = createHandler(accessTokenRetriever, configs);

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
        OAuthBearerLoginCallbackHandler handler = createHandler(new FileTokenRetriever(accessTokenFile.toPath()), configs);
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
        assertThrowsWithMessage(IllegalStateException.class, () -> handler.handle(new Callback[] {}), "first call the configure or init method");
    }

    @Test
    public void testConfigureWithAccessTokenFile() throws Exception {
        String expected = "{}";

        File tmpDir = createTempDir("access-token");
        File accessTokenFile = createTempFile(tmpDir, "access-token-", ".json", expected);

        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, accessTokenFile.toURI().toString());
        Map<String, Object> jaasConfigs = Collections.emptyMap();
        configureHandler(handler, configs, jaasConfigs);
        assertTrue(handler.getAccessTokenRetriever() instanceof FileTokenRetriever);
    }

    @Test
    public void testConfigureWithAccessClientCredentials() {
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, "http://www.example.com");
        Map<String, Object> jaasConfigs = new HashMap<>();
        jaasConfigs.put(CLIENT_ID_CONFIG, "an ID");
        jaasConfigs.put(CLIENT_SECRET_CONFIG, "a secret");
        configureHandler(handler, configs, jaasConfigs);
        assertTrue(handler.getAccessTokenRetriever() instanceof HttpAccessTokenRetriever);
    }

    private void testInvalidAccessToken(String accessToken, String expectedMessageSubstring) throws Exception {
        Map<String, ?> configs = getSaslConfigs();
        OAuthBearerLoginCallbackHandler handler = createHandler(() -> accessToken, configs);

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

    private String createAccessKey(String header, String payload, String signature) {
        Base64.Encoder enc = Base64.getEncoder();
        header = enc.encodeToString(Utils.utf8(header));
        payload = enc.encodeToString(Utils.utf8(payload));
        signature = enc.encodeToString(Utils.utf8(signature));
        return String.format("%s.%s.%s", header, payload, signature);
    }

    private OAuthBearerLoginCallbackHandler createHandler(AccessTokenRetriever accessTokenRetriever, Map<String, ?> configs) {
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        AccessTokenValidator accessTokenValidator = AccessTokenValidatorFactory.create(configs);
        handler.init(accessTokenRetriever, accessTokenValidator);
        return handler;
    }

}
