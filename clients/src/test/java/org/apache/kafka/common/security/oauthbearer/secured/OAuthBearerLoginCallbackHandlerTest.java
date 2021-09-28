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

package org.apache.kafka.common.security.oauthbearer.secured;

import static org.apache.kafka.common.security.oauthbearer.secured.LoginCallbackHandlerConfiguration.ACCESS_TOKEN_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.LoginCallbackHandlerConfiguration.ACCESS_TOKEN_FILE_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.LoginCallbackHandlerConfiguration.CLIENT_ID_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.LoginCallbackHandlerConfiguration.CLIENT_SECRET_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.LoginCallbackHandlerConfiguration.TOKEN_ENDPOINT_URI_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

public class OAuthBearerLoginCallbackHandlerTest extends OAuthBearerTest {

    @Test
    public void testHandleTokenCallback() throws Exception {
        AccessTokenBuilder builder = new AccessTokenBuilder();
        String accessToken = builder.build();

        OAuthBearerLoginCallbackHandler handler = createHandler(new StaticAccessTokenRetriever(accessToken));

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
        Map<String, Object> options = new HashMap<>();
        options.put(CLIENT_ID_CONFIG, "an ID");
        options.put(CLIENT_SECRET_CONFIG, "a secret");
        options.put(TOKEN_ENDPOINT_URI_CONFIG, "http://www.example.com");
        options.put("Extension_foo", "1");
        options.put("Extension_bar", 2);
        options.put("EXTENSION_baz", "3");
        configureHandler(handler, options);

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
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        Map<String, Object> options = new HashMap<>();
        options.put(CLIENT_ID_CONFIG, "an ID");
        options.put(CLIENT_SECRET_CONFIG, "a secret");
        options.put(TOKEN_ENDPOINT_URI_CONFIG, "http://www.example.com");
        String illegalKey = "Extension_" + OAuthBearerClientInitialResponse.AUTH_KEY;
        options.put(illegalKey, "this key isn't allowed per OAuthBearerClientInitialResponse.validateExtensions");
        configureHandler(handler, options);

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
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        LoginCallbackHandlerConfiguration conf = new LoginCallbackHandlerConfiguration(Collections.emptyMap());
        AccessTokenRetriever accessTokenRetriever = new StaticAccessTokenRetriever("foo");
        AccessTokenValidator accessTokenValidator = OAuthBearerLoginCallbackHandler.configureAccessTokenValidator(conf);
        handler.configure(accessTokenRetriever, accessTokenValidator);

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
        OAuthBearerLoginCallbackHandler handler = createHandler(() -> {
            throw new IOException("The token endpoint response access_token value must be non-null");
        });

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
    public void testCreateHandlerWithInvalidRetrieverOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put(ACCESS_TOKEN_CONFIG, "test1");
        options.put(CLIENT_ID_CONFIG, "test2");

        LoginCallbackHandlerConfiguration conf = new LoginCallbackHandlerConfiguration(options);

        assertThrowsWithMessage(ConfigException.class,
            () -> OAuthBearerLoginCallbackHandler.configureAccessTokenRetriever(conf),
            "must include only one of");
    }

    @Test
    public void testConfigureStaticAccessTokenRetriever() throws Exception {
        String expected = "test1";
        Map<String, Object> options = Collections.singletonMap(ACCESS_TOKEN_CONFIG, expected);
        LoginCallbackHandlerConfiguration conf = new LoginCallbackHandlerConfiguration(options);
        AccessTokenRetriever accessTokenRetriever = OAuthBearerLoginCallbackHandler.configureAccessTokenRetriever(conf);
        assertEquals(expected, accessTokenRetriever.retrieve());
    }

    @Test
    public void testConfigureRefreshingFileAccessTokenRetriever() throws Exception {
        String expected = "{}";

        File tmpDir = createTempDir("access-token");
        File accessTokenFile = createTempFile(tmpDir, "access-token-", ".json", expected);

        Map<String, Object> options = Collections.singletonMap(ACCESS_TOKEN_FILE_CONFIG, accessTokenFile.getAbsolutePath());
        LoginCallbackHandlerConfiguration conf = new LoginCallbackHandlerConfiguration(options);
        AccessTokenRetriever accessTokenRetriever = OAuthBearerLoginCallbackHandler.configureAccessTokenRetriever(conf);
        accessTokenRetriever.init();
        assertEquals(expected, accessTokenRetriever.retrieve());
    }

    @Test
    public void testConfigureRefreshingFileAccessTokenRetrieverWithInvalidDirectory() {
        // Should fail because the parent path doesn't exist.
        Map<String, Object> options = Collections.singletonMap(ACCESS_TOKEN_FILE_CONFIG, "/tmp/this-directory-does-not-exist/foo.json");
        assertThrowsWithMessage(ConfigException.class, () -> new LoginCallbackHandlerConfiguration(options), "that doesn't exist");
    }

    @Test
    public void testConfigureRefreshingFileAccessTokenRetrieverWithInvalidFile() throws Exception {
        // Should fail because the while the parent path exists, the file itself doesn't.
        File tmpDir = createTempDir("this-directory-does-exist");
        File accessTokenFile = new File(tmpDir, "this-file-does-not-exist.json");

        Map<String, Object> options = Collections.singletonMap(ACCESS_TOKEN_FILE_CONFIG, accessTokenFile.getAbsolutePath());
        assertThrowsWithMessage(ConfigException.class, () -> new LoginCallbackHandlerConfiguration(options), "that doesn't exist");
    }

    @Test
    public void testNotConfigured() {
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        assertThrowsWithMessage(IllegalStateException.class, () -> handler.handle(new Callback[] {}), "first call configure method");
    }

    @Test
    public void testConfigureThrowsExceptionOnAccessTokenValidatorInit() {
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        LoginCallbackHandlerConfiguration conf = new LoginCallbackHandlerConfiguration(Collections.emptyMap());
        AccessTokenRetriever accessTokenRetriever = new AccessTokenRetriever() {
            @Override
            public void init() throws IOException {
                throw new IOException("My init had an error!");
            }
            @Override
            public String retrieve() {
                return "dummy";
            }
        };

        AccessTokenValidator accessTokenValidator = OAuthBearerLoginCallbackHandler.configureAccessTokenValidator(conf);

        assertThrowsWithMessage(KafkaException.class, () -> handler.configure(accessTokenRetriever, accessTokenValidator), "encountered an error when initializing");
    }

    @Test
    public void testConfigureThrowsExceptionOnAccessTokenValidatorClose() {
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        LoginCallbackHandlerConfiguration conf = new LoginCallbackHandlerConfiguration(Collections.emptyMap());
        AccessTokenRetriever accessTokenRetriever = new AccessTokenRetriever() {
            @Override
            public void close() throws IOException {
                throw new IOException("My close had an error!");
            }
            @Override
            public String retrieve() {
                return "dummy";
            }
        };

        AccessTokenValidator accessTokenValidator = OAuthBearerLoginCallbackHandler.configureAccessTokenValidator(conf);
        handler.configure(accessTokenRetriever, accessTokenValidator);

        // Basically asserting this doesn't throw an exception :(
        handler.close();
    }

    @Test
    public void testConfigureWithAccessToken() {
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        Map<String, Object> options = Collections.singletonMap(ACCESS_TOKEN_CONFIG, "some.access.token");
        configureHandler(handler, options);
    }

    @Test
    public void testConfigureWithAccessTokenFile() throws Exception {
        String expected = "{}";

        File tmpDir = createTempDir("access-token");
        File accessTokenFile = createTempFile(tmpDir, "access-token-", ".json", expected);

        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        Map<String, Object> options = Collections.singletonMap(ACCESS_TOKEN_FILE_CONFIG, accessTokenFile.getAbsolutePath());
        configureHandler(handler, options);
    }

    @Test
    public void testConfigureWithAccessClientCredentials() {
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        Map<String, Object> options = new HashMap<>();
        options.put(CLIENT_ID_CONFIG, "an ID");
        options.put(CLIENT_SECRET_CONFIG, "a secret");
        options.put(TOKEN_ENDPOINT_URI_CONFIG, "http://www.example.com");
        configureHandler(handler, options);
    }

    private OAuthBearerLoginCallbackHandler createHandler(AccessTokenRetriever accessTokenRetriever) {
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        LoginCallbackHandlerConfiguration conf = new LoginCallbackHandlerConfiguration(Collections.emptyMap());
        AccessTokenValidator accessTokenValidator = OAuthBearerLoginCallbackHandler.configureAccessTokenValidator(conf);
        handler.configure(accessTokenRetriever, accessTokenValidator);
        return handler;
    }

    private void testInvalidAccessToken(String accessToken, String expectedMessageSubstring) throws Exception {
        OAuthBearerLoginCallbackHandler handler = createHandler(new StaticAccessTokenRetriever(accessToken));

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

}
