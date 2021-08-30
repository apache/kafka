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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

public class OAuthBearerLoginCallbackHandlerTest extends OAuthBearerTest {

    @Test
    public void test() throws Exception {
        AccessTokenBuilder builder = new AccessTokenBuilder();
        String accessToken = builder.build();

        // Set the response from the fake URL
        String response = createJsonJwtSection(node -> node.put("access_token", accessToken));
        OAuthBearerLoginCallbackHandler handler = createHandler();
        handler.setConnectionSupplier(() -> createHttpURLConnection(response, 200));

        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        handler.handle(new Callback[] {callback});

        assertNotNull(callback.token());
        OAuthBearerToken token = callback.token();
        assertEquals(accessToken, token.value());
        assertEquals(builder.subject(), token.principalName());
        assertEquals(builder.expirationSeconds() * 1000, token.lifetimeMs());
        assertEquals(builder.issuedAtSeconds() * 1000, token.startTimeMs());
    }

    @Test
    public void testInvalidAccessToken() throws Exception {
        assertInvalidAccessTokenFails("this isn't valid", "Malformed JWT provided");
        assertInvalidAccessTokenFails("this.isn't.valid", "malformed Base64 URL encoded value");
        assertInvalidAccessTokenFails(createAccessKey("this", "isn't", "valid"), "malformed JSON");
        assertInvalidAccessTokenFails(createAccessKey("{}", "{}", "{}"), "exp value must be non-null");
    }

    @Test
    public void testMissingAccessToken() {
        String response = "{}";     // No token in there...
        OAuthBearerLoginCallbackHandler handler = createHandler();
        handler.setConnectionSupplier(() -> createHttpURLConnection(response, 200));

        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        assertThrowsWithMessage(IOException.class,
            () -> handler.handle(new Callback[] {callback}),
            "token endpoint response access_token value must be non-null");
    }

    @Test
    public void test404() {
        String response = "Not found!";
        OAuthBearerLoginCallbackHandler handler = createHandler();
        handler.setConnectionSupplier(() -> createHttpURLConnection(response, 404));

        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        assertThrowsWithMessage(IOException.class,
            () -> handler.handle(new Callback[] {callback}),
            "unexpected response code");
    }

    private OAuthBearerLoginCallbackHandler createHandler() {
        Map<String, Object> options = new HashMap<>();
        options.put(LoginCallbackHandlerConfiguration.CLIENT_ID_CONFIG, "test");
        options.put(LoginCallbackHandlerConfiguration.CLIENT_SECRET_CONFIG, "test");
        options.put(LoginCallbackHandlerConfiguration.TOKEN_ENDPOINT_URI_CONFIG, "https://www.example.com");
        return createHandler(options);
    }

    private OAuthBearerLoginCallbackHandler createHandler(Map<String, Object> options) {
        TestJaasConfig config = new TestJaasConfig();
        config.createOrUpdateEntry("KafkaClient", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule", options);
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        List<AppConfigurationEntry> kafkaClient = Collections.singletonList(config.getAppConfigurationEntry("KafkaClient")[0]);
        handler.configure(Collections.emptyMap(), OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, kafkaClient);
        return handler;
    }

    private void assertInvalidAccessTokenFails(String accessToken, String expectedMessageSubstring) throws Exception {
        // Set the response from the fake URL
        String response = createJsonJwtSection(node -> node.put("access_token", accessToken));
        OAuthBearerLoginCallbackHandler handler = createHandler();
        handler.setConnectionSupplier(() -> createHttpURLConnection(response, 200));

        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        handler.handle(new Callback[] {callback});

        assertNull(callback.token());
        String actualMessage = callback.errorDescription();
        assertNotNull(actualMessage);
        assertTrue(actualMessage.contains(expectedMessageSubstring), String.format("The error message \"%s\" didn't contain the expected substring \"%s\"", actualMessage, expectedMessageSubstring));
    }

    private String createAccessKey(String header, String payload, String signature) {
        Base64.Encoder enc = Base64.getEncoder();
        header = enc.encodeToString(Utils.utf8(header));
        payload = enc.encodeToString(Utils.utf8(payload));
        signature = enc.encodeToString(Utils.utf8(signature));
        return String.format("%s.%s.%s", header, payload, signature);
    }

}
