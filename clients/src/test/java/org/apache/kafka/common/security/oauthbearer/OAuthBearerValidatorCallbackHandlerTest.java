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
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenBuilder;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidatorFactory;
import org.apache.kafka.common.security.oauthbearer.internals.secured.CloseableVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest;
import org.apache.kafka.common.utils.Utils;

import org.jose4j.jws.AlgorithmIdentifiers;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OAuthBearerValidatorCallbackHandlerTest extends OAuthBearerTest {

    private static final String EXPECTED_ISSUER = "https://idp.legit.example/";

    private static final String EXPECTED_AUDIENCE = "kafka-cluster";

    @Test
    public void testBasic() throws Exception {
        String expectedAudience = "a";
        List<String> allAudiences = Arrays.asList(expectedAudience, "b", "c");
        AccessTokenBuilder builder = new AccessTokenBuilder()
            .audience(expectedAudience)
            .addCustomClaim("iss", EXPECTED_ISSUER)
            .jwk(createRsaJwk())
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256);
        String accessToken = builder.build();

        Map<String, Object> options = new HashMap<>();
        options.put(SASL_OAUTHBEARER_EXPECTED_AUDIENCE, allAudiences);
        options.put(SASL_OAUTHBEARER_EXPECTED_ISSUER, EXPECTED_ISSUER);
        Map<String, ?> configs = getSaslConfigs(options);
        OAuthBearerValidatorCallbackHandler handler = createHandler(configs, builder);

        try {
            OAuthBearerValidatorCallback callback = new OAuthBearerValidatorCallback(accessToken);
            handler.handle(new Callback[]{callback});

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
    public void testFailsToStartWhenExpectedAudienceMissingWithJwksUrl() {
        // A broker validator handler configured with a JWKS endpoint URL and expected.issuer but no expected.audience
        // must fail to start, rather than come up and accept tokens whose audience cannot be verified.
        Map<String, Object> options = new HashMap<>();
        options.put(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, "https://example.com/jwks");
        options.put(SASL_OAUTHBEARER_EXPECTED_ISSUER, EXPECTED_ISSUER);
        Map<String, ?> configs = getSaslConfigs(options);
        OAuthBearerValidatorCallbackHandler handler = new OAuthBearerValidatorCallbackHandler();
        assertThrowsWithMessage(ConfigException.class,
            () -> configureHandler(handler, configs, Collections.emptyMap()),
            SASL_OAUTHBEARER_EXPECTED_AUDIENCE);
    }

    @Test
    public void testFailsToStartWhenExpectedIssuerMissingWithJwksUrl() {
        // A broker validator handler configured with a JWKS endpoint URL and expected.audience but no expected.issuer
        // must fail to start, rather than come up and accept tokens whose issuer cannot be verified.
        Map<String, Object> options = new HashMap<>();
        options.put(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, "https://example.com/jwks");
        options.put(SASL_OAUTHBEARER_EXPECTED_AUDIENCE, Collections.singletonList(EXPECTED_AUDIENCE));
        Map<String, ?> configs = getSaslConfigs(options);
        OAuthBearerValidatorCallbackHandler handler = new OAuthBearerValidatorCallbackHandler();
        assertThrowsWithMessage(ConfigException.class,
            () -> configureHandler(handler, configs, Collections.emptyMap()),
            SASL_OAUTHBEARER_EXPECTED_ISSUER);
    }

    @Test
    public void testInvalidAccessToken() throws Exception {
        // There aren't different error messages for the validation step, so these are all the
        // same :(
        String substring = "invalid_token";
        assertInvalidAccessTokenFails("this isn't valid", substring);
        assertInvalidAccessTokenFails("this.isn't.valid", substring);
        assertInvalidAccessTokenFails(createAccessKey("this", "isn't", "valid"), substring);
        assertInvalidAccessTokenFails(createAccessKey("{}", "{}", "{}"), substring);
    }

    private void assertInvalidAccessTokenFails(String accessToken, String expectedMessageSubstring) throws Exception {
        // The injected validator is created by AccessTokenValidatorFactory, which requires expected.issuer and expected.audience.
        Map<String, Object> options = new HashMap<>();
        options.put(SASL_OAUTHBEARER_EXPECTED_ISSUER, EXPECTED_ISSUER);
        options.put(SASL_OAUTHBEARER_EXPECTED_AUDIENCE, Collections.singletonList(EXPECTED_AUDIENCE));
        Map<String, ?> configs = getSaslConfigs(options);
        OAuthBearerValidatorCallbackHandler handler = createHandler(configs, new AccessTokenBuilder());

        try {
            OAuthBearerValidatorCallback callback = new OAuthBearerValidatorCallback(accessToken);
            handler.handle(new Callback[] {callback});

            assertNull(callback.token());
            String actualMessage = callback.errorStatus();
            assertNotNull(actualMessage);
            assertTrue(actualMessage.contains(expectedMessageSubstring), String.format("The error message \"%s\" didn't contain the expected substring \"%s\"", actualMessage, expectedMessageSubstring));
        } finally {
            handler.close();
        }
    }

    private OAuthBearerValidatorCallbackHandler createHandler(Map<String, ?> options,
        AccessTokenBuilder builder) {
        OAuthBearerValidatorCallbackHandler handler = new OAuthBearerValidatorCallbackHandler();
        CloseableVerificationKeyResolver verificationKeyResolver = (jws, nestingContext) ->
                builder.jwk().getPublicKey();
        AccessTokenValidator accessTokenValidator = AccessTokenValidatorFactory.create(options, verificationKeyResolver);
        handler.init(verificationKeyResolver, accessTokenValidator);
        return handler;
    }

    private String createAccessKey(String header, String payload, String signature) {
        Base64.Encoder enc = Base64.getEncoder();
        header = enc.encodeToString(Utils.utf8(header));
        payload = enc.encodeToString(Utils.utf8(payload));
        signature = enc.encodeToString(Utils.utf8(signature));
        return String.format("%s.%s.%s", header, payload, signature);
    }

}
