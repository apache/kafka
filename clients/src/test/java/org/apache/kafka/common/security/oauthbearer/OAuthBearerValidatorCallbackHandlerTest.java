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
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenBuilder;
import org.apache.kafka.common.security.oauthbearer.internals.secured.CloseableVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.internals.secured.DefaultJwtValidator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JwtValidator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ValidateException;
import org.apache.kafka.common.utils.Utils;

import org.jose4j.jws.AlgorithmIdentifiers;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OAuthBearerValidatorCallbackHandlerTest extends OAuthBearerTest {

    @Test
    public void testBasic() throws Exception {
        String expectedAudience = "a";
        List<String> allAudiences = Arrays.asList(expectedAudience, "b", "c");
        AccessTokenBuilder builder = new AccessTokenBuilder()
            .audience(expectedAudience)
            .jwk(createRsaJwk())
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256);
        String accessToken = builder.build();

        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_EXPECTED_AUDIENCE, allAudiences);
        CloseableVerificationKeyResolver verificationKeyResolver = createVerificationKeyResolver(builder);
        JwtValidator jwtValidator = createJwtValidator(configs, verificationKeyResolver);
        OAuthBearerValidatorCallbackHandler handler = new TestOAuthBearerValidatorCallbackHandler(verificationKeyResolver, jwtValidator);
        configureHandler(handler, configs);

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
    public void testInvalidAccessToken() throws Exception {
        // There aren't different error messages for the validation step, so these are all the
        // same :(
        String substring = "invalid_token";
        assertInvalidAccessTokenFails("this isn't valid", substring);
        assertInvalidAccessTokenFails("this.isn't.valid", substring);
        assertInvalidAccessTokenFails(createAccessKey("this", "isn't", "valid"), substring);
        assertInvalidAccessTokenFails(createAccessKey("{}", "{}", "{}"), substring);
    }

    @Test
    public void testConfigureThrowsExceptionOnJwtValidatorInit() throws IOException {
        JwtValidator jwtValidator = new JwtValidator() {
            @Override
            public void init() throws IOException {
                throw new IOException("My init had an error!");
            }

            @Override
            public OAuthBearerToken validate(String accessToken) throws ValidateException {
                return null;
            }
        };

        AccessTokenBuilder builder = new AccessTokenBuilder()
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256);
        Map<String, ?> configs = getSaslConfigs();
        CloseableVerificationKeyResolver verificationKeyResolver = createVerificationKeyResolver(builder);
        OAuthBearerValidatorCallbackHandler handler = new TestOAuthBearerValidatorCallbackHandler(verificationKeyResolver, jwtValidator);

        assertThrowsWithMessage(
            KafkaException.class,
            () -> configureHandler(handler, configs),
            "encountered an error when initializing"
        );
    }

    @Test
    public void testConfigureThrowsExceptionOnJwtValidatorClose() throws IOException {
        JwtValidator jwtValidator = new JwtValidator() {
            @Override
            public void close() throws IOException {
                throw new IOException("My close had an error!");
            }

            @Override
            public OAuthBearerToken validate(String accessToken) throws ValidateException {
                return null;
            }
        };

        AccessTokenBuilder builder = new AccessTokenBuilder()
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256);
        Map<String, ?> configs = getSaslConfigs();
        CloseableVerificationKeyResolver verificationKeyResolver = createVerificationKeyResolver(builder);
        OAuthBearerValidatorCallbackHandler handler = new TestOAuthBearerValidatorCallbackHandler(verificationKeyResolver, jwtValidator);
        configureHandler(handler, configs);

        assertDoesNotThrow(handler::close);
    }

    private void assertInvalidAccessTokenFails(String accessToken, String expectedMessageSubstring) throws Exception {
        AccessTokenBuilder builder = new AccessTokenBuilder()
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256);
        Map<String, ?> configs = getSaslConfigs();
        CloseableVerificationKeyResolver verificationKeyResolver = createVerificationKeyResolver(builder);
        JwtValidator jwtValidator = createJwtValidator(configs, verificationKeyResolver);
        OAuthBearerValidatorCallbackHandler handler = new TestOAuthBearerValidatorCallbackHandler(verificationKeyResolver, jwtValidator);
        configureHandler(handler, configs);

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

    private JwtValidator createJwtValidator(Map<String, ?> configs, CloseableVerificationKeyResolver verificationKeyResolver) {
        return new DefaultJwtValidator(configs, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, verificationKeyResolver);
    }

    private CloseableVerificationKeyResolver createVerificationKeyResolver(AccessTokenBuilder builder) {
        return (jws, nestingContext) -> builder.jwk().getPublicKey();
    }

    private String createAccessKey(String header, String payload, String signature) {
        Base64.Encoder enc = Base64.getEncoder();
        header = enc.encodeToString(Utils.utf8(header));
        payload = enc.encodeToString(Utils.utf8(payload));
        signature = enc.encodeToString(Utils.utf8(signature));
        return String.format("%s.%s.%s", header, payload, signature);
    }

    private static class TestOAuthBearerValidatorCallbackHandler extends OAuthBearerValidatorCallbackHandler {

        public TestOAuthBearerValidatorCallbackHandler(CloseableVerificationKeyResolver verificationKeyResolver,
                                                       JwtValidator jwtValidator) {
            this.verificationKeyResolver = verificationKeyResolver;
            this.jwtValidator = jwtValidator;
        }

        @Override
        public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
            try {
                verificationKeyResolver.init();
            } catch (Exception e) {
                throw new KafkaException("The OAuth validator callback encountered an error when initializing the VerificationKeyResolver", e);
            }

            try {
                jwtValidator.init();
            } catch (IOException e) {
                throw new KafkaException("The OAuth validator callback encountered an error when initializing the JwtValidator", e);
            }
        }
    }
}
