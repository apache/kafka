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

import org.jose4j.jws.AlgorithmIdentifiers;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        OAuthBearerValidatorCallbackHandler handler = new OAuthBearerValidatorCallbackHandler();
        handler.init(verificationKeyResolver, jwtValidator);

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
    public void testHandlerInitThrowsException() throws IOException {
        IOException initError = new IOException("init() error");

        AccessTokenBuilder builder = new AccessTokenBuilder()
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256);
        CloseableVerificationKeyResolver verificationKeyResolver = createVerificationKeyResolver(builder);
        JwtValidator jwtValidator = new JwtValidator() {
            @Override
            public void init() throws IOException {
                throw initError;
            }

            @Override
            public OAuthBearerToken validate(String accessToken) throws ValidateException {
                return null;
            }
        };

        OAuthBearerValidatorCallbackHandler handler = new OAuthBearerValidatorCallbackHandler();

        // An error initializing the JwtValidator should cause OAuthBearerValidatorCallbackHandler.init() to fail.
        KafkaException root = assertThrows(
            KafkaException.class,
            () -> handler.init(verificationKeyResolver, jwtValidator)
        );
        assertNotNull(root.getCause());
        assertEquals(initError, root.getCause());
    }

    @Test
    public void testHandlerCloseDoesNotThrowException() throws IOException {
        AccessTokenBuilder builder = new AccessTokenBuilder()
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256);
        CloseableVerificationKeyResolver verificationKeyResolver = createVerificationKeyResolver(builder);
        JwtValidator jwtValidator = new JwtValidator() {
            @Override
            public void close() throws IOException {
                throw new IOException("close() error");
            }

            @Override
            public OAuthBearerToken validate(String accessToken) throws ValidateException {
                return null;
            }
        };

        OAuthBearerValidatorCallbackHandler handler = new OAuthBearerValidatorCallbackHandler();
        handler.init(verificationKeyResolver, jwtValidator);

        // An error closings the JwtValidator should *not* cause OAuthBearerValidatorCallbackHandler.close() to fail.
        assertDoesNotThrow(handler::close);
    }

    private void assertInvalidAccessTokenFails(String accessToken, String expectedMessageSubstring) throws Exception {
        AccessTokenBuilder builder = new AccessTokenBuilder()
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256);
        Map<String, ?> configs = getSaslConfigs();
        CloseableVerificationKeyResolver verificationKeyResolver = createVerificationKeyResolver(builder);
        JwtValidator jwtValidator = createJwtValidator(configs, verificationKeyResolver);
        OAuthBearerValidatorCallbackHandler handler = new OAuthBearerValidatorCallbackHandler();
        handler.init(verificationKeyResolver, jwtValidator);

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
}
