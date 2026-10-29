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
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenBuilder;
import org.apache.kafka.common.security.oauthbearer.internals.secured.CloseableVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OAuthBearerValidatorCallbackHandlerTest extends OAuthBearerTest {

    private static final String ATTACKER_PRINCIPAL = "kafka-admin";

    private static final String EXPECTED_ISSUER = "https://idp.legit.example/";

    private static final String EXPECTED_AUDIENCE = "kafka-cluster";

    @AfterEach
    public void tearDown() {
        System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
    }

    @Test
    public void testBasic() throws Exception {
        String expectedAudience = "a";
        List<String> allAudiences = Arrays.asList(expectedAudience, "b", "c");
        AccessTokenBuilder builder = new AccessTokenBuilder()
            .audience(expectedAudience)
            .jwk(createRsaJwk())
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256)
            .addCustomClaim("iss", EXPECTED_ISSUER);
        String accessToken = builder.build();

        Map<String, ?> configs = getSaslConfigs(Map.of(
            SASL_OAUTHBEARER_EXPECTED_AUDIENCE, allAudiences,
            SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER, EXPECTED_ISSUER));
        CloseableVerificationKeyResolver verificationKeyResolver = createVerificationKeyResolver(builder);
        JwtValidator jwtValidator = createJwtValidator(verificationKeyResolver);
        OAuthBearerValidatorCallbackHandler handler = new OAuthBearerValidatorCallbackHandler();
        handler.configure(
            configs,
            OAUTHBEARER_MECHANISM,
            getJaasConfigEntries(),
            verificationKeyResolver,
            jwtValidator
        );

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
        assertInvalidAccessTokenFails(createJwt("this", "isn't", "valid"), substring);
        assertInvalidAccessTokenFails(createJwt("{}", "{}", "{}"), substring);
    }

    @Test
    public void testHandlerConfigureThrowsException() throws IOException {
        KafkaException configureError = new KafkaException("configure() error");

        AccessTokenBuilder builder = new AccessTokenBuilder()
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256);
        CloseableVerificationKeyResolver verificationKeyResolver = createVerificationKeyResolver(builder);
        JwtValidator jwtValidator = new JwtValidator() {
            @Override
            public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
                throw configureError;
            }

            @Override
            public OAuthBearerToken validate(String accessToken) throws JwtValidatorException {
                return null;
            }
        };

        OAuthBearerValidatorCallbackHandler handler = new OAuthBearerValidatorCallbackHandler();

        // An error initializing the JwtValidator should cause OAuthBearerValidatorCallbackHandler.init() to fail.
        KafkaException error = assertThrows(
            KafkaException.class,
            () -> handler.configure(
                getSaslConfigs(),
                OAUTHBEARER_MECHANISM,
                getJaasConfigEntries(),
                verificationKeyResolver,
                jwtValidator
            )
        );
        assertEquals(configureError, error);
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
            public OAuthBearerToken validate(String accessToken) throws JwtValidatorException {
                return null;
            }
        };

        OAuthBearerValidatorCallbackHandler handler = new OAuthBearerValidatorCallbackHandler();
        handler.configure(
            getSaslConfigs(),
            OAUTHBEARER_MECHANISM,
            getJaasConfigEntries(),
            verificationKeyResolver,
            jwtValidator
        );

        // An error closings the JwtValidator should *not* cause OAuthBearerValidatorCallbackHandler.close() to fail.
        assertDoesNotThrow(handler::close);
    }

    @Test
    public void testFailsFastWhenJwksUrlAbsent() {
        // Default validator, no JWKS endpoint URL and no verification key resolver: the broker
        // validator handler must fail fast rather than silently using a validator that does not
        // verify token signatures.
        Map<String, ?> configs = getSaslConfigs();
        assertNull(configs.get(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL));
        assertEquals(SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS,
                ((Class<?>) configs.get(SaslConfigs.SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS)).getName());

        OAuthBearerValidatorCallbackHandler handler = new OAuthBearerValidatorCallbackHandler();
        ConfigException e = assertThrows(ConfigException.class,
                () -> handler.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries()));
        assertTrue(e.getMessage().contains(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL),
                "Expected the failure to reference " + SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL
                        + ", but was: " + e.getMessage());
    }

    @Test
    public void testRejectsForgedUnsignedTokenWhenJwksUrlSet() throws Exception {
        // Build a real JWKS file so BrokerJwtValidator can configure successfully.
        PublicJsonWebKey jwk = createRsaJwk();
        JsonWebKeySet jwks = new JsonWebKeySet(jwk);
        String jwksJson = jwks.toJson(JsonWebKey.OutputControlLevel.PUBLIC_ONLY);
        String fileUrl = tempFile(jwksJson).toURI().toString();
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, fileUrl);

        Map<String, ?> configs = getSaslConfigs(Map.of(
            SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, fileUrl,
            SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER, EXPECTED_ISSUER,
            SASL_OAUTHBEARER_EXPECTED_AUDIENCE, List.of(EXPECTED_AUDIENCE)));

        OAuthBearerValidatorCallbackHandler handler = new OAuthBearerValidatorCallbackHandler();
        assertDoesNotThrow(() -> handler.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries()));

        try {
            String forgedToken = forgeUnsignedJwt(ATTACKER_PRINCIPAL);
            OAuthBearerValidatorCallback callback = new OAuthBearerValidatorCallback(forgedToken);
            handler.handle(new Callback[]{callback});

            assertNull(callback.token(), "BrokerJwtValidator must not accept the forged unsigned token");
            assertNotNull(callback.errorStatus(), "Expected an invalid_token error");
            assertEquals("invalid_token", callback.errorStatus());
        } finally {
            handler.close();
        }
    }

    @Test
    public void testFailsToStartWhenExpectedAudienceMissingWithJwksUrl() {
        // A broker validator handler configured with a JWKS endpoint URL and expected.issuer but no expected.audience
        // must fail to start, rather than come up and accept tokens whose audience cannot be verified.
        Map<String, ?> configs = getSaslConfigs(Map.of(
            SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, "https://example.com/jwks",
            SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER, EXPECTED_ISSUER));
        OAuthBearerValidatorCallbackHandler handler = new OAuthBearerValidatorCallbackHandler();
        assertThrowsWithMessage(ConfigException.class,
            () -> handler.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries()),
            SASL_OAUTHBEARER_EXPECTED_AUDIENCE);
    }

    @Test
    public void testFailsToStartWhenExpectedIssuerMissingWithJwksUrl() {
        // A broker validator handler configured with a JWKS endpoint URL and expected.audience but no expected.issuer
        // must fail to start, rather than come up and accept tokens whose issuer cannot be verified.
        Map<String, ?> configs = getSaslConfigs(Map.of(
            SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, "https://example.com/jwks",
            SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE, List.of(EXPECTED_AUDIENCE)));
        OAuthBearerValidatorCallbackHandler handler = new OAuthBearerValidatorCallbackHandler();
        assertThrowsWithMessage(ConfigException.class,
            () -> handler.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries()),
            SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER);
    }

    @Test
    public void testConfigureAcceptsCustomValidatorClass() {
        // A custom (non-default) validator class must be accepted; the startup check only applies
        // when the default validator is in use.
        Map<String, ?> configs = getSaslConfigs(SaslConfigs.SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS,
                NoOpVerifyingJwtValidator.class.getName());
        OAuthBearerValidatorCallbackHandler handler = new OAuthBearerValidatorCallbackHandler();
        try {
            assertDoesNotThrow(() -> handler.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries()));
        } finally {
            handler.close();
        }
    }

    private void assertInvalidAccessTokenFails(String accessToken, String expectedMessageSubstring) throws Exception {
        AccessTokenBuilder builder = new AccessTokenBuilder()
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256);
        // The injected resolver routes to BrokerJwtValidator, which requires expected.issuer and expected.audience.
        Map<String, ?> configs = getSaslConfigs(Map.of(
            SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER, EXPECTED_ISSUER,
            SASL_OAUTHBEARER_EXPECTED_AUDIENCE, List.of(EXPECTED_AUDIENCE)));
        CloseableVerificationKeyResolver verificationKeyResolver = createVerificationKeyResolver(builder);
        JwtValidator jwtValidator = createJwtValidator(verificationKeyResolver);

        OAuthBearerValidatorCallbackHandler handler = new OAuthBearerValidatorCallbackHandler();
        handler.configure(
            configs,
            OAUTHBEARER_MECHANISM,
            getJaasConfigEntries(),
            verificationKeyResolver,
            jwtValidator
        );

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

    private JwtValidator createJwtValidator(CloseableVerificationKeyResolver verificationKeyResolver) {
        return new DefaultJwtValidator(verificationKeyResolver);
    }

    private CloseableVerificationKeyResolver createVerificationKeyResolver(AccessTokenBuilder builder) {
        return (jws, nestingContext) -> builder.jwk().getPublicKey();
    }

    private String forgeUnsignedJwt(String subject) {
        long nowSeconds = Time.SYSTEM.milliseconds() / 1000;
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();

        String header = enc.encodeToString(Utils.utf8("{\"alg\":\"none\",\"typ\":\"JWT\"}"));
        String payload = enc.encodeToString(Utils.utf8(String.format(
                "{\"sub\":\"%s\",\"scope\":\"engineering\",\"iat\":%d,\"exp\":%d}",
                subject,
                nowSeconds,
                nowSeconds + 3600)));
        String garbageSignature = enc.encodeToString(Utils.utf8("forged"));
        return String.format("%s.%s.%s", header, payload, garbageSignature);
    }

    /**
     * Stand-in for a custom operator-supplied {@link JwtValidator}, used to verify that the
     * startup configuration check does not apply when a non-default validator class is set.
     */
    public static class NoOpVerifyingJwtValidator implements JwtValidator {
        @Override
        public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) { }

        @Override
        public OAuthBearerToken validate(String accessToken) {
            return null;
        }
    }
}
