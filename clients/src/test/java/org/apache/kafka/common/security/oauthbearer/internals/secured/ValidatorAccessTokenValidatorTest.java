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

package org.apache.kafka.common.security.oauthbearer.internals.secured;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.utils.LogCaptureAppender;

import org.apache.log4j.Level;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.lang.InvalidAlgorithmException;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ALLOW_UNVERIFIED_AUDIENCE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ALLOW_UNVERIFIED_ISSUER;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ValidatorAccessTokenValidatorTest extends AccessTokenValidatorTest {

    private static final String ATTACKER_ISSUER = "https://evil.example/";

    private static final String EXPECTED_ISSUER = "https://legit.example/";

    private static final String ATTACKER_AUDIENCE = "https://evil.example/other-service";

    private static final String EXPECTED_AUDIENCE = "https://legit.example/other-service";

    @Override
    protected AccessTokenValidator createAccessTokenValidator(AccessTokenBuilder builder) {
        return new ValidatorAccessTokenValidator(30,
            Collections.emptySet(),
            null,
            (jws, nestingContext) -> builder.jwk().getKey(),
            builder.scopeClaimName(),
            builder.subjectClaimName());
    }

    @Test
    public void testRsaEncryptionAlgorithm() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        testEncryptionAlgorithm(jwk, AlgorithmIdentifiers.RSA_USING_SHA256);
    }

    @Test
    public void testEcdsaEncryptionAlgorithm() throws Exception {
        PublicJsonWebKey jwk = createEcJwk();
        testEncryptionAlgorithm(jwk, AlgorithmIdentifiers.ECDSA_USING_P256_CURVE_AND_SHA256);
    }

    @Test
    public void testInvalidEncryptionAlgorithm() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();

        assertThrowsWithMessage(InvalidAlgorithmException.class,
            () -> testEncryptionAlgorithm(jwk, "fake"),
            "fake is an unknown, unsupported or unavailable alg algorithm");
    }

    @Test
    public void testMissingSubShouldBeValid() throws Exception {
        String subClaimName = "client_id";
        String subject = "otherSub";
        PublicJsonWebKey jwk = createRsaJwk();
        AccessTokenBuilder tokenBuilder = new AccessTokenBuilder()
            .jwk(jwk)
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256)
            .addCustomClaim(subClaimName, subject)
            .subjectClaimName(subClaimName)
            .subject(null);
        AccessTokenValidator validator = createAccessTokenValidator(tokenBuilder);

        // Validation should succeed (e.g. signature verification) even if sub claim is missing
        OAuthBearerToken token = validator.validate(tokenBuilder.build());

        assertEquals(subject, token.principalName());
    }

    @Test
    public void testRejectMissingIssuerClaimWhenExpectedIssuerSet() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        AccessTokenBuilder builder = new AccessTokenBuilder()
                .jwk(jwk)
                .alg(AlgorithmIdentifiers.RSA_USING_SHA256)
                .audience(EXPECTED_AUDIENCE);   // valid audience, but no iss claim is set
        String accessToken = builder.build();

        CloseableVerificationKeyResolver resolver = (jws, nestingContext) -> jwk.getKey();
        Map<String, Object> configs = new HashMap<>();
        configs.put(SASL_OAUTHBEARER_EXPECTED_ISSUER, EXPECTED_ISSUER);
        configs.put(SASL_OAUTHBEARER_EXPECTED_AUDIENCE, Collections.singletonList(EXPECTED_AUDIENCE));
        AccessTokenValidator validator = AccessTokenValidatorFactory.create(getSaslConfigs(configs), OAUTHBEARER_MECHANISM, resolver);

        // With an expected issuer configured, jose4j requires the "iss" claim to be present; a token without it
        // must be rejected at validation time rather than silently accepted.
        ValidateException e = assertThrows(ValidateException.class,
                () -> validator.validate(accessToken));
        assertErrorMessageContains(e.getMessage(), EXPECTED_ISSUER);
    }

    @Test
    public void testConfigureFailsWhenExpectedIssuerUnset() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        CloseableVerificationKeyResolver resolver = (jws, nestingContext) -> jwk.getKey();
        assertThrowsWithMessage(ConfigException.class,
                () -> AccessTokenValidatorFactory.create(
                        getSaslConfigs(SASL_OAUTHBEARER_EXPECTED_AUDIENCE, Collections.singletonList(EXPECTED_AUDIENCE)),
                        OAUTHBEARER_MECHANISM, resolver),
                SASL_OAUTHBEARER_EXPECTED_ISSUER);
    }

    @Test
    public void testExpectedIssuerSet() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        AccessTokenBuilder builder = new AccessTokenBuilder()
                .jwk(jwk)
                .alg(AlgorithmIdentifiers.RSA_USING_SHA256)
                .addCustomClaim("iss", ATTACKER_ISSUER)
                .audience(EXPECTED_AUDIENCE);
        String accessToken = builder.build();

        CloseableVerificationKeyResolver resolver = (jws, nestingContext) -> jwk.getKey();
        Map<String, Object> configs = new HashMap<>();
        configs.put(SASL_OAUTHBEARER_EXPECTED_ISSUER, EXPECTED_ISSUER);
        configs.put(SASL_OAUTHBEARER_EXPECTED_AUDIENCE, Collections.singletonList(EXPECTED_AUDIENCE));
        AccessTokenValidator validator = AccessTokenValidatorFactory.create(getSaslConfigs(configs), OAUTHBEARER_MECHANISM, resolver);

        ValidateException e = assertThrows(ValidateException.class,
                () -> validator.validate(accessToken));
        // jose4j reports the issuer mismatch; the message includes the offending issuer value
        assertErrorMessageContains(e.getMessage(), ATTACKER_ISSUER);
    }

    @Test
    public void testConfigureFailsWhenExpectedAudienceUnset() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        CloseableVerificationKeyResolver resolver = (jws, nestingContext) -> jwk.getKey();
        assertThrowsWithMessage(ConfigException.class,
                () -> AccessTokenValidatorFactory.create(
                        getSaslConfigs(SASL_OAUTHBEARER_EXPECTED_ISSUER, EXPECTED_ISSUER),
                        OAUTHBEARER_MECHANISM, resolver),
                SASL_OAUTHBEARER_EXPECTED_AUDIENCE);
    }

    @Test
    public void testRejectWrongAudienceWhenExpectedAudienceSet() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        AccessTokenBuilder builder = new AccessTokenBuilder()
                .jwk(jwk)
                .alg(AlgorithmIdentifiers.RSA_USING_SHA256)
                .addCustomClaim("iss", EXPECTED_ISSUER)
                .audience(ATTACKER_AUDIENCE);
        String accessToken = builder.build();

        CloseableVerificationKeyResolver resolver = (jws, nestingContext) -> jwk.getKey();
        Map<String, Object> configs = new HashMap<>();
        configs.put(SASL_OAUTHBEARER_EXPECTED_ISSUER, EXPECTED_ISSUER);
        configs.put(SASL_OAUTHBEARER_EXPECTED_AUDIENCE, Collections.singletonList(EXPECTED_AUDIENCE));
        AccessTokenValidator validator = AccessTokenValidatorFactory.create(getSaslConfigs(configs), OAUTHBEARER_MECHANISM, resolver);

        ValidateException e = assertThrows(ValidateException.class,
                () -> validator.validate(accessToken));
        // jose4j reports the audience mismatch; the message includes the offending audience value
        assertErrorMessageContains(e.getMessage(), ATTACKER_AUDIENCE);
    }

    @Test
    public void testAllowUnverifiedAudience() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        AccessTokenBuilder builder = new AccessTokenBuilder()
                .jwk(jwk)
                .alg(AlgorithmIdentifiers.RSA_USING_SHA256)
                .addCustomClaim("iss", EXPECTED_ISSUER)
                .audience(ATTACKER_AUDIENCE);   // an audience that matches no expected audience
        String accessToken = builder.build();

        CloseableVerificationKeyResolver resolver = (jws, nestingContext) -> jwk.getKey();
        Map<String, Object> configs = new HashMap<>();
        configs.put(SASL_OAUTHBEARER_EXPECTED_ISSUER, EXPECTED_ISSUER);
        configs.put(SASL_OAUTHBEARER_ALLOW_UNVERIFIED_AUDIENCE, true);
        AccessTokenValidator validator = AccessTokenValidatorFactory.create(getSaslConfigs(configs), OAUTHBEARER_MECHANISM, resolver);

        OAuthBearerToken token = validator.validate(accessToken);
        assertEquals(builder.subject(), token.principalName());
    }

    @Test
    public void testAllowUnverifiedIssuer() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        AccessTokenBuilder builder = new AccessTokenBuilder()
                .jwk(jwk)
                .alg(AlgorithmIdentifiers.RSA_USING_SHA256)
                .addCustomClaim("iss", ATTACKER_ISSUER)   // an issuer that matches no expected issuer
                .audience(EXPECTED_AUDIENCE);
        String accessToken = builder.build();

        CloseableVerificationKeyResolver resolver = (jws, nestingContext) -> jwk.getKey();
        Map<String, Object> configs = new HashMap<>();
        configs.put(SASL_OAUTHBEARER_EXPECTED_AUDIENCE, Collections.singletonList(EXPECTED_AUDIENCE));
        configs.put(SASL_OAUTHBEARER_ALLOW_UNVERIFIED_ISSUER, true);
        AccessTokenValidator validator = AccessTokenValidatorFactory.create(getSaslConfigs(configs), OAUTHBEARER_MECHANISM, resolver);

        OAuthBearerToken token = validator.validate(accessToken);
        assertEquals(builder.subject(), token.principalName());
    }

    @Test
    public void testAllowUnverifiedIssuerAndAudience() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        AccessTokenBuilder builder = new AccessTokenBuilder()
                .jwk(jwk)
                .alg(AlgorithmIdentifiers.RSA_USING_SHA256)
                .addCustomClaim("iss", ATTACKER_ISSUER)   // neither the issuer nor the audience matches anything expected
                .audience(ATTACKER_AUDIENCE);
        String accessToken = builder.build();

        CloseableVerificationKeyResolver resolver = (jws, nestingContext) -> jwk.getKey();
        // Neither expected.issuer nor expected.audience is set; both opt-outs are enabled so configuration succeeds.
        Map<String, Object> configs = new HashMap<>();
        configs.put(SASL_OAUTHBEARER_ALLOW_UNVERIFIED_ISSUER, true);
        configs.put(SASL_OAUTHBEARER_ALLOW_UNVERIFIED_AUDIENCE, true);
        AccessTokenValidator validator;

        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(AccessTokenValidatorFactory.class)) {
            appender.setClassLogger(AccessTokenValidatorFactory.class, Level.WARN);
            validator = AccessTokenValidatorFactory.create(getSaslConfigs(configs), OAUTHBEARER_MECHANISM, resolver);

            // Each opt-out path must log a WARN naming the config that disabled the check, so the weakened
            // posture is visible in the broker logs.
            List<String> warnings = appender.getMessages("WARN");
            assertTrue(warnings.stream().anyMatch(m -> m.contains(SASL_OAUTHBEARER_ALLOW_UNVERIFIED_ISSUER)),
                    "Expected a WARN mentioning " + SASL_OAUTHBEARER_ALLOW_UNVERIFIED_ISSUER + ", got " + warnings);
            assertTrue(warnings.stream().anyMatch(m -> m.contains(SASL_OAUTHBEARER_ALLOW_UNVERIFIED_AUDIENCE)),
                    "Expected a WARN mentioning " + SASL_OAUTHBEARER_ALLOW_UNVERIFIED_AUDIENCE + ", got " + warnings);
        }

        // With both opt-outs in effect, a token whose issuer and audience match nothing configured is still accepted.
        OAuthBearerToken token = validator.validate(accessToken);
        assertEquals(builder.subject(), token.principalName());
    }

    @Test
    public void testConfigureFailsWhenNeitherExpectedSet() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        CloseableVerificationKeyResolver resolver = (jws, nestingContext) -> jwk.getKey();
        // With neither expected value set and no opt-out, configuration must fail fast. The audience guard runs
        // first, so its message is the one that surfaces.
        assertThrowsWithMessage(ConfigException.class,
                () -> AccessTokenValidatorFactory.create(getSaslConfigs(), OAUTHBEARER_MECHANISM, resolver),
                SASL_OAUTHBEARER_EXPECTED_AUDIENCE);
    }

    private void testEncryptionAlgorithm(PublicJsonWebKey jwk, String alg) throws Exception {
        AccessTokenBuilder builder = new AccessTokenBuilder().jwk(jwk).alg(alg);
        AccessTokenValidator validator = createAccessTokenValidator(builder);
        String accessToken = builder.build();
        OAuthBearerToken token = validator.validate(accessToken);

        assertEquals(builder.subject(), token.principalName());
        assertEquals(builder.issuedAtSeconds() * 1000, token.startTimeMs());
        assertEquals(builder.expirationSeconds() * 1000, token.lifetimeMs());
        assertEquals(1, token.scope().size());
    }

}
