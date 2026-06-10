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

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.utils.LogCaptureAppender;

import org.apache.logging.log4j.Level;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.lang.InvalidAlgorithmException;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ValidatorAccessTokenValidatorTest extends AccessTokenValidatorTest {

    private static final String ATTACKER_ISSUER = "https://evil.example/attacker";

    private static final String EXPECTED_ISSUER = "https://idp.legit.example/";

    @Override
    protected AccessTokenValidator createAccessTokenValidator(AccessTokenBuilder builder) {
        return createAccessTokenValidator(builder, null);
    }

    private AccessTokenValidator createAccessTokenValidator(AccessTokenBuilder builder, String expectedIssuer) {
        return new ValidatorAccessTokenValidator(30,
            Collections.emptySet(),
            expectedIssuer,
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
            .addCustomClaim("iss", EXPECTED_ISSUER)
            .subjectClaimName(subClaimName)
            .subject(null);
        AccessTokenValidator validator = createAccessTokenValidator(tokenBuilder, EXPECTED_ISSUER);

        // Validation should succeed (e.g. signature verification) even if sub claim is missing
        OAuthBearerToken token = validator.validate(tokenBuilder.build());

        assertEquals(subject, token.principalName());
    }

    @Test
    public void testExpectedIssuerMismatch() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        AccessTokenBuilder builder = new AccessTokenBuilder()
                .jwk(jwk)
                .alg(AlgorithmIdentifiers.RSA_USING_SHA256)
                .addCustomClaim("iss", ATTACKER_ISSUER);
        String accessToken = builder.build();

        AccessTokenValidator validator = createAccessTokenValidator(builder, EXPECTED_ISSUER);

        ValidateException e = assertThrows(ValidateException.class,
                () -> validator.validate(accessToken));
        assertErrorMessageContains(e.getMessage(), ATTACKER_ISSUER);
    }

    @Test
    public void testWarnsWhenExpectedIssuerUnset() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        CloseableVerificationKeyResolver resolver = (jws, nestingContext) -> jwk.getKey();

        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            appender.setClassLogger(ValidatorAccessTokenValidator.class, Level.WARN);
            new ValidatorAccessTokenValidator(30, Collections.emptySet(), null, resolver, "scope", "sub");

            List<String> warnings = appender.getMessages("WARN");
            assertTrue(warnings.stream().anyMatch(message -> message.contains(SASL_OAUTHBEARER_EXPECTED_ISSUER)),
                    "Expected a WARN log mentioning " + SASL_OAUTHBEARER_EXPECTED_ISSUER + ", but got: " + warnings);
        }
    }

    @Test
    public void testWarnsWhenExpectedAudienceUnset() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        CloseableVerificationKeyResolver resolver = (jws, nestingContext) -> jwk.getKey();

        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            appender.setClassLogger(ValidatorAccessTokenValidator.class, Level.WARN);
            new ValidatorAccessTokenValidator(30, null, EXPECTED_ISSUER, resolver, "scope", "sub");

            List<String> warnings = appender.getMessages("WARN");
            assertTrue(warnings.stream().anyMatch(message -> message.contains(SASL_OAUTHBEARER_EXPECTED_AUDIENCE)),
                    "Expected a WARN log mentioning " + SASL_OAUTHBEARER_EXPECTED_AUDIENCE + ", but got: " + warnings);
        }
    }

    private void testEncryptionAlgorithm(PublicJsonWebKey jwk, String alg) throws Exception {
        AccessTokenBuilder builder = new AccessTokenBuilder().jwk(jwk).alg(alg).addCustomClaim("iss", EXPECTED_ISSUER);
        AccessTokenValidator validator = createAccessTokenValidator(builder, EXPECTED_ISSUER);
        String accessToken = builder.build();
        OAuthBearerToken token = validator.validate(accessToken);

        assertEquals(builder.subject(), token.principalName());
        assertEquals(builder.issuedAtSeconds() * 1000, token.startTimeMs());
        assertEquals(builder.expirationSeconds() * 1000, token.lifetimeMs());
        assertEquals(1, token.scope().size());
    }

}
