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

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenBuilder;
import org.apache.kafka.common.security.oauthbearer.internals.secured.CloseableVerificationKeyResolver;

import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.lang.InvalidAlgorithmException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BrokerJwtValidatorTest extends JwtValidatorTest {

    @Override
    protected JwtValidator createJwtValidator(AccessTokenBuilder builder) {
        CloseableVerificationKeyResolver resolver = (jws, nestingContext) -> builder.jwk().getKey();
        return new BrokerJwtValidator(resolver);
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
        JwtValidator validator = createJwtValidator(tokenBuilder);
        Map<String, ?> saslConfigs = getSaslConfigs(SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME, subClaimName);
        validator.configure(saslConfigs, OAUTHBEARER_MECHANISM, getJaasConfigEntries());

        // Validation should succeed (e.g. signature verification) even if sub claim is missing
        OAuthBearerToken token = validator.validate(tokenBuilder.build());

        assertEquals(subject, token.principalName());
    }

    private void testEncryptionAlgorithm(PublicJsonWebKey jwk, String alg) throws Exception {
        AccessTokenBuilder builder = new AccessTokenBuilder().jwk(jwk).alg(alg);
        JwtValidator validator = createJwtValidator(builder);
        validator.configure(getSaslConfigs(), OAUTHBEARER_MECHANISM, getJaasConfigEntries());
        String accessToken = builder.build();
        OAuthBearerToken token = validator.validate(accessToken);

        assertEquals(builder.subject(), token.principalName());
        assertEquals(builder.issuedAtSeconds() * 1000, token.startTimeMs());
        assertEquals(builder.expirationSeconds() * 1000, token.lifetimeMs());
        assertEquals(1, token.scope().size());
    }

}
