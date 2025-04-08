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

import org.apache.kafka.common.security.oauthbearer.BrokerJwtValidator;
import org.apache.kafka.common.security.oauthbearer.JwtValidator;
import org.apache.kafka.common.security.oauthbearer.JwtValidatorTest;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.lang.InvalidAlgorithmException;
import org.junit.jupiter.api.Test;

import java.security.Key;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BrokerJwtValidatorTest extends JwtValidatorTest {

    @Override
    protected JwtValidator createValidator(JwtBuilder builder) throws Exception {
        Key key = builder.jwk() != null ? builder.jwk().getKey() : null;
        CloseableVerificationKeyResolver keyResolver = mock(CloseableVerificationKeyResolver.class);
        when(keyResolver.resolveKey(any(), any())).thenReturn(key);

        return new BrokerJwtValidator() {
            @Override
            public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
                super.configure(keyResolver, configs, saslMechanism);
            }
        };
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
        JwtBuilder tokenBuilder = new JwtBuilder()
            .jwk(jwk)
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256)
            .addCustomClaim(subClaimName, subject)
            .subjectClaimName(subClaimName)
            .subject(null);
        JwtValidator validator = createValidator(tokenBuilder);
        Map<String, Object> configs = Collections.singletonMap(SASL_OAUTHBEARER_SUB_CLAIM_NAME, tokenBuilder.subjectClaimName());
        validator.configure(getSaslConfigs(configs), OAUTHBEARER_MECHANISM, List.of());

        // Validation should succeed (e.g. signature verification) even if sub claim is missing
        OAuthBearerToken token = validator.validate(tokenBuilder.build());

        assertEquals(subject, token.principalName());
    }

    private void testEncryptionAlgorithm(PublicJsonWebKey jwk, String alg) throws Exception {
        JwtBuilder builder = new JwtBuilder().jwk(jwk).alg(alg);
        JwtValidator validator = createValidator(builder);
        validator.configure(getSaslConfigs(), OAUTHBEARER_MECHANISM, List.of());
        String jwt = builder.build();
        OAuthBearerToken token = validator.validate(jwt);

        assertEquals(builder.subject(), token.principalName());
        assertEquals(builder.issuedAtSeconds() * 1000, token.startTimeMs());
        assertEquals(builder.expirationSeconds() * 1000, token.lifetimeMs());
        assertEquals(1, token.scope().size());
    }

}
