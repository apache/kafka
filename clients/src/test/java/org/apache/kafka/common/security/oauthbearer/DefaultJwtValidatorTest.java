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
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenBuilder;
import org.apache.kafka.common.security.oauthbearer.internals.secured.CloseableVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest;

import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class DefaultJwtValidatorTest extends OAuthBearerTest {

    @AfterEach
    public void tearDown() {
        System.clearProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
    }

    @Test
    public void testConfigureWithVerificationKeyResolver() {
        AccessTokenBuilder builder = new AccessTokenBuilder()
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256);
        CloseableVerificationKeyResolver verificationKeyResolver = createVerificationKeyResolver(builder);
        Map<String, ?> configs = getSaslConfigs();
        DefaultJwtValidator jwtValidator = new DefaultJwtValidator(verificationKeyResolver);
        assertDoesNotThrow(() -> jwtValidator.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries()));
        assertInstanceOf(BrokerJwtValidator.class, jwtValidator.delegate());
    }

    @Test
    public void testConfigureWithoutVerificationKeyResolver() {
        Map<String, ?> configs = getSaslConfigs();
        DefaultJwtValidator jwtValidator = new DefaultJwtValidator();
        assertDoesNotThrow(() -> jwtValidator.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries()));
        assertInstanceOf(ClientJwtValidator.class, jwtValidator.delegate());
    }

    @Test
    public void testConfigureWithJwksUrl() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        AccessTokenBuilder builder = new AccessTokenBuilder()
            .jwk(jwk)
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256);
        String accessToken = builder.build();

        JsonWebKeySet jwks = new JsonWebKeySet(jwk);
        String jwksJson = jwks.toJson(JsonWebKey.OutputControlLevel.PUBLIC_ONLY);
        String fileUrl = tempFile(jwksJson).toURI().toString();
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, fileUrl);
        Map<String, ?> configs = getSaslConfigs(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, fileUrl);

        DefaultJwtValidator jwtValidator = new DefaultJwtValidator();
        assertDoesNotThrow(() -> jwtValidator.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries()));
        assertInstanceOf(BrokerJwtValidator.class, jwtValidator.delegate());
        assertDoesNotThrow(() -> jwtValidator.validate(accessToken));
    }

    private CloseableVerificationKeyResolver createVerificationKeyResolver(AccessTokenBuilder builder) {
        return (jws, nestingContext) -> builder.jwk().getPublicKey();
    }
}
