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

import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;

import org.jose4j.jws.AlgorithmIdentifiers;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class DefaultJwtValidatorTest extends OAuthBearerTest {

    @Test
    public void testConfigureWithVerificationKeyResolver() {
        AccessTokenBuilder builder = new AccessTokenBuilder()
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256);
        CloseableVerificationKeyResolver verificationKeyResolver = createVerificationKeyResolver(builder);
        Map<String, ?> configs = getSaslConfigs();
        DefaultJwtValidator jwtValidator = new DefaultJwtValidator(
            configs,
            OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
            verificationKeyResolver
        );
        assertDoesNotThrow(jwtValidator::init);
        assertInstanceOf(BrokerJwtValidator.class, jwtValidator.delegate());
    }

    @Test
    public void testConfigureWithoutVerificationKeyResolver() {
        Map<String, ?> configs = getSaslConfigs();
        DefaultJwtValidator jwtValidator = new DefaultJwtValidator(
            configs,
            OAuthBearerLoginModule.OAUTHBEARER_MECHANISM
        );
        assertDoesNotThrow(jwtValidator::init);
        assertInstanceOf(ClientJwtValidator.class, jwtValidator.delegate());
    }

    private CloseableVerificationKeyResolver createVerificationKeyResolver(AccessTokenBuilder builder) {
        return (jws, nestingContext) -> builder.jwk().getPublicKey();
    }
}
