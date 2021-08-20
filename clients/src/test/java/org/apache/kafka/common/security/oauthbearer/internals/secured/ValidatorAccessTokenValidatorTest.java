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

import java.util.Collections;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jwk.RsaJwkGenerator;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.ReservedClaimNames;
import org.jose4j.keys.resolvers.VerificationKeyResolver;
import org.jose4j.lang.JoseException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ValidatorAccessTokenValidatorTest extends AbstractAccessTokenValidatorTest {

    @Override
    protected AccessTokenValidator createBasicValidator() throws Exception {
        RsaJsonWebKey rsaJsonWebKey = RsaJwkGenerator.generateJwk(2048);
        rsaJsonWebKey.setKeyId("key-1");
        VerificationKeyResolver vkr = (jws, nestingContext) -> rsaJsonWebKey.getKey();

        return new ValidatorAccessTokenValidator(30,
            Collections.emptySet(),
            null,
            vkr,
            "scope",
            "sub");
    }

    @Test
    public void testBasicEncryption() throws JoseException {
        String keyId = "key-1";
        RsaJsonWebKey rsaJsonWebKey = RsaJwkGenerator.generateJwk(2048);
        rsaJsonWebKey.setKeyId(keyId);
        VerificationKeyResolver vkr = (jws, nestingContext) -> rsaJsonWebKey.getKey();

        AccessTokenValidator validator = new ValidatorAccessTokenValidator(30,
            Collections.emptySet(),
            null,
            vkr,
            "scope",
            "sub");

        JsonWebSignature jws = new JsonWebSignature();

        String subject = "jdoe";
        long issuedAtSeconds = System.currentTimeMillis() / 1000;
        long expirationSeconds = issuedAtSeconds + 60;

        // The payload of the JWS is JSON content of the JWT Claims
        jws.setPayload(createJsonJwtSection(node -> {
            node.put(ReservedClaimNames.SUBJECT, subject);
            node.put(ReservedClaimNames.ISSUED_AT, issuedAtSeconds);
            node.put(ReservedClaimNames.EXPIRATION_TIME, expirationSeconds);
        }));

        jws.setKey(rsaJsonWebKey.getPrivateKey());
        jws.setKeyIdHeaderValue(rsaJsonWebKey.getKeyId());
        jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);
        String accessToken = jws.getCompactSerialization();

        OAuthBearerToken token = validator.validate(accessToken);

        assertEquals(subject, token.principalName());
        assertEquals(issuedAtSeconds * 1000, token.startTimeMs());
        assertEquals(expirationSeconds * 1000, token.lifetimeMs());
        assertEquals(0, token.scope().size());
    }

}
