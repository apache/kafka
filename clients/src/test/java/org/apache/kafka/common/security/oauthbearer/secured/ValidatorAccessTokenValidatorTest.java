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

package org.apache.kafka.common.security.oauthbearer.secured;

import java.util.Collections;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ValidatorAccessTokenValidatorTest extends AccessTokenValidatorTest {

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
    public void testBasicEncryption() throws Exception {
        AccessTokenBuilder builder = new AccessTokenBuilder();
        AccessTokenValidator validator = createAccessTokenValidator(builder);

        JsonWebSignature jws = new JsonWebSignature();
        jws.setKey(builder.jwk().getPrivateKey());
        jws.setKeyIdHeaderValue(builder.jwk().getKeyId());
        jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);
        String accessToken = builder.build();

        OAuthBearerToken token = validator.validate(accessToken);

        assertEquals(builder.subject(), token.principalName());
        assertEquals(builder.issuedAtSeconds() * 1000, token.startTimeMs());
        assertEquals(builder.expirationSeconds() * 1000, token.lifetimeMs());
        assertEquals(1, token.scope().size());
    }

}
