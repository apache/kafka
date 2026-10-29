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

import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenBuilder;
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClientJwtValidatorTest extends JwtValidatorTest {

    @Override
    protected JwtValidator createJwtValidator(AccessTokenBuilder builder) {
        return new ClientJwtValidator();
    }

    @Test
    void testJwtRequiresBase64UrlDecoding() throws Exception {
        String header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
        String payload = "{\"sub\": \"jdoe\", \"exp\": 0, \"iat\": 0, \"data\":\">>>___<<<---\"}";
        String signature = "dummysignature";
        String jwt = createJwt(header, payload, signature);

        // Verify that decoding the payload fails for "plain" base 64, but works with URL-safe base 64.
        String urlEncodedPayload = Base64.getUrlEncoder().encodeToString(Utils.utf8(payload));
        assertThrows(IllegalArgumentException.class, () -> Base64.getDecoder().decode(urlEncodedPayload));
        assertDoesNotThrow(() -> Base64.getUrlDecoder().decode(urlEncodedPayload));

        try (JwtValidator validator = createJwtValidator()) {
            validator.configure(getSaslConfigs(), OAUTHBEARER_MECHANISM, getJaasConfigEntries());
            assertDoesNotThrow(
                () -> validator.validate(jwt),
                "Valid, URL-safe base 64-encoded JWT should be decodable"
            );
        }
    }
}
