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

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jwx.HeaderParameterNames;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class AccessTokenValidatorTest extends OAuthBearerTest {

    protected abstract AccessTokenValidator createAccessTokenValidator(AccessTokenBuilder accessTokenBuilder) throws Exception;

    protected AccessTokenValidator createAccessTokenValidator() throws Exception {
        AccessTokenBuilder builder = new AccessTokenBuilder();
        return createAccessTokenValidator(builder);
    }

    @Test
    public void testNull() throws Exception {
        AccessTokenValidator validator = createAccessTokenValidator();
        assertThrowsWithMessage(ValidateException.class, () -> validator.validate(null), "Empty JWT provided");
    }

    @Test
    public void testEmptyString() throws Exception {
        AccessTokenValidator validator = createAccessTokenValidator();
        assertThrowsWithMessage(ValidateException.class, () -> validator.validate(""), "Empty JWT provided");
    }

    @Test
    public void testWhitespace() throws Exception {
        AccessTokenValidator validator = createAccessTokenValidator();
        assertThrowsWithMessage(ValidateException.class, () -> validator.validate("    "), "Empty JWT provided");
    }

    @Test
    public void testEmptySections() throws Exception {
        AccessTokenValidator validator = createAccessTokenValidator();
        assertThrowsWithMessage(ValidateException.class, () -> validator.validate(".."), "Malformed JWT provided");
    }

    @Test
    public void testMissingHeader() throws Exception {
        AccessTokenValidator validator = createAccessTokenValidator();
        String header = "";
        String payload = createBase64JsonJwtSection(node -> { });
        String signature = "";
        String accessToken = String.format("%s.%s.%s", header, payload, signature);
        assertThrows(ValidateException.class, () -> validator.validate(accessToken));
    }

    @Test
    public void testMissingPayload() throws Exception {
        AccessTokenValidator validator = createAccessTokenValidator();
        String header = createBase64JsonJwtSection(node -> node.put(HeaderParameterNames.ALGORITHM, AlgorithmIdentifiers.NONE));
        String payload = "";
        String signature = "";
        String accessToken = String.format("%s.%s.%s", header, payload, signature);
        assertThrows(ValidateException.class, () -> validator.validate(accessToken));
    }

    @Test
    public void testMissingSignature() throws Exception {
        AccessTokenValidator validator = createAccessTokenValidator();
        String header = createBase64JsonJwtSection(node -> node.put(HeaderParameterNames.ALGORITHM, AlgorithmIdentifiers.NONE));
        String payload = createBase64JsonJwtSection(node -> { });
        String signature = "";
        String accessToken = String.format("%s.%s.%s", header, payload, signature);
        assertThrows(ValidateException.class, () -> validator.validate(accessToken));
    }

}