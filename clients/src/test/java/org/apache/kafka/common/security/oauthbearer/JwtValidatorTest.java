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
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest;

import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jwx.HeaderParameterNames;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class JwtValidatorTest extends OAuthBearerTest {

    protected abstract JwtValidator createJwtValidator(AccessTokenBuilder accessTokenBuilder) throws Exception;

    protected JwtValidator createJwtValidator() throws Exception {
        AccessTokenBuilder builder = new AccessTokenBuilder();
        return createJwtValidator(builder);
    }

    @Test
    public void testNull() throws Exception {
        JwtValidator validator = createJwtValidator();
        assertThrowsWithMessage(JwtValidatorException.class, () -> validator.validate(null), "Malformed JWT provided; expected three sections (header, payload, and signature)");
    }

    @Test
    public void testEmptyString() throws Exception {
        JwtValidator validator = createJwtValidator();
        assertThrowsWithMessage(JwtValidatorException.class, () -> validator.validate(""), "Malformed JWT provided; expected three sections (header, payload, and signature)");
    }

    @Test
    public void testWhitespace() throws Exception {
        JwtValidator validator = createJwtValidator();
        assertThrowsWithMessage(JwtValidatorException.class, () -> validator.validate("    "), "Malformed JWT provided; expected three sections (header, payload, and signature)");
    }

    @Test
    public void testEmptySections() throws Exception {
        JwtValidator validator = createJwtValidator();
        assertThrowsWithMessage(JwtValidatorException.class, () -> validator.validate(".."), "Malformed JWT provided; expected three sections (header, payload, and signature)");
    }

    @Test
    public void testMissingHeader() throws Exception {
        JwtValidator validator = createJwtValidator();
        String header = "";
        String payload = createBase64JsonJwtSection(node -> { });
        String signature = "";
        String accessToken = String.format("%s.%s.%s", header, payload, signature);
        assertThrows(JwtValidatorException.class, () -> validator.validate(accessToken));
    }

    @Test
    public void testMissingPayload() throws Exception {
        JwtValidator validator = createJwtValidator();
        String header = createBase64JsonJwtSection(node -> node.put(HeaderParameterNames.ALGORITHM, AlgorithmIdentifiers.NONE));
        String payload = "";
        String signature = "";
        String accessToken = String.format("%s.%s.%s", header, payload, signature);
        assertThrows(JwtValidatorException.class, () -> validator.validate(accessToken));
    }

    @Test
    public void testMissingSignature() throws Exception {
        JwtValidator validator = createJwtValidator();
        String header = createBase64JsonJwtSection(node -> node.put(HeaderParameterNames.ALGORITHM, AlgorithmIdentifiers.NONE));
        String payload = createBase64JsonJwtSection(node -> { });
        String signature = "";
        String accessToken = String.format("%s.%s.%s", header, payload, signature);
        assertThrows(JwtValidatorException.class, () -> validator.validate(accessToken));
    }

}