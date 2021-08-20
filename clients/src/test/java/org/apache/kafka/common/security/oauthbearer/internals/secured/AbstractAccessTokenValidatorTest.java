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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Base64;
import java.util.function.Consumer;
import org.apache.kafka.common.utils.Utils;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jwx.HeaderParameterNames;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.function.Executable;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractAccessTokenValidatorTest {

    protected ObjectMapper mapper;

    @BeforeAll
    public void setup() {
        mapper = new ObjectMapper();
    }

    protected abstract AccessTokenValidator createBasicValidator() throws Exception;

    @Test
    public void testNull() throws Exception {
        AccessTokenValidator validator = createBasicValidator();
        assertThrowsWithMessage(() -> validator.validate(null), "must be non-null");
    }

    @Test
    public void testEmptyString() throws Exception {
        AccessTokenValidator validator = createBasicValidator();
        assertThrowsWithMessage(() -> validator.validate(""), "must be non-empty");
    }

    @Test
    public void testWhitespace() throws Exception {
        AccessTokenValidator validator = createBasicValidator();
        assertThrowsWithMessage(() -> validator.validate("    "), "must not contain only whitespace");
    }

    @Test
    public void testEmptySections() throws Exception {
        AccessTokenValidator validator = createBasicValidator();
        assertThrowsWithMessage(() -> validator.validate(".."), "Malformed JWT access token");
    }

    @Test
    public void testMissingHeader() throws Exception {
        AccessTokenValidator validator = createBasicValidator();
        String header = "";
        String payload = createBase64JsonJwtSection(node -> {});
        String signature = "";
        String accessToken = String.format("%s.%s.%s", header, payload, signature);
        assertThrows(ValidateException.class, () -> validator.validate(accessToken));
    }

    @Test
    public void testMissingPayload() throws Exception {
        AccessTokenValidator validator = createBasicValidator();
        String header = createBase64JsonJwtSection(node -> node.put(HeaderParameterNames.ALGORITHM, AlgorithmIdentifiers.NONE));
        String payload = "";
        String signature = "";
        String accessToken = String.format("%s.%s.%s", header, payload, signature);
        assertThrows(ValidateException.class, () -> validator.validate(accessToken));
    }

    @Test
    public void testMissingSignature() throws Exception {
        AccessTokenValidator validator = createBasicValidator();
        String header = createBase64JsonJwtSection(node -> node.put(HeaderParameterNames.ALGORITHM, AlgorithmIdentifiers.NONE));
        String payload = createBase64JsonJwtSection(node -> {});
        String signature = "";
        String accessToken = String.format("%s.%s.%s", header, payload, signature);
        assertThrows(ValidateException.class, () -> validator.validate(accessToken));
    }

    protected void assertThrowsWithMessage(Executable validatorExecutable, String substring) {
        assertThrows(ValidateException.class, validatorExecutable);

        try {
            validatorExecutable.execute();
        } catch (Throwable e) {
            assertTrue(e.getMessage().contains(substring),
                String.format("Expected exception message (\"%s\") to contain substring (\"%s\")",
                    e.getMessage(), substring));
        }
    }

    protected String createBase64JsonJwtSection(Consumer<ObjectNode> c) {
        String json = createJsonJwtSection(c);

        try {
            return Utils.utf8(Base64.getEncoder().encode(Utils.utf8(json)));
        } catch (Throwable t) {
            fail(t);

            // Shouldn't get to here...
            return null;
        }
    }

    protected String createJsonJwtSection(Consumer<ObjectNode> c) {
        ObjectNode node = mapper.createObjectNode();
        c.accept(node);

        try {
            return mapper.writeValueAsString(node);
        } catch (Throwable t) {
            fail(t);

            // Shouldn't get to here...
            return null;
        }
    }

}