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

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.apache.kafka.common.security.oauthbearer.JwtBearerRequestGenerator.GRANT_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class JwtBearerRequestGeneratorTest extends HttpRequestGeneratorTest {

    private static final String FAKE_ASSERTION = "this-is-fake";

    @Test
    public void testRequestBodyParameters() throws Exception {
        Builder builder = new Builder();
        JwtBearerRequestGenerator requestGenerator = builder.build();
        String requestBody = requestGenerator.generateBody();
        String expected = "grant_type=" + URLEncoder.encode(GRANT_TYPE, StandardCharsets.UTF_8) + "&assertion=" + FAKE_ASSERTION;
        assertEquals(
            expected,
            requestBody
        );
    }

    private static class Builder {

        private JwtBearerRequestGenerator build() throws Exception {
            AssertionCreator assertionCreator = t -> FAKE_ASSERTION;
            AssertionJwtTemplate assertionJwtTemplate = new AssertionJwtTemplate() {
                @Override
                public Map<String, Object> header() {
                    return Map.of();
                }

                @Override
                public Map<String, Object> payload() {
                    return Map.of();
                }
            };

            return new JwtBearerRequestGenerator(
                new URL("http://www.example.com"),
                assertionCreator,
                assertionJwtTemplate
            );
        }
    }
}
