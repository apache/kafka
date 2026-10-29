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
package org.apache.kafka.common.security.oauthbearer.internals.secured.assertion;

import org.apache.kafka.common.KafkaException;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileAssertionJwtTemplateTest {

    @Test
    public void testBasicUsage() throws Exception {
        String expected = createTemplateJson(
            Map.of("typ", "JWT", "alg", "RS256"),
            Map.of("sub", "jdoe")
        );

        File tmpFile = tempFile(expected);

        try (AssertionJwtTemplate template = new FileAssertionJwtTemplate(tmpFile)) {
            Map<String, Object> header = template.header();
            assertNotNull(header);
            assertEquals("JWT", header.get("typ"));
            assertEquals("RS256", header.get("alg"));

            Map<String, Object> payload = template.payload();
            assertNotNull(payload);
            assertEquals("jdoe", payload.get("sub"));
        }
    }

    @Test
    public void testHeaderOnly() throws Exception {
        String expected = toJson(
            Map.of(
                "header",
                Map.of("typ", "JWT", "alg", "RS256")
            )
        );

        File tmpFile = tempFile(expected);

        try (AssertionJwtTemplate template = new FileAssertionJwtTemplate(tmpFile)) {
            Map<String, Object> header = template.header();
            assertNotNull(header);
            assertEquals("JWT", header.get("typ"));
            assertEquals("RS256", header.get("alg"));

            Map<String, Object> payload = template.payload();
            assertNotNull(payload);
            assertTrue(payload.isEmpty());
        }
    }

    @Test
    public void testPayloadOnly() throws Exception {
        String expected = toJson(
            Map.of(
                "payload",
                Map.of("sub", "jdoe")
            )
        );

        File tmpFile = tempFile(expected);

        try (AssertionJwtTemplate template = new FileAssertionJwtTemplate(tmpFile)) {
            Map<String, Object> header = template.header();
            assertNotNull(header);
            assertTrue(header.isEmpty());

            Map<String, Object> payload = template.payload();
            assertNotNull(payload);
            assertEquals("jdoe", payload.get("sub"));
        }
    }

    @Test
    public void testMalformedFile() throws Exception {
        String expected = "{invalid-json}";
        File tmpFile = tempFile(expected);

        assertThrows(KafkaException.class, () -> new FileAssertionJwtTemplate(tmpFile));
    }

    @Test
    public void testMalformedFormat() throws Exception {
        String expected = toJson(Map.of("header", List.of("foo", "bar", "baz")));
        File tmpFile = tempFile(expected);

        assertThrows(KafkaException.class, () -> new FileAssertionJwtTemplate(tmpFile));
    }

    private String createTemplateJson(Map<String, Object> header, Map<String, Object> payload) {
        Map<String, Object> topLevel = Map.of("header", header, "payload", payload);
        return toJson(topLevel);
    }

    private String toJson(Map<String, Object> map) {
        ObjectMapper mapper = new ObjectMapper();
        return assertDoesNotThrow(() -> mapper.writeValueAsString(map));
    }
}
