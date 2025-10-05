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

import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class DynamicAssertionJwtTemplateTest {

    private final MockTime time = new MockTime();

    @Test
    public void testBasicUsage() throws IOException {
        String algorithm = "somealg";
        int expiration = 1;
        int notBefore = 20;
        boolean includeJti = false;

        try (AssertionJwtTemplate template = new DynamicAssertionJwtTemplate(time, algorithm, expiration, notBefore, includeJti)) {
            Map<String, Object> header = template.header();
            assertNotNull(header);
            assertEquals("JWT", header.get("typ"));
            assertEquals(algorithm, header.get("alg"));

            long currSeconds = time.milliseconds() / 1000L;

            Map<String, Object> payload = template.payload();
            assertNotNull(payload);
            assertEquals(currSeconds, payload.get("iat"));
            assertEquals(currSeconds + expiration, payload.get("exp"));
            assertEquals(currSeconds - notBefore, payload.get("nbf"));
            assertNull(payload.get("jti"));
        }
    }

    @Test
    public void testJtiUniqueness() throws IOException {
        List<String> jwtIds = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            try (AssertionJwtTemplate template = new DynamicAssertionJwtTemplate(time, "RSA", 1, 2, true)) {
                Map<String, Object> payload = template.payload();
                assertNotNull(payload);
                String jwtId = (String) payload.get("jti");
                jwtIds.add(jwtId);
            }
        }

        // A list of JWT IDs will be the same size as a set if there are no duplicates.
        List<String> jwtIds2 = new ArrayList<>(new HashSet<>(jwtIds));
        assertEquals(jwtIds.size(), jwtIds2.size());

        jwtIds.sort(Comparator.naturalOrder());
        jwtIds2.sort(Comparator.naturalOrder());
        assertEquals(jwtIds, jwtIds2);
    }
}
