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

import org.junit.jupiter.api.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JwtBearerRequestFormatterTest {

    private static final String ASSERTION = "test.assertion.token";

    @Test
    public void testFormatBodyContainsGrantType() {
        JwtBearerRequestFormatter formatter = new JwtBearerRequestFormatter(null, () -> ASSERTION);
        assertTrue(formatter.formatBody().contains("grant_type=" + URLEncoder.encode(JwtBearerRequestFormatter.GRANT_TYPE, StandardCharsets.UTF_8)));
    }

    @Test
    public void testFormatBodyContainsAssertion() {
        JwtBearerRequestFormatter formatter = new JwtBearerRequestFormatter(null, () -> ASSERTION);
        assertTrue(formatter.formatBody().contains("assertion=" + URLEncoder.encode(ASSERTION, StandardCharsets.UTF_8)));
    }

    @Test
    public void testFormatBodyIncludesScopeWhenPresent() {
        JwtBearerRequestFormatter formatter = new JwtBearerRequestFormatter("my-scope", () -> ASSERTION);
        assertTrue(formatter.formatBody().contains("scope=my-scope"));
    }

    @Test
    public void testFormatBodyExcludesScopeWhenNull() {
        JwtBearerRequestFormatter formatter = new JwtBearerRequestFormatter(null, () -> ASSERTION);
        assertFalse(formatter.formatBody().contains("scope"));
    }

    @Test
    public void testFormatBodyExcludesScopeWhenBlank() {
        JwtBearerRequestFormatter formatter = new JwtBearerRequestFormatter("   ", () -> ASSERTION);
        assertFalse(formatter.formatBody().contains("scope"));
    }

    @Test
    public void testFormatBodyTrimsScopeWhitespace() {
        JwtBearerRequestFormatter formatter = new JwtBearerRequestFormatter("  my-scope  ", () -> ASSERTION);
        assertTrue(formatter.formatBody().contains("scope=my-scope"));
    }

    @Test
    public void testFormatHeadersContainsRequiredHeaders() {
        JwtBearerRequestFormatter formatter = new JwtBearerRequestFormatter(null, () -> ASSERTION);
        Map<String, String> headers = formatter.formatHeaders();
        assertEquals("application/json", headers.get("Accept"));
        assertEquals("no-cache", headers.get("Cache-Control"));
        assertEquals("application/x-www-form-urlencoded", headers.get("Content-Type"));
    }
}