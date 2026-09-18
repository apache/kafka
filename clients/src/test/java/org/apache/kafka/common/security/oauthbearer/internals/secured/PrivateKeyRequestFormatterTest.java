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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PrivateKeyRequestFormatterTest extends OAuthBearerTest {

    private static final String TEST_ASSERTION = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.test.signature";
    private static final String TEST_CLIENT_ID = "test-client";
    private static final String TEST_SCOPE = "test-scope";
    private static final Supplier<String> ASSERTION_SUPPLIER = () -> TEST_ASSERTION;

    @Test
    public void testFormatHeaders() {
        PrivateKeyRequestFormatter formatter = new PrivateKeyRequestFormatter(
            TEST_SCOPE,
            ASSERTION_SUPPLIER,
            Optional.of(TEST_CLIENT_ID)
        );

        Map<String, String> headers = formatter.formatHeaders();

        assertEquals("application/json", headers.get("Accept"));
        assertEquals("no-cache", headers.get("Cache-Control"));
        assertEquals("application/x-www-form-urlencoded", headers.get("Content-Type"));
        assertEquals(3, headers.size());
    }

    @Test
    public void testFormatBodyWithAllParameters() {
        PrivateKeyRequestFormatter formatter = new PrivateKeyRequestFormatter(
            TEST_SCOPE,
            ASSERTION_SUPPLIER,
            Optional.of(TEST_CLIENT_ID)
        );

        String body = formatter.formatBody();

        assertTrue(body.contains("grant_type=client_credentials"));
        assertTrue(body.contains("client_assertion=" + TEST_ASSERTION));
        assertTrue(body.contains("client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer"));
        assertTrue(body.contains("client_id=" + TEST_CLIENT_ID));
        assertTrue(body.contains("scope=" + TEST_SCOPE));
    }

    @Test
    public void testFormatBodyWithoutClientId() {
        PrivateKeyRequestFormatter formatter = new PrivateKeyRequestFormatter(
            TEST_SCOPE,
            ASSERTION_SUPPLIER,
            Optional.empty()
        );

        String body = formatter.formatBody();

        assertTrue(body.contains("grant_type=client_credentials"));
        assertTrue(body.contains("client_assertion=" + TEST_ASSERTION));
        assertTrue(body.contains("client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer"));
        assertTrue(body.contains("scope=" + TEST_SCOPE));
        assertTrue(!body.contains("client_id="));
    }

    @Test
    public void testFormatBodyWithoutScope() {
        PrivateKeyRequestFormatter formatter = new PrivateKeyRequestFormatter(
            null,
            ASSERTION_SUPPLIER,
            Optional.of(TEST_CLIENT_ID)
        );

        String body = formatter.formatBody();

        assertTrue(body.contains("grant_type=client_credentials"));
        assertTrue(body.contains("client_assertion=" + TEST_ASSERTION));
        assertTrue(body.contains("client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer"));
        assertTrue(body.contains("client_id=" + TEST_CLIENT_ID));
        assertTrue(!body.contains("scope="));
    }

    @Test
    public void testFormatBodyWithEmptyScope() {
        PrivateKeyRequestFormatter formatter = new PrivateKeyRequestFormatter(
            "",
            ASSERTION_SUPPLIER,
            Optional.of(TEST_CLIENT_ID)
        );

        String body = formatter.formatBody();

        assertTrue(body.contains("grant_type=client_credentials"));
        assertTrue(body.contains("client_assertion=" + TEST_ASSERTION));
        assertTrue(body.contains("client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer"));
        assertTrue(body.contains("client_id=" + TEST_CLIENT_ID));
        assertTrue(!body.contains("scope="));
    }

    @Test
    public void testFormatBodyWithWhitespaceScope() {
        PrivateKeyRequestFormatter formatter = new PrivateKeyRequestFormatter(
            "   ",
            ASSERTION_SUPPLIER,
            Optional.of(TEST_CLIENT_ID)
        );

        String body = formatter.formatBody();

        assertTrue(body.contains("grant_type=client_credentials"));
        assertTrue(body.contains("client_assertion=" + TEST_ASSERTION));
        assertTrue(body.contains("client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer"));
        assertTrue(body.contains("client_id=" + TEST_CLIENT_ID));
        assertTrue(!body.contains("scope="));
    }

    @ParameterizedTest
    @MethodSource("testScopeEncodingSource")
    public void testScopeEncoding(String scope, String expectedEncodedScope) {
        PrivateKeyRequestFormatter formatter = new PrivateKeyRequestFormatter(
            scope,
            ASSERTION_SUPPLIER,
            Optional.of(TEST_CLIENT_ID)
        );

        String body = formatter.formatBody();
        assertTrue(body.contains("scope=" + expectedEncodedScope));
    }

    @ParameterizedTest
    @MethodSource("testClientIdEncodingSource")
    public void testClientIdEncoding(String clientId, String expectedEncodedClientId) {
        PrivateKeyRequestFormatter formatter = new PrivateKeyRequestFormatter(
            TEST_SCOPE,
            ASSERTION_SUPPLIER,
            Optional.of(clientId)
        );

        String body = formatter.formatBody();
        assertTrue(body.contains("client_id=" + expectedEncodedClientId));
    }

    @Test
    public void testGetGrantType() {
        PrivateKeyRequestFormatter formatter = new PrivateKeyRequestFormatter(
            TEST_SCOPE,
            ASSERTION_SUPPLIER,
            Optional.of(TEST_CLIENT_ID)
        );

        assertEquals("client_credentials", formatter.getGrantType());
    }

    @Test
    public void testGetClientAssertionType() {
        PrivateKeyRequestFormatter formatter = new PrivateKeyRequestFormatter(
            TEST_SCOPE,
            ASSERTION_SUPPLIER,
            Optional.of(TEST_CLIENT_ID)
        );

        assertEquals("urn:ietf:params:oauth:client-assertion-type:jwt-bearer", formatter.getClientAssertionType());
    }

    @Test
    public void testAssertionSupplierCalled() {
        final String[] capturedAssertion = new String[1];
        Supplier<String> trackingSupplier = () -> {
            capturedAssertion[0] = TEST_ASSERTION;
            return TEST_ASSERTION;
        };

        PrivateKeyRequestFormatter formatter = new PrivateKeyRequestFormatter(
            TEST_SCOPE,
            trackingSupplier,
            Optional.of(TEST_CLIENT_ID)
        );

        formatter.formatBody();
        assertEquals(TEST_ASSERTION, capturedAssertion[0]);
    }

    private static Stream<Arguments> testScopeEncodingSource() {
        return Stream.of(
            Arguments.of("simple-scope", "simple-scope"),
            Arguments.of("scope with spaces", "scope+with+spaces"),
            Arguments.of("scope@special!chars", "scope%40special%21chars"),
            Arguments.of("read:user write:repo", "read%3Auser+write%3Arepo")
        );
    }

    private static Stream<Arguments> testClientIdEncodingSource() {
        return Stream.of(
            Arguments.of("simple-client", "simple-client"),
            Arguments.of("client with spaces", "client+with+spaces"),
            Arguments.of("client@domain.com", "client%40domain.com"),
            Arguments.of("client:service", "client%3Aservice")
        );
    }
}