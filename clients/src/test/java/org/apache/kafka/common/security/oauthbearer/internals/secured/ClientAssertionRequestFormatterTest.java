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

import org.apache.kafka.common.security.oauthbearer.JwtRetrieverException;
import org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.CloseableSupplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.kafka.common.security.oauthbearer.internals.secured.ClientAssertionRequestFormatter.GRANT_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClientAssertionRequestFormatterTest extends OAuthBearerTest {

    private static final String CLIENT_ID = "test-client-id";
    private static final String SCOPE = "openid profile";
    private static final String ASSERTION = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.signature";

    @Test
    public void testFormatHeaders() {
        ClientAssertionRequestFormatter formatter = createFormatter(CLIENT_ID, SCOPE, ASSERTION);

        Map<String, String> headers = formatter.formatHeaders();

        assertEquals(3, headers.size());
        assertEquals("application/json", headers.get("Accept"));
        assertEquals("no-cache", headers.get("Cache-Control"));
        assertEquals("application/x-www-form-urlencoded", headers.get("Content-Type"));
    }

    @Test
    public void testFormatHeadersDoNotContainAuthorization() {
        // Client assertion auth doesn't use an Authorization header (unlike client secret)
        ClientAssertionRequestFormatter formatter = createFormatter(CLIENT_ID, SCOPE, ASSERTION);

        Map<String, String> headers = formatter.formatHeaders();

        assertFalse(headers.containsKey("Authorization"));
    }

    @Test
    public void testFormatBodyContainsAssertionType() {
        ClientAssertionRequestFormatter formatter = createFormatter(CLIENT_ID, SCOPE, ASSERTION);

        String body = formatter.formatBody();

        assertTrue(body.contains("client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer"));
    }

    @Test
    public void testFormatBodyContainsGrantType() {
        ClientAssertionRequestFormatter formatter = createFormatter(CLIENT_ID, SCOPE, ASSERTION);

        String body = formatter.formatBody();

        assertTrue(body.contains("grant_type=client_credentials"));
    }

    @Test
    public void testFormatBodyWithClientIdAndScope() {
        ClientAssertionRequestFormatter formatter = createFormatter(CLIENT_ID, SCOPE, ASSERTION);

        String body = formatter.formatBody();

        assertTrue(body.contains("client_assertion=" + ASSERTION));
        assertTrue(body.contains("client_id=" + CLIENT_ID));
        assertTrue(body.contains("scope=openid+profile"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    public void testFormatBodyOmitsClientIdWhenNullOrEmpty(String clientId) {
        ClientAssertionRequestFormatter formatter = createFormatter(clientId, SCOPE, ASSERTION);

        String body = formatter.formatBody();

        assertFalse(body.contains("client_id="));
        assertTrue(body.contains("scope=openid+profile"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   ", "\t"})
    public void testFormatBodyOmitsScopeWhenBlankOrNull(String scope) {
        ClientAssertionRequestFormatter formatter = createFormatter(CLIENT_ID, scope, ASSERTION);

        String body = formatter.formatBody();

        assertTrue(body.contains("client_id=" + CLIENT_ID));
        assertFalse(body.contains("scope="));
    }

    @Test
    public void testFormatBodyMinimal() {
        ClientAssertionRequestFormatter formatter = createFormatter(null, null, ASSERTION);

        String body = formatter.formatBody();

        String expected = "client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer"
            + "&client_assertion=" + ASSERTION
            + "&grant_type=client_credentials";
        assertEquals(expected, body);
    }

    @ParameterizedTest
    @MethodSource("scopeUrlEncodingSource")
    public void testScopeUrlEncoding(String scope, String expectedScope) {
        ClientAssertionRequestFormatter formatter = createFormatter(null, scope, ASSERTION);

        String body = formatter.formatBody();

        assertTrue(body.endsWith("&scope=" + expectedScope), "Body: " + body);
    }

    private static Stream<Arguments> scopeUrlEncodingSource() {
        return Stream.of(
            Arguments.of("simple-scope", "simple-scope"),
            Arguments.of("openid profile", "openid+profile"),
            Arguments.of("scope!special", "scope%21special"),
            Arguments.of("what?query", "what%3Fquery"),
            Arguments.of("  trimmed  ", "trimmed")
        );
    }

    @Test
    public void testAssertionValueComesFromSupplier() {
        String customAssertion = "my-custom-assertion-jwt-value";
        ClientAssertionRequestFormatter formatter = createFormatter(null, null, customAssertion);

        String body = formatter.formatBody();

        assertTrue(body.contains("client_assertion=" + customAssertion));
    }

    @Test
    public void testFormatBodyParameterOrder() {
        ClientAssertionRequestFormatter formatter = createFormatter(CLIENT_ID, SCOPE, ASSERTION);

        String body = formatter.formatBody();

        // Verify parameter ordering: assertion_type, assertion, grant_type, client_id, scope
        int assertionTypeIdx = body.indexOf("client_assertion_type=");
        int assertionIdx = body.indexOf("&client_assertion=");
        int grantTypeIdx = body.indexOf("&grant_type=");
        int clientIdIdx = body.indexOf("&client_id=");
        int scopeIdx = body.indexOf("&scope=");

        assertTrue(assertionTypeIdx < assertionIdx, "client_assertion_type should come before client_assertion");
        assertTrue(assertionIdx < grantTypeIdx, "client_assertion should come before grant_type");
        assertTrue(grantTypeIdx < clientIdIdx, "grant_type should come before client_id");
        assertTrue(clientIdIdx < scopeIdx, "client_id should come before scope");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCloseDelegatesToSupplier() throws IOException {
        CloseableSupplier<String> supplier = mock(CloseableSupplier.class);
        ClientAssertionRequestFormatter formatter = new ClientAssertionRequestFormatter(CLIENT_ID, SCOPE, supplier);

        formatter.close();

        verify(supplier).close();
    }

    @Test
    public void testGrantTypeConstant() {
        assertEquals("client_credentials", GRANT_TYPE);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFormatBodyPropagatesSupplierException() {
        CloseableSupplier<String> supplier = mock(CloseableSupplier.class);
        when(supplier.get()).thenThrow(new JwtRetrieverException("supplier failed"));
        ClientAssertionRequestFormatter formatter = new ClientAssertionRequestFormatter(CLIENT_ID, SCOPE, supplier);
        assertThrows(JwtRetrieverException.class, formatter::formatBody);
    }

    @SuppressWarnings("unchecked")
    private ClientAssertionRequestFormatter createFormatter(String clientId, String scope, String assertionValue) {
        CloseableSupplier<String> supplier = mock(CloseableSupplier.class);
        when(supplier.get()).thenReturn(assertionValue);
        return new ClientAssertionRequestFormatter(clientId, scope, supplier);
    }
}
