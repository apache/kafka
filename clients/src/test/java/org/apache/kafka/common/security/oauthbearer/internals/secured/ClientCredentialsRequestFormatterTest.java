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

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.kafka.common.security.oauthbearer.internals.secured.ClientCredentialsRequestFormatter.GRANT_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClientCredentialsRequestFormatterTest extends OAuthBearerTest {

    public static final String CLIENT_ID = "jdoe";
    public static final String CLIENT_SECRET = "secret";
    public static final String SCOPE = "everythingeverything";

    @Test
    public void testFormatAuthorizationHeaderEncoding() {
        // according to RFC-7617, we need to use the *non-URL safe* base64 encoder. See KAFKA-14496.
        assertAuthorizationHeaderEquals("SOME_RANDOM_LONG_USER_01234", "9Q|0`8i~ute-n9ksjLWb\\50\"AX@UUED5E", false, "Basic U09NRV9SQU5ET01fTE9OR19VU0VSXzAxMjM0OjlRfDBgOGl+dXRlLW45a3NqTFdiXDUwIkFYQFVVRUQ1RQ==");
        // according to RFC-6749 clientId & clientSecret must be urlencoded, see https://tools.ietf.org/html/rfc6749#section-2.3.1
        assertAuthorizationHeaderEquals("user!@~'", "secret-(*)!", true, "Basic dXNlciUyMSU0MCU3RSUyNzpzZWNyZXQtJTI4KiUyOSUyMQ==");
    }

    @ParameterizedTest
    @MethodSource("testFormatterMissingValuesSource")
    public void testFormatterMissingValues(String clientId, String clientSecret, boolean urlencode) {
        assertThrows(
            ConfigException.class,
            () -> new ClientCredentialsRequestFormatter(
                clientId,
                clientSecret,
                SCOPE,
                urlencode
            )
        );
    }

    @ParameterizedTest
    @MethodSource("testScopeEscapingSource")
    public void testScopeEscaping(String scope, boolean urlencode, String expectedScope) {
        String expected = "grant_type=" + GRANT_TYPE + "&scope=" + expectedScope;
        assertRequestBodyEquals(scope, urlencode, expected);
    }

    @ParameterizedTest
    @MethodSource("testMissingScopesSource")
    public void testMissingScopes(String scope, boolean urlencode) {
        String expected = "grant_type=" + GRANT_TYPE;
        assertRequestBodyEquals(scope, urlencode, expected);
    }

    private static Stream<Arguments> testFormatterMissingValuesSource() {
        String[] clientIds = new String[] {null, "", "  ", CLIENT_ID};
        String[] clientSecrets = new String[] {null, "", "  ", CLIENT_SECRET};
        boolean[] urlencodes = new boolean[] {true, false};

        List<Arguments> list = new ArrayList<>();

        for (String clientId : clientIds) {
            for (String clientSecret : clientSecrets) {
                for (boolean urlencode : urlencodes) {
                    if (CLIENT_ID.equals(clientId) && CLIENT_SECRET.equals(clientSecret))
                        continue;

                    list.add(Arguments.of(clientId, clientSecret, urlencode));
                }
            }
        }

        return list.stream();
    }

    private static Stream<Arguments> testMissingScopesSource() {
        String[] scopes = new String[] {null, "", "  "};
        boolean[] urlencodes = new boolean[] {true, false};

        List<Arguments> list = new ArrayList<>();

        for (String scope : scopes) {
            for (boolean urlencode : urlencodes) {
                list.add(Arguments.of(scope, urlencode));
            }
        }

        return list.stream();
    }

    private static Stream<Arguments> testScopeEscapingSource() {
        return Stream.of(
            Arguments.of("test-scope", true, "test-scope"),
            Arguments.of("test-scope", false, "test-scope"),
            Arguments.of("earth is great!", true, "earth+is+great%21"),
            Arguments.of("earth is great!", false, "earth is great!"),
            Arguments.of("what on earth?!?!?", true, "what+on+earth%3F%21%3F%21%3F"),
            Arguments.of("what on earth?!?!?", false, "what on earth?!?!?")
        );
    }

    private void assertRequestBodyEquals(String scope, boolean urlencode, String expected) {
        ClientCredentialsRequestFormatter formatter = new ClientCredentialsRequestFormatter(
            CLIENT_ID,
            CLIENT_SECRET,
            scope,
            urlencode
        );
        String actual = formatter.formatBody();
        assertEquals(expected, actual);
    }

    private void assertAuthorizationHeaderEquals(String clientId, String clientSecret, boolean urlencode, String expected) {
        ClientCredentialsRequestFormatter formatter = new ClientCredentialsRequestFormatter(clientId, clientSecret, SCOPE, urlencode);
        Map<String, String> headers = formatter.formatHeaders();
        String actual = headers.get("Authorization");
        assertEquals(expected, actual, String.format("Expected the HTTP Authorization header generated for client ID \"%s\" and client secret \"%s\" to match", clientId, clientSecret));
    }
}
