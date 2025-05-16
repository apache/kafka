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

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientCredentialsRequestFormatterTest extends OAuthBearerTest {

    @Test
    public void testFormatAuthorizationHeaderEncoding() {
        // according to RFC-7617, we need to use the *non-URL safe* base64 encoder. See KAFKA-14496.
        assertAuthorizationHeader("SOME_RANDOM_LONG_USER_01234", "9Q|0`8i~ute-n9ksjLWb\\50\"AX@UUED5E", false, "Basic U09NRV9SQU5ET01fTE9OR19VU0VSXzAxMjM0OjlRfDBgOGl+dXRlLW45a3NqTFdiXDUwIkFYQFVVRUQ1RQ==");
        // according to RFC-6749 clientId & clientSecret must be urlencoded, see https://tools.ietf.org/html/rfc6749#section-2.3.1
        assertAuthorizationHeader("user!@~'", "secret-(*)!", true, "Basic dXNlciUyMSU0MCU3RSUyNzpzZWNyZXQtJTI4KiUyOSUyMQ==");
    }

    @Test
    public void testFormatAuthorizationHeaderMissingValues() {
        assertThrows(ConfigException.class, () -> formatAuthorizationHeader(null, "secret", false));
        assertThrows(ConfigException.class, () -> formatAuthorizationHeader("id", null, false));
        assertThrows(ConfigException.class, () -> formatAuthorizationHeader(null, null, false));
        assertThrows(ConfigException.class, () -> formatAuthorizationHeader("", "secret", false));
        assertThrows(ConfigException.class, () -> formatAuthorizationHeader("id", "", false));
        assertThrows(ConfigException.class, () -> formatAuthorizationHeader("", "", false));
        assertThrows(ConfigException.class, () -> formatAuthorizationHeader("  ", "secret", false));
        assertThrows(ConfigException.class, () -> formatAuthorizationHeader("id", "  ", false));
        assertThrows(ConfigException.class, () -> formatAuthorizationHeader("  ", "  ", false));
    }

    @Test
    public void testFormatRequestBody() {
        String expected = "grant_type=client_credentials&scope=scope";
        String actual = formatRequestBody("scope");
        assertEquals(expected, actual);
    }

    @Test
    public void testFormatRequestBodyWithEscaped() {
        String questionMark = "%3F";
        String exclamationMark = "%21";

        String expected = String.format("grant_type=client_credentials&scope=earth+is+great%s", exclamationMark);
        String actual = formatRequestBody("earth is great!");
        assertEquals(expected, actual);

        expected = String.format("grant_type=client_credentials&scope=what+on+earth%s%s%s%s%s", questionMark, exclamationMark, questionMark, exclamationMark, questionMark);
        actual = formatRequestBody("what on earth?!?!?");
        assertEquals(expected, actual);
    }

    @Test
    public void testFormatRequestBodyMissingValues() {
        String expected = "grant_type=client_credentials";
        String actual = formatRequestBody(null);
        assertEquals(expected, actual);

        actual = formatRequestBody("");
        assertEquals(expected, actual);

        actual = formatRequestBody("  ");
        assertEquals(expected, actual);
    }

    private String formatAuthorizationHeader(String clientId, String clientSecret, boolean urlencode) {
        ClientCredentialsRequestFormatter formatter = new ClientCredentialsRequestFormatter(clientId, clientSecret, "", urlencode);
        Map<String, String> headers = formatter.formatHeaders();
        assertTrue(headers.containsKey("Authorization"));
        return headers.get("Authorization");
    }

    private String formatRequestBody(String scope) {
        ClientCredentialsRequestFormatter formatter = new ClientCredentialsRequestFormatter("jdoe", "secret", scope, false);
        return formatter.formatBody();
    }

    private void assertAuthorizationHeader(String clientId, String clientSecret, boolean urlencode, String expected) {
        String actual = formatAuthorizationHeader(clientId, clientSecret, urlencode);
        assertEquals(expected, actual, String.format("Expected the HTTP Authorization header generated for client ID \"%s\" and client secret \"%s\" to match", clientId, clientSecret));
    }
}
