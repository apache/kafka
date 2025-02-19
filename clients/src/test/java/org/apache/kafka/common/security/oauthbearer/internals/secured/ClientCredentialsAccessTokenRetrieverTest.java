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

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClientCredentialsAccessTokenRetrieverTest extends OAuthBearerTest {

    @Test
    public void testFormatAuthorizationHeader() {
        assertAuthorizationHeader("id", "secret", false, "Basic aWQ6c2VjcmV0");
    }

    @Test
    public void testFormatAuthorizationHeaderEncoding() {
        // according to RFC-7617, we need to use the *non-URL safe* base64 encoder. See KAFKA-14496.
        assertAuthorizationHeader("SOME_RANDOM_LONG_USER_01234", "9Q|0`8i~ute-n9ksjLWb\\50\"AX@UUED5E", false, "Basic U09NRV9SQU5ET01fTE9OR19VU0VSXzAxMjM0OjlRfDBgOGl+dXRlLW45a3NqTFdiXDUwIkFYQFVVRUQ1RQ==");
        // according to RFC-6749 clientId & clientSecret must be urlencoded, see https://tools.ietf.org/html/rfc6749#section-2.3.1
        assertAuthorizationHeader("user!@~'", "secret-(*)!", true, "Basic dXNlciUyMSU0MCU3RSUyNzpzZWNyZXQtJTI4KiUyOSUyMQ==");
    }

    @Test
    public void testFormatAuthorizationHeaderMissingValues() {
        assertThrows(IllegalArgumentException.class, () -> ClientCredentialsAccessTokenRetriever.formatAuthorizationHeader(null, "secret", false));
        assertThrows(IllegalArgumentException.class, () -> ClientCredentialsAccessTokenRetriever.formatAuthorizationHeader("id", null, false));
        assertThrows(IllegalArgumentException.class, () -> ClientCredentialsAccessTokenRetriever.formatAuthorizationHeader(null, null, false));
        assertThrows(IllegalArgumentException.class, () -> ClientCredentialsAccessTokenRetriever.formatAuthorizationHeader("", "secret", false));
        assertThrows(IllegalArgumentException.class, () -> ClientCredentialsAccessTokenRetriever.formatAuthorizationHeader("id", "", false));
        assertThrows(IllegalArgumentException.class, () -> ClientCredentialsAccessTokenRetriever.formatAuthorizationHeader("", "", false));
        assertThrows(IllegalArgumentException.class, () -> ClientCredentialsAccessTokenRetriever.formatAuthorizationHeader("  ", "secret", false));
        assertThrows(IllegalArgumentException.class, () -> ClientCredentialsAccessTokenRetriever.formatAuthorizationHeader("id", "  ", false));
        assertThrows(IllegalArgumentException.class, () -> ClientCredentialsAccessTokenRetriever.formatAuthorizationHeader("  ", "  ", false));
    }

    @Test
    public void testFormatRequestBody() {
        String expected = "grant_type=client_credentials&scope=scope";
        String actual = ClientCredentialsAccessTokenRetriever.formatRequestBody("scope");
        assertEquals(expected, actual);
    }

    @Test
    public void testFormatRequestBodyWithEscaped() {
        String questionMark = "%3F";
        String exclamationMark = "%21";

        String expected = String.format("grant_type=client_credentials&scope=earth+is+great%s", exclamationMark);
        String actual = ClientCredentialsAccessTokenRetriever.formatRequestBody("earth is great!");
        assertEquals(expected, actual);

        expected = String.format("grant_type=client_credentials&scope=what+on+earth%s%s%s%s%s", questionMark, exclamationMark, questionMark, exclamationMark, questionMark);
        actual = ClientCredentialsAccessTokenRetriever.formatRequestBody("what on earth?!?!?");
        assertEquals(expected, actual);
    }

    @Test
    public void testFormatRequestBodyMissingValues() {
        String expected = "grant_type=client_credentials";
        String actual = ClientCredentialsAccessTokenRetriever.formatRequestBody(null);
        assertEquals(expected, actual);

        actual = ClientCredentialsAccessTokenRetriever.formatRequestBody("");
        assertEquals(expected, actual);

        actual = ClientCredentialsAccessTokenRetriever.formatRequestBody("  ");
        assertEquals(expected, actual);
    }

    @ParameterizedTest
    @MethodSource("urlencodeHeaderSupplier")
    public void testUrlencodeHeader(Map<String, Object> configs, boolean expectedValue) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, OAUTHBEARER_MECHANISM);
        boolean actualValue = ClientCredentialsAccessTokenRetriever.validateUrlencodeHeader(cu);
        assertEquals(expectedValue, actualValue);
    }

    private static Stream<Arguments> urlencodeHeaderSupplier() {
        return Stream.of(
            Arguments.of(Collections.emptyMap(), DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE),
            Arguments.of(Collections.singletonMap(SASL_OAUTHBEARER_HEADER_URLENCODE, null), DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE),
            Arguments.of(Collections.singletonMap(SASL_OAUTHBEARER_HEADER_URLENCODE, true), true),
            Arguments.of(Collections.singletonMap(SASL_OAUTHBEARER_HEADER_URLENCODE, false), false)
        );
    }

    private void assertAuthorizationHeader(String clientId, String clientSecret, boolean urlencode, String expected) {
        String actual = ClientCredentialsAccessTokenRetriever.formatAuthorizationHeader(clientId, clientSecret, urlencode);
        assertEquals(expected, actual, String.format("Expected the HTTP Authorization header generated for client ID \"%s\" and client secret \"%s\" to match", clientId, clientSecret));
    }
}
