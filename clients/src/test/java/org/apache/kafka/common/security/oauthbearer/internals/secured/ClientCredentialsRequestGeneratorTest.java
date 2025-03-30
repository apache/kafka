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

import org.apache.kafka.common.security.oauthbearer.ClientCredentialsJwtRetriever;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ClientCredentialsRequestGeneratorTest extends HttpRequestGeneratorTest {

    @Test
    public void testFormatAuthorizationHeader() {
        ClientCredentialsRequestGenerator requestGenerator = new Builder()
            .setClientId("id")
            .setClientSecret("secret")
            .build();
        assertAuthorizationHeaderEquals(requestGenerator, "Basic aWQ6c2VjcmV0");
    }

    @Test
    public void testFormatAuthorizationHeaderEncoding() {
        ClientCredentialsRequestGenerator requestGenerator = new Builder()
            .setClientId("SOME_RANDOM_LONG_USER_01234")
            .setClientSecret("9Q|0`8i~ute-n9ksjLWb\\50\"AX@UUED5E")
            .build();
        // according to RFC-7617, we need to use the *non-URL safe* base64 encoder. See KAFKA-14496.
        assertAuthorizationHeaderEquals(requestGenerator, "Basic U09NRV9SQU5ET01fTE9OR19VU0VSXzAxMjM0OjlRfDBgOGl+dXRlLW45a3NqTFdiXDUwIkFYQFVVRUQ1RQ==");

        requestGenerator = new Builder()
            .setClientId("user!@~'")
            .setClientSecret("secret-(*)!")
            .setUrlencode(true)
            .build();
        // according to RFC-6749 clientId & clientSecret must be urlencoded, see https://tools.ietf.org/html/rfc6749#section-2.3.1
        assertAuthorizationHeaderEquals(requestGenerator, "Basic dXNlciUyMSU0MCU3RSUyNzpzZWNyZXQtJTI4KiUyOSUyMQ==");
    }

    @Test
    public void testFormatRequestBody() {
        ClientCredentialsRequestGenerator requestGenerator = new Builder()
            .setScope("test")
            .build();
        assertBodyEquals(requestGenerator, "grant_type=client_credentials&scope=test");
    }

    @Test
    public void testFormatRequestBodyWithEscaped() {
        String questionMark = "%3F";
        String exclamationMark = "%21";

        Builder builder = new Builder()
            .setUrlencode(false);

        String expected = String.format("grant_type=client_credentials&scope=earth+is+great%s", exclamationMark);
        assertBodyEquals(builder.setScope("earth is great!").build(), expected);

        expected = String.format("grant_type=client_credentials&scope=what+on+earth%s%s%s%s%s", questionMark, exclamationMark, questionMark, exclamationMark, questionMark);
        assertBodyEquals(builder.setScope("what on earth?!?!?").build(), expected);
    }

    @Test
    public void testFormatRequestBodyMissingValues() {
        Builder builder = new Builder();

        String expected = "grant_type=client_credentials";
        assertBodyEquals(builder.setScope(null).build(), expected);
        assertBodyEquals(builder.setScope("").build(), expected);
        assertBodyEquals(builder.setScope("  ").build(), expected);
    }

    @ParameterizedTest
    @MethodSource("urlencodeHeaderSupplier")
    public void testUrlencodeHeader(Map<String, Object> configs, boolean expectedValue) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, OAUTHBEARER_MECHANISM);
        boolean actualValue = ClientCredentialsJwtRetriever.validateUrlencodeHeader(cu);
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

    private void assertAuthorizationHeaderEquals(ClientCredentialsRequestGenerator requestGenerator, String expected) {
        String actual = requestGenerator.generateHeaders().get("Authorization");
        assertNotNull(actual);
        assertEquals(expected, actual);
    }

    private static class Builder {

        private String clientId = "testClientId";
        private String clientSecret = "testSecret";
        private String scope = "testScope";
        private boolean urlencode = false;

        public Builder setClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder setClientSecret(String clientSecret) {
            this.clientSecret = clientSecret;
            return this;
        }

        public Builder setScope(String scope) {
            this.scope = scope;
            return this;
        }

        public Builder setUrlencode(boolean urlencode) {
            this.urlencode = urlencode;
            return this;
        }

        private ClientCredentialsRequestGenerator build() {
            return new ClientCredentialsRequestGenerator(
                URI.create("http://www.example.com"),
                clientId,
                clientSecret,
                scope,
                urlencode
            );
        }
    }
}
