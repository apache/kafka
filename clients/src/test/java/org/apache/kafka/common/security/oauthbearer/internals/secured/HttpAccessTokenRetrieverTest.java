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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HttpAccessTokenRetrieverTest extends OAuthBearerTest {

    @Test
    public void testErrorResponseUnretryableCode() {
        testErrorResponse(
            UnretryableException.class,
            HttpURLConnection.HTTP_BAD_REQUEST,
            "{\"error\":\"some_arg\", \"error_description\":\"some problem with arg\"}",
            "{\"some_arg\" - \"some problem with arg\"}"
        );
    }

    @Test
    public void testErrorResponseRetryableCode() throws IOException {
        testErrorResponse(
            IOException.class,
            HttpURLConnection.HTTP_INTERNAL_ERROR,
            "{\"error\":\"some_arg\", \"error_description\":\"some problem with arg\"}",
            "{\"some_arg\" - \"some problem with arg\"}"
        );

        // error response body has different keys
        testErrorResponse(
            IOException.class,
            HttpURLConnection.HTTP_INTERNAL_ERROR,
            "{\"errorCode\":\"some_arg\", \"errorSummary\":\"some problem with arg\"}",
            "{\"some_arg\" - \"some problem with arg\"}"
        );

        // error response is valid json but unknown keys
        testErrorResponse(
            IOException.class,
            HttpURLConnection.HTTP_INTERNAL_ERROR,
            "{\"err\":\"some_arg\", \"err_des\":\"some problem with arg\"}",
            "{\"err\":\"some_arg\", \"err_des\":\"some problem with arg\"}"
        );
    }

    @Test
    public void testErrorResponseIsInvalidJson() {
        testErrorResponse(
            IOException.class,
            HttpURLConnection.HTTP_INTERNAL_ERROR,
            "non json error output",
            "{non json error output}"
        );
    }

    @Test
    public void testParseAccessToken() throws IOException {
        String expected = "abc";
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("access_token", expected);

        String actual = HttpAccessTokenRetriever.parseAccessToken(mapper.writeValueAsString(node));
        assertEquals(expected, actual);
    }

    @Test
    public void testParseAccessTokenEmptyAccessToken() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("access_token", "");

        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.parseAccessToken(mapper.writeValueAsString(node)));
    }

    @Test
    public void testParseAccessTokenMissingAccessToken() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("sub", "jdoe");

        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.parseAccessToken(mapper.writeValueAsString(node)));
    }

    @Test
    public void testParseAccessTokenInvalidJson() {
        assertThrows(IOException.class, () -> HttpAccessTokenRetriever.parseAccessToken("not valid JSON"));
    }

    @ParameterizedTest
    @MethodSource("urlencodeHeaderSupplier")
    public void testUrlencodeHeader(Map<String, Object> configs, boolean expectedValue) {
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        boolean actualValue = HttpAccessTokenRetriever.validateUrlencodeHeader(cu);
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

    private <T extends Exception> void testErrorResponse(Class<T> exceptionClazz,
                                                         int responseCode,
                                                         String errorResponse,
                                                         String substringMatch) {
        OAuthBearerHttpClient.HttpResponse response = new OAuthBearerHttpClient.HttpResponse(
            responseCode,
            Optional.empty(),
            Optional.of(errorResponse.getBytes(StandardCharsets.UTF_8))
        );
        Exception e = assertThrows(
            exceptionClazz,
            () -> HttpAccessTokenRetriever.handleOutput("https://www.example.com", response)
        );
        assertTrue(e.getMessage().contains(substringMatch), e.getMessage());
    }
}
