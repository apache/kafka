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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpAccessTokenRetrieverTest extends OAuthBearerTest {

    @Test
    public void test() throws IOException {
        String expectedResponse = "Hiya, buddy";
        HttpURLConnection mockedCon = createHttpURLConnection(expectedResponse);
        String response = HttpAccessTokenRetriever.post(mockedCon, null, null, null, null);
        assertEquals(expectedResponse, response);
    }

    @Test
    public void testEmptyResponse() throws IOException {
        HttpURLConnection mockedCon = createHttpURLConnection("");
        assertThrows(IOException.class, () -> HttpAccessTokenRetriever.post(mockedCon, null, null, null, null));
    }

    @Test
    public void testErrorReadingResponse() throws IOException {
        HttpURLConnection mockedCon = createHttpURLConnection("dummy");
        when(mockedCon.getInputStream()).thenThrow(new IOException("Can't read"));

        assertThrows(IOException.class, () -> HttpAccessTokenRetriever.post(mockedCon, null, null, null, null));
    }

    @Test
    public void testErrorResponseUnretryableCode() throws IOException {
        HttpURLConnection mockedCon = createHttpURLConnection("dummy");
        when(mockedCon.getInputStream()).thenThrow(new IOException("Can't read"));
        when(mockedCon.getErrorStream()).thenReturn(new ByteArrayInputStream(
            "{\"error\":\"some_arg\", \"error_description\":\"some problem with arg\"}"
                .getBytes(StandardCharsets.UTF_8)));
        when(mockedCon.getResponseCode()).thenReturn(HttpURLConnection.HTTP_BAD_REQUEST);
        UnretryableException ioe = assertThrows(UnretryableException.class,
            () -> HttpAccessTokenRetriever.post(mockedCon, null, null, null, null));
        assertTrue(ioe.getMessage().contains("{\"some_arg\" - \"some problem with arg\"}"));
    }

    @Test
    public void testErrorResponseRetryableCode() throws IOException {
        HttpURLConnection mockedCon = createHttpURLConnection("dummy");
        when(mockedCon.getInputStream()).thenThrow(new IOException("Can't read"));
        when(mockedCon.getErrorStream()).thenReturn(new ByteArrayInputStream(
            "{\"error\":\"some_arg\", \"error_description\":\"some problem with arg\"}"
                .getBytes(StandardCharsets.UTF_8)));
        when(mockedCon.getResponseCode()).thenReturn(HttpURLConnection.HTTP_INTERNAL_ERROR);
        IOException ioe = assertThrows(IOException.class,
            () -> HttpAccessTokenRetriever.post(mockedCon, null, null, null, null));
        assertTrue(ioe.getMessage().contains("{\"some_arg\" - \"some problem with arg\"}"));

        // error response body has different keys
        when(mockedCon.getErrorStream()).thenReturn(new ByteArrayInputStream(
            "{\"errorCode\":\"some_arg\", \"errorSummary\":\"some problem with arg\"}"
                .getBytes(StandardCharsets.UTF_8)));
        ioe = assertThrows(IOException.class,
            () -> HttpAccessTokenRetriever.post(mockedCon, null, null, null, null));
        assertTrue(ioe.getMessage().contains("{\"some_arg\" - \"some problem with arg\"}"));

        // error response is valid json but unknown keys
        when(mockedCon.getErrorStream()).thenReturn(new ByteArrayInputStream(
            "{\"err\":\"some_arg\", \"err_des\":\"some problem with arg\"}"
                .getBytes(StandardCharsets.UTF_8)));
        ioe = assertThrows(IOException.class,
            () -> HttpAccessTokenRetriever.post(mockedCon, null, null, null, null));
        assertTrue(ioe.getMessage().contains("{\"err\":\"some_arg\", \"err_des\":\"some problem with arg\"}"));
    }

    @Test
    public void testErrorResponseIsInvalidJson() throws IOException {
        HttpURLConnection mockedCon = createHttpURLConnection("dummy");
        when(mockedCon.getInputStream()).thenThrow(new IOException("Can't read"));
        when(mockedCon.getErrorStream()).thenReturn(new ByteArrayInputStream(
            "non json error output".getBytes(StandardCharsets.UTF_8)));
        when(mockedCon.getResponseCode()).thenReturn(HttpURLConnection.HTTP_INTERNAL_ERROR);
        IOException ioe = assertThrows(IOException.class,
            () -> HttpAccessTokenRetriever.post(mockedCon, null, null, null, null));
        assertTrue(ioe.getMessage().contains("{non json error output}"));
    }

    @Test
    public void testCopy() throws IOException {
        byte[] expected = new byte[4096 + 1];
        Random r = new Random();
        r.nextBytes(expected);
        InputStream in = new ByteArrayInputStream(expected);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpAccessTokenRetriever.copy(in, out);
        assertArrayEquals(expected, out.toByteArray());
    }

    @Test
    public void testCopyError() throws IOException {
        InputStream mockedIn = mock(InputStream.class);
        OutputStream out = new ByteArrayOutputStream();
        when(mockedIn.read(any(byte[].class))).thenThrow(new IOException());
        assertThrows(IOException.class, () -> HttpAccessTokenRetriever.copy(mockedIn, out));
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

    private void assertAuthorizationHeader(String clientId, String clientSecret, boolean urlencode, String expected) {
        String actual = HttpAccessTokenRetriever.formatAuthorizationHeader(clientId, clientSecret, urlencode);
        assertEquals(expected, actual, String.format("Expected the HTTP Authorization header generated for client ID \"%s\" and client secret \"%s\" to match", clientId, clientSecret));
    }

    @Test
    public void testFormatAuthorizationHeaderMissingValues() {
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader(null, "secret", false));
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader("id", null, false));
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader(null, null, false));
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader("", "secret", false));
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader("id", "", false));
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader("", "", false));
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader("  ", "secret", false));
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader("id", "  ", false));
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader("  ", "  ", false));
    }

    @Test
    public void testFormatRequestBody() {
        String expected = "grant_type=client_credentials&scope=scope";
        String actual = HttpAccessTokenRetriever.formatRequestBody("scope");
        assertEquals(expected, actual);
    }

    @Test
    public void testFormatRequestBodyWithEscaped() {
        String questionMark = "%3F";
        String exclamationMark = "%21";

        String expected = String.format("grant_type=client_credentials&scope=earth+is+great%s", exclamationMark);
        String actual = HttpAccessTokenRetriever.formatRequestBody("earth is great!");
        assertEquals(expected, actual);

        expected = String.format("grant_type=client_credentials&scope=what+on+earth%s%s%s%s%s", questionMark, exclamationMark, questionMark, exclamationMark, questionMark);
        actual = HttpAccessTokenRetriever.formatRequestBody("what on earth?!?!?");
        assertEquals(expected, actual);
    }

    @Test
    public void testFormatRequestBodyMissingValues() {
        String expected = "grant_type=client_credentials";
        String actual = HttpAccessTokenRetriever.formatRequestBody(null);
        assertEquals(expected, actual);

        actual = HttpAccessTokenRetriever.formatRequestBody("");
        assertEquals(expected, actual);

        actual = HttpAccessTokenRetriever.formatRequestBody("  ");
        assertEquals(expected, actual);
    }

}
