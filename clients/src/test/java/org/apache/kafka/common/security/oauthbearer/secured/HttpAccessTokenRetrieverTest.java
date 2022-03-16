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

package org.apache.kafka.common.security.oauthbearer.secured;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.Base64;
import java.util.Random;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

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
    public void testFormatAuthorizationHeader() throws IOException {
        String expected = "Basic " + Base64.getUrlEncoder().encodeToString(Utils.utf8("id:secret"));

        String actual = HttpAccessTokenRetriever.formatAuthorizationHeader("id", "secret");
        assertEquals(expected, actual);
    }

    @Test
    public void testFormatAuthorizationHeaderMissingValues() {
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader(null, "secret"));
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader("id", null));
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader(null, null));
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader("", "secret"));
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader("id", ""));
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader("", ""));
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader("  ", "secret"));
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader("id", "  "));
        assertThrows(IllegalArgumentException.class, () -> HttpAccessTokenRetriever.formatAuthorizationHeader("  ", "  "));
    }

    @Test
    public void testFormatRequestBody() throws IOException {
        String expected = "grant_type=client_credentials&scope=scope";
        String actual = HttpAccessTokenRetriever.formatRequestBody("scope");
        assertEquals(expected, actual);
    }

    @Test
    public void testFormatRequestBodyWithEscaped() throws IOException {
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
    public void testFormatRequestBodyMissingValues() throws IOException {
        String expected = "grant_type=client_credentials";
        String actual = HttpAccessTokenRetriever.formatRequestBody(null);
        assertEquals(expected, actual);

        actual = HttpAccessTokenRetriever.formatRequestBody("");
        assertEquals(expected, actual);

        actual = HttpAccessTokenRetriever.formatRequestBody("  ");
        assertEquals(expected, actual);
    }

}
