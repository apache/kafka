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

public class HttpJwtRetrieverTest extends OAuthBearerTest {

    @Test
    public void test() throws IOException {
        String expectedResponse = "Hiya, buddy";
        HttpURLConnection mockedCon = createHttpURLConnection(expectedResponse);
        String response = HttpJwtRetriever.post(mockedCon, null, null, null, null);
        assertEquals(expectedResponse, response);
    }

    @Test
    public void testEmptyResponse() throws IOException {
        HttpURLConnection mockedCon = createHttpURLConnection("");
        assertThrows(IOException.class, () -> HttpJwtRetriever.post(mockedCon, null, null, null, null));
    }

    @Test
    public void testErrorReadingResponse() throws IOException {
        HttpURLConnection mockedCon = createHttpURLConnection("dummy");
        when(mockedCon.getInputStream()).thenThrow(new IOException("Can't read"));

        assertThrows(IOException.class, () -> HttpJwtRetriever.post(mockedCon, null, null, null, null));
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
            () -> HttpJwtRetriever.post(mockedCon, null, null, null, null));
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
            () -> HttpJwtRetriever.post(mockedCon, null, null, null, null));
        assertTrue(ioe.getMessage().contains("{\"some_arg\" - \"some problem with arg\"}"));

        // error response body has different keys
        when(mockedCon.getErrorStream()).thenReturn(new ByteArrayInputStream(
            "{\"errorCode\":\"some_arg\", \"errorSummary\":\"some problem with arg\"}"
                .getBytes(StandardCharsets.UTF_8)));
        ioe = assertThrows(IOException.class,
            () -> HttpJwtRetriever.post(mockedCon, null, null, null, null));
        assertTrue(ioe.getMessage().contains("{\"some_arg\" - \"some problem with arg\"}"));

        // error response is valid json but unknown keys
        when(mockedCon.getErrorStream()).thenReturn(new ByteArrayInputStream(
            "{\"err\":\"some_arg\", \"err_des\":\"some problem with arg\"}"
                .getBytes(StandardCharsets.UTF_8)));
        ioe = assertThrows(IOException.class,
            () -> HttpJwtRetriever.post(mockedCon, null, null, null, null));
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
            () -> HttpJwtRetriever.post(mockedCon, null, null, null, null));
        assertTrue(ioe.getMessage().contains("{non json error output}"));
    }

    @Test
    public void testCopy() throws IOException {
        byte[] expected = new byte[4096 + 1];
        Random r = new Random();
        r.nextBytes(expected);
        InputStream in = new ByteArrayInputStream(expected);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpJwtRetriever.copy(in, out);
        assertArrayEquals(expected, out.toByteArray());
    }

    @Test
    public void testCopyError() throws IOException {
        InputStream mockedIn = mock(InputStream.class);
        OutputStream out = new ByteArrayOutputStream();
        when(mockedIn.read(any(byte[].class))).thenThrow(new IOException());
        assertThrows(IOException.class, () -> HttpJwtRetriever.copy(mockedIn, out));
    }
}
