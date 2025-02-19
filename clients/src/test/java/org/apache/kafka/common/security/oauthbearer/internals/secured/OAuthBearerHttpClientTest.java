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
import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OAuthBearerHttpClientTest extends OAuthBearerTest {

    @Test
    public void testPost() throws IOException {
        String expectedResponse = "Hiya, buddy";
        HttpURLConnection mockedCon = createHttpURLConnection(expectedResponse);
        Optional<String> actualResponseOpt = post(mockedCon);
        assertTrue(actualResponseOpt.isPresent());
        assertEquals(expectedResponse, actualResponseOpt.get());
    }

    @Test
    public void testPostWithEmptyResponse() throws IOException {
        HttpURLConnection mockedCon = createHttpURLConnection("");
        Optional<String> actualResponseOpt = post(mockedCon);
        assertFalse(actualResponseOpt.isPresent());
    }

    @Test
    public void testPostWithErrorReadingResponse() throws IOException {
        HttpURLConnection mockedCon = createHttpURLConnection("dummy");
        when(mockedCon.getInputStream()).thenThrow(new IOException("Can't read"));
        Optional<String> actualResponseOpt = post(mockedCon);
        assertFalse(actualResponseOpt.isPresent());
    }

    @Test
    public void testCopy() throws IOException {
        byte[] expected = new byte[4096 + 1];
        Random r = new Random();
        r.nextBytes(expected);
        InputStream in = new ByteArrayInputStream(expected);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OAuthBearerHttpClient.copy(in, out);
        assertArrayEquals(expected, out.toByteArray());
    }

    @Test
    public void testCopyError() throws IOException {
        try (InputStream mockedIn = mock(InputStream.class)) {
            OutputStream out = new ByteArrayOutputStream();
            when(mockedIn.read(any(byte[].class))).thenThrow(new IOException());
            assertThrows(IOException.class, () -> OAuthBearerHttpClient.copy(mockedIn, out));
        }
    }

    private Optional<String> post(HttpURLConnection con) throws IOException {
        OAuthBearerHttpClient client = new OAuthBearerHttpClient("URL");
        client.write(con, new byte[0]);
        OAuthBearerHttpClient.HttpResponse response = client.read(con);
        return response.responseBody.map(bytes -> new String(bytes, StandardCharsets.UTF_8));
    }
}
