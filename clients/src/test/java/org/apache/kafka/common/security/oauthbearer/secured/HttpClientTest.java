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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

public class HttpClientTest extends OAuthBearerTest {

    @Test
    public void test() throws IOException {
        String expectedResponse = "Hiya, buddy";
        HttpURLConnection mockedCon = mock(HttpURLConnection.class);
        when(mockedCon.getResponseCode()).thenReturn(200);
        when(mockedCon.getInputStream()).thenReturn(new ByteArrayInputStream(Utils.utf8(expectedResponse)));

        HttpClient client = new HttpClient(() -> mockedCon, null, null);
        String response = client.post(null, null);
        assertEquals(expectedResponse, response);
    }

    @Test
    public void testEmptyResponse() throws IOException {
        HttpURLConnection mockedCon = mock(HttpURLConnection.class);
        when(mockedCon.getResponseCode()).thenReturn(200);
        when(mockedCon.getInputStream()).thenReturn(new ByteArrayInputStream(new byte[0]));

        HttpClient client = new HttpClient(() -> mockedCon, null, null);
        assertThrows(IOException.class, () -> client.post(null, null));
    }

    @Test
    public void testErrorReadingResponse() throws IOException {
        HttpURLConnection mockedCon = mock(HttpURLConnection.class);
        when(mockedCon.getResponseCode()).thenReturn(200);
        when(mockedCon.getInputStream()).thenThrow(new IOException("Can't read"));

        HttpClient client = new HttpClient(() -> mockedCon, null, null);
        assertThrows(IOException.class, () -> client.post(null, null));
    }

}
