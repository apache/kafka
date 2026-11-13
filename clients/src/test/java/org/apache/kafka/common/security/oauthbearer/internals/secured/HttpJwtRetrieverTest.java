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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_LOGIN_CONNECT_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_LOGIN_READ_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HttpJwtRetrieverTest extends OAuthBearerTest {

    private static final String TOKEN_ENDPOINT_URL = "http://www.example.com";

    @AfterEach
    public void tearDown() {
        System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
    }

    @Test
    public void testLoginTimeoutsDefaultWhenNotConfigured() {
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, TOKEN_ENDPOINT_URL);
        // A plain map has no config definition defaults, so the retriever has to apply them.
        Map<String, Object> configs = new HashMap<>();
        configs.put(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, TOKEN_ENDPOINT_URL);
        configs.put(SASL_LOGIN_RETRY_BACKOFF_MS, 100L);
        configs.put(SASL_LOGIN_RETRY_BACKOFF_MAX_MS, 10000L);

        HttpJwtRetriever retriever = new HttpJwtRetriever(mock(HttpRequestFormatter.class));
        retriever.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries());

        assertEquals(DEFAULT_SASL_LOGIN_CONNECT_TIMEOUT_MS, retriever.loginConnectTimeoutMs());
        assertEquals(DEFAULT_SASL_LOGIN_READ_TIMEOUT_MS, retriever.loginReadTimeoutMs());
        assertEquals(10000, retriever.loginConnectTimeoutMs());
        assertEquals(10000, retriever.loginReadTimeoutMs());
    }

    @Test
    public void testLoginTimeoutsDefaultFromConfigDefinition() {
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, TOKEN_ENDPOINT_URL);
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, TOKEN_ENDPOINT_URL);

        HttpJwtRetriever retriever = new HttpJwtRetriever(mock(HttpRequestFormatter.class));
        retriever.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries());

        assertEquals(DEFAULT_SASL_LOGIN_CONNECT_TIMEOUT_MS, retriever.loginConnectTimeoutMs());
        assertEquals(DEFAULT_SASL_LOGIN_READ_TIMEOUT_MS, retriever.loginReadTimeoutMs());
    }

    @Test
    public void testLoginTimeoutsCanBeOverridden() {
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, TOKEN_ENDPOINT_URL);
        Map<String, Object> overrides = new HashMap<>();
        overrides.put(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, TOKEN_ENDPOINT_URL);
        overrides.put(SASL_LOGIN_CONNECT_TIMEOUT_MS, 1234);
        overrides.put(SASL_LOGIN_READ_TIMEOUT_MS, 5678);
        Map<String, ?> configs = getSaslConfigs(overrides);

        HttpJwtRetriever retriever = new HttpJwtRetriever(mock(HttpRequestFormatter.class));
        retriever.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries());

        assertEquals(1234, retriever.loginConnectTimeoutMs());
        assertEquals(5678, retriever.loginReadTimeoutMs());
    }

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
    public void testCloseClosesFormatter() throws IOException {
        ClientAssertionRequestFormatter formatter = mock(ClientAssertionRequestFormatter.class);
        HttpJwtRetriever retriever = new HttpJwtRetriever(formatter);
        retriever.close();
        verify(formatter).close();
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
