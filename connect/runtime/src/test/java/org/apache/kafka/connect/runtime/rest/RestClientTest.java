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

package org.apache.kafka.connect.runtime.rest;

import org.apache.kafka.connect.runtime.rest.entities.ErrorMessage;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import javax.crypto.SecretKey;
import javax.ws.rs.core.Response;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class RestClientTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String MOCK_URL = "http://localhost:1234/api/endpoint";
    private static final String TEST_METHOD = "GET";
    private static final TestDTO TEST_DTO = new TestDTO("requestBodyData");
    private static final TypeReference<TestDTO> TEST_TYPE = new TypeReference<>() { };
    private static final SecretKey MOCK_SECRET_KEY = getMockSecretKey();
    private static final String TEST_SIGNATURE_ALGORITHM = "HmacSHA1";

    @Mock
    private HttpClient httpClient;

    private static void assertIsInternalServerError(ConnectRestException e) {
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.statusCode());
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.errorCode());
    }

    private static SecretKey getMockSecretKey() {
        SecretKey mockKey = mock(SecretKey.class);
        when(mockKey.getFormat()).thenReturn("RAW");
        when(mockKey.getEncoded()).thenReturn("SomeKey".getBytes(StandardCharsets.UTF_8));
        return mockKey;
    }

    private static <T> RestClient.HttpResponse<T> httpRequest(
            HttpClient httpClient,
            String url,
            String method,
            TypeReference<T> responseFormat,
            String requestSignatureAlgorithm
    ) {
        RestClient client = spy(new RestClient(null));
        doReturn(httpClient).when(client).httpClient(any());
        return client.httpRequest(
                url,
                method,
                null,
                TEST_DTO,
                responseFormat,
                MOCK_SECRET_KEY,
                requestSignatureAlgorithm
        );
    }

    private static Stream<Arguments> requestExceptions() {
        return Stream.of(
                Arguments.of(new InterruptedException()),
                Arguments.of(new ExecutionException(null)),
                Arguments.of(new TimeoutException())
        );
    }

    private static Request buildThrowingMockRequest(Throwable t) throws ExecutionException, InterruptedException, TimeoutException {
        Request req = mock(Request.class);
        when(req.header(anyString(), anyString())).thenReturn(req);
        when(req.send()).thenThrow(t);
        return req;
    }

    @ParameterizedTest
    @MethodSource("requestExceptions")
    public void testFailureDuringRequestCausesInternalServerError(Throwable requestException) throws Exception {
        Request request = buildThrowingMockRequest(requestException);
        when(httpClient.newRequest(anyString())).thenReturn(request);
        ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(
                httpClient, MOCK_URL, TEST_METHOD, TEST_TYPE, TEST_SIGNATURE_ALGORITHM
        ));
        assertIsInternalServerError(e);
        assertEquals(requestException, e.getCause());
    }

    @Test
    public void testNullUrl() {
        RestClient client = spy(new RestClient(null));
        assertThrows(NullPointerException.class, () -> client.httpRequest(
                null,
                TEST_METHOD,
                null,
                TEST_DTO,
                TEST_TYPE,
                MOCK_SECRET_KEY,
                TEST_SIGNATURE_ALGORITHM
        ));
    }

    @Test
    public void testNullMethod() {
        RestClient client = spy(new RestClient(null));
        assertThrows(NullPointerException.class, () -> client.httpRequest(
                MOCK_URL,
                null,
                null,
                TEST_DTO,
                TEST_TYPE,
                MOCK_SECRET_KEY,
                TEST_SIGNATURE_ALGORITHM
        ));
    }

    @Test
    public void testNullResponseType() {
        RestClient client = spy(new RestClient(null));
        assertThrows(NullPointerException.class, () -> client.httpRequest(
                MOCK_URL,
                TEST_METHOD,
                null,
                TEST_DTO,
                null,
                MOCK_SECRET_KEY,
                TEST_SIGNATURE_ALGORITHM
        ));
    }

    @Test
    public void testSuccess() throws Exception {
        int statusCode = Response.Status.OK.getStatusCode();
        Request req = mock(Request.class);
        ContentResponse resp = mock(ContentResponse.class);
        when(resp.getContentAsString()).thenReturn(toJsonString(TEST_DTO));
        setupHttpClient(statusCode, req, resp);

        RestClient.HttpResponse<TestDTO> httpResp = httpRequest(
                httpClient, MOCK_URL, TEST_METHOD, TEST_TYPE, TEST_SIGNATURE_ALGORITHM
        );
        assertEquals(statusCode, httpResp.status());
        assertEquals(TEST_DTO, httpResp.body());
    }

    @Test
    public void testNoContent() throws Exception {
        int statusCode = Response.Status.NO_CONTENT.getStatusCode();
        Request req = mock(Request.class);
        ContentResponse resp = mock(ContentResponse.class);
        setupHttpClient(statusCode, req, resp);

        RestClient.HttpResponse<TestDTO> httpResp = httpRequest(
                httpClient, MOCK_URL, TEST_METHOD, TEST_TYPE, TEST_SIGNATURE_ALGORITHM
        );
        assertEquals(statusCode, httpResp.status());
        assertNull(httpResp.body());
    }

    @Test
    public void testStatusCodeAndErrorMessagePreserved() throws Exception {
        int statusCode = Response.Status.CONFLICT.getStatusCode();
        ErrorMessage errorMsg = new ErrorMessage(Response.Status.GONE.getStatusCode(), "Some Error Message");
        Request req = mock(Request.class);
        ContentResponse resp = mock(ContentResponse.class);
        when(resp.getContentAsString()).thenReturn(toJsonString(errorMsg));
        setupHttpClient(statusCode, req, resp);

        ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(
                httpClient, MOCK_URL, TEST_METHOD, TEST_TYPE, TEST_SIGNATURE_ALGORITHM
        ));
        assertEquals(statusCode, e.statusCode());
        assertEquals(errorMsg.errorCode(), e.errorCode());
        assertEquals(errorMsg.message(), e.getMessage());
    }

    @Test
    public void testNonEmptyResponseWithVoidResponseType() throws Exception {
        int statusCode = Response.Status.OK.getStatusCode();
        Request req = mock(Request.class);
        ContentResponse resp = mock(ContentResponse.class);
        when(resp.getContentAsString()).thenReturn(toJsonString(TEST_DTO));
        setupHttpClient(statusCode, req, resp);

        TypeReference<Void> voidResponse = new TypeReference<>() { };
        RestClient.HttpResponse<Void> httpResp = httpRequest(
                httpClient, MOCK_URL, TEST_METHOD, voidResponse, TEST_SIGNATURE_ALGORITHM
        );
        assertEquals(statusCode, httpResp.status());
        assertNull(httpResp.body());
    }

    @Test
    public void testUnexpectedHttpResponseCausesInternalServerError() throws Exception {
        int statusCode = Response.Status.NOT_MODIFIED.getStatusCode();
        Request req = mock(Request.class);
        ContentResponse resp = mock(ContentResponse.class);

        setupHttpClient(statusCode, req, resp);
        ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(
                httpClient, MOCK_URL, TEST_METHOD, TEST_TYPE, TEST_SIGNATURE_ALGORITHM
        ));
        assertIsInternalServerError(e);
    }

    @Test
    public void testRuntimeExceptionCausesInternalServerError() {
        when(httpClient.newRequest(anyString())).thenThrow(new RuntimeException());

        ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(
                httpClient, MOCK_URL, TEST_METHOD, TEST_TYPE, TEST_SIGNATURE_ALGORITHM
        ));
        assertIsInternalServerError(e);
    }

    @Test
    public void testRequestSignatureFailureCausesInternalServerError() {
        String invalidRequestSignatureAlgorithm = "Foo";
        ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(
                httpClient, MOCK_URL, TEST_METHOD, TEST_TYPE, invalidRequestSignatureAlgorithm
        ));
        assertIsInternalServerError(e);
    }

    @Test
    public void testIOExceptionCausesInternalServerError() throws Exception {
        Request req = mock(Request.class);
        ContentResponse resp = mock(ContentResponse.class);
        setupHttpClient(201, req, resp);

        ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(
                httpClient, MOCK_URL, TEST_METHOD, TEST_TYPE, TEST_SIGNATURE_ALGORITHM
        ));
        assertIsInternalServerError(e);
    }

    @Test
    public void testUseSslConfigsOnlyWhenNecessary() throws Exception {
        int statusCode = Response.Status.OK.getStatusCode();
        Request req = mock(Request.class);
        ContentResponse resp = mock(ContentResponse.class);
        when(resp.getContentAsString()).thenReturn(toJsonString(TEST_DTO));
        setupHttpClient(statusCode, req, resp);
        assertDoesNotThrow(() -> httpRequest(
                httpClient, MOCK_URL, TEST_METHOD, TEST_TYPE, TEST_SIGNATURE_ALGORITHM
        ));
        String httpsUrl = "https://localhost:1234/api/endpoint";
        RestClient client = spy(new RestClient(null));
        assertThrows(RuntimeException.class, () -> client.httpRequest(
                httpsUrl,
                TEST_METHOD,
                null,
                TEST_DTO,
                TEST_TYPE,
                MOCK_SECRET_KEY,
                TEST_SIGNATURE_ALGORITHM
        ));
    }

    @Test
    public void testHttpRequestInterrupted() throws ExecutionException, InterruptedException, TimeoutException {
        Request req = mock(Request.class);
        doThrow(new InterruptedException()).when(req).send();
        doReturn(req).when(req).header(anyString(), anyString());
        doReturn(req).when(httpClient).newRequest(anyString());
        ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(
                httpClient, MOCK_URL, TEST_METHOD, TEST_TYPE, TEST_SIGNATURE_ALGORITHM
        ));
        assertIsInternalServerError(e);
        assertInstanceOf(InterruptedException.class, e.getCause());
        assertTrue(Thread.interrupted());
    }

    private void setupHttpClient(int responseCode, Request req, ContentResponse resp) throws Exception {
        when(resp.getStatus()).thenReturn(responseCode);
        when(req.send()).thenReturn(resp);
        when(req.header(anyString(), anyString())).thenReturn(req);
        when(httpClient.newRequest(anyString())).thenReturn(req);
    }

    private String toJsonString(Object obj) {
        return assertDoesNotThrow(() -> OBJECT_MAPPER.writeValueAsString(obj));
    }

    private static class TestDTO {
        private final String content;

        @JsonCreator
        private TestDTO(@JsonProperty(value = "content") String content) {
            this.content = content;
        }

        public String getContent() {
            return content;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestDTO testDTO = (TestDTO) o;
            return content.equals(testDTO.content);
        }

        @Override
        public int hashCode() {
            return Objects.hash(content);
        }
    }
}