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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.runtime.rest.entities.ErrorMessage;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import javax.crypto.SecretKey;
import javax.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(Enclosed.class)
public class RestClientTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<TestDTO> TEST_TYPE = new TypeReference<TestDTO>() {
    };
    private static final SecretKey MOCK_SECRET_KEY = getMockSecretKey();

    private static void assertIsInternalServerError(ConnectRestException e) {
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.statusCode());
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.errorCode());
    }

    private static SecretKey getMockSecretKey() {
        SecretKey mockKey = mock(SecretKey.class);
        when(mockKey.getFormat()).thenReturn("RAW"); // supported format by
        when(mockKey.getEncoded()).thenReturn("SomeKey".getBytes(StandardCharsets.UTF_8));
        return mockKey;
    }

    private static RestClient.HttpResponse<TestDTO> httpRequest(HttpClient httpClient, String requestSignatureAlgorithm, boolean https) {
        RestClient client = spy(new RestClient(null));
        doReturn(httpClient).when(client).httpClient(any());
        String protocol = https ? "https" : "http";
        String url = protocol + "://localhost:1234/api/endpoint";
        return client.httpRequest(
                url,
                "GET",
                null,
                new TestDTO("requestBodyData"),
                TEST_TYPE,
                MOCK_SECRET_KEY,
                requestSignatureAlgorithm
        );
    }

    private static RestClient.HttpResponse<TestDTO> httpRequest(HttpClient httpClient, String requestSignatureAlgorithm) {
        return httpRequest(httpClient, requestSignatureAlgorithm, false);
    }

    private static RestClient.HttpResponse<TestDTO> httpRequest(HttpClient httpClient) throws Exception {
        String validRequestSignatureAlgorithm = "HmacSHA1";
        return httpRequest(httpClient, validRequestSignatureAlgorithm);
    }


    @RunWith(Parameterized.class)
    public static class RequestFailureParameterizedTest {

        @Rule
        public MockitoRule initRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

        @Mock
        private HttpClient httpClient;

        @Parameterized.Parameter
        public Throwable requestException;
        
        @Parameterized.Parameters
        public static Collection<Object[]> requestExceptions() {
            return Arrays.asList(new Object[][]{
                    {new InterruptedException()},
                    {new ExecutionException(null)},
                    {new TimeoutException()}
            });
        }

        private static Request buildThrowingMockRequest(Throwable t) throws ExecutionException, InterruptedException, TimeoutException {
            Request req = mock(Request.class);
            when(req.header(anyString(), anyString())).thenReturn(req);
            when(req.send()).thenThrow(t);
            return req;
        }

        @Test
        public void testFailureDuringRequestCausesInternalServerError() throws Exception {
            Request request = buildThrowingMockRequest(requestException);
            when(httpClient.newRequest(anyString())).thenReturn(request);
            ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(httpClient));
            assertIsInternalServerError(e);
            assertEquals(requestException, e.getCause());
        }
    }


    @RunWith(MockitoJUnitRunner.StrictStubs.class)
    public static class Tests {
        @Mock
        private HttpClient httpClient;

        private static String toJsonString(Object obj) {
            try {
                return OBJECT_MAPPER.writeValueAsString(obj);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        private void setupHttpClient(int responseCode, String responseJsonString) throws Exception {
            Request req = mock(Request.class);
            ContentResponse resp = mock(ContentResponse.class);
            when(resp.getStatus()).thenReturn(responseCode);
            when(resp.getContentAsString()).thenReturn(responseJsonString);
            when(req.send()).thenReturn(resp);
            when(req.header(anyString(), anyString())).thenReturn(req);
            when(httpClient.newRequest(anyString())).thenReturn(req);
        }

        @Test
        public void testSuccess() throws Exception {
            int statusCode = Response.Status.OK.getStatusCode();
            TestDTO expectedResponse = new TestDTO("someContent");
            setupHttpClient(statusCode, toJsonString(expectedResponse));

            RestClient.HttpResponse<TestDTO> httpResp = httpRequest(httpClient);
            assertEquals(statusCode, httpResp.status());
            assertEquals(expectedResponse, httpResp.body());
        }

        @Test
        public void testNoContent() throws Exception {
            int statusCode = Response.Status.NO_CONTENT.getStatusCode();
            setupHttpClient(statusCode, null);

            RestClient.HttpResponse<TestDTO> httpResp = httpRequest(httpClient);
            assertEquals(statusCode, httpResp.status());
            assertNull(httpResp.body());
        }

        @Test
        public void testStatusCodeAndErrorMessagePreserved() throws Exception {
            int statusCode = Response.Status.CONFLICT.getStatusCode();
            ErrorMessage errorMsg = new ErrorMessage(Response.Status.GONE.getStatusCode(), "Some Error Message");
            setupHttpClient(statusCode, toJsonString(errorMsg));

            ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(httpClient));
            assertEquals(statusCode, e.statusCode());
            assertEquals(errorMsg.errorCode(), e.errorCode());
            assertEquals(errorMsg.message(), e.getMessage());
        }

        @Test
        public void testUnexpectedHttpResponseCausesInternalServerError() throws Exception {
            int statusCode = Response.Status.NOT_MODIFIED.getStatusCode(); // never thrown explicitly -
            // should be treated as an unexpected error and translated into 500 INTERNAL_SERVER_ERROR

            setupHttpClient(statusCode, null);
            ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(httpClient));
            assertIsInternalServerError(e);
        }

        @Test
        public void testRuntimeExceptionCausesInternalServerError() {
            when(httpClient.newRequest(anyString())).thenThrow(new RuntimeException());

            ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(httpClient));
            assertIsInternalServerError(e);
        }

        @Test
        public void testRequestSignatureFailureCausesInternalServerError() throws Exception {
            setupHttpClient(0, null);

            String invalidRequestSignatureAlgorithm = "Foo";
            ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(httpClient, invalidRequestSignatureAlgorithm));
            assertIsInternalServerError(e);
        }

        @Test
        public void testIOExceptionCausesInternalServerError() throws Exception {
            String invalidJsonString = "Invalid";
            setupHttpClient(201, invalidJsonString);

            ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(httpClient));
            assertIsInternalServerError(e);
        }

        @Test
        public void testUseSslConfigsOnlyWhenNecessary() throws Exception {
            // See KAFKA-14816; we want to make sure that even if the worker is configured with invalid SSL properties,
            // REST requests only fail if we try to contact a URL using HTTPS (but not HTTP)
            int statusCode = Response.Status.OK.getStatusCode();
            TestDTO expectedResponse = new TestDTO("someContent");
            setupHttpClient(statusCode, toJsonString(expectedResponse));

            String requestSignatureAlgorithm = "HmacSHA1";
            assertDoesNotThrow(() -> httpRequest(httpClient, requestSignatureAlgorithm, false));
            assertThrows(RuntimeException.class, () -> httpRequest(httpClient, requestSignatureAlgorithm, true));
        }
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
