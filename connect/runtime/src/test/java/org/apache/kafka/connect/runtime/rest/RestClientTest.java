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
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.rest.entities.ErrorMessage;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RestClientTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<TestDTO> TEST_TYPE = new TypeReference<TestDTO>() {
    };
    private HttpClient httpClient;

    private static String toJsonString(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> RestClient.HttpResponse<T> httpRequest(HttpClient httpClient, TypeReference<T> typeReference) {
        return RestClient.httpRequest(
                httpClient, null, null, null, null, typeReference, null, null);
    }

    @BeforeEach
    public void mockSetup() {
        httpClient = niceMock(HttpClient.class);
    }

    @Test
    public void testSuccess() throws ExecutionException, InterruptedException, TimeoutException {
        int statusCode = Response.Status.OK.getStatusCode();
        String expectedResponse = toJsonString(new TestDTO("someContent"));
        setupHttpClient(statusCode, expectedResponse);

        RestClient.HttpResponse<TestDTO> httpResp = httpRequest(httpClient, TEST_TYPE);
        assertEquals(httpResp.status(), statusCode);
        assertEquals(toJsonString(httpResp.body()), expectedResponse);
    }

    @Test
    public void testNoContent() throws ExecutionException, InterruptedException, TimeoutException {
        int statusCode = Response.Status.NO_CONTENT.getStatusCode();
        setupHttpClient(statusCode, null);

        RestClient.HttpResponse<TestDTO> httpResp = httpRequest(httpClient, TEST_TYPE);
        assertEquals(httpResp.status(), statusCode);
        assertNull(httpResp.body());
    }

    @Test
    public void testError() throws ExecutionException, InterruptedException, TimeoutException {
        int statusCode = Response.Status.CONFLICT.getStatusCode();
        ErrorMessage errorMsg = new ErrorMessage(Response.Status.GONE.getStatusCode(), "Some Error Message");
        setupHttpClient(statusCode, toJsonString(errorMsg));
        ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(httpClient, TEST_TYPE));
        assertEquals(e.statusCode(), statusCode);
        assertEquals(e.errorCode(), errorMsg.errorCode());
        assertEquals(e.getMessage(), errorMsg.message());
    }

    private void setupHttpClient(int responseCode, String responseJsonString) throws ExecutionException, InterruptedException, TimeoutException {
        WorkerConfig workerConf = niceMock(WorkerConfig.class);
        expect(workerConf.originals()).andReturn(Collections.emptyMap());
        Request req = niceMock(Request.class);
        ContentResponse resp = niceMock(ContentResponse.class);
        expect(resp.getStatus()).andReturn(responseCode);
        expect(resp.getContentAsString()).andReturn(responseJsonString);
        expect(req.send()).andReturn(resp);
        expect(httpClient.newRequest(anyString())).andReturn(req);
        replay(workerConf, httpClient, req, resp);
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
    }
}
