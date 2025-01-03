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
package org.apache.kafka.connect.runtime.rest.resources;

import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.InternalRequestSignature;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestRequestTimeout;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.util.Callback;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Stubber;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.crypto.Mac;

import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.UriInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class InternalConnectResourceTest {

    private static final Boolean FORWARD = true;
    private static final String CONNECTOR_NAME = "test";
    private static final HttpHeaders NULL_HEADERS = null;
    private static final List<Map<String, String>> TASK_CONFIGS = new ArrayList<>();
    static {
        TASK_CONFIGS.add(Collections.singletonMap("config", "value"));
        TASK_CONFIGS.add(Collections.singletonMap("config", "other_value"));
    }
    private static final String FENCE_PATH = "/connectors/" + CONNECTOR_NAME + "/fence";
    private static final String TASK_CONFIGS_PATH = "/connectors/" + CONNECTOR_NAME + "/tasks";
    private static final RestRequestTimeout REST_REQUEST_TIMEOUT = RestRequestTimeout.constant(
            RestServer.DEFAULT_REST_REQUEST_TIMEOUT_MS,
            RestServer.DEFAULT_HEALTH_CHECK_TIMEOUT_MS
    );

    @Mock
    private UriInfo uriInfo;
    @Mock
    private Herder herder;
    @Mock
    private RestClient restClient;

    private InternalConnectResource internalResource;

    @BeforeEach
    public void setup() {
        internalResource = new InternalConnectResource(herder, restClient, REST_REQUEST_TIMEOUT);
        internalResource.uriInfo = uriInfo;
    }

    @Test
    public void testPutConnectorTaskConfigsNoInternalRequestSignature() throws Throwable {
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Callback<Void>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb, null).when(herder).putTaskConfigs(
                eq(CONNECTOR_NAME),
                eq(TASK_CONFIGS),
                cb.capture(),
                any()
        );
        expectRequestPath(TASK_CONFIGS_PATH);

        internalResource.putTaskConfigs(CONNECTOR_NAME, NULL_HEADERS, FORWARD, serializeAsBytes(TASK_CONFIGS));
    }

    @Test
    public void testPutConnectorTaskConfigsWithInternalRequestSignature() throws Throwable {
        final String signatureAlgorithm = "HmacSHA256";
        final String encodedSignature = "Kv1/OSsxzdVIwvZ4e30avyRIVrngDfhzVUm/kAZEKc4=";

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Callback<Void>> cb = ArgumentCaptor.forClass(Callback.class);
        final ArgumentCaptor<InternalRequestSignature> signatureCapture = ArgumentCaptor.forClass(InternalRequestSignature.class);
        expectAndCallbackResult(cb, null).when(herder).putTaskConfigs(
                eq(CONNECTOR_NAME),
                eq(TASK_CONFIGS),
                cb.capture(),
                signatureCapture.capture()
        );

        HttpHeaders headers = mock(HttpHeaders.class);
        when(headers.getHeaderString(InternalRequestSignature.SIGNATURE_ALGORITHM_HEADER))
                .thenReturn(signatureAlgorithm);
        when(headers.getHeaderString(InternalRequestSignature.SIGNATURE_HEADER))
                .thenReturn(encodedSignature);
        expectRequestPath(TASK_CONFIGS_PATH);

        internalResource.putTaskConfigs(CONNECTOR_NAME, headers, FORWARD, serializeAsBytes(TASK_CONFIGS));

        InternalRequestSignature expectedSignature = new InternalRequestSignature(
                serializeAsBytes(TASK_CONFIGS),
                Mac.getInstance(signatureAlgorithm),
                Base64.getDecoder().decode(encodedSignature)
        );
        assertEquals(
                expectedSignature,
                signatureCapture.getValue()
        );
    }

    @Test
    public void testPutConnectorTaskConfigsConnectorNotFound() {
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Callback<Void>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackException(cb, new NotFoundException("not found")).when(herder).putTaskConfigs(
                eq(CONNECTOR_NAME),
                eq(TASK_CONFIGS),
                cb.capture(),
                any()
        );
        expectRequestPath(TASK_CONFIGS_PATH);

        assertThrows(NotFoundException.class, () -> internalResource.putTaskConfigs(CONNECTOR_NAME, NULL_HEADERS,
                FORWARD, serializeAsBytes(TASK_CONFIGS)));
    }

    @Test
    public void testFenceZombiesNoInternalRequestSignature() throws Throwable {
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Callback<Void>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb, null)
                .when(herder).fenceZombieSourceTasks(eq(CONNECTOR_NAME), cb.capture(), isNull());
        expectRequestPath(FENCE_PATH);

        internalResource.fenceZombies(CONNECTOR_NAME, NULL_HEADERS, FORWARD, serializeAsBytes(null));
    }

    @Test
    public void testFenceZombiesWithInternalRequestSignature() throws Throwable {
        final String signatureAlgorithm = "HmacSHA256";
        final String encodedSignature = "Kv1/OSsxzdVIwvZ4e30avyRIVrngDfhzVUm/kAZEKc4=";

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Callback<Void>> cb = ArgumentCaptor.forClass(Callback.class);
        final ArgumentCaptor<InternalRequestSignature> signatureCapture = ArgumentCaptor.forClass(InternalRequestSignature.class);
        expectAndCallbackResult(cb, null)
                .when(herder).fenceZombieSourceTasks(eq(CONNECTOR_NAME), cb.capture(), signatureCapture.capture());

        HttpHeaders headers = mock(HttpHeaders.class);
        when(headers.getHeaderString(InternalRequestSignature.SIGNATURE_ALGORITHM_HEADER))
                .thenReturn(signatureAlgorithm);
        when(headers.getHeaderString(InternalRequestSignature.SIGNATURE_HEADER))
                .thenReturn(encodedSignature);
        expectRequestPath(FENCE_PATH);

        internalResource.fenceZombies(CONNECTOR_NAME, headers, FORWARD, serializeAsBytes(null));

        InternalRequestSignature expectedSignature = new InternalRequestSignature(
                serializeAsBytes(null),
                Mac.getInstance(signatureAlgorithm),
                Base64.getDecoder().decode(encodedSignature)
        );
        assertEquals(
                expectedSignature,
                signatureCapture.getValue()
        );
    }

    @Test
    public void testFenceZombiesConnectorNotFound() {
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Callback<Void>> cb = ArgumentCaptor.forClass(Callback.class);

        expectAndCallbackException(cb, new NotFoundException("not found"))
                .when(herder).fenceZombieSourceTasks(eq(CONNECTOR_NAME), cb.capture(), any());
        expectRequestPath(FENCE_PATH);

        assertThrows(NotFoundException.class,
                () -> internalResource.fenceZombies(CONNECTOR_NAME, NULL_HEADERS, FORWARD, serializeAsBytes(null)));
    }

    private <T> byte[] serializeAsBytes(final T value) throws IOException {
        return new ObjectMapper().writeValueAsBytes(value);
    }

    private <T> Stubber expectAndCallbackResult(final ArgumentCaptor<Callback<T>> cb, final T value) {
        return doAnswer(invocation -> {
            cb.getValue().onCompletion(null, value);
            return null;
        });
    }

    private <T> Stubber expectAndCallbackException(final ArgumentCaptor<Callback<T>> cb, final Throwable t) {
        return doAnswer(invocation -> {
            cb.getValue().onCompletion(t, null);
            return null;
        });
    }

    private void expectRequestPath(String path) {
        when(uriInfo.getPath()).thenReturn(path);
    }

}
