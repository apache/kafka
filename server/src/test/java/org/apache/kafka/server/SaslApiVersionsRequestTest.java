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
package org.apache.kafka.server;

import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;

import java.io.IOException;
import java.net.Socket;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SaslApiVersionsRequestTest extends AbstractApiVersionsRequestTest {
    private final ClusterInstance cluster;

    public SaslApiVersionsRequestTest(ClusterInstance cluster) {
        super(cluster);
        this.cluster = cluster;
    }

    @ClusterTest(types = {Type.KRAFT},
        brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
        controllerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT
    )
    public void testApiVersionsRequestBeforeSaslHandshakeRequest() throws IOException {
        try (Socket socket = IntegrationTestUtils.connect(cluster.brokerBoundPorts().get(0))) {
            ApiVersionsResponse apiVersionsResponse = IntegrationTestUtils.sendAndReceive(
                new ApiVersionsRequest.Builder().build((short) 0), socket);
            validateApiVersionsResponse(
                apiVersionsResponse,
                cluster.clientListener(),
                !"false".equals(
                    cluster.config().serverProperties().get("unstable.api.versions.enable")),
                false,
                (short) 0
            );
            sendSaslHandshakeRequestValidateResponse(socket);
        }
    }

    @ClusterTest(types = {Type.KRAFT},
        brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
        controllerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT
    )
    public void testApiVersionsRequestAfterSaslHandshakeRequest() throws IOException {
        try (Socket socket = IntegrationTestUtils.connect(cluster.brokerBoundPorts().get(0))) {
            sendSaslHandshakeRequestValidateResponse(socket);
            ApiVersionsResponse response = IntegrationTestUtils.sendAndReceive(
                new ApiVersionsRequest.Builder().build((short) 0), socket);
            assertEquals(Errors.ILLEGAL_SASL_STATE.code(), response.data().errorCode());
        }
    }

    @ClusterTest(types = {Type.KRAFT},
        brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
        controllerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT
    )
    public void testApiVersionsRequestWithUnsupportedVersion() throws IOException {
        try (Socket socket = IntegrationTestUtils.connect(cluster.brokerBoundPorts().get(0))) {
            ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest.Builder().build((short) 0);
            ApiVersionsResponse apiVersionsResponse = sendUnsupportedApiVersionRequest(apiVersionsRequest);
            assertEquals(Errors.UNSUPPORTED_VERSION.code(), apiVersionsResponse.data().errorCode());
            ApiVersionsResponse apiVersionsResponse2 = IntegrationTestUtils.sendAndReceive(
                new ApiVersionsRequest.Builder().build((short) 0), socket);
            validateApiVersionsResponse(
                apiVersionsResponse2,
                cluster.clientListener(),
                !"false".equals(
                    cluster.config().serverProperties().get("unstable.api.versions.enable")),
                false,
                (short) 0
            );
            sendSaslHandshakeRequestValidateResponse(socket);
        }
    }

    private void sendSaslHandshakeRequestValidateResponse(Socket socket) throws IOException {
        SaslHandshakeRequest request = new SaslHandshakeRequest(new SaslHandshakeRequestData().setMechanism("PLAIN"),
            ApiKeys.SASL_HANDSHAKE.latestVersion());
        SaslHandshakeResponse response = IntegrationTestUtils.sendAndReceive(request, socket);
        assertEquals(Errors.NONE, response.error());
        assertEquals(Collections.singletonList("PLAIN"), response.enabledMechanisms());
    }
}
