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

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.AppInfoParser;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.GroupVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.ShareVersion;
import org.apache.kafka.server.common.StreamsVersion;
import org.apache.kafka.server.common.TransactionVersion;
import org.apache.kafka.test.TestUtils;

import java.io.IOException;
import java.net.Socket;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ApiVersionsRequestTest {
    private final ClusterInstance cluster;

    public ApiVersionsRequestTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT}, serverProperties = {
        @ClusterConfigProperty(key = "unstable.api.versions.enable", value = "false"),
        @ClusterConfigProperty(key = "unstable.feature.versions.enable", value = "true")
    })
    public void testApiVersionsRequest() throws IOException {
        ApiVersionsRequest request = new ApiVersionsRequest.Builder().build();
        ApiVersionsResponse apiVersionsResponse = IntegrationTestUtils.connectAndReceive(request, cluster.brokerBoundPorts().get(0));
        validateApiVersionsResponse(apiVersionsResponse, cluster.clientListener(), false, false, ApiKeys.API_VERSIONS.latestVersion());
    }

    @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT}, serverProperties = {
        @ClusterConfigProperty(key = "unstable.api.versions.enable", value = "true"),
        @ClusterConfigProperty(key = "unstable.feature.versions.enable", value = "true")
    })
    public void testApiVersionsRequestIncludesUnreleasedApis() throws IOException {
        ApiVersionsRequest request = new ApiVersionsRequest.Builder().build();
        ApiVersionsResponse apiVersionsResponse = IntegrationTestUtils.connectAndReceive(request, cluster.brokerBoundPorts().get(0));
        validateApiVersionsResponse(apiVersionsResponse, cluster.clientListener(), true, false, ApiKeys.API_VERSIONS.latestVersion());
    }

    @ClusterTest(types = {Type.KRAFT})
    public void testApiVersionsRequestThroughControllerListener() throws IOException {
        ApiVersionsRequest request = new ApiVersionsRequest.Builder().build();
        ApiVersionsResponse apiVersionsResponse = IntegrationTestUtils.connectAndReceive(request, cluster.controllerBoundPorts().get(0));
        validateApiVersionsResponse(apiVersionsResponse, cluster.controllerListenerName(), true, false, ApiKeys.API_VERSIONS.latestVersion());
    }

    @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT})
    public void testApiVersionsRequestWithUnsupportedVersion() throws IOException {
        ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest.Builder().build();
        ApiVersionsResponse apiVersionsResponse = sendUnsupportedApiVersionRequest(apiVersionsRequest);
        assertEquals(Errors.UNSUPPORTED_VERSION.code(), apiVersionsResponse.data().errorCode());
        assertFalse(apiVersionsResponse.data().apiKeys().isEmpty());
        ApiVersionsResponseData.ApiVersion apiVersion = apiVersionsResponse.data().apiKeys().find(ApiKeys.API_VERSIONS.id);
        assertEquals(ApiKeys.API_VERSIONS.id, apiVersion.apiKey());
        assertEquals(ApiKeys.API_VERSIONS.oldestVersion(), apiVersion.minVersion());
        assertEquals(ApiKeys.API_VERSIONS.latestVersion(), apiVersion.maxVersion());
    }

    // Use the latest production MV for this test
    @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT}, metadataVersion = MetadataVersion.IBP_3_8_IV0, serverProperties = {
        @ClusterConfigProperty(key = "unstable.api.versions.enable", value = "false"),
        @ClusterConfigProperty(key = "unstable.feature.versions.enable", value = "false"),
    })
    public void testApiVersionsRequestValidationV0() throws IOException {
        ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest.Builder().build((short) 0);
        ApiVersionsResponse apiVersionsResponse = IntegrationTestUtils.connectAndReceive(apiVersionsRequest, cluster.brokerBoundPorts().get(0));
        validateApiVersionsResponse(
            apiVersionsResponse,
            cluster.clientListener(),
            !"false".equals(cluster.config().serverProperties().get("unstable.api.versions.enable")),
            false,
            (short) 0
        );
    }

    @ClusterTest(types = {Type.KRAFT})
    public void testApiVersionsRequestValidationV0ThroughControllerListener() throws IOException {
        ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest.Builder().build((short) 0);
        ApiVersionsResponse apiVersionsResponse = IntegrationTestUtils.connectAndReceive(apiVersionsRequest, cluster.controllerBoundPorts().get(0));
        validateApiVersionsResponse(apiVersionsResponse, cluster.controllerListenerName(), true, false, (short) 0);
    }

    @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT})
    public void testApiVersionsRequestValidationV3() throws IOException {
        // Invalid request because Name and Version are empty by default
        ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest(new ApiVersionsRequestData(), (short) 3);
        ApiVersionsResponse apiVersionsResponse = IntegrationTestUtils.connectAndReceive(apiVersionsRequest, cluster.brokerBoundPorts().get(0));
        assertEquals(Errors.INVALID_REQUEST.code(), apiVersionsResponse.data().errorCode());
    }

    @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT})
    public void testApiVersionsRequestMetadataClusterCheckPass() throws IOException {
        ApiVersionsRequestData requestData = new ApiVersionsRequestData();
        requestData.setClientSoftwareName("apache-kafka-java");
        requestData.setClientSoftwareVersion(AppInfoParser.getVersion());
        requestData.setClusterId(cluster.clusterId());
        requestData.setNodeId(0);
        ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest(requestData, ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION);
        ApiVersionsResponse apiVersionsResponse = IntegrationTestUtils.connectAndReceive(apiVersionsRequest, cluster.brokerBoundPorts().get(0));
        validateApiVersionsResponse(apiVersionsResponse, cluster.clientListener(), true, false, ApiKeys.API_VERSIONS.latestVersion());
    }

    @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT})
    public void testApiVersionsRequestMetadataClusterCheckFail() throws IOException {
        ApiVersionsRequestData requestData = new ApiVersionsRequestData();
        requestData.setClientSoftwareName("apache-kafka-java");
        requestData.setClientSoftwareVersion(AppInfoParser.getVersion());
        requestData.setClusterId(cluster.clusterId());
        requestData.setNodeId(1); // wrong node ID
        ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest(requestData, ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION);
        ApiVersionsResponse apiVersionsResponse = IntegrationTestUtils.connectAndReceive(apiVersionsRequest, cluster.brokerBoundPorts().get(0));
        assertEquals(Errors.REBOOTSTRAP_REQUIRED.code(), apiVersionsResponse.data().errorCode());
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
    public void testApiVersionsRequestWithUnsupportedVersionOverSasl() throws IOException {
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

    private ApiVersionsResponse sendUnsupportedApiVersionRequest(ApiVersionsRequest request) throws IOException {
        RequestHeader overrideHeader = IntegrationTestUtils.nextRequestHeader(ApiKeys.API_VERSIONS, Short.MAX_VALUE);
        try (Socket socket = IntegrationTestUtils.connect(cluster.brokerBoundPorts().get(0))) {
            byte[] serializedBytes = Utils.toArray(
                    RequestUtils.serialize(overrideHeader.data(), overrideHeader.headerVersion(), request.data(), request.version()));
            IntegrationTestUtils.sendRequest(socket, serializedBytes);
            return IntegrationTestUtils.receive(socket, ApiKeys.API_VERSIONS, (short) 0);
        }
    }

    private void validateApiVersionsResponse(ApiVersionsResponse apiVersionsResponse,
                                     ListenerName listenerName,
                                     boolean enableUnstableLastVersion,
                                     boolean clientTelemetryEnabled,
                                     short apiVersion) {
        if (apiVersion >= 3) {
            assertEquals(6, apiVersionsResponse.data().finalizedFeatures().size());
            assertEquals(MetadataVersion.latestTesting().featureLevel(), apiVersionsResponse.data().finalizedFeatures().find(MetadataVersion.FEATURE_NAME).minVersionLevel());
            assertEquals(MetadataVersion.latestTesting().featureLevel(), apiVersionsResponse.data().finalizedFeatures().find(MetadataVersion.FEATURE_NAME).maxVersionLevel());

            assertEquals(7, apiVersionsResponse.data().supportedFeatures().size());
            assertEquals(MetadataVersion.MINIMUM_VERSION.featureLevel(), apiVersionsResponse.data().supportedFeatures().find(MetadataVersion.FEATURE_NAME).minVersion());
            if (apiVersion < 4) {
                assertEquals(1, apiVersionsResponse.data().supportedFeatures().find("kraft.version").minVersion());
            } else {
                assertEquals(0, apiVersionsResponse.data().supportedFeatures().find("kraft.version").minVersion());
            }
            assertEquals(MetadataVersion.latestTesting().featureLevel(), apiVersionsResponse.data().supportedFeatures().find(MetadataVersion.FEATURE_NAME).maxVersion());

            assertEquals(0, apiVersionsResponse.data().supportedFeatures().find(TransactionVersion.FEATURE_NAME).minVersion());
            assertEquals(TransactionVersion.TV_2.featureLevel(), apiVersionsResponse.data().supportedFeatures().find(TransactionVersion.FEATURE_NAME).maxVersion());

            assertEquals(0, apiVersionsResponse.data().supportedFeatures().find(GroupVersion.FEATURE_NAME).minVersion());
            assertEquals(GroupVersion.GV_1.featureLevel(), apiVersionsResponse.data().supportedFeatures().find(GroupVersion.FEATURE_NAME).maxVersion());

            assertEquals(0, apiVersionsResponse.data().supportedFeatures().find(EligibleLeaderReplicasVersion.FEATURE_NAME).minVersion());
            assertEquals(EligibleLeaderReplicasVersion.ELRV_1.featureLevel(), apiVersionsResponse.data().supportedFeatures().find(EligibleLeaderReplicasVersion.FEATURE_NAME).maxVersion());

            assertEquals(0, apiVersionsResponse.data().supportedFeatures().find(ShareVersion.FEATURE_NAME).minVersion());
            assertEquals(ShareVersion.SV_2.featureLevel(), apiVersionsResponse.data().supportedFeatures().find(ShareVersion.FEATURE_NAME).maxVersion());

            assertEquals(0, apiVersionsResponse.data().supportedFeatures().find(StreamsVersion.FEATURE_NAME).minVersion());
            assertEquals(StreamsVersion.SV_1.featureLevel(), apiVersionsResponse.data().supportedFeatures().find(StreamsVersion.FEATURE_NAME).maxVersion());
        }
        ApiVersionsResponseData.ApiVersionCollection expectedApis;
        if (cluster.controllerListenerName().equals(listenerName)) {
            expectedApis = ApiVersionsResponse.collectApis(
                    ApiMessageType.ListenerType.CONTROLLER,
                    ApiKeys.apisForListener(ApiMessageType.ListenerType.CONTROLLER),
                    enableUnstableLastVersion
            );
        } else {
            expectedApis = ApiVersionsResponse.intersectForwardableApis(
                    ApiMessageType.ListenerType.BROKER,
                    NodeApiVersions.create(ApiKeys.controllerApis().stream()
                            .map(ApiVersionsResponse::toApiVersion)
                            .collect(Collectors.toList())).allSupportedApiVersions(),
                    enableUnstableLastVersion,
                    clientTelemetryEnabled
            );
        }

        assertEquals(expectedApis.size(), apiVersionsResponse.data().apiKeys().size(),
                "API keys in ApiVersionsResponse must match API keys supported by broker.");

        ApiVersionsResponse defaultApiVersionsResponse;
        if (cluster.controllerListenerName().equals(listenerName)) {
            defaultApiVersionsResponse = TestUtils.defaultApiVersionsResponse(0, ApiMessageType.ListenerType.CONTROLLER, enableUnstableLastVersion);
        } else {
            defaultApiVersionsResponse = TestUtils.createApiVersionsResponse(0, expectedApis);
        }

        for (ApiVersionsResponseData.ApiVersion expectedApiVersion : defaultApiVersionsResponse.data().apiKeys()) {
            ApiVersionsResponseData.ApiVersion actualApiVersion = apiVersionsResponse.apiVersion(expectedApiVersion.apiKey());
            assertNotNull(actualApiVersion, "API key " + expectedApiVersion.apiKey() + " is supported by broker, but not received in ApiVersionsResponse.");
            assertEquals(expectedApiVersion.apiKey(), actualApiVersion.apiKey(), "API key must be supported by the broker.");
            assertEquals(expectedApiVersion.minVersion(), actualApiVersion.minVersion(), "Received unexpected min version for API key " + actualApiVersion.apiKey() + ".");
            assertEquals(expectedApiVersion.maxVersion(), actualApiVersion.maxVersion(), "Received unexpected max version for API key " + actualApiVersion.apiKey() + ".");
        }

        if (listenerName.equals(cluster.clientListener())) {
            assertEquals(ApiKeys.PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION, apiVersionsResponse.apiVersion(ApiKeys.PRODUCE.id).minVersion());
        }
    }
}
