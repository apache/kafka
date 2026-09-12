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

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.internals.AppInfoParser;
import org.apache.kafka.server.common.MetadataVersion;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class ApiVersionsRequestTest extends AbstractApiVersionsRequestTest {
    private final ClusterInstance cluster;

    public ApiVersionsRequestTest(ClusterInstance cluster) {
        super(cluster);
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
}
