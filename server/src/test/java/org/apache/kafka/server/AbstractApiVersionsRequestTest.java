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
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.GroupVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.ShareVersion;
import org.apache.kafka.server.common.StreamsVersion;
import org.apache.kafka.server.common.TransactionVersion;
import org.apache.kafka.test.TestUtils;

import java.io.IOException;
import java.net.Socket;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public abstract class AbstractApiVersionsRequestTest {
    private final ClusterInstance cluster;

    public AbstractApiVersionsRequestTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    ApiVersionsResponse sendUnsupportedApiVersionRequest(ApiVersionsRequest request) throws IOException {
        RequestHeader overrideHeader = IntegrationTestUtils.nextRequestHeader(ApiKeys.API_VERSIONS, Short.MAX_VALUE);
        Socket socket = IntegrationTestUtils.connect(cluster.brokerBoundPorts().get(0));
        try {
            byte[] serializedBytes = Utils.toArray(
                    RequestUtils.serialize(overrideHeader.data(), overrideHeader.headerVersion(), request.data(), request.version()));
            IntegrationTestUtils.sendRequest(socket, serializedBytes);
            return IntegrationTestUtils.receive(socket, ApiKeys.API_VERSIONS, (short) 0);
        } finally {
            socket.close();
        }
    }


    void validateApiVersionsResponse(ApiVersionsResponse apiVersionsResponse,
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
