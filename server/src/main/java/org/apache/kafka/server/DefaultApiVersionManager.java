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
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.common.FinalizedFeatures;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * The default ApiVersionManager that supports forwarding and has metadata cache, used in brokers.
 * The enabled APIs are determined by the broker listener type and the controller APIs.
 */
public class DefaultApiVersionManager implements ApiVersionManager {

    private final ApiMessageType.ListenerType listenerType;
    private final Supplier<Optional<NodeApiVersions>> nodeApiVersionsSupplier;
    private final BrokerFeatures brokerFeatures;
    private final MetadataCache metadataCache;
    private final boolean enableUnstableLastVersion;
    private final Optional<ClientMetricsManager> clientMetricsManager;

    /**
     * DefaultApiVersionManager constructor
     * @param listenerType the listener type
     * @param nodeApiVersionsSupplier the supplier of NodeApiVersions
     * @param brokerFeatures the broker features
     * @param metadataCache the metadata cache, used to get the finalized features and the metadata version
     * @param enableUnstableLastVersion whether to enable unstable last version, see
     *   {@link org.apache.kafka.server.config.ServerConfigs#UNSTABLE_API_VERSIONS_ENABLE_CONFIG}
     * @param clientMetricsManager the client metrics manager, helps to determine whether client telemetry is enabled
     */
    public DefaultApiVersionManager(
            ApiMessageType.ListenerType listenerType,
            Supplier<Optional<NodeApiVersions>> nodeApiVersionsSupplier,
            BrokerFeatures brokerFeatures,
            MetadataCache metadataCache,
            boolean enableUnstableLastVersion,
            Optional<ClientMetricsManager> clientMetricsManager) {
        this.listenerType = listenerType;
        this.nodeApiVersionsSupplier = nodeApiVersionsSupplier;
        this.brokerFeatures = brokerFeatures;
        this.metadataCache = metadataCache;
        this.enableUnstableLastVersion = enableUnstableLastVersion;
        this.clientMetricsManager = clientMetricsManager;
    }

    @Override
    public boolean enableUnstableLastVersion() {
        return enableUnstableLastVersion;
    }

    @Override
    public ApiMessageType.ListenerType listenerType() {
        return listenerType;
    }

    @Override
    public ApiVersionsResponse apiVersionResponse(int throttleTimeMs, boolean alterFeatureLevel0) {
        FinalizedFeatures finalizedFeatures = metadataCache.features();
        Optional<NodeApiVersions> controllerApiVersions = nodeApiVersionsSupplier.get();
        boolean clientTelemetryEnabled = clientMetricsManager.map(ClientMetricsManager::isTelemetryExporterConfigured).orElse(false);
        ApiVersionsResponseData.ApiVersionCollection apiVersions = controllerApiVersions
                .map(nodeApiVersions -> ApiVersionsResponse.controllerApiVersions(
                    nodeApiVersions,
                    listenerType,
                    enableUnstableLastVersion,
                    clientTelemetryEnabled))
                .orElseGet(() -> ApiVersionsResponse.brokerApiVersions(
                    listenerType,
                    enableUnstableLastVersion,
                    clientTelemetryEnabled));

        return new ApiVersionsResponse.Builder()
            .setThrottleTimeMs(throttleTimeMs)
            .setApiVersions(apiVersions)
            .setSupportedFeatures(brokerFeatures.supportedFeatures())
            .setFinalizedFeatures(finalizedFeatures.finalizedFeatures())
            .setFinalizedFeaturesEpoch(finalizedFeatures.finalizedFeaturesEpoch())
            .setAlterFeatureLevel0(alterFeatureLevel0)
            .build();
    }

    @Override
    public FinalizedFeatures features() {
        return metadataCache.features();
    }
}
