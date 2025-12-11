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

import org.apache.kafka.common.feature.Features;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.server.common.FinalizedFeatures;

import java.util.function.Supplier;

/**
 * A simple ApiVersionManager used in controllers. It does not support forwarding and does not have metadata cache.
 * Its enabled APIs are determined by the listener type, its finalized features are dynamically determined by the controller.
 */
public class SimpleApiVersionManager implements ApiVersionManager {

    private final ApiMessageType.ListenerType listenerType;
    private final Features<SupportedVersionRange> brokerFeatures;
    private final boolean enableUnstableLastVersion;
    private final Supplier<FinalizedFeatures> featuresProvider;
    private final ApiVersionsResponseData.ApiVersionCollection apiVersions;

    /**
     * SimpleApiVersionManager constructor
     * @param listenerType the listener type
     * @param enableUnstableLastVersion whether to enable unstable last version, see
     *   {@link org.apache.kafka.server.config.ServerConfigs#UNSTABLE_API_VERSIONS_ENABLE_CONFIG}
     * @param featuresProvider a provider to the finalized features supported
     */
    public SimpleApiVersionManager(ApiMessageType.ListenerType listenerType,
                                   boolean enableUnstableLastVersion,
                                   Supplier<FinalizedFeatures> featuresProvider) {
        this.listenerType = listenerType;
        this.brokerFeatures = BrokerFeatures.defaultSupportedFeatures(enableUnstableLastVersion);
        this.enableUnstableLastVersion = enableUnstableLastVersion;
        this.featuresProvider = featuresProvider;
        this.apiVersions = ApiVersionsResponse.collectApis(listenerType, ApiKeys.apisForListener(listenerType), enableUnstableLastVersion);
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
        FinalizedFeatures currentFeatures = features();
        return new ApiVersionsResponse.Builder()
                .setThrottleTimeMs(throttleTimeMs)
                .setApiVersions(apiVersions)
                .setSupportedFeatures(brokerFeatures)
                .setFinalizedFeatures(currentFeatures.finalizedFeatures())
                .setFinalizedFeaturesEpoch(currentFeatures.finalizedFeaturesEpoch())
                .setAlterFeatureLevel0(alterFeatureLevel0)
                .build();
    }

    @Override
    public FinalizedFeatures features() {
        return featuresProvider.get();
    }
}
