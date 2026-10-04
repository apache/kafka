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
package org.apache.kafka.clients;

import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

/**
 * Owns per-node API version negotiation. The caller supplies transport readiness and applies
 * connection and metadata effects; this class has no access to the network or metadata updater.
 * This class is not thread-safe.
 */
final class ApiVersionNegotiator {
    enum Outcome {
        READY,
        RETRY,
        DISCONNECT,
        REBOOTSTRAP
    }

    /**
     * True if we should send an ApiVersionRequest when first connecting to a broker.
     */
    private final boolean enabled;
    private final ApiVersions apiVersions;
    private final MetadataRecoveryStrategy recoveryStrategy;
    /* Whether to send the cluster ID and node ID on ApiVersions RPC for checking by the broker */
    private final boolean checkCluster;
    private final Map<String, ApiVersionsRequest.Builder> requestsToSend = new HashMap<>();

    ApiVersionNegotiator(boolean enabled, ApiVersions apiVersions,
                         MetadataRecoveryStrategy recoveryStrategy, boolean checkCluster) {
        this.enabled = enabled;
        this.apiVersions = apiVersions;
        this.recoveryStrategy = recoveryStrategy;
        this.checkCluster = checkCluster;
    }

    boolean isEnabled() {
        return enabled;
    }

    void onConnected(String node) {
        if (enabled)
            requestsToSend.put(node, new ApiVersionsRequest.Builder());
    }

    void onDisconnected(String node) {
        apiVersions.remove(node);
        requestsToSend.remove(node);
    }

    /**
     * Offer requests awaiting send to the caller, retaining each request until its send is accepted.
     * The callback must not modify requestsToSend while this method is iterating.
     */
    void maybeSendRequests(BiPredicate<String, ApiVersionsRequest.Builder> trySend) {
        requestsToSend.entrySet().removeIf(entry -> trySend.test(entry.getKey(), entry.getValue()));
    }

    void prepareRequest(String node, ApiVersionsRequest.Builder request, Supplier<String> clusterIdSupplier) {
        // If we know the cluster ID and node ID we are connecting to, we can include
        // those details in the ApiVersions request for checking in the broker,
        // provided that the metadata recovery strategy is not NONE. (KIP-1242)
        if (recoveryStrategy != MetadataRecoveryStrategy.NONE && checkCluster) {
            String clusterId = clusterIdSupplier.get();
            int nodeId = Integer.parseInt(node);
            if (clusterId != null && nodeId >= 0) {
                request.setClusterId(clusterId);
                request.setNodeId(nodeId);
            }
        }
    }

    Outcome handleResponse(String node, short requestVersion, ApiVersionsResponse response) {
        if (response.data().errorCode() != Errors.NONE.code()) {
            if (recoveryStrategy == MetadataRecoveryStrategy.REBOOTSTRAP && response.data().errorCode() == Errors.REBOOTSTRAP_REQUIRED.code())
                return Outcome.REBOOTSTRAP;

            if (requestVersion == 0 || response.data().errorCode() != Errors.UNSUPPORTED_VERSION.code())
                return Outcome.DISCONNECT;

            // Starting from Apache Kafka 2.4, ApiKeys field is populated with the supported versions of
            // the ApiVersionsRequest when an UNSUPPORTED_VERSION error is returned.
            // If not provided, the client falls back to version 0.
            short maxApiVersion = 0;
            ApiVersion apiVersion = response.data().apiKeys().find(ApiKeys.API_VERSIONS.id);
            if (apiVersion != null)
                maxApiVersion = apiVersion.maxVersion();
            requestsToSend.put(node, new ApiVersionsRequest.Builder(maxApiVersion));
            return Outcome.RETRY;
        }

        apiVersions.update(node, new NodeApiVersions(
            response.data().apiKeys(),
            response.data().supportedFeatures(),
            response.data().finalizedFeatures(),
            response.data().finalizedFeaturesEpoch()));
        return Outcome.READY;
    }
}
