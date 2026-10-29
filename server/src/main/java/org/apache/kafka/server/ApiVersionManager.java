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

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.network.metrics.RequestChannelMetrics;
import org.apache.kafka.server.common.FinalizedFeatures;

/**
 * ApiVersionManagers are used to define the APIs supported by servers
 */
public interface ApiVersionManager {

    /**
     * Whether to mark unstable API versions as enabled
     * @return true if unstable API versions are enabled, otherwise false
     */
    boolean enableUnstableLastVersion();

    /**
     * The listener type
     * @return Broker or Controller depending on the server's role
     */
    ApiMessageType.ListenerType listenerType();

    /**
     * The ApiVersionsResponse to send back to client when they send an ApiVersionsRequest
     * @param throttleTimeMs The throttle time in milliseconds
     * @param alterFeatureLevel0 Whether to filter feature v0 in the response
     * @return the ApiVersionsResponse to send back to the client
     */
    ApiVersionsResponse apiVersionResponse(int throttleTimeMs, boolean alterFeatureLevel0);

    /**
     * The features supported by the server
     * @return the FinalizedFeatures
     */
    FinalizedFeatures features();

    /**
     * Whether the specified API and version is supported
     * @param apiKey the API key
     * @param apiVersion the API version
     * @return true if the API key and version is supported, otherwise false
     */
    default boolean isApiEnabled(ApiKeys apiKey, short apiVersion) {
        return apiKey != null && apiKey.inScope(listenerType()) && apiKey.isVersionEnabled(apiVersion, enableUnstableLastVersion());
    }

    /**
     * Create a new RequestChannelMetrics for the enabled APIs
     * @return the RequestChannelMetrics
     */
    default RequestChannelMetrics newRequestMetrics() {
        return new RequestChannelMetrics(ApiKeys.apisForListener(listenerType()));
    }
}
