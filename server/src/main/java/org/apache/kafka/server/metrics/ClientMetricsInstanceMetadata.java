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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.RequestContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Information from the client's metadata is gathered from the client's request.
 */
public class ClientMetricsInstanceMetadata {

    private final Map<String, String> attributesMap;

    public ClientMetricsInstanceMetadata(Uuid clientInstanceId, RequestContext requestContext) {
        Objects.requireNonNull(clientInstanceId);
        Objects.requireNonNull(requestContext);

        attributesMap = new HashMap<>();

        attributesMap.put(ClientMetricsConfigs.CLIENT_INSTANCE_ID, clientInstanceId.toString());
        attributesMap.put(ClientMetricsConfigs.CLIENT_ID, requestContext.clientId());
        attributesMap.put(ClientMetricsConfigs.CLIENT_SOFTWARE_NAME, requestContext.clientInformation != null ?
            requestContext.clientInformation.softwareName() : null);
        attributesMap.put(ClientMetricsConfigs.CLIENT_SOFTWARE_VERSION, requestContext.clientInformation != null ?
            requestContext.clientInformation.softwareVersion() : null);
        attributesMap.put(ClientMetricsConfigs.CLIENT_SOURCE_ADDRESS, requestContext.clientAddress != null ?
            requestContext.clientAddress.getHostAddress() : null);
        attributesMap.put(ClientMetricsConfigs.CLIENT_SOURCE_PORT, requestContext.clientPort.map(String::valueOf).orElse(null));
    }

    public boolean isMatch(Map<String, Pattern> patterns) {
        if (!patterns.isEmpty()) {
            return matchPatterns(patterns);
        }
        // Empty pattern is still considered as a match.
        return true;
    }

    private boolean matchPatterns(Map<String, Pattern> matchingPatterns) {
        return matchingPatterns.entrySet().stream()
            .allMatch(entry -> {
                String attribute = attributesMap.get(entry.getKey());
                return attribute != null && entry.getValue().matcher(attribute).matches();
            });
    }
}
