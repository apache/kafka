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

package org.apache.kafka.server.config;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.metadata.SupportedConfigChecker;
import org.apache.kafka.server.metrics.ClientMetricsConfigs;
import org.apache.kafka.storage.internals.log.LogConfig;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of SupportedConfigChecker that checks if a configuration name
 * is supported for a given resource type based on the actual config definitions.
 *
 * This class maintains a whitelist of valid configuration names per resource type:
 * - TOPIC: Configurations defined in LogConfig
 * - BROKER: Configurations defined in DynamicConfig.Broker
 * - CLIENT_METRICS: Configurations defined in ClientMetricsConfigs
 * - GROUP: Configurations defined in GroupConfig
 */
public final class DefaultSupportedConfigChecker implements SupportedConfigChecker {
    private final Map<ConfigResource.Type, Set<String>> validConfigsByType;

    public DefaultSupportedConfigChecker() {
        this.validConfigsByType = Map.of(
            ConfigResource.Type.TOPIC, new HashSet<>(LogConfig.configNames()),
            ConfigResource.Type.BROKER, DynamicConfig.Broker.names(),
            ConfigResource.Type.CLIENT_METRICS, ClientMetricsConfigs.configDef().names(),
            ConfigResource.Type.GROUP, GroupConfig.configDef().names()
        );
    }

    @Override
    public boolean isSupported(ConfigResource.Type resourceType, String configName) {
        Set<String> configs = validConfigsByType.get(resourceType);
        return configs != null && configs.contains(configName);
    }
}
