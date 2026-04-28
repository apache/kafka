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

import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Default implementation of SupportedConfigChecker that checks if a configuration name
 * is supported for a given resource type based on the actual config definitions.
 *
 * This class maintains a predicate per resource type:
 * - TOPIC: Configurations defined in LogConfig
 * - BROKER: All config names are accepted. Broker configs include listener-specific
 *   prefixed configs (e.g., listener.name.&lt;name&gt;.ssl.keystore.location) whose names
 *   are user-defined at runtime and cannot be pre-enumerated. They also include
 *   plugin-defined configs (e.g., custom authorizer or quota callback configs) with
 *   arbitrary names. For these reasons BROKER configs are not filtered by name.
 * - CLIENT_METRICS: Configurations defined in ClientMetricsConfigs
 * - GROUP: Configurations defined in GroupConfig
 *
 * Config names for resource types not in this map are considered unsupported.
 */
public final class DefaultSupportedConfigChecker implements SupportedConfigChecker {
    static final class SetContainsPredicate implements Predicate<String> {
        private final Set<String> keys;

        SetContainsPredicate(Set<String> keys) {
            this.keys = keys;
        }

        @Override
        public boolean test(String key) {
            return keys.contains(key);
        }
    }

    private final Map<ConfigResource.Type, Predicate<String>> validConfigsByType;

    public DefaultSupportedConfigChecker() {
        this.validConfigsByType = Map.of(
            ConfigResource.Type.TOPIC, new SetContainsPredicate(LogConfig.configNames()),
            ConfigResource.Type.BROKER, ignore -> true,
            ConfigResource.Type.CLIENT_METRICS, new SetContainsPredicate(ClientMetricsConfigs.configNames()),
            ConfigResource.Type.GROUP, new SetContainsPredicate(GroupConfig.configNames())
        );
    }

    @Override
    public boolean isSupported(ConfigResource.Type resourceType, String configName) {
        Predicate<String> predicate = validConfigsByType.get(resourceType);
        return predicate != null && predicate.test(configName);
    }
}
