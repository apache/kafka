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

package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.errors.InvalidRequestException;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The group config manager is responsible for config modification and cleaning.
 */
public class GroupConfigManager implements AutoCloseable {

    private final GroupConfig defaultConfig;

    private final Map<String, GroupConfig> configMap;

    private static final Properties GROUP_CONFIG_BOUNDS = new Properties();

    public GroupConfigManager(
        Map<?, ?> defaultConfig,
        int consumerGroupMinSessionTimeoutMs,
        int consumerGroupMaxSessionTimeoutMs,
        int consumerGroupMinHeartbeatIntervalMs,
        int consumerGroupMaxHeartbeatIntervalMs
    ) {
        this.configMap = new ConcurrentHashMap<>();
        this.defaultConfig = new GroupConfig(defaultConfig);
        GROUP_CONFIG_BOUNDS.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, consumerGroupMinSessionTimeoutMs);
        GROUP_CONFIG_BOUNDS.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, consumerGroupMaxSessionTimeoutMs);
        GROUP_CONFIG_BOUNDS.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, consumerGroupMinHeartbeatIntervalMs);
        GROUP_CONFIG_BOUNDS.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, consumerGroupMaxHeartbeatIntervalMs);
    }

    /**
     * Update the configuration of the provided group.
     *
     * @param groupId                   The group id.
     * @param newGroupConfig            The new group config.
     */
    public void updateGroupConfig(String groupId, Properties newGroupConfig) {
        if (null == groupId || groupId.isEmpty()) {
            throw new InvalidRequestException("Group name can't be empty.");
        }

        // Validate the configuration
        validate(newGroupConfig, defaultConfig.originals());

        final GroupConfig newConfig = GroupConfig.fromProps(
            defaultConfig.originals(),
            newGroupConfig
        );
        configMap.put(groupId, newConfig);
    }

    /**
     * Get the group config if it exists, otherwise return None.
     *
     * @param groupId  The group id.
     * @return The group config.
     */
    public Optional<GroupConfig> groupConfig(String groupId) {
        return Optional.ofNullable(configMap.get(groupId));
    }

    public static void validate(Properties newGroupConfig, Map<?, ?> configuredProps) {
        Properties combinedConfigs = new Properties();
        combinedConfigs.putAll(configuredProps);
        combinedConfigs.putAll(newGroupConfig);
        GroupConfig.validate(combinedConfigs, GROUP_CONFIG_BOUNDS);
    }

    /**
     * Remove all group configs.
     */
    public void close() {
        configMap.clear();
    }
}
