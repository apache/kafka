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
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The group config manager is responsible for config modification and cleaning.
 */
public class GroupConfigManager implements AutoCloseable {

    /**
     * The group configs for each group.
     *
     * Groups are only present in this map when they have config overrides.
     */
    private final Map<String, GroupConfig> configMap;

    /**
     * The group coordinator config.
     */
    private final GroupCoordinatorConfig groupCoordinatorConfig;

    /**
     * The share group config.
     */
    private final ShareGroupConfig shareGroupConfig;

    /**
     * Constructor.
     *
     * @param groupCoordinatorConfig The group coordinator config.
     * @param shareGroupConfig       The share group config.
     */
    public GroupConfigManager(
        GroupCoordinatorConfig groupCoordinatorConfig,
        ShareGroupConfig shareGroupConfig
    ) {
        this.configMap = new ConcurrentHashMap<>();
        this.groupCoordinatorConfig = Objects.requireNonNull(groupCoordinatorConfig);
        this.shareGroupConfig = Objects.requireNonNull(shareGroupConfig);
    }

    /**
     * Update the configuration of the provided group.
     *
     * This method evaluates all configuration values within broker-level bounds.
     *
     * @param groupId        The group id.
     * @param newGroupConfig The new group config overrides.
     */
    public void updateGroupConfig(String groupId, Properties newGroupConfig) {
        if (null == groupId || groupId.isEmpty()) {
            throw new InvalidRequestException("Group name can't be empty.");
        }

        if (newGroupConfig.isEmpty()) {
            configMap.remove(groupId);
            return;
        }

        // Evaluate ensures configs respect broker-level bounds. For the Admin API path,
        // values are pre-validated so this is effectively a no-op. For the broker startup
        // path, configs from metadata may need evaluation if bounds have changed.
        Properties evaluatedProps = GroupConfig.evaluate(
            newGroupConfig, groupId, groupCoordinatorConfig, shareGroupConfig);

        final GroupConfig newConfig = new GroupConfig(evaluatedProps);
        configMap.put(groupId, newConfig);
    }

    /**
     * Get the group config if it has any overrides, otherwise return {@link Optional#empty()}.
     * The returned config has already been evaluated within broker-level bounds.
     *
     * @param groupId  The group id.
     * @return The group config.
     */
    public Optional<GroupConfig> groupConfig(String groupId) {
        return Optional.ofNullable(configMap.get(groupId));
    }

    public List<String> groupIds() {
        return List.copyOf(configMap.keySet());
    }

    public Optional<String> shareGroupDlqTopicPrefix() {
        return Optional.ofNullable(groupCoordinatorConfig.errorsDLQTopicNamePrefix());
    }

    public boolean isDlqAutoTopicCreateEnabled() {
        return groupCoordinatorConfig.errorsDLQAutoCreateTopicsEnable();
    }

    /**
     * Remove all group configs.
     */
    public void close() {
        configMap.clear();
    }
}
