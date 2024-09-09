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

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.server.share.ShareGroupConfig;
import org.apache.kafka.server.share.ShareGroupDynamicConfig;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The group config manager is responsible for config modification and cleaning.
 */
public class GroupConfigManager implements AutoCloseable {

    private final ConsumerGroupDynamicConfig defaultConsumerGroupConfig;
    private final ShareGroupDynamicConfig defaultShareGroupConfig;

    private final Map<String, ConsumerGroupDynamicConfig> consumerGroupConfigMap;
    private final Map<String, ShareGroupDynamicConfig> shareGroupConfigMap;

    public GroupConfigManager(Map<?, ?> defaultConsumerGroupConfig, Map<?, ?> defaultShareGroupConfig) {
        this.consumerGroupConfigMap = new ConcurrentHashMap<>();
        this.shareGroupConfigMap = new ConcurrentHashMap<>();
        this.defaultConsumerGroupConfig = new ConsumerGroupDynamicConfig(defaultConsumerGroupConfig);
        this.defaultShareGroupConfig = new ShareGroupDynamicConfig(defaultShareGroupConfig);
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
        Properties consumerGroupDynamicConfig = new Properties();
        Properties shareGroupDynamicConfig = new Properties();

        newGroupConfig.forEach((key, value) -> {
            if (ConsumerGroupDynamicConfig.isConsumerGroupConfig((String) key)) {
                consumerGroupDynamicConfig.put(key, value);
            } else {
                shareGroupDynamicConfig.put(key, value);
            }
        });

        updateConsumerGroupConfig(groupId, consumerGroupDynamicConfig);
        updateShareGroupConfig(groupId, shareGroupDynamicConfig);
    }

    /**
     * Get the consumer group config if it exists, otherwise return None.
     *
     * @param groupId  The group id.
     * @return The consumer group config.
     */
    public Optional<ConsumerGroupDynamicConfig> consumerGroupConfig(String groupId) {
        return Optional.ofNullable(consumerGroupConfigMap.get(groupId));
    }

    /**
     * Get the share group config if it exists, otherwise return None.
     *
     * @param groupId  The group id.
     * @return The share group config.
     */
    public Optional<ShareGroupDynamicConfig> shareGroupConfig(String groupId) {
        return Optional.ofNullable(shareGroupConfigMap.get(groupId));
    }

    /**
     * Validate the given properties.
     *
     * @param newGroupConfig                 The new group config.
     * @param groupCoordinatorConfig         The group coordinator config.
     * @param shareGroupConfig               The share coordinator config
     * @throws InvalidConfigurationException If validation fails
     */
    public static void validate(
        Properties newGroupConfig,
        GroupCoordinatorConfig groupCoordinatorConfig,
        ShareGroupConfig shareGroupConfig
    ) {
        Properties combinedConfigs = new Properties();
        combinedConfigs.putAll(groupCoordinatorConfig.extractConsumerGroupConfigMap());
        combinedConfigs.putAll(shareGroupConfig.extractShareGroupConfigMap());
        combinedConfigs.putAll(newGroupConfig);
        DynamicGroupConfig.validate(combinedConfigs, groupCoordinatorConfig, shareGroupConfig);
    }

    /**
     * Remove all group configs.
     */
    public void close() {
        consumerGroupConfigMap.clear();
        shareGroupConfigMap.clear();
    }

    /**
     * Update the consumer group configuration of the provided group.
     *
     * @param groupId                   The group id.
     * @param newConsumerGroupConfig    The new consumer group config.
     */
    private void updateConsumerGroupConfig(String groupId, Properties newConsumerGroupConfig) {
        final ConsumerGroupDynamicConfig newConfig = ConsumerGroupDynamicConfig.fromProps(
            defaultConsumerGroupConfig.originals(),
            newConsumerGroupConfig
        );
        consumerGroupConfigMap.put(groupId, newConfig);
    }

    /**
     * Update the share group configuration of the provided group.
     *
     * @param groupId                   The group id.
     * @param newShareGroupConfig       The new share group config.
     */
    private void updateShareGroupConfig(String groupId, Properties newShareGroupConfig) {
        final ShareGroupDynamicConfig newConfig = ShareGroupDynamicConfig.fromProps(
            defaultShareGroupConfig.originals(),
            newShareGroupConfig
        );
        shareGroupConfigMap.put(groupId, newConfig);
    }
}
