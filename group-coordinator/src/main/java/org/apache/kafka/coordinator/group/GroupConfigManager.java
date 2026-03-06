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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger log = LoggerFactory.getLogger(GroupConfigManager.class);

    private final GroupConfig defaultConfig;

    private final Map<String, GroupConfig> configMap;

    private final GroupCoordinatorConfig groupCoordinatorConfig;

    private final ShareGroupConfig shareGroupConfig;

    public GroupConfigManager(
        Map<?, ?> defaultConfig,
        GroupCoordinatorConfig groupCoordinatorConfig,
        ShareGroupConfig shareGroupConfig
    ) {
        this.configMap = new ConcurrentHashMap<>();
        this.defaultConfig = new GroupConfig(defaultConfig);
        this.groupCoordinatorConfig = Objects.requireNonNull(groupCoordinatorConfig);
        this.shareGroupConfig = Objects.requireNonNull(shareGroupConfig);
    }

    /**
     * Update the configuration of the provided group.
     *
     * This method ensures all configuration values are within the broker's
     * allowed min/max bounds. Values outside the range are clamped and a WARN
     * log is emitted.
     *
     * @param groupId        The group id.
     * @param newGroupConfig The new group config.
     */
    public void updateGroupConfig(String groupId, Properties newGroupConfig) {
        if (null == groupId || groupId.isEmpty()) {
            throw new InvalidRequestException("Group name can't be empty.");
        }

        // Admin API updates are pre-validated by validate(), so clamping is a no-op.
        // Configs loaded on broker startup are not validated, so clamping may adjust values.
        Properties clampedProps = maybeClampConfigs(groupId, newGroupConfig);

        final GroupConfig newConfig = GroupConfig.fromProps(
            defaultConfig.originals(),
            clampedProps
        );
        configMap.put(groupId, newConfig);
    }

    /**
     * Clamp integer config values to their broker-level [min, max] bounds.
     * A WARN log is emitted for each value that is adjusted.
     *
     * @param groupId The group id.
     * @param props   The raw group config properties.
     * @return A new Properties with out-of-range integer values clamped.
     */
    private Properties maybeClampConfigs(String groupId, Properties props) {
        Properties clamped = new Properties();
        clamped.putAll(props);

        // Consumer group configs
        maybeClampConfig(clamped, groupId, GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.consumerGroupMinSessionTimeoutMs(),
            groupCoordinatorConfig.consumerGroupMaxSessionTimeoutMs());
        maybeClampConfig(clamped, groupId, GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.consumerGroupMinHeartbeatIntervalMs(),
            groupCoordinatorConfig.consumerGroupMaxHeartbeatIntervalMs());

        // Share group configs
        maybeClampConfig(clamped, groupId, GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.shareGroupMinSessionTimeoutMs(),
            groupCoordinatorConfig.shareGroupMaxSessionTimeoutMs());
        maybeClampConfig(clamped, groupId, GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.shareGroupMinHeartbeatIntervalMs(),
            groupCoordinatorConfig.shareGroupMaxHeartbeatIntervalMs());
        maybeClampConfig(clamped, groupId, GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG,
            shareGroupConfig.shareGroupMinRecordLockDurationMs(),
            shareGroupConfig.shareGroupMaxRecordLockDurationMs());
        maybeClampConfig(clamped, groupId, GroupConfig.SHARE_DELIVERY_COUNT_LIMIT_CONFIG,
            shareGroupConfig.shareGroupMinDeliveryCountLimit(),
            shareGroupConfig.shareGroupMaxDeliveryCountLimit());

        // Streams group configs
        maybeClampConfig(clamped, groupId, GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.streamsGroupMinSessionTimeoutMs(),
            groupCoordinatorConfig.streamsGroupMaxSessionTimeoutMs());
        maybeClampConfig(clamped, groupId, GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.streamsGroupMinHeartbeatIntervalMs(),
            groupCoordinatorConfig.streamsGroupMaxHeartbeatIntervalMs());
        maybeClampConfig(clamped, groupId, GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG,
            0,
            groupCoordinatorConfig.streamsGroupMaxNumStandbyReplicas());

        return clamped;
    }

    /**
     * Clamp a single integer config value to [min, max]. If the value is out of range,
     * it is replaced in the properties and a WARN log is emitted.
     *
     * @param props   The properties to modify in place.
     * @param groupId The group id.
     * @param key     The config key name.
     * @param min     The broker-level minimum (inclusive).
     * @param max     The broker-level maximum (inclusive).
     */
    private static void maybeClampConfig(Properties props, String groupId, String key, int min, int max) {
        Object rawValue = props.get(key);
        if (rawValue == null) return;

        int value = Integer.parseInt(rawValue.toString());

        if (value < min) {
            log.warn("The group config '{}' for group '{}' has value {} which is below the broker's " +
                    "allowed minimum {}. The effective value will be capped to {}.",
                key, groupId, value, min, min);
            props.put(key, min);
        } else if (value > max) {
            log.warn("The group config '{}' for group '{}' has value {} which exceeds the broker's " +
                    "allowed maximum {}. The effective value will be capped to {}.",
                key, groupId, value, max, max);
            props.put(key, max);
        }
    }

    /**
     * Get the group config if it exists, otherwise return None.
     * The returned config has already been clamped to broker-level min/max bounds.
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

    /**
     * Remove all group configs.
     */
    public void close() {
        configMap.clear();
    }
}
