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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * Group configuration related parameters and supporting methods like validation, etc. are
 * defined in this class.
 */
public final class GroupConfig extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(GroupConfig.class);

    public static final String CONSUMER_SESSION_TIMEOUT_MS_CONFIG = "consumer.session.timeout.ms";

    public static final String CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG = "consumer.heartbeat.interval.ms";

    public static final String SHARE_SESSION_TIMEOUT_MS_CONFIG = "share.session.timeout.ms";

    public static final String SHARE_HEARTBEAT_INTERVAL_MS_CONFIG = "share.heartbeat.interval.ms";

    public static final String SHARE_RECORD_LOCK_DURATION_MS_CONFIG = "share.record.lock.duration.ms";

    public static final String SHARE_DELIVERY_COUNT_LIMIT_CONFIG = "share.delivery.count.limit";

    public static final String SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG = "share.partition.max.record.locks";

    public static final String SHARE_AUTO_OFFSET_RESET_CONFIG = "share.auto.offset.reset";
    public static final String SHARE_AUTO_OFFSET_RESET_DEFAULT = ShareGroupAutoOffsetResetStrategy.LATEST.name();
    public static final String SHARE_AUTO_OFFSET_RESET_DOC = "The strategy to initialize the share-partition start offset. " +
        "<ul><li>earliest: automatically reset the offset to the earliest offset</li>" +
        "<li>latest: automatically reset the offset to the latest offset</li>" +
        "<li>by_duration:&lt;duration&gt;: automatically reset the offset to a configured duration from the current timestamp. " +
        "&lt;duration&gt; must be specified in ISO8601 format (PnDTnHnMn.nS). " +
        "Negative duration is not allowed.</li>" +
        "<li>anything else: throw exception to the share consumer.</li></ul>";

    public static final String SHARE_ISOLATION_LEVEL_CONFIG = "share.isolation.level";
    public static final String SHARE_ISOLATION_LEVEL_DEFAULT = IsolationLevel.READ_UNCOMMITTED.toString();
    public static final String SHARE_ISOLATION_LEVEL_DOC = "Controls how to read records written transactionally. " +
        "If set to \"read_committed\", the share group will only deliver transactional records which have been committed. " +
        "If set to \"read_uncommitted\", the share group will return all messages, even transactional messages which have been aborted. " +
        "Non-transactional records will be returned unconditionally in either mode.";

    public static final String SHARE_RENEW_ACKNOWLEDGE_ENABLE_CONFIG = "share.renew.acknowledge.enable";
    public static final boolean SHARE_RENEW_ACKNOWLEDGE_ENABLE_DEFAULT = true;
    public static final String SHARE_RENEW_ACKNOWLEDGE_ENABLE_DOC = "Whether the renew acknowledge type is enabled for the share group.";

    public static final String STREAMS_SESSION_TIMEOUT_MS_CONFIG = "streams.session.timeout.ms";

    public static final String STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG = "streams.heartbeat.interval.ms";

    public static final String STREAMS_NUM_STANDBY_REPLICAS_CONFIG = "streams.num.standby.replicas";

    public static final String STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG = "streams.initial.rebalance.delay.ms";

    public final int consumerSessionTimeoutMs;

    public final int consumerHeartbeatIntervalMs;

    public final int shareSessionTimeoutMs;

    public final int shareHeartbeatIntervalMs;

    public final int shareRecordLockDurationMs;

    public final int shareDeliveryCountLimit;

    public final int sharePartitionMaxRecordLocks;

    public final String shareAutoOffsetReset;

    public final int streamsSessionTimeoutMs;

    public final int streamsHeartbeatIntervalMs;

    public final int streamsNumStandbyReplicas;

    public final int streamsInitialRebalanceDelayMs;

    public final String shareIsolationLevel;

    public final boolean shareRenewAcknowledgeEnable;

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(CONSUMER_SESSION_TIMEOUT_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_DEFAULT,
            atLeast(1),
            MEDIUM,
            GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_DOC)
        .define(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT,
            atLeast(1),
            MEDIUM,
            GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DOC)
        .define(SHARE_SESSION_TIMEOUT_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.SHARE_GROUP_SESSION_TIMEOUT_MS_DEFAULT,
            atLeast(1),
            MEDIUM,
            GroupCoordinatorConfig.SHARE_GROUP_SESSION_TIMEOUT_MS_DOC)
        .define(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.SHARE_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT,
            atLeast(1),
            MEDIUM,
            GroupCoordinatorConfig.SHARE_GROUP_HEARTBEAT_INTERVAL_MS_DOC)
        .define(SHARE_RECORD_LOCK_DURATION_MS_CONFIG,
            INT,
            ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_DEFAULT,
            atLeast(1000),
            MEDIUM,
            ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_DOC)
        .define(SHARE_DELIVERY_COUNT_LIMIT_CONFIG,
            INT,
            ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_DEFAULT,
            atLeast(2),
            MEDIUM,
            ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_DOC)
        .define(SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG,
            INT,
            ShareGroupConfig.SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_DEFAULT,
            atLeast(100),
            MEDIUM,
            ShareGroupConfig.SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_DOC)
        .define(SHARE_AUTO_OFFSET_RESET_CONFIG,
            STRING,
            SHARE_AUTO_OFFSET_RESET_DEFAULT,
            new ShareGroupAutoOffsetResetStrategy.Validator(),
            MEDIUM,
            SHARE_AUTO_OFFSET_RESET_DOC)
        .define(SHARE_ISOLATION_LEVEL_CONFIG,
            STRING,
            SHARE_ISOLATION_LEVEL_DEFAULT,
            in(IsolationLevel.READ_COMMITTED.toString(), IsolationLevel.READ_UNCOMMITTED.toString()),
            MEDIUM,
            SHARE_ISOLATION_LEVEL_DOC)
        .define(SHARE_RENEW_ACKNOWLEDGE_ENABLE_CONFIG,
            BOOLEAN,
            SHARE_RENEW_ACKNOWLEDGE_ENABLE_DEFAULT,
            MEDIUM,
            SHARE_RENEW_ACKNOWLEDGE_ENABLE_DOC)
        .define(STREAMS_SESSION_TIMEOUT_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_DEFAULT,
            atLeast(1),
            MEDIUM,
            GroupCoordinatorConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_DOC)
        .define(STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT,
            atLeast(1),
            MEDIUM,
            GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_DOC)
        .define(STREAMS_NUM_STANDBY_REPLICAS_CONFIG,
            INT,
            GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_DEFAULT,
            atLeast(0),
            MEDIUM,
            GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_DOC)
        .define(STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT,
            atLeast(0),
            MEDIUM,
            GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DOC);

    public GroupConfig(Map<?, ?> props) {
        super(CONFIG, props, false);
        this.consumerSessionTimeoutMs = getInt(CONSUMER_SESSION_TIMEOUT_MS_CONFIG);
        this.consumerHeartbeatIntervalMs = getInt(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG);
        this.shareSessionTimeoutMs = getInt(SHARE_SESSION_TIMEOUT_MS_CONFIG);
        this.shareHeartbeatIntervalMs = getInt(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG);
        this.shareRecordLockDurationMs = getInt(SHARE_RECORD_LOCK_DURATION_MS_CONFIG);
        this.shareDeliveryCountLimit = getInt(SHARE_DELIVERY_COUNT_LIMIT_CONFIG);
        this.sharePartitionMaxRecordLocks = getInt(SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG);
        this.shareAutoOffsetReset = getString(SHARE_AUTO_OFFSET_RESET_CONFIG);
        this.streamsSessionTimeoutMs = getInt(STREAMS_SESSION_TIMEOUT_MS_CONFIG);
        this.streamsHeartbeatIntervalMs = getInt(STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG);
        this.streamsNumStandbyReplicas = getInt(STREAMS_NUM_STANDBY_REPLICAS_CONFIG);
        this.streamsInitialRebalanceDelayMs = getInt(STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG);
        this.shareIsolationLevel = getString(SHARE_ISOLATION_LEVEL_CONFIG);
        this.shareRenewAcknowledgeEnable = getBoolean(SHARE_RENEW_ACKNOWLEDGE_ENABLE_CONFIG);
    }

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public static Optional<Type> configType(String configName) {
        return Optional.ofNullable(CONFIG.configKeys().get(configName)).map(c -> c.type);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    /**
     * Check that property names are valid
     */
    public static void validateNames(Properties props) {
        Set<String> names = configNames();
        for (String name : props.stringPropertyNames()) {
            if (!names.contains(name)) {
                throw new InvalidConfigurationException("Unknown group config name: " + name);
            }
        }
    }

    /**
     * Validates the values of the given properties.
     */
    @SuppressWarnings({"CyclomaticComplexity", "NPathComplexity"})
    private static void validateValues(Map<?, ?> valueMaps, GroupCoordinatorConfig groupCoordinatorConfig, ShareGroupConfig shareGroupConfig) {
        int consumerHeartbeatInterval = (Integer) valueMaps.get(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG);
        int consumerSessionTimeout = (Integer) valueMaps.get(CONSUMER_SESSION_TIMEOUT_MS_CONFIG);
        int shareHeartbeatInterval = (Integer) valueMaps.get(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG);
        int shareSessionTimeout = (Integer) valueMaps.get(SHARE_SESSION_TIMEOUT_MS_CONFIG);
        int shareRecordLockDurationMs = (Integer) valueMaps.get(SHARE_RECORD_LOCK_DURATION_MS_CONFIG);
        int shareDeliveryCountLimit = (Integer) valueMaps.get(SHARE_DELIVERY_COUNT_LIMIT_CONFIG);
        int sharePartitionMaxRecordLocks = (Integer) valueMaps.get(SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG);
        int streamsSessionTimeoutMs = (Integer) valueMaps.get(STREAMS_SESSION_TIMEOUT_MS_CONFIG);
        int streamsHeartbeatIntervalMs = (Integer) valueMaps.get(STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG);
        int streamsNumStandbyReplicas = (Integer) valueMaps.get(STREAMS_NUM_STANDBY_REPLICAS_CONFIG);
        if (consumerHeartbeatInterval < groupCoordinatorConfig.consumerGroupMinHeartbeatIntervalMs()) {
            throw new InvalidConfigurationException(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG + " must be greater than or equal to " +
                GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (consumerHeartbeatInterval > groupCoordinatorConfig.consumerGroupMaxHeartbeatIntervalMs()) {
            throw new InvalidConfigurationException(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG + " must be less than or equal to " +
                GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (consumerSessionTimeout < groupCoordinatorConfig.consumerGroupMinSessionTimeoutMs()) {
            throw new InvalidConfigurationException(CONSUMER_SESSION_TIMEOUT_MS_CONFIG + " must be greater than or equal to " +
                GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG);
        }
        if (consumerSessionTimeout > groupCoordinatorConfig.consumerGroupMaxSessionTimeoutMs()) {
            throw new InvalidConfigurationException(CONSUMER_SESSION_TIMEOUT_MS_CONFIG + " must be less than or equal to " +
                GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG);
        }
        if (shareHeartbeatInterval < groupCoordinatorConfig.shareGroupMinHeartbeatIntervalMs()) {
            throw new InvalidConfigurationException(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG + " must be greater than or equal to " +
                GroupCoordinatorConfig.SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (shareHeartbeatInterval > groupCoordinatorConfig.shareGroupMaxHeartbeatIntervalMs()) {
            throw new InvalidConfigurationException(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG + " must be less than or equal to " +
                GroupCoordinatorConfig.SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (shareSessionTimeout < groupCoordinatorConfig.shareGroupMinSessionTimeoutMs()) {
            throw new InvalidConfigurationException(SHARE_SESSION_TIMEOUT_MS_CONFIG + " must be greater than or equal to " +
                GroupCoordinatorConfig.SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG);
        }
        if (shareSessionTimeout > groupCoordinatorConfig.shareGroupMaxSessionTimeoutMs()) {
            throw new InvalidConfigurationException(SHARE_SESSION_TIMEOUT_MS_CONFIG + " must be less than or equal to " +
                GroupCoordinatorConfig.SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG);
        }
        if (shareRecordLockDurationMs < shareGroupConfig.shareGroupMinRecordLockDurationMs()) {
            throw new InvalidConfigurationException(SHARE_RECORD_LOCK_DURATION_MS_CONFIG + " must be greater than or equal to " +
                ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG);
        }
        if (shareRecordLockDurationMs > shareGroupConfig.shareGroupMaxRecordLockDurationMs()) {
            throw new InvalidConfigurationException(SHARE_RECORD_LOCK_DURATION_MS_CONFIG + " must be less than or equal to " +
                ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG);
        }
        if (shareDeliveryCountLimit < shareGroupConfig.shareGroupMinDeliveryCountLimit()) {
            throw new InvalidConfigurationException(SHARE_DELIVERY_COUNT_LIMIT_CONFIG + " must be greater than or equal to " +
                ShareGroupConfig.SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_CONFIG);
        }
        if (shareDeliveryCountLimit > shareGroupConfig.shareGroupMaxDeliveryCountLimit()) {
            throw new InvalidConfigurationException(SHARE_DELIVERY_COUNT_LIMIT_CONFIG + " must be less than or equal to " +
                ShareGroupConfig.SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT_CONFIG);
        }
        if (sharePartitionMaxRecordLocks < shareGroupConfig.shareGroupMinPartitionMaxRecordLocks()) {
            throw new InvalidConfigurationException(SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG + " must be greater than or equal to " +
                ShareGroupConfig.SHARE_GROUP_MIN_PARTITION_MAX_RECORD_LOCKS_CONFIG);
        }
        if (sharePartitionMaxRecordLocks > shareGroupConfig.shareGroupMaxPartitionMaxRecordLocks()) {
            throw new InvalidConfigurationException(SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG + " must be less than or equal to " +
                ShareGroupConfig.SHARE_GROUP_MAX_PARTITION_MAX_RECORD_LOCKS_CONFIG);
        }
        if (streamsHeartbeatIntervalMs < groupCoordinatorConfig.streamsGroupMinHeartbeatIntervalMs()) {
            throw new InvalidConfigurationException(STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG + " must be greater than or equal to " +
                GroupCoordinatorConfig.STREAMS_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (streamsHeartbeatIntervalMs > groupCoordinatorConfig.streamsGroupMaxHeartbeatIntervalMs()) {
            throw new InvalidConfigurationException(STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG + " must be less than or equal to " +
                GroupCoordinatorConfig.STREAMS_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (streamsSessionTimeoutMs < groupCoordinatorConfig.streamsGroupMinSessionTimeoutMs()) {
            throw new InvalidConfigurationException(STREAMS_SESSION_TIMEOUT_MS_CONFIG + " must be greater than or equal to " +
                GroupCoordinatorConfig.STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG);
        }
        if (streamsSessionTimeoutMs > groupCoordinatorConfig.streamsGroupMaxSessionTimeoutMs()) {
            throw new InvalidConfigurationException(STREAMS_SESSION_TIMEOUT_MS_CONFIG + " must be less than or equal to " +
                GroupCoordinatorConfig.STREAMS_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG);
        }
        if (streamsNumStandbyReplicas > groupCoordinatorConfig.streamsGroupMaxNumStandbyReplicas()) {
            throw new InvalidConfigurationException(STREAMS_NUM_STANDBY_REPLICAS_CONFIG + " must be less than or equal to " +
                GroupCoordinatorConfig.STREAMS_GROUP_MAX_STANDBY_REPLICAS_CONFIG);
        }
        if (consumerSessionTimeout <= consumerHeartbeatInterval) {
            throw new InvalidConfigurationException(CONSUMER_SESSION_TIMEOUT_MS_CONFIG + " must be greater than " +
                CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (shareSessionTimeout <= shareHeartbeatInterval) {
            throw new InvalidConfigurationException(SHARE_SESSION_TIMEOUT_MS_CONFIG + " must be greater than " +
                SHARE_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (streamsSessionTimeoutMs <= streamsHeartbeatIntervalMs) {
            throw new InvalidConfigurationException(STREAMS_SESSION_TIMEOUT_MS_CONFIG + " must be greater than " +
                STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
    }

    /**
     * Check that the given properties contain only valid group config names and that
     * all values can be parsed and are valid. The provided properties are merged with
     * the broker-level defaults before validation.
     */
    public static void validate(Properties props, GroupCoordinatorConfig groupCoordinatorConfig, ShareGroupConfig shareGroupConfig) {
        Properties combinedConfigs = new Properties();
        combinedConfigs.putAll(groupCoordinatorConfig.extractGroupConfigMap(shareGroupConfig));
        combinedConfigs.putAll(props);

        validateNames(combinedConfigs);
        Map<?, ?> valueMaps = CONFIG.parse(combinedConfigs);
        validateValues(valueMaps, groupCoordinatorConfig, shareGroupConfig);
    }

    /**
     * Evaluate group config values to their effective values within broker-level bounds.
     * Out-of-range values are capped and a WARN log is emitted.
     *
     * @param props                  The raw group config properties.
     * @param groupId                The group id.
     * @param groupCoordinatorConfig The group coordinator config.
     * @param shareGroupConfig       The share group config.
     * @return A new Properties with out-of-range values capped.
     */
    public static Properties evaluate(
        Properties props,
        String groupId,
        GroupCoordinatorConfig groupCoordinatorConfig,
        ShareGroupConfig shareGroupConfig
    ) {
        Properties effective = new Properties();
        effective.putAll(props);
        evaluateValues(effective, groupId, groupCoordinatorConfig, shareGroupConfig);
        return effective;
    }

    private static void evaluateValues(
        Properties props,
        String groupId,
        GroupCoordinatorConfig groupCoordinatorConfig,
        ShareGroupConfig shareGroupConfig
    ) {
        // Consumer group configs
        clampToRange(props, groupId, CONSUMER_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.consumerGroupMinSessionTimeoutMs(),
            groupCoordinatorConfig.consumerGroupMaxSessionTimeoutMs());
        clampToRange(props, groupId, CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.consumerGroupMinHeartbeatIntervalMs(),
            groupCoordinatorConfig.consumerGroupMaxHeartbeatIntervalMs());

        // Share group configs
        clampToRange(props, groupId, SHARE_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.shareGroupMinSessionTimeoutMs(),
            groupCoordinatorConfig.shareGroupMaxSessionTimeoutMs());
        clampToRange(props, groupId, SHARE_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.shareGroupMinHeartbeatIntervalMs(),
            groupCoordinatorConfig.shareGroupMaxHeartbeatIntervalMs());
        clampToRange(props, groupId, SHARE_RECORD_LOCK_DURATION_MS_CONFIG,
            shareGroupConfig.shareGroupMinRecordLockDurationMs(),
            shareGroupConfig.shareGroupMaxRecordLockDurationMs());
        clampToRange(props, groupId, SHARE_DELIVERY_COUNT_LIMIT_CONFIG,
            shareGroupConfig.shareGroupMinDeliveryCountLimit(),
            shareGroupConfig.shareGroupMaxDeliveryCountLimit());
        clampToRange(props, groupId, SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG,
            shareGroupConfig.shareGroupMinPartitionMaxRecordLocks(),
            shareGroupConfig.shareGroupMaxPartitionMaxRecordLocks());

        // Streams group configs
        clampToRange(props, groupId, STREAMS_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.streamsGroupMinSessionTimeoutMs(),
            groupCoordinatorConfig.streamsGroupMaxSessionTimeoutMs());
        clampToRange(props, groupId, STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.streamsGroupMinHeartbeatIntervalMs(),
            groupCoordinatorConfig.streamsGroupMaxHeartbeatIntervalMs());
        clampToMax(props, groupId, STREAMS_NUM_STANDBY_REPLICAS_CONFIG,
            groupCoordinatorConfig.streamsGroupMaxNumStandbyReplicas());

        // Verify that clamping did not break the session > heartbeat invariant.
        checkSessionExceedsHeartbeat(props, groupId,
            CONSUMER_SESSION_TIMEOUT_MS_CONFIG, groupCoordinatorConfig.consumerGroupSessionTimeoutMs(),
            CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, groupCoordinatorConfig.consumerGroupHeartbeatIntervalMs());
        checkSessionExceedsHeartbeat(props, groupId,
            SHARE_SESSION_TIMEOUT_MS_CONFIG, groupCoordinatorConfig.shareGroupSessionTimeoutMs(),
            SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, groupCoordinatorConfig.shareGroupHeartbeatIntervalMs());
        checkSessionExceedsHeartbeat(props, groupId,
            STREAMS_SESSION_TIMEOUT_MS_CONFIG, groupCoordinatorConfig.streamsGroupSessionTimeoutMs(),
            STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, groupCoordinatorConfig.streamsGroupHeartbeatIntervalMs());
    }

    /**
     * Log a WARN if the session timeout is not greater than the heartbeat interval after
     * evaluation. When a key is absent from props, the broker-level default is used.
     */
    private static void checkSessionExceedsHeartbeat(
        Properties props,
        String groupId,
        String sessionKey,
        int defaultSession,
        String heartbeatKey,
        int defaultHeartbeat
    ) {
        Object rawSession = props.get(sessionKey);
        Object rawHeartbeat = props.get(heartbeatKey);
        if (rawSession == null && rawHeartbeat == null) return;

        int session = rawSession != null ? Integer.parseInt(rawSession.toString()) : defaultSession;
        int heartbeat = rawHeartbeat != null ? Integer.parseInt(rawHeartbeat.toString()) : defaultHeartbeat;
        if (session <= heartbeat) {
            log.warn("The effective {} ({}) for group '{}' is not greater than {} ({}). "
                    + "Check that the broker-level min/max bounds for session timeout "
                    + "and heartbeat interval do not overlap.",
                sessionKey, session, groupId, heartbeatKey, heartbeat);
        }
    }

    /**
     * Clamp a config value to [min, max]. A WARN log is emitted on adjustment.
     * No-op when the key is absent from props.
     *
     * @param props   The properties to modify in place.
     * @param groupId The group id.
     * @param key     The config key.
     * @param min     The minimum allowed value (inclusive).
     * @param max     The maximum allowed value (inclusive).
     */
    private static void clampToRange(
        Properties props,
        String groupId,
        String key,
        int min,
        int max
    ) {
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
     * Clamp a config value to at most max. A WARN log is emitted on adjustment.
     * No-op when the key is absent from props.
     *
     * @param props   The properties to modify in place.
     * @param groupId The group id.
     * @param key     The config key.
     * @param max     The maximum allowed value (inclusive).
     */
    private static void clampToMax(
        Properties props,
        String groupId,
        String key,
        int max
    ) {
        Object rawValue = props.get(key);
        if (rawValue == null) return;

        int value = Integer.parseInt(rawValue.toString());
        if (value > max) {
            log.warn("The group config '{}' for group '{}' has value {} which exceeds the broker's " +
                    "allowed maximum {}. The effective value will be capped to {}.",
                key, groupId, value, max, max);
            props.put(key, max);
        }
    }

    /**
     * Create a group config instance using the given properties and defaults.
     */
    public static GroupConfig fromProps(Map<?, ?> defaults, Properties overrides) {
        Properties props = new Properties();
        props.putAll(defaults);
        props.putAll(overrides);
        return new GroupConfig(props);
    }

    /**
     * The default share group auto offset reset strategy.
     */
    public static ShareGroupAutoOffsetResetStrategy defaultShareAutoOffsetReset() {
        return ShareGroupAutoOffsetResetStrategy.fromString(SHARE_AUTO_OFFSET_RESET_DEFAULT);
    }

    /**
     * The default share group isolation level.
     */
    public static IsolationLevel defaultShareIsolationLevel() {
        return IsolationLevel.valueOf(SHARE_ISOLATION_LEVEL_DEFAULT.toUpperCase(Locale.ROOT));
    }

    /**
     * The consumer group session timeout in milliseconds.
     */
    public int consumerSessionTimeoutMs() {
        return consumerSessionTimeoutMs;
    }

    /**
     * The consumer group heartbeat interval in milliseconds.
     */
    public int consumerHeartbeatIntervalMs() {
        return consumerHeartbeatIntervalMs;
    }

    /**
     * The share group session timeout in milliseconds.
     */
    public int shareSessionTimeoutMs() {
        return shareSessionTimeoutMs;
    }

    /**
     * The share group heartbeat interval in milliseconds.
     */
    public int shareHeartbeatIntervalMs() {
        return shareHeartbeatIntervalMs;
    }

    /**
     * The share group delivery count limit.
     */
    public int shareDeliveryCountLimit() {
        return shareDeliveryCountLimit;
    }

    /**
     * The share group partition max record locks.
     */
    public int sharePartitionMaxRecordLocks() {
        return sharePartitionMaxRecordLocks;
    }

    /**
     * The share group record lock duration milliseconds.
     */
    public int shareRecordLockDurationMs() {
        return shareRecordLockDurationMs;
    }

    /**
     * The share group auto offset reset strategy.
     */
    public ShareGroupAutoOffsetResetStrategy shareAutoOffsetReset() {
        return ShareGroupAutoOffsetResetStrategy.fromString(shareAutoOffsetReset);
    }

    /**
     * The streams group session timeout in milliseconds.
     */
    public int streamsSessionTimeoutMs() {
        return streamsSessionTimeoutMs;
    }

    /**
     * The streams group heartbeat interval in milliseconds.
     */
    public int streamsHeartbeatIntervalMs() {
        return streamsHeartbeatIntervalMs;
    }

    /**
     * The number of streams standby replicas for each task.
     */
    public int streamsNumStandbyReplicas() {
        return streamsNumStandbyReplicas;
    }

    /**
     * The initial rebalance delay for streams groups.
     */
    public int streamsInitialRebalanceDelayMs() {
        return streamsInitialRebalanceDelayMs;
    }

    /**
     * The share group isolation level.
     */
    public IsolationLevel shareIsolationLevel() {
        if (shareIsolationLevel == null) {
            throw new IllegalArgumentException("Share isolation level is null");
        }
        try {
            return IsolationLevel.valueOf(shareIsolationLevel.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown Share isolation level: " + shareIsolationLevel);
        }
    }

    /**
     * The share group renew acknowledge enable.
     */
    public boolean shareRenewAcknowledgeEnable() {
        return shareRenewAcknowledgeEnable;
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtml(4, config -> "groupconfigs_" + config));
    }
}
