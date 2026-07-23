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
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * Group configuration related parameters and supporting methods like validation, etc. are
 * defined in this class.
 */
public final class GroupConfig extends AbstractConfig {

    private static final Logger LOG = LoggerFactory.getLogger(GroupConfig.class);

    public static final String CONSUMER_SESSION_TIMEOUT_MS_CONFIG = "consumer.session.timeout.ms";

    public static final String CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG = "consumer.heartbeat.interval.ms";

    public static final String CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG = "consumer.assignment.interval.ms";

    public static final String CONSUMER_ASSIGNOR_OFFLOAD_ENABLE_CONFIG = "consumer.assignor.offload.enable";

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

    public static final String SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG = "share.assignment.interval.ms";

    public static final String SHARE_ASSIGNOR_OFFLOAD_ENABLE_CONFIG = "share.assignor.offload.enable";

    public static final String STREAMS_SESSION_TIMEOUT_MS_CONFIG = "streams.session.timeout.ms";

    public static final String STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG = "streams.heartbeat.interval.ms";

    public static final String STREAMS_NUM_STANDBY_REPLICAS_CONFIG = "streams.num.standby.replicas";

    public static final String STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG = "streams.initial.rebalance.delay.ms";

    public static final String STREAMS_ASSIGNMENT_INTERVAL_MS_CONFIG = "streams.assignment.interval.ms";

    public static final String STREAMS_ASSIGNOR_OFFLOAD_ENABLE_CONFIG = "streams.assignor.offload.enable";

    public static final String STREAMS_TASK_OFFSET_INTERVAL_MS_CONFIG = "streams.task.offset.interval.ms";

    public static final String STREAMS_NUM_WARMUP_REPLICAS_CONFIG = "streams.num.warmup.replicas";

    public static final String STREAMS_ACCEPTABLE_RECOVERY_LAG_CONFIG = "streams.acceptable.recovery.lag";

    public static final String ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG = "errors.deadletterqueue.topic.name";
    public static final String ERRORS_DEADLETTERQUEUE_TOPIC_NAME_DEFAULT = "";
    public static final String ERRORS_DEADLETTERQUEUE_TOPIC_NAME_DOC = "The name of the topic to be used as the dead-letter queue (DLQ) topic for this share group. If blank (the default), the group does not have a DLQ topic.";

    public static final String ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG = "errors.deadletterqueue.copy.record.enable";
    public static final boolean ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_DEFAULT = false;
    public static final String ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_DOC = "When writing onto the dead-letter queue topic, whether to copy the original record onto the DLQ topic, or just write a record containing the context information headers.";

    private final Optional<Integer> consumerSessionTimeoutMs;

    private final Optional<Integer> consumerHeartbeatIntervalMs;

    private final Optional<Integer> consumerAssignmentIntervalMs;

    private final Optional<Boolean> consumerAssignorOffloadEnable;

    private final Optional<Integer> shareSessionTimeoutMs;

    private final Optional<Integer> shareHeartbeatIntervalMs;

    private final Optional<Integer> shareRecordLockDurationMs;

    private final Optional<Integer> shareDeliveryCountLimit;

    private final Optional<Integer> sharePartitionMaxRecordLocks;

    private final Optional<ShareGroupAutoOffsetResetStrategy> shareAutoOffsetReset;

    private final Optional<Integer> shareAssignmentIntervalMs;

    private final Optional<Boolean> shareAssignorOffloadEnable;

    private final Optional<Integer> streamsSessionTimeoutMs;

    private final Optional<Integer> streamsHeartbeatIntervalMs;

    private final Optional<Integer> streamsNumStandbyReplicas;

    private final Optional<Integer> streamsInitialRebalanceDelayMs;

    private final Optional<Integer> streamsAssignmentIntervalMs;

    private final Optional<Boolean> streamsAssignorOffloadEnable;

    private final Optional<Integer> streamsTaskOffsetIntervalMs;

    private final Optional<Integer> streamsNumWarmupReplicas;

    private final Optional<Long> streamsAcceptableRecoveryLag;

    private final Optional<IsolationLevel> shareIsolationLevel;

    private final Optional<Boolean> shareRenewAcknowledgeEnable;

    public final String errorsDLQTopicName;

    public final boolean errorsDLQCopyRecordEnable;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
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
        .define(CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_DEFAULT,
            atLeast(0),
            MEDIUM,
            GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_DOC)
        .define(CONSUMER_ASSIGNOR_OFFLOAD_ENABLE_CONFIG,
            BOOLEAN,
            GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNOR_OFFLOAD_ENABLE_DEFAULT,
            MEDIUM,
            GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNOR_OFFLOAD_ENABLE_DOC)
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
        .define(SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_DEFAULT,
            atLeast(0),
            MEDIUM,
            GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_DOC)
        .define(SHARE_ASSIGNOR_OFFLOAD_ENABLE_CONFIG,
            BOOLEAN,
            GroupCoordinatorConfig.SHARE_GROUP_ASSIGNOR_OFFLOAD_ENABLE_DEFAULT,
            MEDIUM,
            GroupCoordinatorConfig.SHARE_GROUP_ASSIGNOR_OFFLOAD_ENABLE_DOC)
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
            GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DOC)
        .define(STREAMS_ASSIGNMENT_INTERVAL_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNMENT_INTERVAL_MS_DEFAULT,
            atLeast(0),
            MEDIUM,
            GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNMENT_INTERVAL_MS_DOC)
        .define(STREAMS_ASSIGNOR_OFFLOAD_ENABLE_CONFIG,
            BOOLEAN,
            GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNOR_OFFLOAD_ENABLE_DEFAULT,
            MEDIUM,
            GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNOR_OFFLOAD_ENABLE_DOC)
        .define(STREAMS_TASK_OFFSET_INTERVAL_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.STREAMS_GROUP_TASK_OFFSET_INTERVAL_MS_DEFAULT,
            atLeast(1),
            MEDIUM,
            GroupCoordinatorConfig.STREAMS_GROUP_TASK_OFFSET_INTERVAL_MS_DOC)
        .define(STREAMS_NUM_WARMUP_REPLICAS_CONFIG,
            INT,
            GroupCoordinatorConfig.STREAMS_GROUP_NUM_WARMUP_REPLICAS_DEFAULT,
            atLeast(0),
            MEDIUM,
            GroupCoordinatorConfig.STREAMS_GROUP_NUM_WARMUP_REPLICAS_DOC)
        .define(STREAMS_ACCEPTABLE_RECOVERY_LAG_CONFIG,
            LONG,
            GroupCoordinatorConfig.STREAMS_GROUP_ACCEPTABLE_RECOVERY_LAG_DEFAULT,
            atLeast(0),
            MEDIUM,
            GroupCoordinatorConfig.STREAMS_GROUP_ACCEPTABLE_RECOVERY_LAG_DOC)

        // DLQ configurations (KIP-1191)
        .define(ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG,
            STRING,
            ERRORS_DEADLETTERQUEUE_TOPIC_NAME_DEFAULT,
            MEDIUM,
            ERRORS_DEADLETTERQUEUE_TOPIC_NAME_DOC)
        .define(ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG,
            BOOLEAN,
            ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_DEFAULT,
            MEDIUM,
            ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_DOC);

    /**
     * Mapping from GroupConfig name to its broker-level synonym config name.
     * {@code Optional.empty()} indicates that the config has no broker-level synonym.
     */
    public static final Map<String, Optional<String>> ALL_GROUP_CONFIG_SYNONYMS = Map.ofEntries(
        // Consumer group configs.
        Map.entry(CONSUMER_SESSION_TIMEOUT_MS_CONFIG, Optional.of(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG)),
        Map.entry(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, Optional.of(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG)),
        Map.entry(CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG, Optional.of(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG)),
        Map.entry(CONSUMER_ASSIGNOR_OFFLOAD_ENABLE_CONFIG, Optional.of(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNOR_OFFLOAD_ENABLE_CONFIG)),

        // Share group configs.
        Map.entry(SHARE_SESSION_TIMEOUT_MS_CONFIG, Optional.of(GroupCoordinatorConfig.SHARE_GROUP_SESSION_TIMEOUT_MS_CONFIG)),
        Map.entry(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, Optional.of(GroupCoordinatorConfig.SHARE_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG)),
        Map.entry(SHARE_RECORD_LOCK_DURATION_MS_CONFIG, Optional.of(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG)),
        Map.entry(SHARE_DELIVERY_COUNT_LIMIT_CONFIG, Optional.of(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG)),
        Map.entry(SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG, Optional.of(ShareGroupConfig.SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG)),
        Map.entry(SHARE_AUTO_OFFSET_RESET_CONFIG, Optional.empty()),
        Map.entry(SHARE_ISOLATION_LEVEL_CONFIG, Optional.empty()),
        Map.entry(SHARE_RENEW_ACKNOWLEDGE_ENABLE_CONFIG, Optional.empty()),
        Map.entry(SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG, Optional.of(GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG)),
        Map.entry(SHARE_ASSIGNOR_OFFLOAD_ENABLE_CONFIG, Optional.of(GroupCoordinatorConfig.SHARE_GROUP_ASSIGNOR_OFFLOAD_ENABLE_CONFIG)),

        // Streams group configs.
        Map.entry(STREAMS_SESSION_TIMEOUT_MS_CONFIG, Optional.of(GroupCoordinatorConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_CONFIG)),
        Map.entry(STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, Optional.of(GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG)),
        Map.entry(STREAMS_NUM_STANDBY_REPLICAS_CONFIG, Optional.of(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG)),
        Map.entry(STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG, Optional.of(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG)),
        Map.entry(STREAMS_ASSIGNMENT_INTERVAL_MS_CONFIG, Optional.of(GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG)),
        Map.entry(STREAMS_ASSIGNOR_OFFLOAD_ENABLE_CONFIG, Optional.of(GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNOR_OFFLOAD_ENABLE_CONFIG)),
        Map.entry(STREAMS_TASK_OFFSET_INTERVAL_MS_CONFIG, Optional.of(GroupCoordinatorConfig.STREAMS_GROUP_TASK_OFFSET_INTERVAL_MS_CONFIG)),
        Map.entry(STREAMS_NUM_WARMUP_REPLICAS_CONFIG, Optional.of(GroupCoordinatorConfig.STREAMS_GROUP_NUM_WARMUP_REPLICAS_CONFIG)),
        Map.entry(STREAMS_ACCEPTABLE_RECOVERY_LAG_CONFIG, Optional.of(GroupCoordinatorConfig.STREAMS_GROUP_ACCEPTABLE_RECOVERY_LAG_CONFIG)),

        // DLQ configs
        Map.entry(ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, Optional.empty()),
        Map.entry(ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG, Optional.empty())
    );

    /**
     * Returns {@code true} if the given config name is defined as internal in {@link #CONFIG_DEF}.
     * Returns {@code false} for unknown names or non-internal configs.
     */
    public static boolean isInternal(String configName) {
        ConfigDef.ConfigKey configKey = CONFIG_DEF.configKeys().get(configName);
        return configKey != null && configKey.internalConfig;
    }

    /**
     * Returns the broker-level synonym config name for the given group config name,
     * or {@code Optional.empty()} if no broker-level synonym exists.
     *
     * @param groupConfigName The group-level config name.
     * @return The broker-level config name, or {@code Optional.empty()} if no broker-level config exists.
     */
    public static Optional<String> brokerSynonym(String groupConfigName) {
        return ALL_GROUP_CONFIG_SYNONYMS.getOrDefault(groupConfigName, Optional.empty());
    }

    public GroupConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props, false);
        this.consumerSessionTimeoutMs = optionalInt(CONSUMER_SESSION_TIMEOUT_MS_CONFIG);
        this.consumerHeartbeatIntervalMs = optionalInt(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG);
        this.consumerAssignmentIntervalMs = optionalInt(CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG);
        this.consumerAssignorOffloadEnable = optionalBoolean(CONSUMER_ASSIGNOR_OFFLOAD_ENABLE_CONFIG);
        this.shareSessionTimeoutMs = optionalInt(SHARE_SESSION_TIMEOUT_MS_CONFIG);
        this.shareHeartbeatIntervalMs = optionalInt(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG);
        this.shareRecordLockDurationMs = optionalInt(SHARE_RECORD_LOCK_DURATION_MS_CONFIG);
        this.shareDeliveryCountLimit = optionalInt(SHARE_DELIVERY_COUNT_LIMIT_CONFIG);
        this.sharePartitionMaxRecordLocks = optionalInt(SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG);
        this.shareAutoOffsetReset = optionalString(SHARE_AUTO_OFFSET_RESET_CONFIG)
            .map(ShareGroupAutoOffsetResetStrategy::fromString);
        this.shareAssignmentIntervalMs = optionalInt(SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG);
        this.shareAssignorOffloadEnable = optionalBoolean(SHARE_ASSIGNOR_OFFLOAD_ENABLE_CONFIG);
        this.streamsSessionTimeoutMs = optionalInt(STREAMS_SESSION_TIMEOUT_MS_CONFIG);
        this.streamsHeartbeatIntervalMs = optionalInt(STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG);
        this.streamsNumStandbyReplicas = optionalInt(STREAMS_NUM_STANDBY_REPLICAS_CONFIG);
        this.streamsInitialRebalanceDelayMs = optionalInt(STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG);
        this.streamsAssignmentIntervalMs = optionalInt(STREAMS_ASSIGNMENT_INTERVAL_MS_CONFIG);
        this.streamsAssignorOffloadEnable = optionalBoolean(STREAMS_ASSIGNOR_OFFLOAD_ENABLE_CONFIG);
        this.streamsTaskOffsetIntervalMs = optionalInt(STREAMS_TASK_OFFSET_INTERVAL_MS_CONFIG);
        this.streamsNumWarmupReplicas = optionalInt(STREAMS_NUM_WARMUP_REPLICAS_CONFIG);
        this.streamsAcceptableRecoveryLag = optionalLong(STREAMS_ACCEPTABLE_RECOVERY_LAG_CONFIG);
        this.shareIsolationLevel = optionalString(SHARE_ISOLATION_LEVEL_CONFIG)
            .map(s -> IsolationLevel.valueOf(s.toUpperCase(Locale.ROOT)));
        this.shareRenewAcknowledgeEnable = optionalBoolean(SHARE_RENEW_ACKNOWLEDGE_ENABLE_CONFIG);
        this.errorsDLQTopicName = getString(ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG);
        this.errorsDLQCopyRecordEnable = getBoolean(ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG);
    }

    private Optional<Integer> optionalInt(String key) {
        return originals().containsKey(key) ? Optional.of(getInt(key)) : Optional.empty();
    }

    private Optional<Long> optionalLong(String key) {
        return originals().containsKey(key) ? Optional.of(getLong(key)) : Optional.empty();
    }

    private Optional<Boolean> optionalBoolean(String key) {
        return originals().containsKey(key) ? Optional.of(getBoolean(key)) : Optional.empty();
    }

    private Optional<String> optionalString(String key) {
        return originals().containsKey(key) ? Optional.of(getString(key)) : Optional.empty();
    }

    public static Optional<Type> configType(String configName) {
        return Optional.ofNullable(CONFIG_DEF.configKeys().get(configName)).map(c -> c.type);
    }

    public static Set<String> configNames() {
        return CONFIG_DEF.names();
    }

    /**
     * Check that property names are valid.
     *
     * @param newGroupConfig         The new group config overrides.
     */
    public static void validateNames(Map<String, ?> newGroupConfig) {
        Set<String> names = configNames();
        for (String name : newGroupConfig.keySet()) {
            if (!names.contains(name)) {
                throw new InvalidConfigurationException("Unknown group config name: " + name);
            }
        }
    }

    /**
     * Check that the given properties contain only valid group config names and that
     * all values can be parsed and are valid.
     *
     * @param newGroupConfig         The new unparsed group config overrides.
     * @param groupCoordinatorConfig The group coordinator config.
     * @param shareGroupConfig       The share group config.
     */
    public static void validate(
        Map<String, ?> newGroupConfig,
        GroupCoordinatorConfig groupCoordinatorConfig,
        ShareGroupConfig shareGroupConfig
    ) {
        validateNames(newGroupConfig);
        var parsed = CONFIG_DEF.parse(newGroupConfig);
        parsed.keySet().retainAll(newGroupConfig.keySet());
        validateValues(
            parsed,
            groupCoordinatorConfig,
            shareGroupConfig
        );
    }

    /**
     * Validates the parsed values against broker-level bounds.
     * Only configs explicitly present in the parsed map are validated.
     *
     * @param parsed                 The parsed group config overrides.
     * @param groupCoordinatorConfig The group coordinator config.
     * @param shareGroupConfig       The share group config.
     */
    private static void validateValues(
        Map<String, Object> parsed,
        GroupCoordinatorConfig groupCoordinatorConfig,
        ShareGroupConfig shareGroupConfig
    ) {
        // Consumer group configs.
        validateIntRange(
            parsed,
            CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.consumerGroupMinHeartbeatIntervalMs(),
            groupCoordinatorConfig.consumerGroupMaxHeartbeatIntervalMs()
        );
        validateIntRange(
            parsed,
            CONSUMER_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.consumerGroupMinSessionTimeoutMs(),
            groupCoordinatorConfig.consumerGroupMaxSessionTimeoutMs()
        );
        validateIntRange(
            parsed,
            CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.consumerGroupMinAssignmentIntervalMs(),
            groupCoordinatorConfig.consumerGroupMaxAssignmentIntervalMs()
        );

        // Share group configs.
        validateIntRange(
            parsed,
            SHARE_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.shareGroupMinHeartbeatIntervalMs(),
            groupCoordinatorConfig.shareGroupMaxHeartbeatIntervalMs()
        );
        validateIntRange(
            parsed,
            SHARE_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.shareGroupMinSessionTimeoutMs(),
            groupCoordinatorConfig.shareGroupMaxSessionTimeoutMs()
        );
        validateIntRange(
            parsed,
            SHARE_RECORD_LOCK_DURATION_MS_CONFIG,
            shareGroupConfig.shareGroupMinRecordLockDurationMs(),
            shareGroupConfig.shareGroupMaxRecordLockDurationMs()
        );
        validateIntRange(
            parsed,
            SHARE_DELIVERY_COUNT_LIMIT_CONFIG,
            shareGroupConfig.shareGroupMinDeliveryCountLimit(),
            shareGroupConfig.shareGroupMaxDeliveryCountLimit()
        );
        validateIntRange(
            parsed,
            SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG,
            shareGroupConfig.shareGroupMinPartitionMaxRecordLocks(),
            shareGroupConfig.shareGroupMaxPartitionMaxRecordLocks()
        );
        validateIntRange(
            parsed,
            SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.shareGroupMinAssignmentIntervalMs(),
            groupCoordinatorConfig.shareGroupMaxAssignmentIntervalMs()
        );

        // Streams group configs.
        validateIntRange(
            parsed,
            STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.streamsGroupMinHeartbeatIntervalMs(),
            groupCoordinatorConfig.streamsGroupMaxHeartbeatIntervalMs()
        );
        validateIntRange(
            parsed,
            STREAMS_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.streamsGroupMinSessionTimeoutMs(),
            groupCoordinatorConfig.streamsGroupMaxSessionTimeoutMs()
        );
        validateIntMax(
            parsed,
            STREAMS_NUM_STANDBY_REPLICAS_CONFIG,
            groupCoordinatorConfig.streamsGroupMaxNumStandbyReplicas()
        );
        validateIntRange(
            parsed,
            STREAMS_ASSIGNMENT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.streamsGroupMinAssignmentIntervalMs(),
            groupCoordinatorConfig.streamsGroupMaxAssignmentIntervalMs()
        );
        validateIntMin(
            parsed,
            STREAMS_TASK_OFFSET_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.streamsGroupMinTaskOffsetIntervalMs()
        );
        validateIntMax(
            parsed,
            STREAMS_NUM_WARMUP_REPLICAS_CONFIG,
            groupCoordinatorConfig.streamsGroupMaxWarmupReplicas()
        );

        // Cross-field validations: session timeout must be greater than heartbeat interval.
        validateSessionExceedsHeartbeat(
            parsed,
            CONSUMER_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.consumerGroupSessionTimeoutMs(),
            CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.consumerGroupHeartbeatIntervalMs()
        );
        validateSessionExceedsHeartbeat(
            parsed,
            SHARE_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.shareGroupSessionTimeoutMs(),
            SHARE_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.shareGroupHeartbeatIntervalMs()
        );
        validateSessionExceedsHeartbeat(
            parsed,
            STREAMS_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.streamsGroupSessionTimeoutMs(),
            STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.streamsGroupHeartbeatIntervalMs()
        );

        // DLQ validation (KIP-1191)
        // DLQ topic name must not start with "__" (reserved for internal topics)
        String dlqTopicName = (String) parsed.get(ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG);
        if (dlqTopicName != null && !dlqTopicName.isEmpty() && dlqTopicName.startsWith("__")) {
            throw new InvalidConfigurationException(ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG +
                ": DLQ topic name must not start with '__'");
        }
    }

    /**
     * Validates that an integer config value falls within [min, max].
     * No-op when the key is absent from the parsed map.
     *
     * @param parsed                 The parsed group config overrides.
     * @param key                    The config key.
     * @param min                    The minimum allowed value (inclusive).
     * @param max                    The maximum allowed value (inclusive).
     */
    private static void validateIntRange(
        Map<String, Object> parsed,
        String key,
        int min,
        int max
    ) {
        if (!parsed.containsKey(key)) return;
        int value = (Integer) parsed.get(key);
        if (value < min || value > max)
            throw new InvalidConfigurationException(key + " must be in the range " + min + " to " + max + " inclusive.");
    }

    /**
     * Validates that an integer config value does not exceed max.
     * No-op when the key is absent from the parsed map.
     *
     * @param parsed                 The parsed group config overrides.
     * @param key                    The config key.
     * @param max                    The maximum allowed value (inclusive).
     */
    private static void validateIntMax(
        Map<String, Object> parsed,
        String key,
        int max
    ) {
        if (!parsed.containsKey(key)) return;
        int value = (Integer) parsed.get(key);
        if (value > max)
            throw new InvalidConfigurationException(key + " must be less than or equal to " + max);
    }

    /**
     * Validates that an integer config value is at least min.
     * No-op when the key is absent from the parsed map.
     *
     * @param parsed                 The parsed group config overrides.
     * @param key                    The config key.
     * @param min                    The minimum allowed value (inclusive).
     */
    private static void validateIntMin(
        Map<String, Object> parsed,
        String key,
        int min
    ) {
        if (!parsed.containsKey(key)) return;
        int value = (Integer) parsed.get(key);
        if (value < min)
            throw new InvalidConfigurationException(key + " must be greater than or equal to " + min);
    }

    /**
     * Validates that the session timeout is greater than the heartbeat interval.
     * Uses broker defaults for any config not present in the parsed map.
     *
     * @param parsed                 The parsed group config overrides.
     * @param sessionKey             The session timeout config key.
     * @param defaultSession         The default session timeout value when there is no override.
     * @param heartbeatKey           The heartbeat interval config key.
     * @param defaultHeartbeat       The default heartbeat interval value when there is no override.
     */
    private static void validateSessionExceedsHeartbeat(
        Map<String, Object> parsed,
        String sessionKey,
        int defaultSession,
        String heartbeatKey,
        int defaultHeartbeat
    ) {
        if (parsed.containsKey(sessionKey) || parsed.containsKey(heartbeatKey)) {
            int effectiveSession = parsed.containsKey(sessionKey)
                ? (Integer) parsed.get(sessionKey) : defaultSession;
            int effectiveHeartbeat = parsed.containsKey(heartbeatKey)
                ? (Integer) parsed.get(heartbeatKey) : defaultHeartbeat;
            if (effectiveSession <= effectiveHeartbeat)
                throw new InvalidConfigurationException(sessionKey + " must be greater than " + heartbeatKey);
        }
    }

    /**
     * Evaluate group config values to their effective values within broker-level bounds.
     * Out-of-range values are capped and a WARN log is emitted.
     *
     * @param newGroupConfig         The new unparsed group config overrides.
     * @param groupId                The group id.
     * @param groupCoordinatorConfig The group coordinator config.
     * @param shareGroupConfig       The share group config.
     * @return A new {@link Properties} with out-of-range values capped.
     */
    public static Properties evaluate(
        Properties newGroupConfig,
        String groupId,
        GroupCoordinatorConfig groupCoordinatorConfig,
        ShareGroupConfig shareGroupConfig
    ) {
        Properties evaluatedGroupConfig = new Properties();
        evaluatedGroupConfig.putAll(newGroupConfig);
        evaluateValues(
            evaluatedGroupConfig,
            groupId,
            groupCoordinatorConfig,
            shareGroupConfig
        );
        return evaluatedGroupConfig;
    }

    /**
     * Evaluate group config values to their effective values within broker-level bounds.
     * Out-of-range values are capped and a WARN log is emitted.
     *
     * @param evaluatedGroupConfig   The unparsed group config overrides to modify in place.
     * @param groupId                The group id.
     * @param groupCoordinatorConfig The group coordinator config.
     * @param shareGroupConfig       The share group config.
     */
    private static void evaluateValues(
        Properties evaluatedGroupConfig,
        String groupId,
        GroupCoordinatorConfig groupCoordinatorConfig,
        ShareGroupConfig shareGroupConfig
    ) {
        // Consumer group configs.
        clampToRange(
            evaluatedGroupConfig,
            groupId,
            CONSUMER_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.consumerGroupMinSessionTimeoutMs(),
            groupCoordinatorConfig.consumerGroupMaxSessionTimeoutMs()
        );
        clampToRange(
            evaluatedGroupConfig,
            groupId,
            CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.consumerGroupMinHeartbeatIntervalMs(),
            groupCoordinatorConfig.consumerGroupMaxHeartbeatIntervalMs()
        );
        clampToRange(
            evaluatedGroupConfig,
            groupId,
            CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.consumerGroupMinAssignmentIntervalMs(),
            groupCoordinatorConfig.consumerGroupMaxAssignmentIntervalMs()
        );

        // Share group configs.
        clampToRange(
            evaluatedGroupConfig,
            groupId,
            SHARE_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.shareGroupMinSessionTimeoutMs(),
            groupCoordinatorConfig.shareGroupMaxSessionTimeoutMs()
        );
        clampToRange(
            evaluatedGroupConfig,
            groupId,
            SHARE_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.shareGroupMinHeartbeatIntervalMs(),
            groupCoordinatorConfig.shareGroupMaxHeartbeatIntervalMs()
        );
        clampToRange(
            evaluatedGroupConfig,
            groupId,
            SHARE_RECORD_LOCK_DURATION_MS_CONFIG,
            shareGroupConfig.shareGroupMinRecordLockDurationMs(),
            shareGroupConfig.shareGroupMaxRecordLockDurationMs()
        );
        clampToRange(
            evaluatedGroupConfig,
            groupId,
            SHARE_DELIVERY_COUNT_LIMIT_CONFIG,
            shareGroupConfig.shareGroupMinDeliveryCountLimit(),
            shareGroupConfig.shareGroupMaxDeliveryCountLimit()
        );
        clampToRange(
            evaluatedGroupConfig,
            groupId,
            SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG,
            shareGroupConfig.shareGroupMinPartitionMaxRecordLocks(),
            shareGroupConfig.shareGroupMaxPartitionMaxRecordLocks()
        );
        clampToRange(
            evaluatedGroupConfig,
            groupId,
            SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.shareGroupMinAssignmentIntervalMs(),
            groupCoordinatorConfig.shareGroupMaxAssignmentIntervalMs()
        );

        // Streams group configs.
        clampToRange(
            evaluatedGroupConfig,
            groupId,
            STREAMS_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.streamsGroupMinSessionTimeoutMs(),
            groupCoordinatorConfig.streamsGroupMaxSessionTimeoutMs()
        );
        clampToRange(
            evaluatedGroupConfig,
            groupId,
            STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.streamsGroupMinHeartbeatIntervalMs(),
            groupCoordinatorConfig.streamsGroupMaxHeartbeatIntervalMs()
        );
        clampToMax(
            evaluatedGroupConfig,
            groupId,
            STREAMS_NUM_STANDBY_REPLICAS_CONFIG,
            groupCoordinatorConfig.streamsGroupMaxNumStandbyReplicas()
        );
        clampToRange(
            evaluatedGroupConfig,
            groupId,
            STREAMS_ASSIGNMENT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.streamsGroupMinAssignmentIntervalMs(),
            groupCoordinatorConfig.streamsGroupMaxAssignmentIntervalMs()
        );
        clampToMin(
            evaluatedGroupConfig,
            groupId,
            STREAMS_TASK_OFFSET_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.streamsGroupMinTaskOffsetIntervalMs()
        );
        clampToMax(
            evaluatedGroupConfig,
            groupId,
            STREAMS_NUM_WARMUP_REPLICAS_CONFIG,
            groupCoordinatorConfig.streamsGroupMaxWarmupReplicas()
        );

        // Verify that clamping did not break the session > heartbeat invariant.
        checkSessionExceedsHeartbeat(
            evaluatedGroupConfig,
            groupId,
            CONSUMER_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.consumerGroupSessionTimeoutMs(),
            CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.consumerGroupHeartbeatIntervalMs()
        );
        checkSessionExceedsHeartbeat(
            evaluatedGroupConfig,
            groupId,
            SHARE_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.shareGroupSessionTimeoutMs(),
            SHARE_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.shareGroupHeartbeatIntervalMs()
        );
        checkSessionExceedsHeartbeat(
            evaluatedGroupConfig,
            groupId,
            STREAMS_SESSION_TIMEOUT_MS_CONFIG,
            groupCoordinatorConfig.streamsGroupSessionTimeoutMs(),
            STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG,
            groupCoordinatorConfig.streamsGroupHeartbeatIntervalMs()
        );
    }

    /**
     * Log a WARN if the session timeout is not greater than the heartbeat interval after
     * evaluation. When a key is absent from newGroupConfig, the broker-level default is used.
     *
     * @param newGroupConfig         The new unparsed group config overrides.
     * @param groupId                The group id.
     * @param sessionKey             The session timeout config key.
     * @param defaultSession         The default session timeout value when there is no override.
     * @param heartbeatKey           The heartbeat interval config key.
     * @param defaultHeartbeat       The default heartbeat interval value when there is no override.
     */
    private static void checkSessionExceedsHeartbeat(
        Properties newGroupConfig,
        String groupId,
        String sessionKey,
        int defaultSession,
        String heartbeatKey,
        int defaultHeartbeat
    ) {
        Object rawSession = newGroupConfig.get(sessionKey);
        Object rawHeartbeat = newGroupConfig.get(heartbeatKey);
        if (rawSession == null && rawHeartbeat == null) return;

        int session = rawSession != null ? Integer.parseInt(rawSession.toString()) : defaultSession;
        int heartbeat = rawHeartbeat != null ? Integer.parseInt(rawHeartbeat.toString()) : defaultHeartbeat;
        if (session <= heartbeat) {
            LOG.warn("The effective {} ({}) for group '{}' is not greater than {} ({}). "
                    + "Check that the broker-level min/max bounds for session timeout "
                    + "and heartbeat interval do not overlap.",
                sessionKey, session, groupId, heartbeatKey, heartbeat);
        }
    }

    /**
     * Clamp a config value to [min, max]. A WARN log is emitted on adjustment.
     * No-op when the key is absent from evaluatedGroupConfig.
     *
     * @param evaluatedGroupConfig   The unparsed group config overrides to modify in place.
     * @param groupId                The group id.
     * @param key                    The config key.
     * @param min                    The minimum allowed value (inclusive).
     * @param max                    The maximum allowed value (inclusive).
     */
    private static void clampToRange(
        Properties evaluatedGroupConfig,
        String groupId,
        String key,
        int min,
        int max
    ) {
        Object rawValue = evaluatedGroupConfig.get(key);
        if (rawValue == null) return;

        int value = Integer.parseInt(rawValue.toString());
        if (value < min) {
            LOG.warn("The group config '{}' for group '{}' has value {} which is below the broker's " +
                    "allowed minimum {}. The effective value will be capped to {}.",
                key, groupId, value, min, min);
            evaluatedGroupConfig.put(key, min);
        } else if (value > max) {
            LOG.warn("The group config '{}' for group '{}' has value {} which exceeds the broker's " +
                    "allowed maximum {}. The effective value will be capped to {}.",
                key, groupId, value, max, max);
            evaluatedGroupConfig.put(key, max);
        }
    }

    /**
     * Clamp a config value to at most max. A WARN log is emitted on adjustment.
     * No-op when the key is absent from evaluatedGroupConfig.
     *
     * @param evaluatedGroupConfig   The unparsed group config overrides to modify in place.
     * @param groupId                The group id.
     * @param key                    The config key.
     * @param max                    The maximum allowed value (inclusive).
     */
    private static void clampToMax(
        Properties evaluatedGroupConfig,
        String groupId,
        String key,
        int max
    ) {
        Object rawValue = evaluatedGroupConfig.get(key);
        if (rawValue == null) return;

        int value = Integer.parseInt(rawValue.toString());
        if (value > max) {
            LOG.warn("The group config '{}' for group '{}' has value {} which exceeds the broker's " +
                    "allowed maximum {}. The effective value will be capped to {}.",
                key, groupId, value, max, max);
            evaluatedGroupConfig.put(key, max);
        }
    }

    /**
     * Clamp a config value to at least min. A WARN log is emitted on adjustment.
     * No-op when the key is absent from evaluatedGroupConfig.
     *
     * @param evaluatedGroupConfig   The unparsed group config overrides to modify in place.
     * @param groupId                The group id.
     * @param key                    The config key.
     * @param min                    The minimum allowed value (inclusive).
     */
    private static void clampToMin(
        Properties evaluatedGroupConfig,
        String groupId,
        String key,
        int min
    ) {
        Object rawValue = evaluatedGroupConfig.get(key);
        if (rawValue == null) return;

        int value = Integer.parseInt(rawValue.toString());
        if (value < min) {
            LOG.warn("The group config '{}' for group '{}' has value {} which is below the broker's " +
                    "allowed minimum {}. The effective value will be capped to {}.",
                key, groupId, value, min, min);
            evaluatedGroupConfig.put(key, min);
        }
    }

    /**
     * Create a group config instance using the given properties and defaults.
     *
     * @param defaults  The full default group config values.
     * @param overrides The group config overrides.
     */
    public static GroupConfig fromProps(
        Map<?, ?> defaults,
        Properties overrides
    ) {
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
    public Optional<Integer> consumerSessionTimeoutMs() {
        return consumerSessionTimeoutMs;
    }

    /**
     * The consumer group heartbeat interval in milliseconds.
     */
    public Optional<Integer> consumerHeartbeatIntervalMs() {
        return consumerHeartbeatIntervalMs;
    }

    /**
     * The interval between assignment updates for a consumer group.
     */
    public Optional<Integer> consumerAssignmentIntervalMs() {
        return consumerAssignmentIntervalMs;
    }

    /**
     * Whether to offload consumer group assignment to a group coordinator background thread.
     */
    public Optional<Boolean> consumerAssignorOffloadEnable() {
        return consumerAssignorOffloadEnable;
    }

    /**
     * The share group session timeout in milliseconds.
     */
    public Optional<Integer> shareSessionTimeoutMs() {
        return shareSessionTimeoutMs;
    }

    /**
     * The share group heartbeat interval in milliseconds.
     */
    public Optional<Integer> shareHeartbeatIntervalMs() {
        return shareHeartbeatIntervalMs;
    }

    /**
     * The share group delivery count limit.
     */
    public Optional<Integer> shareDeliveryCountLimit() {
        return shareDeliveryCountLimit;
    }

    /**
     * The share group partition max record locks.
     */
    public Optional<Integer> sharePartitionMaxRecordLocks() {
        return sharePartitionMaxRecordLocks;
    }

    /**
     * The share group record lock duration milliseconds.
     */
    public Optional<Integer> shareRecordLockDurationMs() {
        return shareRecordLockDurationMs;
    }

    /**
     * The share group auto offset reset strategy.
     */
    public Optional<ShareGroupAutoOffsetResetStrategy> shareAutoOffsetReset() {
        return shareAutoOffsetReset;
    }

    /**
     * The interval between assignment updates for a share group.
     */
    public Optional<Integer> shareAssignmentIntervalMs() {
        return shareAssignmentIntervalMs;
    }

    /**
     * Whether to offload share group assignment to a group coordinator background thread.
     */
    public Optional<Boolean> shareAssignorOffloadEnable() {
        return shareAssignorOffloadEnable;
    }

    /**
     * The streams group session timeout in milliseconds.
     */
    public Optional<Integer> streamsSessionTimeoutMs() {
        return streamsSessionTimeoutMs;
    }

    /**
     * The streams group heartbeat interval in milliseconds.
     */
    public Optional<Integer> streamsHeartbeatIntervalMs() {
        return streamsHeartbeatIntervalMs;
    }

    /**
     * The number of streams standby replicas for each task.
     */
    public Optional<Integer> streamsNumStandbyReplicas() {
        return streamsNumStandbyReplicas;
    }

    /**
     * The initial rebalance delay for streams groups.
     */
    public Optional<Integer> streamsInitialRebalanceDelayMs() {
        return streamsInitialRebalanceDelayMs;
    }

    /**
     * The interval between assignment updates for a streams group.
     */
    public Optional<Integer> streamsAssignmentIntervalMs() {
        return streamsAssignmentIntervalMs;
    }

    /**
     * Whether to offload streams group assignment to a group coordinator background thread.
     */
    public Optional<Boolean> streamsAssignorOffloadEnable() {
        return streamsAssignorOffloadEnable;
    }

    /**
     * The task offset reporting interval.
     */
    public Optional<Integer> streamsTaskOffsetIntervalMs() {
        return streamsTaskOffsetIntervalMs;
    }

    /**
     * The number of warmup replicas for each task.
     */
    public Optional<Integer> streamsNumWarmupReplicas() {
        return streamsNumWarmupReplicas;
    }

    /**
     * The acceptable recovery lag for streams groups.
     */
    public Optional<Long> streamsAcceptableRecoveryLag() {
        return streamsAcceptableRecoveryLag;
    }

    /**
     * The share group isolation level.
     */
    public Optional<IsolationLevel> shareIsolationLevel() {
        return shareIsolationLevel;
    }

    /**
     * The share group renew acknowledge enable.
     */
    public Optional<Boolean> shareRenewAcknowledgeEnable() {
        return shareRenewAcknowledgeEnable;
    }

    /**
     * The DLQ topic name for this group.
     */
    public String errorsDLQTopicName() {
        return errorsDLQTopicName;
    }

    /**
     * Whether to copy the original record to the DLQ topic.
     */
    public boolean errorsDLQCopyRecordEnable() {
        return errorsDLQCopyRecordEnable;
    }

    public static void main(String[] args) {
        System.out.println(CONFIG_DEF.toHtml(4, config -> "groupconfigs_" + config));
    }
}
