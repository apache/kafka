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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.config.ConfigDef;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MirrorCheckpointConfig extends MirrorConnectorConfig {
    protected static final String REFRESH_GROUPS = "refresh.groups";
    protected static final String EMIT_CHECKPOINTS = "emit.checkpoints";
    protected static final String SYNC_GROUP_OFFSETS = "sync.group.offsets";

    public static final String GROUPS = DefaultGroupFilter.GROUPS_INCLUDE_CONFIG;
    public static final String GROUPS_DEFAULT = DefaultGroupFilter.GROUPS_INCLUDE_DEFAULT;
    private static final String GROUPS_DOC = "Consumer groups to replicate. Supports comma-separated group IDs and regexes.";
    public static final String GROUPS_EXCLUDE = DefaultGroupFilter.GROUPS_EXCLUDE_CONFIG;

    public static final String GROUPS_EXCLUDE_DEFAULT = DefaultGroupFilter.GROUPS_EXCLUDE_DEFAULT;
    private static final String GROUPS_EXCLUDE_DOC = "Exclude groups. Supports comma-separated group IDs and regexes."
            + " Excludes take precedence over includes.";

    public static final String CHECKPOINTS_TOPIC_REPLICATION_FACTOR = "checkpoints.topic.replication.factor";
    public static final String CHECKPOINTS_TOPIC_REPLICATION_FACTOR_DOC = "Replication factor for checkpoints topic.";
    public static final short CHECKPOINTS_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;

    protected static final String CHECKPOINTS_TASKS_MAXIMUM = "checkpoints.tasks.max";
    protected static final String CHECKPOINTS_TASKS_MAXIMUM_DOC = "Maximum number of checkpoint connector tasks.";
    private static final int CHECKPOINTS_TASKS_MAX_DEFAULT = 1;

    protected static final String TASK_CONSUMER_GROUPS = "task.assigned.groups";

    public static final String CONSUMER_POLL_TIMEOUT_MILLIS = "consumer.poll.timeout.ms";
    private static final String CONSUMER_POLL_TIMEOUT_MILLIS_DOC = "Timeout when polling source cluster.";
    public static final long CONSUMER_POLL_TIMEOUT_MILLIS_DEFAULT = 1000L;

    public static final String REFRESH_GROUPS_ENABLED = REFRESH_GROUPS + ENABLED_SUFFIX;
    private static final String REFRESH_GROUPS_ENABLED_DOC = "Whether to periodically check for new consumer groups.";
    public static final boolean REFRESH_GROUPS_ENABLED_DEFAULT = true;
    public static final String REFRESH_GROUPS_INTERVAL_SECONDS = REFRESH_GROUPS + INTERVAL_SECONDS_SUFFIX;
    private static final String REFRESH_GROUPS_INTERVAL_SECONDS_DOC = "Frequency of group refresh.";
    public static final long REFRESH_GROUPS_INTERVAL_SECONDS_DEFAULT = 10 * 60;

    public static final String EMIT_CHECKPOINTS_ENABLED = EMIT_CHECKPOINTS + ENABLED_SUFFIX;
    private static final String EMIT_CHECKPOINTS_ENABLED_DOC = "Whether to replicate consumer offsets to target cluster.";
    public static final boolean EMIT_CHECKPOINTS_ENABLED_DEFAULT = true;
    public static final String EMIT_CHECKPOINTS_INTERVAL_SECONDS = EMIT_CHECKPOINTS + INTERVAL_SECONDS_SUFFIX;
    private static final String EMIT_CHECKPOINTS_INTERVAL_SECONDS_DOC = "Frequency of checkpoints.";
    public static final long EMIT_CHECKPOINTS_INTERVAL_SECONDS_DEFAULT = 60;

    public static final String SYNC_GROUP_OFFSETS_ENABLED = SYNC_GROUP_OFFSETS + ENABLED_SUFFIX;
    private static final String SYNC_GROUP_OFFSETS_ENABLED_DOC = "Whether to periodically write the translated offsets to __consumer_offsets topic in target cluster, as long as no active consumers in that group are connected to the target cluster";
    public static final boolean SYNC_GROUP_OFFSETS_ENABLED_DEFAULT = false;
    public static final String SYNC_GROUP_OFFSETS_INTERVAL_SECONDS = SYNC_GROUP_OFFSETS + INTERVAL_SECONDS_SUFFIX;
    private static final String SYNC_GROUP_OFFSETS_INTERVAL_SECONDS_DOC = "Frequency of consumer group offset sync.";
    public static final long SYNC_GROUP_OFFSETS_INTERVAL_SECONDS_DEFAULT = 60;

    public static final String GROUP_FILTER_CLASS = "group.filter.class";
    private static final String GROUP_FILTER_CLASS_DOC = "GroupFilter to use. Selects consumer groups to replicate.";
    public static final Class<?> GROUP_FILTER_CLASS_DEFAULT = DefaultGroupFilter.class;
    public static final String OFFSET_SYNCS_SOURCE_CONSUMER_ROLE = OFFSET_SYNCS_CLIENT_ROLE_PREFIX + "source-consumer";
    public static final String OFFSET_SYNCS_TARGET_CONSUMER_ROLE = OFFSET_SYNCS_CLIENT_ROLE_PREFIX + "target-consumer";
    public static final String OFFSET_SYNCS_SOURCE_ADMIN_ROLE = OFFSET_SYNCS_CLIENT_ROLE_PREFIX + "source-admin";
    public static final String OFFSET_SYNCS_TARGET_ADMIN_ROLE = OFFSET_SYNCS_CLIENT_ROLE_PREFIX + "target-admin";
    public static final String CHECKPOINTS_TARGET_CONSUMER_ROLE = "checkpoints-target-consumer";

    public MirrorCheckpointConfig(Map<String, String> props) {
        super(CONNECTOR_CONFIG_DEF, props);
    }

    public MirrorCheckpointConfig(ConfigDef configDef, Map<String, String> props) {
        super(configDef, props);
    }

    Duration emitCheckpointsInterval() {
        if (getBoolean(EMIT_CHECKPOINTS_ENABLED)) {
            return Duration.ofSeconds(getLong(EMIT_CHECKPOINTS_INTERVAL_SECONDS));
        } else {
            // negative interval to disable
            return Duration.ofMillis(-1);
        }
    }

    Duration refreshGroupsInterval() {
        if (getBoolean(REFRESH_GROUPS_ENABLED)) {
            return Duration.ofSeconds(getLong(REFRESH_GROUPS_INTERVAL_SECONDS));
        } else {
            // negative interval to disable
            return Duration.ofMillis(-1);
        }
    }

    short checkpointsTopicReplicationFactor() {
        return getShort(CHECKPOINTS_TOPIC_REPLICATION_FACTOR);
    }

    GroupFilter groupFilter() {
        return getConfiguredInstance(GROUP_FILTER_CLASS, GroupFilter.class);
    }

    TopicFilter topicFilter() {
        return getConfiguredInstance(TOPIC_FILTER_CLASS, TopicFilter.class);
    }

    Duration syncGroupOffsetsInterval() {
        if (getBoolean(SYNC_GROUP_OFFSETS_ENABLED)) {
            return Duration.ofSeconds(getLong(SYNC_GROUP_OFFSETS_INTERVAL_SECONDS));
        } else {
            // negative interval to disable
            return Duration.ofMillis(-1);
        }
    }

    Integer getCheckpointConnectorTaskMax() {
        return getInt(CHECKPOINTS_TASKS_MAXIMUM);
    }

    Map<String, String> taskConfigForConsumerGroups(List<String> groups, int taskIndex) {
        Map<String, String> props = originalsStrings();
        props.put(TASK_CONSUMER_GROUPS, String.join(",", groups));
        props.put(TASK_INDEX, String.valueOf(taskIndex));
        return props;
    }

    String offsetSyncsTopic() {
        String otherClusterAlias = SOURCE_CLUSTER_ALIAS_DEFAULT.equals(offsetSyncsTopicLocation())
                ? targetClusterAlias()
                : sourceClusterAlias();
        return replicationPolicy().offsetSyncsTopic(otherClusterAlias);
    }

    String offsetSyncsTopicLocation() {
        return getString(OFFSET_SYNCS_TOPIC_LOCATION);
    }

    String checkpointsTopic() {
        return replicationPolicy().checkpointsTopic(sourceClusterAlias());
    }

    Map<String, Object> offsetSyncsTopicConsumerConfig() {
        return SOURCE_CLUSTER_ALIAS_DEFAULT.equals(offsetSyncsTopicLocation())
                ? sourceConsumerConfig(OFFSET_SYNCS_SOURCE_CONSUMER_ROLE)
                : targetConsumerConfig(OFFSET_SYNCS_TARGET_CONSUMER_ROLE);
    }

    Map<String, Object> offsetSyncsTopicAdminConfig() {
        return SOURCE_CLUSTER_ALIAS_DEFAULT.equals(offsetSyncsTopicLocation())
                ? sourceAdminConfig(OFFSET_SYNCS_SOURCE_ADMIN_ROLE)
                : targetAdminConfig(OFFSET_SYNCS_TARGET_ADMIN_ROLE);
    }

    Duration consumerPollTimeout() {
        return Duration.ofMillis(getLong(CONSUMER_POLL_TIMEOUT_MILLIS));
    }

    public static Map<String, String> validate(Map<String, String> configs) {
        Map<String, String> invalidConfigs = new HashMap<>();

        // No point to validate when connector is disabled.
        if ("false".equals(configs.getOrDefault(ENABLED, "true"))) {
            return invalidConfigs;
        }

        if ("false".equals(configs.get(EMIT_CHECKPOINTS_ENABLED))) {
            invalidConfigs.putIfAbsent(EMIT_CHECKPOINTS_ENABLED, "MirrorCheckpointConnector can't run with " +
                    EMIT_CHECKPOINTS_ENABLED + " set to false");
        }

        if ("false".equals(configs.get(EMIT_OFFSET_SYNCS_ENABLED))) {
            invalidConfigs.putIfAbsent(EMIT_OFFSET_SYNCS_ENABLED, "MirrorCheckpointConnector can't run without offset syncs");
        }

        return invalidConfigs;
    }

    private static ConfigDef defineCheckpointConfig(ConfigDef baseConfig) {
        return baseConfig
                .define(
                        CONSUMER_POLL_TIMEOUT_MILLIS,
                        ConfigDef.Type.LONG,
                        CONSUMER_POLL_TIMEOUT_MILLIS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        CONSUMER_POLL_TIMEOUT_MILLIS_DOC)
                .define(
                        GROUPS,
                        ConfigDef.Type.LIST,
                        GROUPS_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        GROUPS_DOC)
                .define(
                        GROUPS_EXCLUDE,
                        ConfigDef.Type.LIST,
                        GROUPS_EXCLUDE_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        GROUPS_EXCLUDE_DOC)
                .define(
                        GROUP_FILTER_CLASS,
                        ConfigDef.Type.CLASS,
                        GROUP_FILTER_CLASS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        GROUP_FILTER_CLASS_DOC)
                .define(
                        REFRESH_GROUPS_ENABLED,
                        ConfigDef.Type.BOOLEAN,
                        REFRESH_GROUPS_ENABLED_DEFAULT,
                        ConfigDef.Importance.LOW,
                        REFRESH_GROUPS_ENABLED_DOC)
                .define(
                        REFRESH_GROUPS_INTERVAL_SECONDS,
                        ConfigDef.Type.LONG,
                        REFRESH_GROUPS_INTERVAL_SECONDS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        REFRESH_GROUPS_INTERVAL_SECONDS_DOC)
                .define(
                        EMIT_CHECKPOINTS_ENABLED,
                        ConfigDef.Type.BOOLEAN,
                        EMIT_CHECKPOINTS_ENABLED_DEFAULT,
                        ConfigDef.Importance.LOW,
                        EMIT_CHECKPOINTS_ENABLED_DOC)
                .define(
                        EMIT_CHECKPOINTS_INTERVAL_SECONDS,
                        ConfigDef.Type.LONG,
                        EMIT_CHECKPOINTS_INTERVAL_SECONDS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        EMIT_CHECKPOINTS_INTERVAL_SECONDS_DOC)
                .define(
                        SYNC_GROUP_OFFSETS_ENABLED,
                        ConfigDef.Type.BOOLEAN,
                        SYNC_GROUP_OFFSETS_ENABLED_DEFAULT,
                        ConfigDef.Importance.LOW,
                        SYNC_GROUP_OFFSETS_ENABLED_DOC)
                .define(
                        SYNC_GROUP_OFFSETS_INTERVAL_SECONDS,
                        ConfigDef.Type.LONG,
                        SYNC_GROUP_OFFSETS_INTERVAL_SECONDS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        SYNC_GROUP_OFFSETS_INTERVAL_SECONDS_DOC)
                .define(
                        CHECKPOINTS_TOPIC_REPLICATION_FACTOR,
                        ConfigDef.Type.SHORT,
                        CHECKPOINTS_TOPIC_REPLICATION_FACTOR_DEFAULT,
                        ConfigDef.Importance.LOW,
                        CHECKPOINTS_TOPIC_REPLICATION_FACTOR_DOC)
                .define(
                        CHECKPOINTS_TASKS_MAXIMUM,
                        ConfigDef.Type.INT,
                        CHECKPOINTS_TASKS_MAX_DEFAULT,
                        ConfigDef.Importance.LOW,
                        CHECKPOINTS_TASKS_MAXIMUM_DOC)
                .define(
                        OFFSET_SYNCS_TOPIC_LOCATION,
                        ConfigDef.Type.STRING,
                        OFFSET_SYNCS_TOPIC_LOCATION_DEFAULT,
                        ConfigDef.ValidString.in(SOURCE_CLUSTER_ALIAS_DEFAULT, TARGET_CLUSTER_ALIAS_DEFAULT),
                        ConfigDef.Importance.LOW,
                        OFFSET_SYNCS_TOPIC_LOCATION_DOC)
                .define(
                        TOPIC_FILTER_CLASS,
                        ConfigDef.Type.CLASS,
                        TOPIC_FILTER_CLASS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        TOPIC_FILTER_CLASS_DOC);
    }

    protected static final ConfigDef CONNECTOR_CONFIG_DEF = defineCheckpointConfig(new ConfigDef(BASE_CONNECTOR_CONFIG_DEF));

    public static void main(String[] args) {
        System.out.println(defineCheckpointConfig(new ConfigDef()).toHtml(4, config -> "mirror_checkpoint_" + config));
    }
}
