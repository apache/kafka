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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

public class MirrorSourceConfig extends MirrorConnectorConfig {

    protected static final String REFRESH_TOPICS = "refresh.topics";
    protected static final String SYNC_TOPIC_CONFIGS = "sync.topic.configs";
    protected static final String SYNC_TOPIC_ACLS = "sync.topic.acls";

    public static final String REPLICATION_FACTOR = "replication.factor";
    private static final String REPLICATION_FACTOR_DOC = "Replication factor for newly created remote topics.";
    public static final int REPLICATION_FACTOR_DEFAULT = 2;
    public static final String TOPICS = DefaultTopicFilter.TOPICS_INCLUDE_CONFIG;
    public static final String TOPICS_DEFAULT = DefaultTopicFilter.TOPICS_INCLUDE_DEFAULT;
    private static final String TOPICS_DOC = "Topics to replicate. Supports comma-separated topic names and regexes.";
    public static final String TOPICS_EXCLUDE = DefaultTopicFilter.TOPICS_EXCLUDE_CONFIG;
    public static final String TOPICS_EXCLUDE_DEFAULT = DefaultTopicFilter.TOPICS_EXCLUDE_DEFAULT;
    private static final String TOPICS_EXCLUDE_DOC = "Excluded topics. Supports comma-separated topic names and regexes."
            + " Excludes take precedence over includes.";

    public static final String CONFIG_PROPERTIES_EXCLUDE = DefaultConfigPropertyFilter.CONFIG_PROPERTIES_EXCLUDE_CONFIG;
    public static final String CONFIG_PROPERTIES_EXCLUDE_DEFAULT = DefaultConfigPropertyFilter.CONFIG_PROPERTIES_EXCLUDE_DEFAULT;
    private static final String CONFIG_PROPERTIES_EXCLUDE_DOC = "Topic config properties that should not be replicated. Supports "
            + "comma-separated property names and regexes.";

    public static final String OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR = OFFSET_SYNCS_TOPIC_CONFIG_PREFIX + "replication.factor";
    public static final String OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR_DOC = "Replication factor for offset-syncs topic.";
    public static final short OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;

    static final String TASK_TOPIC_PARTITIONS = "task.assigned.partitions";

    public static final String CONSUMER_POLL_TIMEOUT_MILLIS = "consumer.poll.timeout.ms";
    private static final String CONSUMER_POLL_TIMEOUT_MILLIS_DOC = "Timeout when polling source cluster.";
    public static final long CONSUMER_POLL_TIMEOUT_MILLIS_DEFAULT = 1000L;

    public static final String REFRESH_TOPICS_ENABLED = REFRESH_TOPICS + ENABLED_SUFFIX;
    private static final String REFRESH_TOPICS_ENABLED_DOC = "Whether to periodically check for new topics and partitions.";
    public static final boolean REFRESH_TOPICS_ENABLED_DEFAULT = true;
    public static final String REFRESH_TOPICS_INTERVAL_SECONDS = REFRESH_TOPICS + INTERVAL_SECONDS_SUFFIX;
    private static final String REFRESH_TOPICS_INTERVAL_SECONDS_DOC = "Frequency of topic refresh.";
    public static final long REFRESH_TOPICS_INTERVAL_SECONDS_DEFAULT = 10 * 60;

    public static final String SYNC_TOPIC_CONFIGS_ENABLED = SYNC_TOPIC_CONFIGS + ENABLED_SUFFIX;
    private static final String SYNC_TOPIC_CONFIGS_ENABLED_DOC = "Whether to periodically configure remote topics to match their corresponding upstream topics.";
    public static final boolean SYNC_TOPIC_CONFIGS_ENABLED_DEFAULT = true;
    public static final String SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS = SYNC_TOPIC_CONFIGS + INTERVAL_SECONDS_SUFFIX;
    private static final String SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS_DOC = "Frequency of topic config sync.";
    public static final long SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS_DEFAULT = 10 * 60;

    public static final String SYNC_TOPIC_ACLS_ENABLED = SYNC_TOPIC_ACLS + ENABLED_SUFFIX;
    private static final String SYNC_TOPIC_ACLS_ENABLED_DOC = "Whether to periodically configure remote topic ACLs to match their corresponding upstream topics.";
    public static final boolean SYNC_TOPIC_ACLS_ENABLED_DEFAULT = true;
    public static final String SYNC_TOPIC_ACLS_INTERVAL_SECONDS = SYNC_TOPIC_ACLS + INTERVAL_SECONDS_SUFFIX;
    private static final String SYNC_TOPIC_ACLS_INTERVAL_SECONDS_DOC = "Frequency of topic ACL sync.";
    public static final long SYNC_TOPIC_ACLS_INTERVAL_SECONDS_DEFAULT = 10 * 60;

    public static final String CONFIG_PROPERTY_FILTER_CLASS = "config.property.filter.class";
    private static final String CONFIG_PROPERTY_FILTER_CLASS_DOC = "ConfigPropertyFilter to use. Selects topic config "
            + " properties to replicate.";
    public static final Class<?> CONFIG_PROPERTY_FILTER_CLASS_DEFAULT = DefaultConfigPropertyFilter.class;

    public static final String OFFSET_LAG_MAX = "offset.lag.max";
    private static final String OFFSET_LAG_MAX_DOC = "How out-of-sync a remote partition can be before it is resynced. A lower value, even as low as 0, " +
            "can be set to trigger more frequent offset syncs, which will help in scenarios when records may not flow constantly.";
    public static final long OFFSET_LAG_MAX_DEFAULT = 100L;

    public static final String HEARTBEATS_REPLICATION_ENABLED = "heartbeats.replication" + ENABLED_SUFFIX;
    private static final String HEARTBEATS_REPLICATION_ENABLED_DOC = "Whether to replicate the heartbeats topics even when the topic filter does not include them." +
            " If set to true, heartbeats topics identified by the replication policy will always be replicated, regardless of the topic filter configuration." +
            " If set to false, heartbeats topics will only be replicated if the topic filter allows.";
    public static final boolean HEARTBEATS_REPLICATION_ENABLED_DEFAULT = true;

    public static final String OFFSET_SYNCS_SOURCE_PRODUCER_ROLE = OFFSET_SYNCS_CLIENT_ROLE_PREFIX + "source-producer";
    public static final String OFFSET_SYNCS_TARGET_PRODUCER_ROLE = OFFSET_SYNCS_CLIENT_ROLE_PREFIX + "target-producer";
    public static final String OFFSET_SYNCS_SOURCE_ADMIN_ROLE = OFFSET_SYNCS_CLIENT_ROLE_PREFIX + "source-admin";
    public static final String OFFSET_SYNCS_TARGET_ADMIN_ROLE = OFFSET_SYNCS_CLIENT_ROLE_PREFIX + "target-admin";

    public MirrorSourceConfig(Map<String, String> props) {
        super(CONNECTOR_CONFIG_DEF, props);
    }

    public MirrorSourceConfig(ConfigDef configDef, Map<String, String> props) {
        super(configDef, props);
    }

    Map<String, String> taskConfigForTopicPartitions(List<TopicPartition> topicPartitions, int taskIndex) {
        Map<String, String> props = originalsStrings();
        String topicPartitionsString = topicPartitions.stream()
                .map(MirrorUtils::encodeTopicPartition)
                .collect(Collectors.joining(","));
        props.put(TASK_TOPIC_PARTITIONS, topicPartitionsString);
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

    Map<String, Object> offsetSyncsTopicAdminConfig() {
        return SOURCE_CLUSTER_ALIAS_DEFAULT.equals(offsetSyncsTopicLocation())
                ? sourceAdminConfig(OFFSET_SYNCS_SOURCE_ADMIN_ROLE)
                : targetAdminConfig(OFFSET_SYNCS_TARGET_ADMIN_ROLE);
    }

    Map<String, Object> offsetSyncsTopicProducerConfig() {
        return SOURCE_CLUSTER_ALIAS_DEFAULT.equals(offsetSyncsTopicLocation())
                ? sourceProducerConfig(OFFSET_SYNCS_SOURCE_PRODUCER_ROLE)
                : targetProducerConfig(OFFSET_SYNCS_TARGET_PRODUCER_ROLE);
    }

    String checkpointsTopic() {
        return replicationPolicy().checkpointsTopic(sourceClusterAlias());
    }

    long maxOffsetLag() {
        return getLong(OFFSET_LAG_MAX);
    }

    Duration refreshTopicsInterval() {
        if (getBoolean(REFRESH_TOPICS_ENABLED)) {
            return Duration.ofSeconds(getLong(REFRESH_TOPICS_INTERVAL_SECONDS));
        } else {
            // negative interval to disable
            return Duration.ofMillis(-1);
        }
    }

    Duration syncTopicConfigsInterval() {
        if (getBoolean(SYNC_TOPIC_CONFIGS_ENABLED)) {
            return Duration.ofSeconds(getLong(SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS));
        } else {
            // negative interval to disable
            return Duration.ofMillis(-1);
        }
    }

    Duration syncTopicAclsInterval() {
        if (getBoolean(SYNC_TOPIC_ACLS_ENABLED)) {
            return Duration.ofSeconds(getLong(SYNC_TOPIC_ACLS_INTERVAL_SECONDS));
        } else {
            // negative interval to disable
            return Duration.ofMillis(-1);
        }
    }

    int replicationFactor() {
        return getInt(REPLICATION_FACTOR);
    }

    short offsetSyncsTopicReplicationFactor() {
        return getShort(OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR);
    }

    TopicFilter topicFilter() {
        return getConfiguredInstance(TOPIC_FILTER_CLASS, TopicFilter.class);
    }

    ConfigPropertyFilter configPropertyFilter() {
        return getConfiguredInstance(CONFIG_PROPERTY_FILTER_CLASS, ConfigPropertyFilter.class);
    }

    Duration consumerPollTimeout() {
        return Duration.ofMillis(getLong(CONSUMER_POLL_TIMEOUT_MILLIS));
    }

    boolean emitOffsetSyncsEnabled() {
        return getBoolean(EMIT_OFFSET_SYNCS_ENABLED);
    }

    boolean heartbeatsReplicationEnabled() {
        return getBoolean(HEARTBEATS_REPLICATION_ENABLED);
    }

    private static ConfigDef defineSourceConfig(ConfigDef baseConfig) {
        return baseConfig
                .define(
                        TOPICS,
                        ConfigDef.Type.LIST,
                        TOPICS_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        TOPICS_DOC)
                .define(
                        TOPICS_EXCLUDE,
                        ConfigDef.Type.LIST,
                        TOPICS_EXCLUDE_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        TOPICS_EXCLUDE_DOC)
                .define(
                        CONFIG_PROPERTIES_EXCLUDE,
                        ConfigDef.Type.LIST,
                        CONFIG_PROPERTIES_EXCLUDE_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        CONFIG_PROPERTIES_EXCLUDE_DOC)
                .define(
                        TOPIC_FILTER_CLASS,
                        ConfigDef.Type.CLASS,
                        TOPIC_FILTER_CLASS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        TOPIC_FILTER_CLASS_DOC)
                .define(
                        CONFIG_PROPERTY_FILTER_CLASS,
                        ConfigDef.Type.CLASS,
                        CONFIG_PROPERTY_FILTER_CLASS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        CONFIG_PROPERTY_FILTER_CLASS_DOC)
                .define(
                        CONSUMER_POLL_TIMEOUT_MILLIS,
                        ConfigDef.Type.LONG,
                        CONSUMER_POLL_TIMEOUT_MILLIS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        CONSUMER_POLL_TIMEOUT_MILLIS_DOC)
                .define(
                        REFRESH_TOPICS_ENABLED,
                        ConfigDef.Type.BOOLEAN,
                        REFRESH_TOPICS_ENABLED_DEFAULT,
                        ConfigDef.Importance.LOW,
                        REFRESH_TOPICS_ENABLED_DOC)
                .define(
                        REFRESH_TOPICS_INTERVAL_SECONDS,
                        ConfigDef.Type.LONG,
                        REFRESH_TOPICS_INTERVAL_SECONDS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        REFRESH_TOPICS_INTERVAL_SECONDS_DOC)
                .define(
                        SYNC_TOPIC_CONFIGS_ENABLED,
                        ConfigDef.Type.BOOLEAN,
                        SYNC_TOPIC_CONFIGS_ENABLED_DEFAULT,
                        ConfigDef.Importance.LOW,
                        SYNC_TOPIC_CONFIGS_ENABLED_DOC)
                .define(
                        SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS,
                        ConfigDef.Type.LONG,
                        SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS_DOC)
                .define(
                        SYNC_TOPIC_ACLS_ENABLED,
                        ConfigDef.Type.BOOLEAN,
                        SYNC_TOPIC_ACLS_ENABLED_DEFAULT,
                        ConfigDef.Importance.LOW,
                        SYNC_TOPIC_ACLS_ENABLED_DOC)
                .define(
                        SYNC_TOPIC_ACLS_INTERVAL_SECONDS,
                        ConfigDef.Type.LONG,
                        SYNC_TOPIC_ACLS_INTERVAL_SECONDS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        SYNC_TOPIC_ACLS_INTERVAL_SECONDS_DOC)
                .define(
                        REPLICATION_FACTOR,
                        ConfigDef.Type.INT,
                        REPLICATION_FACTOR_DEFAULT,
                        ConfigDef.Importance.LOW,
                        REPLICATION_FACTOR_DOC)
                .define(
                        OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR,
                        ConfigDef.Type.SHORT,
                        OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR_DEFAULT,
                        ConfigDef.Importance.LOW,
                        OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR_DOC)
                .define(
                        OFFSET_LAG_MAX,
                        ConfigDef.Type.LONG,
                        OFFSET_LAG_MAX_DEFAULT,
                        ConfigDef.Importance.LOW,
                        OFFSET_LAG_MAX_DOC)
                .define(
                        OFFSET_SYNCS_TOPIC_LOCATION,
                        ConfigDef.Type.STRING,
                        OFFSET_SYNCS_TOPIC_LOCATION_DEFAULT,
                        in(SOURCE_CLUSTER_ALIAS_DEFAULT, TARGET_CLUSTER_ALIAS_DEFAULT),
                        ConfigDef.Importance.LOW,
                        OFFSET_SYNCS_TOPIC_LOCATION_DOC)
                .define(
                        EMIT_OFFSET_SYNCS_ENABLED,
                        ConfigDef.Type.BOOLEAN,
                        EMIT_OFFSET_SYNCS_ENABLED_DEFAULT,
                        ConfigDef.Importance.LOW,
                        EMIT_OFFSET_SYNCS_ENABLED_DOC
                )
                .define(
                        HEARTBEATS_REPLICATION_ENABLED,
                        ConfigDef.Type.BOOLEAN,
                        HEARTBEATS_REPLICATION_ENABLED_DEFAULT,
                        ConfigDef.Importance.LOW,
                        HEARTBEATS_REPLICATION_ENABLED_DOC
                );
    }

    protected static final ConfigDef CONNECTOR_CONFIG_DEF = defineSourceConfig(new ConfigDef(BASE_CONNECTOR_CONFIG_DEF));

    public static void main(String[] args) {        
        System.out.println(defineSourceConfig(new ConfigDef()).toHtml(4, config -> "mirror_source_" + config));
    }
}
