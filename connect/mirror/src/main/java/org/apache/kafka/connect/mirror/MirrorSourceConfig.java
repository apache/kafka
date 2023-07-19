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
import org.apache.kafka.common.utils.ConfigUtils;

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
    public static final String TOPICS_EXCLUDE_ALIAS = DefaultTopicFilter.TOPICS_EXCLUDE_CONFIG_ALIAS;
    public static final String TOPICS_EXCLUDE_DEFAULT = DefaultTopicFilter.TOPICS_EXCLUDE_DEFAULT;
    private static final String TOPICS_EXCLUDE_DOC = "Excluded topics. Supports comma-separated topic names and regexes."
            + " Excludes take precedence over includes.";

    public static final String CONFIG_PROPERTIES_EXCLUDE = DefaultConfigPropertyFilter.CONFIG_PROPERTIES_EXCLUDE_CONFIG;
    public static final String CONFIG_PROPERTIES_EXCLUDE_ALIAS = DefaultConfigPropertyFilter.CONFIG_PROPERTIES_EXCLUDE_ALIAS_CONFIG;
    public static final String CONFIG_PROPERTIES_EXCLUDE_DEFAULT = DefaultConfigPropertyFilter.CONFIG_PROPERTIES_EXCLUDE_DEFAULT;
    private static final String CONFIG_PROPERTIES_EXCLUDE_DOC = "Topic config properties that should not be replicated. Supports "
            + "comma-separated property names and regexes.";

    public static final String OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR = "offset-syncs.topic.replication.factor";
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
    @Deprecated
    public static final String USE_INCREMENTAL_ALTER_CONFIGS = "use.incremental.alter.configs";
    private static final String USE_INCREMENTAL_ALTER_CONFIG_DOC = "Deprecated. Which API to use for syncing topic configs. " +
            "The valid values are 'requested', 'required' and 'never'. " +
            "By default, set to 'requested', which means the IncrementalAlterConfigs API is being used for syncing topic configurations " +
            "and if any request receives an error from an incompatible broker, it will fallback to using the deprecated AlterConfigs API. " +
            "If explicitly set to 'required', the IncrementalAlterConfigs API is used without the fallback logic and +" +
            "if it receives an error from an incompatible broker, the connector will fail." +
            "If explicitly set to 'never', the AlterConfig is always used." +
            "This setting will be removed and the behaviour of 'required' will be used in Kafka 4.0, therefore users should ensure that target broker is at least 2.3.0";
    public static final String REQUEST_INCREMENTAL_ALTER_CONFIGS = "requested";
    public static final String REQUIRE_INCREMENTAL_ALTER_CONFIGS = "required";
    public static final String NEVER_USE_INCREMENTAL_ALTER_CONFIGS = "never";

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
    private static final String OFFSET_LAG_MAX_DOC = "How out-of-sync a remote partition can be before it is resynced.";
    public static final long OFFSET_LAG_MAX_DEFAULT = 100L;

    public static final String ADD_SOURCE_ALIAS_TO_METRICS = "add.source.alias.to.metrics";
    private static final String ADD_SOURCE_ALIAS_TO_METRICS_DOC = "Deprecated. Whether to tag metrics with the source cluster alias. "
        + "Metrics have the target, topic and partition tags. When this setting is enabled, it adds the source tag. "
        + "This configuration will be removed in Kafka 4.0 and the default behavior will be to always have the source tag.";
    public static final boolean ADD_SOURCE_ALIAS_TO_METRICS_DEFAULT = false;
    public static final String OFFSET_SYNCS_SOURCE_PRODUCER_ROLE = "offset-syncs-source-producer";
    public static final String OFFSET_SYNCS_TARGET_PRODUCER_ROLE = "offset-syncs-target-producer";
    public static final String OFFSET_SYNCS_SOURCE_ADMIN_ROLE = "offset-syncs-source-admin";
    public static final String OFFSET_SYNCS_TARGET_ADMIN_ROLE = "offset-syncs-target-admin";

    public MirrorSourceConfig(Map<String, String> props) {
        super(CONNECTOR_CONFIG_DEF, ConfigUtils.translateDeprecatedConfigs(props, new String[][]{
                {TOPICS_EXCLUDE, TOPICS_EXCLUDE_ALIAS},
                {CONFIG_PROPERTIES_EXCLUDE, CONFIG_PROPERTIES_EXCLUDE_ALIAS}}));
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

    String useIncrementalAlterConfigs() {
        return getString(USE_INCREMENTAL_ALTER_CONFIGS);
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

    boolean addSourceAliasToMetrics() {
        return getBoolean(ADD_SOURCE_ALIAS_TO_METRICS);
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
                        TOPICS_EXCLUDE_ALIAS,
                        ConfigDef.Type.LIST,
                        null,
                        ConfigDef.Importance.HIGH,
                        "Deprecated. Use " + TOPICS_EXCLUDE + " instead.")
                .define(
                        CONFIG_PROPERTIES_EXCLUDE,
                        ConfigDef.Type.LIST,
                        CONFIG_PROPERTIES_EXCLUDE_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        CONFIG_PROPERTIES_EXCLUDE_DOC)
                .define(
                        CONFIG_PROPERTIES_EXCLUDE_ALIAS,
                        ConfigDef.Type.LIST,
                        null,
                        ConfigDef.Importance.HIGH,
                        "Deprecated. Use " + CONFIG_PROPERTIES_EXCLUDE + " instead.")
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
                        USE_INCREMENTAL_ALTER_CONFIGS,
                        ConfigDef.Type.STRING,
                        REQUEST_INCREMENTAL_ALTER_CONFIGS,
                        in(REQUEST_INCREMENTAL_ALTER_CONFIGS, REQUIRE_INCREMENTAL_ALTER_CONFIGS, NEVER_USE_INCREMENTAL_ALTER_CONFIGS),
                        ConfigDef.Importance.LOW,
                        USE_INCREMENTAL_ALTER_CONFIG_DOC)
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
                        ADD_SOURCE_ALIAS_TO_METRICS,
                        ConfigDef.Type.BOOLEAN,
                        ADD_SOURCE_ALIAS_TO_METRICS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        ADD_SOURCE_ALIAS_TO_METRICS_DOC);
    }

    protected final static ConfigDef CONNECTOR_CONFIG_DEF = defineSourceConfig(new ConfigDef(BASE_CONNECTOR_CONFIG_DEF));

    public static void main(String[] args) {        
        System.out.println(defineSourceConfig(new ConfigDef()).toHtml(4, config -> "mirror_source_" + config));
    }
}
