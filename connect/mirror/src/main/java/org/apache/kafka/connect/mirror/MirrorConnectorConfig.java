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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.utils.ConfigUtils;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.time.Duration;

/** Shared config properties used by MirrorSourceConnector, MirrorCheckpointConnector, and MirrorHeartbeatConnector.
 *  <p>
 *  Generally, these properties are filled-in automatically by MirrorMaker based on a top-level mm2.properties file.
 *  However, when running MM2 connectors as plugins on a Connect-as-a-Service cluster, these properties must be configured manually,
 *  e.g. via the Connect REST API.
 *  </p>
 *  <p>
 *  An example configuration when running on Connect (not via MirrorMaker driver):
 *  </p>
 *  <pre>
 *      {
 *        "name": "MirrorSourceConnector",
 *        "connector.class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
 *        "replication.factor": "1",
 *        "source.cluster.alias": "backup",
 *        "target.cluster.alias": "primary",
 *        "source.cluster.bootstrap.servers": "vip1:9092",
 *        "target.cluster.bootstrap.servers": "vip2:9092",
 *        "topics": ".*test-topic-.*",
 *        "groups": "consumer-group-.*",
 *        "emit.checkpoints.interval.seconds": "1",
 *        "emit.heartbeats.interval.seconds": "1",
 *        "sync.topic.acls.enabled": "false"
 *      }
 *  </pre>
 */
public class MirrorConnectorConfig extends AbstractConfig {

    protected static final String ENABLED_SUFFIX = ".enabled";
    protected static final String INTERVAL_SECONDS_SUFFIX = ".interval.seconds";

    protected static final String REFRESH_TOPICS = "refresh.topics";
    protected static final String REFRESH_GROUPS = "refresh.groups";
    protected static final String SYNC_TOPIC_CONFIGS = "sync.topic.configs";
    protected static final String SYNC_TOPIC_ACLS = "sync.topic.acls";
    protected static final String EMIT_HEARTBEATS = "emit.heartbeats";
    protected static final String EMIT_CHECKPOINTS = "emit.checkpoints";
    protected static final String SYNC_GROUP_OFFSETS = "sync.group.offsets";

    public static final String ENABLED = "enabled";
    private static final String ENABLED_DOC = "Whether to replicate source->target.";
    public static final String SOURCE_CLUSTER_ALIAS = "source.cluster.alias";
    private static final String SOURCE_CLUSTER_ALIAS_DOC = "Alias of source cluster";
    public static final String TARGET_CLUSTER_ALIAS = "target.cluster.alias";
    public static final String TARGET_CLUSTER_ALIAS_DEFAULT = "target";
    private static final String TARGET_CLUSTER_ALIAS_DOC = "Alias of target cluster. Used in metrics reporting.";
    public static final String REPLICATION_POLICY_CLASS = MirrorClientConfig.REPLICATION_POLICY_CLASS;
    public static final Class<?> REPLICATION_POLICY_CLASS_DEFAULT = MirrorClientConfig.REPLICATION_POLICY_CLASS_DEFAULT;
    private static final String REPLICATION_POLICY_CLASS_DOC = "Class which defines the remote topic naming convention.";
    public static final String REPLICATION_POLICY_SEPARATOR = MirrorClientConfig.REPLICATION_POLICY_SEPARATOR;
    private static final String REPLICATION_POLICY_SEPARATOR_DOC = "Separator used in remote topic naming convention.";
    public static final String REPLICATION_POLICY_SEPARATOR_DEFAULT =
            MirrorClientConfig.REPLICATION_POLICY_SEPARATOR_DEFAULT;
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
    public static final String GROUPS = DefaultGroupFilter.GROUPS_INCLUDE_CONFIG;
    public static final String GROUPS_DEFAULT = DefaultGroupFilter.GROUPS_INCLUDE_DEFAULT;
    private static final String GROUPS_DOC = "Consumer groups to replicate. Supports comma-separated group IDs and regexes.";
    public static final String GROUPS_EXCLUDE = DefaultGroupFilter.GROUPS_EXCLUDE_CONFIG;
    public static final String GROUPS_EXCLUDE_ALIAS = DefaultGroupFilter.GROUPS_EXCLUDE_CONFIG_ALIAS;

    public static final String GROUPS_EXCLUDE_DEFAULT = DefaultGroupFilter.GROUPS_EXCLUDE_DEFAULT;
    private static final String GROUPS_EXCLUDE_DOC = "Exclude groups. Supports comma-separated group IDs and regexes."
                                                     + " Excludes take precedence over includes.";
    public static final String CONFIG_PROPERTIES_EXCLUDE = DefaultConfigPropertyFilter.CONFIG_PROPERTIES_EXCLUDE_CONFIG;
    public static final String CONFIG_PROPERTIES_EXCLUDE_ALIAS = DefaultConfigPropertyFilter.CONFIG_PROPERTIES_EXCLUDE_ALIAS_CONFIG;
    public static final String CONFIG_PROPERTIES_EXCLUDE_DEFAULT = DefaultConfigPropertyFilter.CONFIG_PROPERTIES_EXCLUDE_DEFAULT;
    private static final String CONFIG_PROPERTIES_EXCLUDE_DOC = "Topic config properties that should not be replicated. Supports "
                                        + "comma-separated property names and regexes.";

    public static final String HEARTBEATS_TOPIC_REPLICATION_FACTOR = "heartbeats.topic.replication.factor";
    public static final String HEARTBEATS_TOPIC_REPLICATION_FACTOR_DOC = "Replication factor for heartbeats topic.";
    public static final short HEARTBEATS_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;

    public static final String CHECKPOINTS_TOPIC_REPLICATION_FACTOR = "checkpoints.topic.replication.factor";
    public static final String CHECKPOINTS_TOPIC_REPLICATION_FACTOR_DOC = "Replication factor for checkpoints topic.";
    public static final short CHECKPOINTS_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;

    public static final String OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR = "offset-syncs.topic.replication.factor";
    public static final String OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR_DOC = "Replication factor for offset-syncs topic.";
    public static final short OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;

    protected static final String TASK_TOPIC_PARTITIONS = "task.assigned.partitions";
    protected static final String TASK_CONSUMER_GROUPS = "task.assigned.groups";

    public static final String CONSUMER_POLL_TIMEOUT_MILLIS = "consumer.poll.timeout.ms";
    private static final String CONSUMER_POLL_TIMEOUT_MILLIS_DOC = "Timeout when polling source cluster.";
    public static final long CONSUMER_POLL_TIMEOUT_MILLIS_DEFAULT = 1000L;

    public static final String ADMIN_TASK_TIMEOUT_MILLIS = "admin.timeout.ms";
    private static final String ADMIN_TASK_TIMEOUT_MILLIS_DOC = "Timeout for administrative tasks, e.g. detecting new topics.";
    public static final long ADMIN_TASK_TIMEOUT_MILLIS_DEFAULT = 60000L;

    public static final String REFRESH_TOPICS_ENABLED = REFRESH_TOPICS + ENABLED_SUFFIX;
    private static final String REFRESH_TOPICS_ENABLED_DOC = "Whether to periodically check for new topics and partitions.";
    public static final boolean REFRESH_TOPICS_ENABLED_DEFAULT = true;
    public static final String REFRESH_TOPICS_INTERVAL_SECONDS = REFRESH_TOPICS + INTERVAL_SECONDS_SUFFIX;
    private static final String REFRESH_TOPICS_INTERVAL_SECONDS_DOC = "Frequency of topic refresh.";
    public static final long REFRESH_TOPICS_INTERVAL_SECONDS_DEFAULT = 10 * 60;

    public static final String REFRESH_GROUPS_ENABLED = REFRESH_GROUPS + ENABLED_SUFFIX;
    private static final String REFRESH_GROUPS_ENABLED_DOC = "Whether to periodically check for new consumer groups.";
    public static final boolean REFRESH_GROUPS_ENABLED_DEFAULT = true;
    public static final String REFRESH_GROUPS_INTERVAL_SECONDS = REFRESH_GROUPS + INTERVAL_SECONDS_SUFFIX;
    private static final String REFRESH_GROUPS_INTERVAL_SECONDS_DOC = "Frequency of group refresh.";
    public static final long REFRESH_GROUPS_INTERVAL_SECONDS_DEFAULT = 10 * 60;

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

    public static final String EMIT_HEARTBEATS_ENABLED = EMIT_HEARTBEATS + ENABLED_SUFFIX;
    private static final String EMIT_HEARTBEATS_ENABLED_DOC = "Whether to emit heartbeats to target cluster.";
    public static final boolean EMIT_HEARTBEATS_ENABLED_DEFAULT = true;
    public static final String EMIT_HEARTBEATS_INTERVAL_SECONDS = EMIT_HEARTBEATS + INTERVAL_SECONDS_SUFFIX;
    private static final String EMIT_HEARTBEATS_INTERVAL_SECONDS_DOC = "Frequency of heartbeats.";
    public static final long EMIT_HEARTBEATS_INTERVAL_SECONDS_DEFAULT = 1;

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

    public static final String TOPIC_FILTER_CLASS = "topic.filter.class";
    private static final String TOPIC_FILTER_CLASS_DOC = "TopicFilter to use. Selects topics to replicate.";
    public static final Class<?> TOPIC_FILTER_CLASS_DEFAULT = DefaultTopicFilter.class;
    public static final String GROUP_FILTER_CLASS = "group.filter.class";
    private static final String GROUP_FILTER_CLASS_DOC = "GroupFilter to use. Selects consumer groups to replicate.";
    public static final Class<?> GROUP_FILTER_CLASS_DEFAULT = DefaultGroupFilter.class;
    public static final String CONFIG_PROPERTY_FILTER_CLASS = "config.property.filter.class";
    private static final String CONFIG_PROPERTY_FILTER_CLASS_DOC = "ConfigPropertyFilter to use. Selects topic config "
            + " properties to replicate.";
    public static final Class<?> CONFIG_PROPERTY_FILTER_CLASS_DEFAULT = DefaultConfigPropertyFilter.class;

    public static final String OFFSET_LAG_MAX = "offset.lag.max";
    private static final String OFFSET_LAG_MAX_DOC = "How out-of-sync a remote partition can be before it is resynced.";
    public static final long OFFSET_LAG_MAX_DEFAULT = 100L;

    protected static final String SOURCE_CLUSTER_PREFIX = MirrorMakerConfig.SOURCE_CLUSTER_PREFIX;
    protected static final String TARGET_CLUSTER_PREFIX = MirrorMakerConfig.TARGET_CLUSTER_PREFIX;
    protected static final String SOURCE_PREFIX = MirrorMakerConfig.SOURCE_PREFIX;
    protected static final String TARGET_PREFIX = MirrorMakerConfig.TARGET_PREFIX;
    protected static final String PRODUCER_CLIENT_PREFIX = "producer.";
    protected static final String CONSUMER_CLIENT_PREFIX = "consumer.";
    protected static final String ADMIN_CLIENT_PREFIX = "admin.";

    public MirrorConnectorConfig(Map<String, String> props) {
        this(CONNECTOR_CONFIG_DEF, ConfigUtils.translateDeprecatedConfigs(props, new String[][]{
            {TOPICS_EXCLUDE, TOPICS_EXCLUDE_ALIAS},
            {GROUPS_EXCLUDE, GROUPS_EXCLUDE_ALIAS},
            {CONFIG_PROPERTIES_EXCLUDE, CONFIG_PROPERTIES_EXCLUDE_ALIAS}}));
    }

    protected MirrorConnectorConfig(ConfigDef configDef, Map<String, String> props) {
        super(configDef, props, true);
    }

    String connectorName() {
        return getString(ConnectorConfig.NAME_CONFIG);
    }

    boolean enabled() {
        return getBoolean(ENABLED);
    }

    Duration consumerPollTimeout() {
        return Duration.ofMillis(getLong(CONSUMER_POLL_TIMEOUT_MILLIS));
    }

    Duration adminTimeout() {
        return Duration.ofMillis(getLong(ADMIN_TASK_TIMEOUT_MILLIS));
    }

    Map<String, Object> sourceProducerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(SOURCE_CLUSTER_PREFIX));
        props.keySet().retainAll(MirrorClientConfig.CLIENT_CONFIG_DEF.names());
        props.putAll(originalsWithPrefix(PRODUCER_CLIENT_PREFIX));
        props.putAll(originalsWithPrefix(SOURCE_PREFIX + PRODUCER_CLIENT_PREFIX));
        return props;
    }

    Map<String, Object> sourceConsumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(SOURCE_CLUSTER_PREFIX));
        props.keySet().retainAll(MirrorClientConfig.CLIENT_CONFIG_DEF.names());
        props.putAll(originalsWithPrefix(CONSUMER_CLIENT_PREFIX));
        props.putAll(originalsWithPrefix(SOURCE_PREFIX + CONSUMER_CLIENT_PREFIX));
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.putIfAbsent(AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    Map<String, String> taskConfigForTopicPartitions(List<TopicPartition> topicPartitions) {
        Map<String, String> props = originalsStrings();
        String topicPartitionsString = topicPartitions.stream()
                .map(MirrorUtils::encodeTopicPartition)
                .collect(Collectors.joining(","));
        props.put(TASK_TOPIC_PARTITIONS, topicPartitionsString);
        return props;
    }

    Map<String, String> taskConfigForConsumerGroups(List<String> groups) {
        Map<String, String> props = originalsStrings();
        props.put(TASK_CONSUMER_GROUPS, String.join(",", groups));
        return props;
    }

    Map<String, Object> targetAdminConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(TARGET_CLUSTER_PREFIX));
        props.keySet().retainAll(MirrorClientConfig.CLIENT_CONFIG_DEF.names());
        props.putAll(originalsWithPrefix(ADMIN_CLIENT_PREFIX));
        props.putAll(originalsWithPrefix(TARGET_PREFIX + ADMIN_CLIENT_PREFIX));
        return props;
    }

    Map<String, Object> sourceAdminConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(SOURCE_CLUSTER_PREFIX));
        props.keySet().retainAll(MirrorClientConfig.CLIENT_CONFIG_DEF.names());
        props.putAll(originalsWithPrefix(ADMIN_CLIENT_PREFIX));
        props.putAll(originalsWithPrefix(SOURCE_PREFIX + ADMIN_CLIENT_PREFIX));
        return props;
    }

    List<MetricsReporter> metricsReporters() {
        List<MetricsReporter> reporters = getConfiguredInstances(
                CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, MetricsReporter.class);
        JmxReporter jmxReporter = new JmxReporter();
        jmxReporter.configure(this.originals());
        reporters.add(jmxReporter);
        MetricsContext metricsContext = new KafkaMetricsContext("kafka.connect.mirror");

        for (MetricsReporter reporter : reporters) {
            reporter.contextChange(metricsContext);
        }

        return reporters;
    }

    String sourceClusterAlias() {
        return getString(SOURCE_CLUSTER_ALIAS);
    }

    String targetClusterAlias() {
        return getString(TARGET_CLUSTER_ALIAS);
    }

    String offsetSyncsTopic() {
        // ".internal" suffix ensures this doesn't get replicated
        return "mm2-offset-syncs." + targetClusterAlias() + ".internal";
    }

    String heartbeatsTopic() {
        return MirrorClientConfig.HEARTBEATS_TOPIC;
    }

    // e.g. source1.heartbeats
    String targetHeartbeatsTopic() {
        return replicationPolicy().formatRemoteTopic(sourceClusterAlias(), heartbeatsTopic());
    }

    String checkpointsTopic() {
        // Checkpoint topics are not "remote topics", as they are not replicated, so we don't
        // need to use ReplicationPolicy here.
        return sourceClusterAlias() + MirrorClientConfig.CHECKPOINTS_TOPIC_SUFFIX;
    }

    long maxOffsetLag() {
        return getLong(OFFSET_LAG_MAX);
    }

    Duration emitHeartbeatsInterval() {
        if (getBoolean(EMIT_HEARTBEATS_ENABLED)) {
            return Duration.ofSeconds(getLong(EMIT_HEARTBEATS_INTERVAL_SECONDS));
        } else {
            // negative interval to disable
            return Duration.ofMillis(-1);
        }
    }

    Duration emitCheckpointsInterval() {
        if (getBoolean(EMIT_CHECKPOINTS_ENABLED)) {
            return Duration.ofSeconds(getLong(EMIT_CHECKPOINTS_INTERVAL_SECONDS));
        } else {
            // negative interval to disable
            return Duration.ofMillis(-1);
        }
    }

    Duration refreshTopicsInterval() {
        if (getBoolean(REFRESH_TOPICS_ENABLED)) {
            return Duration.ofSeconds(getLong(REFRESH_TOPICS_INTERVAL_SECONDS));
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

    ReplicationPolicy replicationPolicy() {
        return getConfiguredInstance(REPLICATION_POLICY_CLASS, ReplicationPolicy.class);
    }

    int replicationFactor() {
        return getInt(REPLICATION_FACTOR);
    }

    short heartbeatsTopicReplicationFactor() {
        return getShort(HEARTBEATS_TOPIC_REPLICATION_FACTOR);
    }

    short checkpointsTopicReplicationFactor() {
        return getShort(CHECKPOINTS_TOPIC_REPLICATION_FACTOR);
    }

    short offsetSyncsTopicReplicationFactor() {
        return getShort(OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR);
    }

    TopicFilter topicFilter() {
        return getConfiguredInstance(TOPIC_FILTER_CLASS, TopicFilter.class);
    }

    GroupFilter groupFilter() {
        return getConfiguredInstance(GROUP_FILTER_CLASS, GroupFilter.class);
    }

    ConfigPropertyFilter configPropertyFilter() {
        return getConfiguredInstance(CONFIG_PROPERTY_FILTER_CLASS, ConfigPropertyFilter.class);
    }

    Duration syncGroupOffsetsInterval() {
        if (getBoolean(SYNC_GROUP_OFFSETS_ENABLED)) {
            return Duration.ofSeconds(getLong(SYNC_GROUP_OFFSETS_INTERVAL_SECONDS));
        } else {
            // negative interval to disable
            return Duration.ofMillis(-1);
        }
    }

    protected static final ConfigDef CONNECTOR_CONFIG_DEF = ConnectorConfig.configDef()
            .define(
                    ENABLED,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.LOW,
                    ENABLED_DOC)
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
                    GROUPS_EXCLUDE_ALIAS,
                    ConfigDef.Type.LIST,
                    null,
                    ConfigDef.Importance.HIGH,
                    "Deprecated. Use " + GROUPS_EXCLUDE + " instead.")
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
                    GROUP_FILTER_CLASS,
                    ConfigDef.Type.CLASS,
                    GROUP_FILTER_CLASS_DEFAULT,
                    ConfigDef.Importance.LOW,
                    GROUP_FILTER_CLASS_DOC)
            .define(
                    CONFIG_PROPERTY_FILTER_CLASS,
                    ConfigDef.Type.CLASS,
                    CONFIG_PROPERTY_FILTER_CLASS_DEFAULT,
                    ConfigDef.Importance.LOW,
                    CONFIG_PROPERTY_FILTER_CLASS_DOC)
            .define(
                    SOURCE_CLUSTER_ALIAS,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    SOURCE_CLUSTER_ALIAS_DOC)
            .define(
                    TARGET_CLUSTER_ALIAS,
                    ConfigDef.Type.STRING,
                    TARGET_CLUSTER_ALIAS_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    TARGET_CLUSTER_ALIAS_DOC)
            .define(
                    CONSUMER_POLL_TIMEOUT_MILLIS,
                    ConfigDef.Type.LONG,
                    CONSUMER_POLL_TIMEOUT_MILLIS_DEFAULT,
                    ConfigDef.Importance.LOW,
                    CONSUMER_POLL_TIMEOUT_MILLIS_DOC)
            .define(
                    ADMIN_TASK_TIMEOUT_MILLIS,
                    ConfigDef.Type.LONG,
                    ADMIN_TASK_TIMEOUT_MILLIS_DEFAULT,
                    ConfigDef.Importance.LOW,
                    ADMIN_TASK_TIMEOUT_MILLIS_DOC)
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
                    EMIT_HEARTBEATS_ENABLED,
                    ConfigDef.Type.BOOLEAN,
                    EMIT_HEARTBEATS_ENABLED_DEFAULT,
                    ConfigDef.Importance.LOW,
                    EMIT_HEARTBEATS_ENABLED_DOC)
            .define(
                    EMIT_HEARTBEATS_INTERVAL_SECONDS,
                    ConfigDef.Type.LONG,
                    EMIT_HEARTBEATS_INTERVAL_SECONDS_DEFAULT,
                    ConfigDef.Importance.LOW,
                    EMIT_HEARTBEATS_INTERVAL_SECONDS_DOC)
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
                    REPLICATION_POLICY_CLASS,
                    ConfigDef.Type.CLASS,
                    REPLICATION_POLICY_CLASS_DEFAULT,
                    ConfigDef.Importance.LOW,
                    REPLICATION_POLICY_CLASS_DOC)
            .define(
                    REPLICATION_POLICY_SEPARATOR,
                    ConfigDef.Type.STRING,
                    REPLICATION_POLICY_SEPARATOR_DEFAULT,
                    ConfigDef.Importance.LOW,
                    REPLICATION_POLICY_SEPARATOR_DOC)
            .define(
                    REPLICATION_FACTOR,
                    ConfigDef.Type.INT,
                    REPLICATION_FACTOR_DEFAULT,
                    ConfigDef.Importance.LOW,
                    REPLICATION_FACTOR_DOC)
            .define(
                    HEARTBEATS_TOPIC_REPLICATION_FACTOR,
                    ConfigDef.Type.SHORT,
                    HEARTBEATS_TOPIC_REPLICATION_FACTOR_DEFAULT,
                    ConfigDef.Importance.LOW,
                    HEARTBEATS_TOPIC_REPLICATION_FACTOR_DOC)
            .define(
                    CHECKPOINTS_TOPIC_REPLICATION_FACTOR,
                    ConfigDef.Type.SHORT,
                    CHECKPOINTS_TOPIC_REPLICATION_FACTOR_DEFAULT,
                    ConfigDef.Importance.LOW,
                    CHECKPOINTS_TOPIC_REPLICATION_FACTOR_DOC)
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
                    CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
                    ConfigDef.Type.LIST,
                    null,
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
            .define(
                    CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                    ConfigDef.Type.STRING,
                    CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                    ConfigDef.Importance.MEDIUM,
                    CommonClientConfigs.SECURITY_PROTOCOL_DOC)
            .withClientSslSupport()
            .withClientSaslSupport();
}
