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
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.regex.Pattern;
import java.time.Duration;

public class MirrorConnectorConfig extends AbstractConfig {

    protected static final String ENABLED_SUFFIX = ".enabled";
    protected static final String INTERVAL_SECONDS_SUFFIX = ".interval.seconds";

    protected static final String REFRESH_TOPICS = "refresh.topics";
    protected static final String REFRESH_GROUPS = "refresh.groups";
    protected static final String SYNC_TOPIC_CONFIGS = "sync.topic.configs";
    protected static final String SYNC_TOPIC_ACLS = "sync.topic.acls";
    protected static final String EMIT_HEARTBEATS = "emit.heartbeats";
    protected static final String EMIT_CHECKPOINTS = "emit.checkpoints";
    protected static final String BOOTSTRAP_SERVERS = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
    
    public static final String ENABLED = "enabled";
    private static final String ENABLED_DOC = "Enable this source->target replication flow.";
    private static final boolean ENABLED_DEFAULT = false;
    public static final String NAME = "name";
    private static final String NAME_DOC = "name";
    public static final String TOPICS = "topics";
    private static final String TOPICS_DOC = "topics";
    public static final String TOPICS_DEFAULT = ".*";
    public static final String TOPICS_BLACKLIST = "topics.blacklist";
    private static final String TOPICS_BLACKLIST_DOC = "topics.blacklist";
    public static final String TOPICS_BLACKLIST_DEFAULT = ".*[\\-\\.]internal, .*\\.replica, __consumer_offsets";
    public static final String GROUPS = "groups";
    private static final String GROUPS_DOC = "groups";
    public static final String GROUPS_DEFAULT = ".*";
    public static final String GROUPS_BLACKLIST = "groups.blacklist";
    private static final String GROUPS_BLACKLIST_DOC = "groups.blacklist";
    public static final String GROUPS_BLACKLIST_DEFAULT = "console-consumer-.*, connect-.*";
    public static final String CONFIG_PROPERTIES = "config.properties";
    private static final String CONFIG_PROPERTIES_DOC = "config.properties";
    public static final String CONFIG_PROPERTIES_DEFAULT = ".*";
    public static final String CONFIG_PROPERTIES_BLACKLIST = "config.properties.blacklist";
    private static final String CONFIG_PROPERTIES_BLACKLIST_DOC = "config.properties.blacklist";
    public static final String CONFIG_PROPERTIES_BLACKLIST_DEFAULT = "segment\\.bytes";
    public static final String SOURCE_CLUSTER_ALIAS = "source.cluster.alias";
    private static final String SOURCE_CLUSTER_ALIAS_DOC = "source.cluster.alias";
    public static final String TARGET_CLUSTER_ALIAS = "target.cluster.alias";
    private static final String TARGET_CLUSTER_ALIAS_DOC = "target.cluster.alias";
    public static final String REPLICATION_POLICY_CLASS = MirrorClientConfig.REPLICATION_POLICY_CLASS;
    private static final String REPLICATION_POLICY_CLASS_DOC = "replication.policy.class";
    public static final String REPLICATION_POLICY_SEPARATOR = MirrorClientConfig.REPLICATION_POLICY_SEPARATOR;
    private static final String REPLICATION_POLICY_SEPARATOR_DOC = "replication.policy.separator";
    public static final String REPLICATION_POLICY_SEPARATOR_DEFAULT =
        MirrorClientConfig.REPLICATION_POLICY_SEPARATOR_DEFAULT;
 
    public static final String SOURCE_CLUSTER_BOOTSTRAP_SERVERS = "source.cluster.bootstrap.servers";
    private static final String SOURCE_CLUSTER_BOOTSTRAP_SERVERS_DOC = "source.cluster.bootstrap.servers";
    public static final String TARGET_CLUSTER_BOOTSTRAP_SERVERS = "target.cluster.bootstrap.servers";
    private static final String TARGET_CLUSTER_BOOTSTRAP_SERVERS_DOC = "target.cluster.bootstrap.servers";
    protected static final String TASK_TOPIC_PARTITIONS = "task.assigned.partitions";
    protected static final String TASK_TOPIC_PARTITIONS_DOC = "task.assigned.partitions";
    protected static final String TASK_CONSUMER_GROUPS = "task.assigned.groups";
    protected static final String TASK_CONSUMER_GROUPS_DOC = "task.assigned.groups";

    public static final String CONSUMER_POLL_TIMEOUT_MILLIS = "consumer.poll.timeout.ms";
    private static final String CONSUMER_POLL_TIMEOUT_MILLIS_DOC = CONSUMER_POLL_TIMEOUT_MILLIS;
    public static final long CONSUMER_POLL_TIMEOUT_MILLIS_DEFAULT = 1000L;

    public static final String REFRESH_TOPICS_ENABLED = REFRESH_TOPICS + ENABLED_SUFFIX;
    private static final String REFRESH_TOPICS_ENABLED_DOC = REFRESH_TOPICS + ENABLED_SUFFIX;
    public static final boolean REFRESH_TOPICS_ENABLED_DEFAULT = true;
    public static final String REFRESH_TOPICS_INTERVAL_SECONDS = REFRESH_TOPICS + INTERVAL_SECONDS_SUFFIX;
    private static final String REFRESH_TOPICS_INTERVAL_SECONDS_DOC = REFRESH_TOPICS + INTERVAL_SECONDS_SUFFIX;
    public static final long REFRESH_TOPICS_INTERVAL_SECONDS_DEFAULT = 5 * 60;

    public static final String REFRESH_GROUPS_ENABLED = REFRESH_GROUPS + ENABLED_SUFFIX;
    private static final String REFRESH_GROUPS_ENABLED_DOC = REFRESH_GROUPS + ENABLED_SUFFIX;
    public static final boolean REFRESH_GROUPS_ENABLED_DEFAULT = true;
    public static final String REFRESH_GROUPS_INTERVAL_SECONDS = REFRESH_GROUPS + INTERVAL_SECONDS_SUFFIX;
    private static final String REFRESH_GROUPS_INTERVAL_SECONDS_DOC = REFRESH_GROUPS + INTERVAL_SECONDS_SUFFIX;
    public static final long REFRESH_GROUPS_INTERVAL_SECONDS_DEFAULT = 5 * 60;

    public static final String SYNC_TOPIC_CONFIGS_ENABLED = SYNC_TOPIC_CONFIGS + ENABLED_SUFFIX;
    private static final String SYNC_TOPIC_CONFIGS_ENABLED_DOC = SYNC_TOPIC_CONFIGS + ENABLED_SUFFIX;
    public static final boolean SYNC_TOPIC_CONFIGS_ENABLED_DEFAULT = true;
    public static final String SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS = SYNC_TOPIC_CONFIGS + INTERVAL_SECONDS_SUFFIX;
    private static final String SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS_DOC = SYNC_TOPIC_CONFIGS + INTERVAL_SECONDS_SUFFIX;
    public static final long SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS_DEFAULT = 10 * 60;

    public static final String SYNC_TOPIC_ACLS_ENABLED = SYNC_TOPIC_ACLS + ENABLED_SUFFIX;
    private static final String SYNC_TOPIC_ACLS_ENABLED_DOC = SYNC_TOPIC_ACLS + ENABLED_SUFFIX;
    public static final boolean SYNC_TOPIC_ACLS_ENABLED_DEFAULT = true;
    public static final String SYNC_TOPIC_ACLS_INTERVAL_SECONDS = SYNC_TOPIC_ACLS + INTERVAL_SECONDS_SUFFIX;
    private static final String SYNC_TOPIC_ACLS_INTERVAL_SECONDS_DOC = SYNC_TOPIC_ACLS + INTERVAL_SECONDS_SUFFIX;
    public static final long SYNC_TOPIC_ACLS_INTERVAL_SECONDS_DEFAULT = 10 * 60;

    public static final String EMIT_HEARTBEATS_ENABLED = EMIT_HEARTBEATS + ENABLED_SUFFIX;
    private static final String EMIT_HEARTBEATS_ENABLED_DOC = EMIT_HEARTBEATS + ENABLED_SUFFIX;
    public static final boolean EMIT_HEARTBEATS_ENABLED_DEFAULT = true;
    public static final String EMIT_HEARTBEATS_INTERVAL_SECONDS = EMIT_HEARTBEATS + INTERVAL_SECONDS_SUFFIX;
    private static final String EMIT_HEARTBEATS_INTERVAL_SECONDS_DOC = EMIT_HEARTBEATS + INTERVAL_SECONDS_SUFFIX;
    public static final long EMIT_HEARTBEATS_INTERVAL_SECONDS_DEFAULT = 1;

    public static final String EMIT_CHECKPOINTS_ENABLED = EMIT_CHECKPOINTS + ENABLED_SUFFIX;
    private static final String EMIT_CHECKPOINTS_ENABLED_DOC = EMIT_CHECKPOINTS + ENABLED_SUFFIX;
    public static final boolean EMIT_CHECKPOINTS_ENABLED_DEFAULT = true;
    public static final String EMIT_CHECKPOINTS_INTERVAL_SECONDS = EMIT_CHECKPOINTS + INTERVAL_SECONDS_SUFFIX;
    private static final String EMIT_CHECKPOINTS_INTERVAL_SECONDS_DOC = EMIT_CHECKPOINTS + INTERVAL_SECONDS_SUFFIX;
    public static final long EMIT_CHECKPOINTS_INTERVAL_SECONDS_DEFAULT = 5;

    protected static final String PRODUCER_CLIENT_PREFIX = "producer.";
    protected static final String CONSUMER_CLIENT_PREFIX = "consumer.";
    protected static final String ADMIN_CLIENT_PREFIX = "admin.";
    protected static final String SOURCE_ADMIN_CLIENT_PREFIX = "source.admin.";
    protected static final String TARGET_ADMIN_CLIENT_PREFIX = "target.admin.";

    private static final Pattern MATCH_NOTHING = Pattern.compile("\\\0");

    public MirrorConnectorConfig(Map<?, ?> props) {
        this(CONNECTOR_CONFIG_DEF, props);
    }

    protected MirrorConnectorConfig(ConfigDef configDef, Map<?, ?> props) {
        super(configDef, props, true);
    }

    String connectorName() {
        return getString(NAME);
    }

    boolean enabled() {
        return getBoolean(ENABLED);
    }

    Duration consumerPollTimeout() {
        return Duration.ofMillis(getLong(CONSUMER_POLL_TIMEOUT_MILLIS));
    }

    Map<String, Object> sourceProducerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(PRODUCER_CLIENT_PREFIX));
        if (props.get(BOOTSTRAP_SERVERS) == null) {
            props.put(BOOTSTRAP_SERVERS, sourceClusterBootstrapServers());
        }
        return props;
    }

    Map<String, Object> sourceConsumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(CONSUMER_CLIENT_PREFIX));
        if (props.get(BOOTSTRAP_SERVERS) == null) {
            props.put(BOOTSTRAP_SERVERS, sourceClusterBootstrapServers());
        }
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
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
        props.putAll(originalsWithPrefix(ADMIN_CLIENT_PREFIX));
        props.putAll(originalsWithPrefix(TARGET_ADMIN_CLIENT_PREFIX));
        if (props.get(BOOTSTRAP_SERVERS) == null) {
            props.put(BOOTSTRAP_SERVERS, targetClusterBootstrapServers());
        }
        return props;
    }

    Map<String, Object> sourceAdminConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(ADMIN_CLIENT_PREFIX));
        props.putAll(originalsWithPrefix(SOURCE_ADMIN_CLIENT_PREFIX));
        if (props.get(BOOTSTRAP_SERVERS) == null) {
            props.put(BOOTSTRAP_SERVERS, sourceClusterBootstrapServers());
        }
        return props;
    }

    String sourceClusterAlias() {
        return getString(SOURCE_CLUSTER_ALIAS);
    }

    String targetClusterAlias() {
        return getString(TARGET_CLUSTER_ALIAS);
    }

    String sourceClusterBootstrapServers() {
        return getString(SOURCE_CLUSTER_BOOTSTRAP_SERVERS);
    }

    String targetClusterBootstrapServers() {
        return getString(TARGET_CLUSTER_BOOTSTRAP_SERVERS);
    }

    // Creates a pattern out of comma-separated names or regexes
    private Pattern patternListOrNothing(String prop) {
        List<String> fields = getList(prop);
        if (fields.isEmpty()) {
            // The empty pattern matches _everything_, but a blank
            // config property should match _nothing_.
            return MATCH_NOTHING;
        } else {
            String joined = String.join("|", fields);
            return Pattern.compile(joined);
        }
    }

    Pattern topicsPattern() {
        return patternListOrNothing(TOPICS);
    }

    Pattern topicsBlacklistPattern() {
        return patternListOrNothing(TOPICS_BLACKLIST);
    }

    Pattern groupsPattern() {
        return patternListOrNothing(GROUPS);
    }

    Pattern groupsBlacklistPattern() {
        return patternListOrNothing(GROUPS_BLACKLIST);
    }

    Pattern configPropertiesPattern() {
        return patternListOrNothing(CONFIG_PROPERTIES);
    }

    Pattern configPropertiesBlacklistPattern() {
        return patternListOrNothing(CONFIG_PROPERTIES_BLACKLIST);
    }

    String offsetSyncTopic() {
        // ".internal" suffix ensures this doesn't get replicated
        return targetClusterAlias() + ".offset-syncs.internal";
    }

    String heartbeatsTopic() {
        return MirrorClientConfig.HEARTBEATS_TOPIC;
    }

    // e.g. source1.heartbeats
    String sourceHeartbeatsTopic() {
        return replicationPolicy().formatRemoteTopic(sourceClusterAlias(), heartbeatsTopic());
    }

    String checkpointsTopic() {
        return replicationPolicy().formatRemoteTopic(sourceClusterAlias(), MirrorClientConfig.CHECKPOINTS_TOPIC);
    }

    long maxOffsetLag() {
        // Hard-coded for now, as we don't expose this property yet.
        return 100;
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

    protected static final ConfigDef CONNECTOR_CONFIG_DEF = new ConfigDef()
        .define(
            ENABLED,
            ConfigDef.Type.BOOLEAN,
            ENABLED_DEFAULT,
            ConfigDef.Importance.HIGH,
            ENABLED_DOC)
        .define(
            NAME,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            NAME_DOC)
        .define(
            TOPICS,
            ConfigDef.Type.LIST,
            TOPICS_DEFAULT,
            ConfigDef.Importance.HIGH,
            TOPICS_DOC)
        .define(
            TOPICS_BLACKLIST,
            ConfigDef.Type.LIST,
            TOPICS_BLACKLIST_DEFAULT,
            ConfigDef.Importance.HIGH,
            TOPICS_BLACKLIST_DOC)
        .define(
            GROUPS,
            ConfigDef.Type.LIST,
            GROUPS_DEFAULT,
            ConfigDef.Importance.HIGH,
            GROUPS_DOC)
        .define(
            GROUPS_BLACKLIST,
            ConfigDef.Type.LIST,
            GROUPS_BLACKLIST_DEFAULT,
            ConfigDef.Importance.HIGH,
            GROUPS_BLACKLIST_DOC)
        .define(
            CONFIG_PROPERTIES,
            ConfigDef.Type.LIST,
            CONFIG_PROPERTIES_DEFAULT,
            ConfigDef.Importance.HIGH,
            CONFIG_PROPERTIES_DOC)
        .define(
            CONFIG_PROPERTIES_BLACKLIST,
            ConfigDef.Type.LIST,
            CONFIG_PROPERTIES_BLACKLIST_DEFAULT,
            ConfigDef.Importance.HIGH,
            CONFIG_PROPERTIES_BLACKLIST_DOC)
        .define(
            SOURCE_CLUSTER_ALIAS,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            SOURCE_CLUSTER_ALIAS_DOC)
        .define(
            TARGET_CLUSTER_ALIAS,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            TARGET_CLUSTER_ALIAS_DOC)
        .define(
            SOURCE_CLUSTER_BOOTSTRAP_SERVERS,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            SOURCE_CLUSTER_BOOTSTRAP_SERVERS_DOC)
         .define(
            TARGET_CLUSTER_BOOTSTRAP_SERVERS,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            TARGET_CLUSTER_BOOTSTRAP_SERVERS_DOC)
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
            REPLICATION_POLICY_CLASS,
            ConfigDef.Type.CLASS,
            DefaultReplicationPolicy.class.getName(),
            ConfigDef.Importance.LOW,
            REPLICATION_POLICY_CLASS_DOC)
        .define(
            REPLICATION_POLICY_SEPARATOR,
            ConfigDef.Type.STRING,
            REPLICATION_POLICY_SEPARATOR_DEFAULT,
            ConfigDef.Importance.LOW,
            REPLICATION_POLICY_SEPARATOR_DEFAULT);

}
