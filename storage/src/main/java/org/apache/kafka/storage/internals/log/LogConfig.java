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
package org.apache.kafka.storage.internals.log;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.DOUBLE;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static org.apache.kafka.server.common.MetadataVersion.IBP_3_0_IV1;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidList;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ConfigUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.MetadataVersionValidator;
import org.apache.kafka.server.config.ServerTopicConfigSynonyms;
import org.apache.kafka.server.record.BrokerCompressionType;

public class LogConfig extends AbstractConfig {

    public static class MessageFormatVersion {
        private final String messageFormatVersionString;
        private final String interBrokerProtocolVersionString;
        private final MetadataVersion messageFormatVersion;
        private final MetadataVersion interBrokerProtocolVersion;

        public MessageFormatVersion(String messageFormatVersionString, String interBrokerProtocolVersionString) {
            this.messageFormatVersionString = messageFormatVersionString;
            this.interBrokerProtocolVersionString = interBrokerProtocolVersionString;
            this.messageFormatVersion = MetadataVersion.fromVersionString(messageFormatVersionString);
            this.interBrokerProtocolVersion = MetadataVersion.fromVersionString(interBrokerProtocolVersionString);
        }

        public MetadataVersion messageFormatVersion() {
            return messageFormatVersion;
        }

        public MetadataVersion interBrokerProtocolVersion() {
            return interBrokerProtocolVersion;
        }

        public boolean shouldIgnore() {
            return shouldIgnoreMessageFormatVersion(interBrokerProtocolVersion);
        }

        public boolean shouldWarn() {
            return interBrokerProtocolVersion.isAtLeast(IBP_3_0_IV1)
                && messageFormatVersion.highestSupportedRecordVersion().precedes(RecordVersion.V2);
        }

        @SuppressWarnings("deprecation")
        public String topicWarningMessage(String topicName) {
            return "Topic configuration " + TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG + " with value `"
                + messageFormatVersionString + "` is ignored for `" + topicName + "` because the "
                + "inter-broker protocol version `" + interBrokerProtocolVersionString + "` is greater or "
                + "equal than 3.0. This configuration is deprecated and it will be removed in Apache Kafka 4.0.";
        }
    }

    public static class RemoteLogConfig {

        public final boolean remoteStorageEnable;

        public final long localRetentionMs;
        public final long localRetentionBytes;

        private RemoteLogConfig(LogConfig config, long retentionMs, long retentionSize) {
            this.remoteStorageEnable = config.getBoolean(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG);

            long localLogRetentionMs = config.getLong(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG);

            // -2 indicates to derive value from retentionMs property.
            if (localLogRetentionMs == -2)
                this.localRetentionMs = retentionMs;
            else {
                // Added validation here to check the effective value should not be more than RetentionMs.
                if (localLogRetentionMs == -1 && retentionMs != -1)
                    throw new ConfigException(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, localLogRetentionMs,
                        "Value must not be -1 as " + TopicConfig.RETENTION_MS_CONFIG + " value is set as " + retentionMs);

                if (localLogRetentionMs > retentionMs)
                    throw new ConfigException(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, localLogRetentionMs,
                        "Value must not be more than property: " + TopicConfig.RETENTION_MS_CONFIG + " value.");

                this.localRetentionMs = localLogRetentionMs;
            }

            long localLogRetentionBytes = config.getLong(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG);

            // -2 indicates to derive value from retentionSize property.
            if (localLogRetentionBytes == -2)
                this.localRetentionBytes = retentionSize;
            else {
                // Added validation here to check the effective value should not be more than RetentionBytes.
                if (localLogRetentionBytes == -1 && retentionSize != -1)
                    throw new ConfigException(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, localLogRetentionBytes,
                        "Value must not be -1 as " + TopicConfig.RETENTION_BYTES_CONFIG + " value is set as " + retentionSize);

                if (localLogRetentionBytes > retentionSize)
                    throw new ConfigException(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, localLogRetentionBytes,
                        "Value must not be more than property: " + TopicConfig.RETENTION_BYTES_CONFIG + " value.");

                this.localRetentionBytes = localLogRetentionBytes;
            }
        }
    }

    // Visible for testing
    public static class LogConfigDef extends ConfigDef {
        public LogConfigDef() {
            this(new ConfigDef());
        }

        public LogConfigDef(ConfigDef base) {
            super(base);
        }

        @Override
        public List<String> headers() {
            return asList("Name", "Description", "Type", "Default", "Valid Values", SERVER_DEFAULT_HEADER_NAME, "Importance");
        }

        // Visible for testing
        @Override
        public String getConfigValue(ConfigKey key, String headerName) {
            if (headerName.equals(SERVER_DEFAULT_HEADER_NAME))
                return ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS.get(key.name);
            else
                return super.getConfigValue(key, headerName);
        }

        public Optional<String> serverConfigName(String configName) {
            return Optional.ofNullable(ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS.get(configName));
        }
    }

    // Visible for testing
    public static final String SERVER_DEFAULT_HEADER_NAME = "Server Default Property";

    public static final int DEFAULT_MAX_MESSAGE_BYTES = 1024 * 1024 + Records.LOG_OVERHEAD;
    public static final int DEFAULT_SEGMENT_BYTES = 1024 * 1024 * 1024;
    public static final long DEFAULT_SEGMENT_MS = 24 * 7 * 60 * 60 * 1000L;
    public static final long DEFAULT_SEGMENT_JITTER_MS = 0;
    public static final long DEFAULT_RETENTION_MS = 24 * 7 * 60 * 60 * 1000L;
    public static final long DEFAULT_RETENTION_BYTES = -1L;
    public static final int DEFAULT_SEGMENT_INDEX_BYTES = 10 * 1024 * 1024;
    public static final int DEFAULT_INDEX_INTERVAL_BYTES = 4096;
    public static final long DEFAULT_FILE_DELETE_DELAY_MS = 60000L;
    public static final String DEFAULT_CLEANUP_POLICY = TopicConfig.CLEANUP_POLICY_DELETE;
    public static final long DEFAULT_FLUSH_MESSAGES_INTERVAL = Long.MAX_VALUE;
    public static final long DEFAULT_FLUSH_MS = Long.MAX_VALUE;
    public static final long DEFAULT_DELETE_RETENTION_MS = 24 * 60 * 60 * 1000L;
    public static final long DEFAULT_MIN_COMPACTION_LAG_MS = 0;
    public static final long DEFAULT_MAX_COMPACTION_LAG_MS = Long.MAX_VALUE;
    public static final double DEFAULT_MIN_CLEANABLE_DIRTY_RATIO = 0.5;
    public static final boolean DEFAULT_UNCLEAN_LEADER_ELECTION_ENABLE = false;
    public static final int DEFAULT_MIN_IN_SYNC_REPLICAS = 1;
    public static final String DEFAULT_COMPRESSION_TYPE = BrokerCompressionType.PRODUCER.name;
    public static final boolean DEFAULT_PREALLOCATE = false;
    public static final String DEFAULT_MESSAGE_TIMESTAMP_TYPE = "CreateTime";
    public static final long DEFAULT_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS = Long.MAX_VALUE;
    public static final boolean DEFAULT_MESSAGE_DOWNCONVERSION_ENABLE = true;

    public static final boolean DEFAULT_REMOTE_STORAGE_ENABLE = false;
    public static final int DEFAULT_LOCAL_RETENTION_BYTES = -2; // It indicates the value to be derived from RetentionBytes
    public static final int DEFAULT_LOCAL_RETENTION_MS = -2; // It indicates the value to be derived from RetentionMs
    public static final List<String> DEFAULT_LEADER_REPLICATION_THROTTLED_REPLICAS = Collections.emptyList();
    public static final List<String> DEFAULT_FOLLOWER_REPLICATION_THROTTLED_REPLICAS = Collections.emptyList();

    /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
    @Deprecated
    public static final String DEFAULT_MESSAGE_FORMAT_VERSION = IBP_3_0_IV1.version();

    // Leave these out of TopicConfig for now as they are replication quota configs
    public static final String LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG = "leader.replication.throttled.replicas";
    public static final String FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG = "follower.replication.throttled.replicas";

    @SuppressWarnings("deprecation")
    private static final String MESSAGE_FORMAT_VERSION_CONFIG = TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG;

    // Visible for testing
    public static final Set<String> CONFIGS_WITH_NO_SERVER_DEFAULTS = Collections.unmodifiableSet(Utils.mkSet(
        TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG,
        TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG,
        TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG,
        LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
        FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG
    ));

    private static final String DEFAULT_LOG_DIR = "/tmp/kafka-logs";
    private static final String LOG_CONFIG_PREFIX = "log.";
    private static final String LOG_DIR_PROP = LOG_CONFIG_PREFIX + "dir";
    private static final String LOG_DIRS_PROP = LOG_CONFIG_PREFIX + "dirs";

    private static final String NODE_ID_PROP = "node.id";
    private static final String BROKER_ID_PROP = "broker.id";
    private static final String MAX_RESERVED_BROKER_ID_PROP = "reserved.broker.max.id";

    private static final int DEFAULT_EMPTY_NODE_ID = -1;

    private static final String METADATA_LOG_DIR_PROP = "metadata.log.dir";

    private static final String PROCESS_ROLES_PROP = "process.roles";

    private static final String INTER_BROKER_PROTOCOL_VERSION_PROP = "inter.broker.protocol.version";

    private static final String BROKER_ID_GENERATION_ENABLE_PROP = "broker.id.generation.enable";

    private static final String ZOOKEEPER_CONNECT_PROP = "zookeeper.connect";

    private static final String INTER_BROKER_PROTOCOL_VERSION_PROP_DEFAULT_VALUE = MetadataVersion.latest().version();
    private static final String LOG_DIR_DOC = "The directory in which the log data is kept (supplemental for " + LOG_DIR_PROP + " property)";
    private static final String METADATA_LOG_DIR_DOC = "This configuration determines where we put the metadata log for clusters in KRaft mode. " +
        "If it is not set, the metadata log is placed in the first log directory from log.dirs.";
    private static final String INTER_BROKER_PROTOCOL_VERSION_DOC = "Specify which version of the inter-broker protocol will be used.\n" +
        " This is typically bumped after all brokers were upgraded to a new version.\n" +
        " Example of some valid values are: 0.8.0, 0.8.1, 0.8.1.1, 0.8.2, 0.8.2.0, 0.8.2.1, 0.9.0.0, 0.9.0.1 Check MetadataVersion for the full list.";

    private static final String LOG_DIRS_DOC = "A comma-separated list of the directories where the log data is stored. If not set, the value in " + LOG_DIRS_PROP + " is used.";

    private static final String BROKER_ID_DOC = "The broker id for this server. If unset, a unique broker id will be generated." +
        "To avoid conflicts between ZooKeeper generated broker id's and user configured broker id's, generated broker ids " +
        "start from " + MAX_RESERVED_BROKER_ID_PROP + " + 1.";

    private static final String NODE_ID_DOC = "The node ID associated with the roles this process is playing when `process.roles` is non-empty. " +
        "This is required configuration when running in KRaft mode.";
    private static final String PROCESS_ROLES_DOC = "The roles that this process plays: 'broker', 'controller', or 'broker,controller' if it is both. " +
        "This configuration is only applicable for clusters in KRaft (Kafka Raft) mode (instead of ZooKeeper). Leave this config undefined or empty for ZooKeeper clusters.";

    private static final String BROKER_ID_GENERATION_ENABLE_DOC = "Enable automatic broker id generation on the server. When enabled the value configured for $MaxReservedBrokerIdProp should be reviewed.";

    private static final String MAX_RESERVED_BROKER_ID_DOC = "Max number that can be used for a broker.id";
    private static final String ZK_CONNECT_DOC = "Specifies the ZooKeeper connection string in the form <code>hostname:port</code> where host and port are the " +
        "host and port of a ZooKeeper server. To allow connecting through other ZooKeeper nodes when that ZooKeeper machine is " +
        "down you can also specify multiple hosts in the form <code>hostname1:port1,hostname2:port2,hostname3:port3</code>.\n" +
        "The server can also have a ZooKeeper chroot path as part of its ZooKeeper connection string which puts its data under some path in the global ZooKeeper namespace. " +
        "For example to give a chroot path of <code>/chroot/path</code> you would give the connection string as <code>hostname1:port1,hostname2:port2,hostname3:port3/chroot/path</code>.";

    public static final String LEADER_REPLICATION_THROTTLED_REPLICAS_DOC = "A list of replicas for which log replication should be throttled on " +
        "the leader side. The list should describe a set of replicas in the form " +
        "[PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can be used to throttle " +
        "all replicas for this topic.";
    public static final String FOLLOWER_REPLICATION_THROTTLED_REPLICAS_DOC = "A list of replicas for which log replication should be throttled on " +
        "the follower side. The list should describe a set of " + "replicas in the form " +
        "[PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can be used to throttle " +
        "all replicas for this topic.";

    @SuppressWarnings("deprecation")
    private static final String MESSAGE_FORMAT_VERSION_DOC = TopicConfig.MESSAGE_FORMAT_VERSION_DOC;

    @SuppressWarnings("deprecation")
    private static final LogConfigDef CONFIG = new LogConfigDef();
    static {
        CONFIG.
            define(TopicConfig.SEGMENT_BYTES_CONFIG, INT, DEFAULT_SEGMENT_BYTES, atLeast(LegacyRecord.RECORD_OVERHEAD_V0), MEDIUM,
                TopicConfig.SEGMENT_BYTES_DOC)
            .define(TopicConfig.SEGMENT_MS_CONFIG, LONG, DEFAULT_SEGMENT_MS, atLeast(1), MEDIUM, TopicConfig.SEGMENT_MS_DOC)
            .define(TopicConfig.SEGMENT_JITTER_MS_CONFIG, LONG, DEFAULT_SEGMENT_JITTER_MS, atLeast(0), MEDIUM,
                TopicConfig.SEGMENT_JITTER_MS_DOC)
            .define(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, INT, DEFAULT_SEGMENT_INDEX_BYTES, atLeast(4), MEDIUM,
                TopicConfig.SEGMENT_INDEX_BYTES_DOC)
            .define(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, LONG, DEFAULT_FLUSH_MESSAGES_INTERVAL, atLeast(1), MEDIUM,
                TopicConfig.FLUSH_MESSAGES_INTERVAL_DOC)
            .define(TopicConfig.FLUSH_MS_CONFIG, LONG, DEFAULT_FLUSH_MS, atLeast(0), MEDIUM,
                TopicConfig.FLUSH_MS_DOC)
            // can be negative. See kafka.log.LogManager.cleanupSegmentsToMaintainSize
            .define(TopicConfig.RETENTION_BYTES_CONFIG, LONG, DEFAULT_RETENTION_BYTES, MEDIUM, TopicConfig.RETENTION_BYTES_DOC)
            // can be negative. See kafka.log.LogManager.cleanupExpiredSegments
            .define(TopicConfig.RETENTION_MS_CONFIG, LONG, DEFAULT_RETENTION_MS, atLeast(-1), MEDIUM,
                TopicConfig.RETENTION_MS_DOC)
            .define(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, INT, DEFAULT_MAX_MESSAGE_BYTES, atLeast(0), MEDIUM,
                TopicConfig.MAX_MESSAGE_BYTES_DOC)
            .define(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, INT, DEFAULT_INDEX_INTERVAL_BYTES, atLeast(0), MEDIUM,
                TopicConfig.INDEX_INTERVAL_BYTES_DOC)
            .define(TopicConfig.DELETE_RETENTION_MS_CONFIG, LONG, DEFAULT_DELETE_RETENTION_MS, atLeast(0), MEDIUM,
                TopicConfig.DELETE_RETENTION_MS_DOC)
            .define(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, LONG, DEFAULT_MIN_COMPACTION_LAG_MS, atLeast(0), MEDIUM,
                TopicConfig.MIN_COMPACTION_LAG_MS_DOC)
            .define(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, LONG, DEFAULT_MAX_COMPACTION_LAG_MS, atLeast(1), MEDIUM,
                TopicConfig.MAX_COMPACTION_LAG_MS_DOC)
            .define(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, LONG, DEFAULT_FILE_DELETE_DELAY_MS, atLeast(0), MEDIUM,
                TopicConfig.FILE_DELETE_DELAY_MS_DOC)
            .define(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, DOUBLE, DEFAULT_MIN_CLEANABLE_DIRTY_RATIO, between(0, 1), MEDIUM,
                TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_DOC)
            .define(TopicConfig.CLEANUP_POLICY_CONFIG, LIST, DEFAULT_CLEANUP_POLICY, ValidList.in(TopicConfig.CLEANUP_POLICY_COMPACT,
                TopicConfig.CLEANUP_POLICY_DELETE), MEDIUM, TopicConfig.CLEANUP_POLICY_DOC)
            .define(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, BOOLEAN, DEFAULT_UNCLEAN_LEADER_ELECTION_ENABLE,
                MEDIUM, TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_DOC)
            .define(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, INT, DEFAULT_MIN_IN_SYNC_REPLICAS, atLeast(1), MEDIUM,
                TopicConfig.MIN_IN_SYNC_REPLICAS_DOC)
            .define(TopicConfig.COMPRESSION_TYPE_CONFIG, STRING, DEFAULT_COMPRESSION_TYPE, in(BrokerCompressionType.names().toArray(new String[0])),
                MEDIUM, TopicConfig.COMPRESSION_TYPE_DOC)
            .define(TopicConfig.PREALLOCATE_CONFIG, BOOLEAN, DEFAULT_PREALLOCATE, MEDIUM, TopicConfig.PREALLOCATE_DOC)
            .define(MESSAGE_FORMAT_VERSION_CONFIG, STRING, DEFAULT_MESSAGE_FORMAT_VERSION, new MetadataVersionValidator(), MEDIUM,
                MESSAGE_FORMAT_VERSION_DOC)
            .define(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, STRING, DEFAULT_MESSAGE_TIMESTAMP_TYPE,
                in("CreateTime", "LogAppendTime"), MEDIUM, TopicConfig.MESSAGE_TIMESTAMP_TYPE_DOC)
            .define(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, LONG, DEFAULT_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS,
                atLeast(0), MEDIUM, TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC)
            .define(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, LIST, DEFAULT_LEADER_REPLICATION_THROTTLED_REPLICAS,
                ThrottledReplicaListValidator.INSTANCE, MEDIUM, LEADER_REPLICATION_THROTTLED_REPLICAS_DOC)
            .define(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, LIST, DEFAULT_FOLLOWER_REPLICATION_THROTTLED_REPLICAS,
                ThrottledReplicaListValidator.INSTANCE, MEDIUM, FOLLOWER_REPLICATION_THROTTLED_REPLICAS_DOC)
            .define(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, BOOLEAN, DEFAULT_MESSAGE_DOWNCONVERSION_ENABLE, LOW,
                TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_DOC)
            .define(LOG_DIRS_PROP, STRING, null, HIGH, LOG_DIRS_DOC)
            .define(LOG_DIR_PROP, STRING, DEFAULT_LOG_DIR, HIGH, LOG_DIR_DOC)
            .define(METADATA_LOG_DIR_PROP, STRING, null, HIGH, METADATA_LOG_DIR_DOC)
            .define(INTER_BROKER_PROTOCOL_VERSION_PROP, STRING, INTER_BROKER_PROTOCOL_VERSION_PROP_DEFAULT_VALUE, new MetadataVersionValidator(), MEDIUM, INTER_BROKER_PROTOCOL_VERSION_DOC)
            .define(NODE_ID_PROP, INT, DEFAULT_EMPTY_NODE_ID, null, HIGH, NODE_ID_DOC)
            .define(BROKER_ID_PROP, INT, -1, HIGH, BROKER_ID_DOC)
            .define(MAX_RESERVED_BROKER_ID_PROP, INT, 1000, atLeast(0), MEDIUM, MAX_RESERVED_BROKER_ID_DOC)
            .define(PROCESS_ROLES_PROP, LIST, Collections.emptyList(), ValidList.in("broker", "controller"), HIGH, PROCESS_ROLES_DOC)
            .define(ZOOKEEPER_CONNECT_PROP, STRING, null, HIGH, ZK_CONNECT_DOC)
            .define(BROKER_ID_GENERATION_ENABLE_PROP, BOOLEAN, true, MEDIUM, BROKER_ID_GENERATION_ENABLE_DOC)
            .defineInternal(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, BOOLEAN, DEFAULT_REMOTE_STORAGE_ENABLE, null,
                MEDIUM, TopicConfig.REMOTE_LOG_STORAGE_ENABLE_DOC)
            .defineInternal(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, LONG, DEFAULT_LOCAL_RETENTION_MS, atLeast(-2), MEDIUM,
                TopicConfig.LOCAL_LOG_RETENTION_MS_DOC)
            .defineInternal(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, LONG, DEFAULT_LOCAL_RETENTION_BYTES, atLeast(-2), MEDIUM,
                TopicConfig.LOCAL_LOG_RETENTION_BYTES_DOC);
    }

    public final Set<String> overriddenConfigs;

    /*
     * Important note: Any configuration parameter that is passed along from KafkaConfig to LogConfig
     * should also be in `KafkaConfig#extractLogConfigMap`.
     */
    public final int segmentSize;
    public final long segmentMs;
    public final long segmentJitterMs;
    public final int maxIndexSize;
    public final long flushInterval;
    public final long flushMs;
    public final long retentionSize;
    public final long retentionMs;
    public final int indexInterval;
    public final long fileDeleteDelayMs;
    public final long deleteRetentionMs;
    public final long compactionLagMs;
    public final long maxCompactionLagMs;
    public final double minCleanableRatio;
    public final boolean compact;
    public final boolean delete;
    public final boolean uncleanLeaderElectionEnable;
    public final int minInSyncReplicas;
    public final String compressionType;
    public final boolean preallocate;
    public final String interBrokerProtocolVersion;
    public int nodeId;
    public int brokerId;
    public final String logDirs;
    public final String logDir;
    public Set<String> processRoles;
    public boolean brokerIdGenerationEnable;
    public int maxReservedBrokerId;
    public String zkConnect;

    /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details regarding the deprecation */
    @Deprecated
    public final MetadataVersion messageFormatVersion;

    public final TimestampType messageTimestampType;
    public final long messageTimestampDifferenceMaxMs;
    public final List<String> leaderReplicationThrottledReplicas;
    public final List<String> followerReplicationThrottledReplicas;
    public final boolean messageDownConversionEnable;
    public final RemoteLogConfig remoteLogConfig;

    private final int maxMessageSize;
    private final Map<?, ?> props;

    public LogConfig(Map<?, ?> props) {
        this(props, Collections.emptySet());
    }

    @SuppressWarnings("deprecation")
    public LogConfig(Map<?, ?> props, Set<String> overriddenConfigs) {
        super(CONFIG, props, false);
        this.props = Collections.unmodifiableMap(props);
        this.overriddenConfigs = Collections.unmodifiableSet(overriddenConfigs);

        this.segmentSize = getInt(TopicConfig.SEGMENT_BYTES_CONFIG);
        this.segmentMs = getLong(TopicConfig.SEGMENT_MS_CONFIG);
        this.segmentJitterMs = getLong(TopicConfig.SEGMENT_JITTER_MS_CONFIG);
        this.maxIndexSize = getInt(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG);
        this.flushInterval = getLong(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG);
        this.flushMs = getLong(TopicConfig.FLUSH_MS_CONFIG);
        this.retentionSize = getLong(TopicConfig.RETENTION_BYTES_CONFIG);
        this.retentionMs = getLong(TopicConfig.RETENTION_MS_CONFIG);
        this.maxMessageSize = getInt(TopicConfig.MAX_MESSAGE_BYTES_CONFIG);
        this.indexInterval = getInt(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG);
        this.fileDeleteDelayMs = getLong(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG);
        this.deleteRetentionMs = getLong(TopicConfig.DELETE_RETENTION_MS_CONFIG);
        this.compactionLagMs = getLong(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG);
        this.maxCompactionLagMs = getLong(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG);
        this.minCleanableRatio = getDouble(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG);
        this.compact = getList(TopicConfig.CLEANUP_POLICY_CONFIG).stream()
            .map(c -> c.toLowerCase(Locale.ROOT))
            .collect(Collectors.toList())
            .contains(TopicConfig.CLEANUP_POLICY_COMPACT);
        this.delete = getList(TopicConfig.CLEANUP_POLICY_CONFIG).stream()
            .map(c -> c.toLowerCase(Locale.ROOT))
            .collect(Collectors.toList())
            .contains(TopicConfig.CLEANUP_POLICY_DELETE);
        this.uncleanLeaderElectionEnable = getBoolean(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG);
        this.minInSyncReplicas = getInt(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
        this.compressionType = getString(TopicConfig.COMPRESSION_TYPE_CONFIG).toLowerCase(Locale.ROOT);
        this.preallocate = getBoolean(TopicConfig.PREALLOCATE_CONFIG);
        this.messageFormatVersion = MetadataVersion.fromVersionString(getString(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG));
        this.messageTimestampType = TimestampType.forName(getString(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG));
        this.messageTimestampDifferenceMaxMs = getLong(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG);
        this.leaderReplicationThrottledReplicas = Collections.unmodifiableList(getList(LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG));
        this.followerReplicationThrottledReplicas = Collections.unmodifiableList(getList(LogConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG));
        this.messageDownConversionEnable = getBoolean(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG);
        this.interBrokerProtocolVersion = getString(INTER_BROKER_PROTOCOL_VERSION_PROP);
        this.brokerId = getInt(BROKER_ID_PROP);
        this.maxReservedBrokerId = getInt(MAX_RESERVED_BROKER_ID_PROP);
        this.nodeId = getInt(LogConfig.NODE_ID_PROP);
        this.brokerIdGenerationEnable = getBoolean(BROKER_ID_GENERATION_ENABLE_PROP);
        this.logDirs = getString(LOG_DIRS_PROP);
        this.logDir = getString(LOG_DIR_PROP);
        processRoles = parseProcessRoles();
        this.zkConnect = getString(ZOOKEEPER_CONNECT_PROP);
        remoteLogConfig = new RemoteLogConfig(this, retentionMs, retentionSize);
    }

    public LogConfig(Map<?, ?> props, Set<String> overriddenConfigs, boolean validateConfig) {
        this(LogConfig.populateSynonyms(props), overriddenConfigs);
        if (validateConfig) {
            validateOtherConfig();
        }
    }

    @SuppressWarnings("deprecation")
    public RecordVersion recordVersion() {
        return messageFormatVersion.highestSupportedRecordVersion();
    }

    // Exposed as a method so it can be mocked
    public int maxMessageSize() {
        return maxMessageSize;
    }

    public long randomSegmentJitter() {
        if (segmentJitterMs == 0)
            return 0;
        else
            return Utils.abs(ThreadLocalRandom.current().nextInt()) % Math.min(segmentJitterMs, segmentMs);
    }

    public long maxSegmentMs() {
        if (compact && maxCompactionLagMs > 0)
            return Math.min(maxCompactionLagMs, segmentMs);
        else
            return segmentMs;
    }

    public int initFileSize() {
        if (preallocate)
            return segmentSize;
        else
            return 0;
    }

    public String overriddenConfigsAsLoggableString() {
        Map<String, Object> overriddenTopicProps = new HashMap<>();
        props.forEach((k, v) -> {
            if (overriddenConfigs.contains(k))
                overriddenTopicProps.put((String) k, v);
        });
        return ConfigUtils.configMapToRedactedString(overriddenTopicProps, CONFIG);
    }

    /**
     * Create a log config instance using the given properties and defaults
     */
    public static LogConfig fromProps(Map<?, ?> defaults, Properties overrides) {
        Properties props = new Properties();
        defaults.forEach((k, v) -> props.put(k, v));
        props.putAll(overrides);
        Set<String> overriddenKeys = overrides.keySet().stream().map(k -> (String) k).collect(Collectors.toSet());
        return new LogConfig(props, overriddenKeys);
    }

    // Visible for testing, return a copy since it's a mutable global variable
    public static LogConfigDef configDefCopy() {
        return new LogConfigDef(CONFIG);
    }

    public static boolean shouldIgnoreMessageFormatVersion(MetadataVersion interBrokerProtocolVersion) {
        return interBrokerProtocolVersion.isAtLeast(IBP_3_0_IV1);
    }

    public static Optional<Type> configType(String configName) {
        return Optional.ofNullable(CONFIG.configKeys().get(configName)).map(c -> c.type);
    }

    public static List<String> configNames() {
        return CONFIG.names().stream().sorted().collect(Collectors.toList());
    }

    public static Optional<String> serverConfigName(String configName) {
        return CONFIG.serverConfigName(configName);
    }

    public static Map<String, ConfigKey> configKeys() {
        return Collections.unmodifiableMap(CONFIG.configKeys());
    }

    /**
     * Check that property names are valid
     */
    public static void validateNames(Properties props) {
        List<String> names = configNames();
        for (Object name : props.keySet())
            if (!names.contains(name))
                throw new InvalidConfigurationException("Unknown topic config name: " + name);
    }

    public static void validateValues(Map<?, ?> props) {
        long minCompactionLag = (Long) props.get(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG);
        long maxCompactionLag = (Long) props.get(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG);
        if (minCompactionLag > maxCompactionLag) {
            throw new InvalidConfigurationException("conflict topic config setting "
                + TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG + " (" + minCompactionLag + ") > "
                + TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG + " (" + maxCompactionLag + ")");
        }
    }

    /**
     * Check that the given properties contain only valid log config names and that all values can be parsed and are valid
     */
    public static void validate(Properties props) {
        validateNames(props);
        Map<?, ?> valueMaps = CONFIG.parse(props);
        validateValues(valueMaps);
    }

    public void validateOtherConfig() {
        if (requiresZookeeper()) {
            if (zkConnect == null) {
                throw new ConfigException("Missing required configuration `" + ZOOKEEPER_CONNECT_PROP + "` which has no default value.");
            }
            if (brokerIdGenerationEnable) {
                require(brokerId >= -1 && brokerId <= maxReservedBrokerId, "broker.id must be greater than or equal to -1 and not greater than reserved.broker.max.id");
            } else {
                require(brokerId >= 0, "broker.id must be greater than or equal to 0");
            }
        } else {
            // KRaft-based metadata quorum
            if (nodeId < 0) {
                throw new ConfigException("Missing configuration `" + NODE_ID_PROP + "` which is required " +
                    "when `process.roles` is defined (i.e. when running in KRaft mode).");
            }
        }
    }

    public static void require(boolean requirement, String message) {
        if (!requirement) {
            throw new IllegalArgumentException("requirement failed: " + message);
        }
    }

    private static final String QUORUM_PREFIX = "controller.quorum.";
    private static final String QUORUM_VOTERS_CONFIG = QUORUM_PREFIX + "voters";
    private static final String CONTROLLER_LISTENER_NAMES_PROP = "controller.listener.names";


    String metadataLogDir = getString(METADATA_LOG_DIR_PROP);

    public String getInterBrokerProtocolVersionString() {
        return getString(INTER_BROKER_PROTOCOL_VERSION_PROP);
    }
    public int getNodeId() {
        return getInt(NODE_ID_PROP);
    }

    public static String getLogDirsProp() {
        return LOG_DIRS_PROP;
    }

    public static String getProcessRolesProp() {
        return PROCESS_ROLES_PROP;
    }

    public static String getNodeIdProp() {
        return NODE_ID_PROP;
    }

    public static String getQuorumVotersProp() {
        return QUORUM_VOTERS_CONFIG;
    }

    public static String getControllerListenerNamesProp() {
        return CONTROLLER_LISTENER_NAMES_PROP;
    }

    public static String getMetadataLogDirProp() {
        return METADATA_LOG_DIR_PROP;
    }

    public Set<String> parseProcessRoles() {
        List<String> roles = getList(PROCESS_ROLES_PROP);

        Set<String> distinctRoles = new HashSet<>();
        for (String role : roles) {
            switch (role) {
                case "broker":
                    distinctRoles.add("BrokerRole");
                    break;
                case "controller":
                    distinctRoles.add("ControllerRole");
                    break;
                default:
                    throw new ConfigException("Unknown process role '" + role + "'" +
                        " (only 'broker' and 'controller' are allowed roles)");
            }
        }

        if (distinctRoles.size() != roles.size()) {
            throw new ConfigException("Duplicate role names found in '" + PROCESS_ROLES_PROP + "': " + roles);
        }

        return distinctRoles;
    }


    public String getMetadataLogDir() {
        if (metadataLogDir != null) {
            return metadataLogDir;
        } else {
            return getLogDirs().get(0);
        }
    }

    public List<String> getLogDirs() {
        return parseCsvList(logDirs != null ? logDirs : logDir);
    }
    public static List<String> parseCsvList(String csvList) {
        if (csvList == null || csvList.isEmpty())
            return new ArrayList<>();
        else
            return Arrays.stream(csvList.split("\\s*,\\s*"))
                .filter(v -> !v.equals(""))
                .collect(Collectors.toList());
    }

    public boolean requiresZookeeper() {
        return processRoles.isEmpty();
    }

    public static Map<Object, Object> populateSynonyms(Map<?, ?> input) {
        Map<Object, Object> output = new HashMap<>(input);
        Object brokerId = output.get(LogConfig.BROKER_ID_PROP);
        Object nodeId = output.get(LogConfig.NODE_ID_PROP);

        if (brokerId == null && nodeId != null) {
            output.put(LogConfig.BROKER_ID_PROP, nodeId);
        } else if (brokerId != null && nodeId == null) {
            output.put(LogConfig.NODE_ID_PROP, brokerId);
        }

        return output;
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtml(4, config -> "topicconfigs_" + config));
    }
}
