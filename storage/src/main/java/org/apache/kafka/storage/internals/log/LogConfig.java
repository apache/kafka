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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidList;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ConfigUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.MetadataVersionValidator;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.config.ServerTopicConfigSynonyms;
import org.apache.kafka.server.record.BrokerCompressionType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;
import static org.apache.kafka.common.config.ConfigDef.Type.DOUBLE;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;
import static org.apache.kafka.server.common.MetadataVersion.IBP_3_0_IV1;

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

    private static class RemoteLogConfig {

        private final boolean remoteStorageEnable;
        private final boolean remoteLogDeleteOnDisable;
        private final boolean remoteLogCopyDisable;
        private final long localRetentionMs;
        private final long localRetentionBytes;

        private RemoteLogConfig(LogConfig config) {
            this.remoteStorageEnable = config.getBoolean(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG);
            this.remoteLogCopyDisable = config.getBoolean(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG);
            this.remoteLogDeleteOnDisable = config.getBoolean(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG);
            this.localRetentionMs = config.getLong(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG);
            this.localRetentionBytes = config.getLong(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG);
        }

        @Override
        public String toString() {
            return "RemoteLogConfig{" +
                    "remoteStorageEnable=" + remoteStorageEnable +
                    ", remoteLogCopyDisable=" + remoteLogCopyDisable +
                    ", remoteLogDeleteOnDisable=" + remoteLogDeleteOnDisable +
                    ", localRetentionMs=" + localRetentionMs +
                    ", localRetentionBytes=" + localRetentionBytes +
                    '}';
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
    public static final long DEFAULT_DELETE_RETENTION_MS = 24 * 60 * 60 * 1000L;
    public static final long DEFAULT_MIN_COMPACTION_LAG_MS = 0;
    public static final long DEFAULT_MAX_COMPACTION_LAG_MS = Long.MAX_VALUE;
    public static final double DEFAULT_MIN_CLEANABLE_DIRTY_RATIO = 0.5;
    public static final boolean DEFAULT_UNCLEAN_LEADER_ELECTION_ENABLE = false;
    public static final String DEFAULT_COMPRESSION_TYPE = BrokerCompressionType.PRODUCER.name;
    public static final boolean DEFAULT_PREALLOCATE = false;
    @Deprecated
    public static final long DEFAULT_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS = ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DEFAULT;

    public static final boolean DEFAULT_REMOTE_STORAGE_ENABLE = false;
    public static final boolean DEFAULT_REMOTE_LOG_COPY_DISABLE_CONFIG = false;
    public static final boolean DEFAULT_REMOTE_LOG_DELETE_ON_DISABLE_CONFIG = false;
    public static final long DEFAULT_LOCAL_RETENTION_BYTES = -2; // It indicates the value to be derived from RetentionBytes
    public static final long DEFAULT_LOCAL_RETENTION_MS = -2; // It indicates the value to be derived from RetentionMs

    /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details
     * Keep DEFAULT_MESSAGE_FORMAT_VERSION as a way to handle the deprecated value */
    @Deprecated
    public static final String DEFAULT_MESSAGE_FORMAT_VERSION = ServerLogConfigs.LOG_MESSAGE_FORMAT_VERSION_DEFAULT;

    /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
    @SuppressWarnings("deprecation")
    private static final String MESSAGE_FORMAT_VERSION_CONFIG = TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG;

    @SuppressWarnings("deprecation")
    private static final String MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG = TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG;

    @SuppressWarnings("deprecation")
    private static final String MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC = TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC;

    // Visible for testing
    public static final Set<String> CONFIGS_WITH_NO_SERVER_DEFAULTS = Set.of(
            TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG,
            TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG,
            TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG,
            QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
            QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG
    );

    @SuppressWarnings("deprecation")
    private static final String MESSAGE_FORMAT_VERSION_DOC = TopicConfig.MESSAGE_FORMAT_VERSION_DOC;

    @SuppressWarnings("deprecation")
    public static final ConfigDef SERVER_CONFIG_DEF = new ConfigDef()
            .define(ServerLogConfigs.NUM_PARTITIONS_CONFIG, INT, ServerLogConfigs.NUM_PARTITIONS_DEFAULT, atLeast(1), MEDIUM, ServerLogConfigs.NUM_PARTITIONS_DOC)
            .define(ServerLogConfigs.LOG_DIR_CONFIG, STRING, ServerLogConfigs.LOG_DIR_DEFAULT, HIGH, ServerLogConfigs.LOG_DIR_DOC)
            .define(ServerLogConfigs.LOG_DIRS_CONFIG, STRING, null, HIGH, ServerLogConfigs.LOG_DIRS_DOC)
            .define(ServerLogConfigs.LOG_SEGMENT_BYTES_CONFIG, INT, DEFAULT_SEGMENT_BYTES, atLeast(LegacyRecord.RECORD_OVERHEAD_V0), HIGH, ServerLogConfigs.LOG_SEGMENT_BYTES_DOC)

            .define(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG, LONG, null, HIGH, ServerLogConfigs.LOG_ROLL_TIME_MILLIS_DOC)
            .define(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG, INT, (int) TimeUnit.MILLISECONDS.toHours(DEFAULT_SEGMENT_MS), atLeast(1), HIGH, ServerLogConfigs.LOG_ROLL_TIME_HOURS_DOC)

            .define(ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_CONFIG, LONG, null, HIGH, ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_DOC)
            .define(ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_CONFIG, INT, (int) TimeUnit.MILLISECONDS.toHours(DEFAULT_SEGMENT_JITTER_MS), atLeast(0), HIGH, ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_DOC)

            .define(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, LONG, null, HIGH, ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_DOC)
            .define(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG, INT, null, HIGH, ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_DOC)
            .define(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, INT, (int) TimeUnit.MILLISECONDS.toHours(DEFAULT_RETENTION_MS), HIGH, ServerLogConfigs.LOG_RETENTION_TIME_HOURS_DOC)

            .define(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG, LONG, ServerLogConfigs.LOG_RETENTION_BYTES_DEFAULT, HIGH, ServerLogConfigs.LOG_RETENTION_BYTES_DOC)
            .define(ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG, LONG, ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_DEFAULT, atLeast(1), MEDIUM, ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_DOC)
            .define(ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG, LIST, ServerLogConfigs.LOG_CLEANUP_POLICY_DEFAULT, ConfigDef.ValidList.in(TopicConfig.CLEANUP_POLICY_COMPACT, TopicConfig.CLEANUP_POLICY_DELETE), MEDIUM, ServerLogConfigs.LOG_CLEANUP_POLICY_DOC)
            .define(ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_CONFIG, INT, ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_DEFAULT, atLeast(4), MEDIUM, ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_DOC)
            .define(ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_CONFIG, INT, ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_DEFAULT, atLeast(0), MEDIUM, ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_DOC)
            .define(ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG, LONG, ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_DEFAULT, atLeast(1), HIGH, ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_DOC)
            .define(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG, LONG, ServerLogConfigs.LOG_DELETE_DELAY_MS_DEFAULT, atLeast(0), HIGH, ServerLogConfigs.LOG_DELETE_DELAY_MS_DOC)
            .define(ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG, LONG, ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_DEFAULT, HIGH, ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_DOC)
            .define(ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG, LONG, null, HIGH, ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_DOC)
            .define(ServerLogConfigs.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG, INT, ServerLogConfigs.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_DEFAULT, atLeast(0), HIGH, ServerLogConfigs.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_DOC)
            .define(ServerLogConfigs.LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG, INT, ServerLogConfigs.LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_DEFAULT, atLeast(0), HIGH, ServerLogConfigs.LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_DOC)
            .define(ServerLogConfigs.LOG_PRE_ALLOCATE_CONFIG, BOOLEAN, DEFAULT_PREALLOCATE, MEDIUM, ServerLogConfigs.LOG_PRE_ALLOCATE_ENABLE_DOC)
            .define(ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG, INT, ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_DEFAULT, atLeast(1), HIGH, ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_DOC)
            .define(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, BOOLEAN, ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_DEFAULT, HIGH, ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_DOC)
            .define(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG, INT, ServerLogConfigs.MIN_IN_SYNC_REPLICAS_DEFAULT, atLeast(1), HIGH, ServerLogConfigs.MIN_IN_SYNC_REPLICAS_DOC)
            .define(ServerLogConfigs.LOG_MESSAGE_FORMAT_VERSION_CONFIG, STRING, ServerLogConfigs.LOG_MESSAGE_FORMAT_VERSION_DEFAULT, new MetadataVersionValidator(), MEDIUM, ServerLogConfigs.LOG_MESSAGE_FORMAT_VERSION_DOC)
            .define(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG, STRING, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_DEFAULT, ConfigDef.ValidString.in("CreateTime", "LogAppendTime"), MEDIUM, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_DOC)
            .define(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, LONG, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DEFAULT, atLeast(0), MEDIUM, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC)
            .define(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, LONG, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DEFAULT, atLeast(0), MEDIUM, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DOC)
            .define(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, LONG, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_DEFAULT, atLeast(0), MEDIUM, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_DOC)
            .define(ServerLogConfigs.CREATE_TOPIC_POLICY_CLASS_NAME_CONFIG, CLASS, null, LOW, ServerLogConfigs.CREATE_TOPIC_POLICY_CLASS_NAME_DOC)
            .define(ServerLogConfigs.ALTER_CONFIG_POLICY_CLASS_NAME_CONFIG, CLASS, null, LOW, ServerLogConfigs.ALTER_CONFIG_POLICY_CLASS_NAME_DOC)
            .define(ServerLogConfigs.LOG_MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, BOOLEAN, ServerLogConfigs.LOG_MESSAGE_DOWNCONVERSION_ENABLE_DEFAULT, LOW, ServerLogConfigs.LOG_MESSAGE_DOWNCONVERSION_ENABLE_DOC)
            .define(ServerLogConfigs.LOG_DIR_FAILURE_TIMEOUT_MS_CONFIG, LONG, ServerLogConfigs.LOG_DIR_FAILURE_TIMEOUT_MS_DEFAULT, atLeast(1), LOW, ServerLogConfigs.LOG_DIR_FAILURE_TIMEOUT_MS_DOC)
            .defineInternal(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG, LONG, ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_DEFAULT, atLeast(0), LOW, ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_DOC);

    private static final LogConfigDef CONFIG = new LogConfigDef();
    static {
        CONFIG.
                define(TopicConfig.SEGMENT_BYTES_CONFIG, INT, DEFAULT_SEGMENT_BYTES, atLeast(LegacyRecord.RECORD_OVERHEAD_V0), MEDIUM,
                        TopicConfig.SEGMENT_BYTES_DOC)
                .define(TopicConfig.SEGMENT_MS_CONFIG, LONG, DEFAULT_SEGMENT_MS, atLeast(1), MEDIUM, TopicConfig.SEGMENT_MS_DOC)
                .define(TopicConfig.SEGMENT_JITTER_MS_CONFIG, LONG, DEFAULT_SEGMENT_JITTER_MS, atLeast(0), MEDIUM,
                        TopicConfig.SEGMENT_JITTER_MS_DOC)
                .define(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, INT, ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_DEFAULT, atLeast(4), MEDIUM,
                        TopicConfig.SEGMENT_INDEX_BYTES_DOC)
                .define(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, LONG, ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_DEFAULT, atLeast(1), MEDIUM,
                        TopicConfig.FLUSH_MESSAGES_INTERVAL_DOC)
                .define(TopicConfig.FLUSH_MS_CONFIG, LONG, ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_DEFAULT, atLeast(0), MEDIUM,
                        TopicConfig.FLUSH_MS_DOC)
                // can be negative. See kafka.log.LogManager.cleanupSegmentsToMaintainSize
                .define(TopicConfig.RETENTION_BYTES_CONFIG, LONG, ServerLogConfigs.LOG_RETENTION_BYTES_DEFAULT, MEDIUM, TopicConfig.RETENTION_BYTES_DOC)
                // can be negative. See kafka.log.LogManager.cleanupExpiredSegments
                .define(TopicConfig.RETENTION_MS_CONFIG, LONG, DEFAULT_RETENTION_MS, atLeast(-1), MEDIUM,
                        TopicConfig.RETENTION_MS_DOC)
                .define(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, INT, DEFAULT_MAX_MESSAGE_BYTES, atLeast(0), MEDIUM,
                        TopicConfig.MAX_MESSAGE_BYTES_DOC)
                .define(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, INT, ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_DEFAULT, atLeast(0), MEDIUM,
                        TopicConfig.INDEX_INTERVAL_BYTES_DOC)
                .define(TopicConfig.DELETE_RETENTION_MS_CONFIG, LONG, DEFAULT_DELETE_RETENTION_MS, atLeast(0), MEDIUM,
                        TopicConfig.DELETE_RETENTION_MS_DOC)
                .define(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, LONG, DEFAULT_MIN_COMPACTION_LAG_MS, atLeast(0), MEDIUM,
                        TopicConfig.MIN_COMPACTION_LAG_MS_DOC)
                .define(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, LONG, DEFAULT_MAX_COMPACTION_LAG_MS, atLeast(1), MEDIUM,
                        TopicConfig.MAX_COMPACTION_LAG_MS_DOC)
                .define(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, LONG, ServerLogConfigs.LOG_DELETE_DELAY_MS_DEFAULT, atLeast(0), MEDIUM,
                        TopicConfig.FILE_DELETE_DELAY_MS_DOC)
                .define(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, DOUBLE, DEFAULT_MIN_CLEANABLE_DIRTY_RATIO, between(0, 1), MEDIUM,
                        TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_DOC)
                .define(TopicConfig.CLEANUP_POLICY_CONFIG, LIST, ServerLogConfigs.LOG_CLEANUP_POLICY_DEFAULT, ValidList.in(TopicConfig.CLEANUP_POLICY_COMPACT,
                        TopicConfig.CLEANUP_POLICY_DELETE), MEDIUM, TopicConfig.CLEANUP_POLICY_DOC)
                .define(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, BOOLEAN, DEFAULT_UNCLEAN_LEADER_ELECTION_ENABLE,
                        MEDIUM, TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_DOC)
                .define(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, INT, ServerLogConfigs.MIN_IN_SYNC_REPLICAS_DEFAULT, atLeast(1), MEDIUM,
                        TopicConfig.MIN_IN_SYNC_REPLICAS_DOC)
                .define(TopicConfig.COMPRESSION_TYPE_CONFIG, STRING, DEFAULT_COMPRESSION_TYPE, in(BrokerCompressionType.names().toArray(new String[0])),
                        MEDIUM, TopicConfig.COMPRESSION_TYPE_DOC)
                .define(TopicConfig.COMPRESSION_GZIP_LEVEL_CONFIG, INT, CompressionType.GZIP.defaultLevel(),
                        CompressionType.GZIP.levelValidator(), MEDIUM, TopicConfig.COMPRESSION_GZIP_LEVEL_DOC)
                .define(TopicConfig.COMPRESSION_LZ4_LEVEL_CONFIG, INT, CompressionType.LZ4.defaultLevel(),
                        CompressionType.LZ4.levelValidator(), MEDIUM, TopicConfig.COMPRESSION_LZ4_LEVEL_DOC)
                .define(TopicConfig.COMPRESSION_ZSTD_LEVEL_CONFIG, INT, CompressionType.ZSTD.defaultLevel(),
                        CompressionType.ZSTD.levelValidator(), MEDIUM, TopicConfig.COMPRESSION_ZSTD_LEVEL_DOC)
                .define(TopicConfig.PREALLOCATE_CONFIG, BOOLEAN, DEFAULT_PREALLOCATE, MEDIUM, TopicConfig.PREALLOCATE_DOC)
                .define(MESSAGE_FORMAT_VERSION_CONFIG, STRING, DEFAULT_MESSAGE_FORMAT_VERSION, new MetadataVersionValidator(), MEDIUM,
                        MESSAGE_FORMAT_VERSION_DOC)
                .define(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, STRING, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_DEFAULT,
                        in("CreateTime", "LogAppendTime"), MEDIUM, TopicConfig.MESSAGE_TIMESTAMP_TYPE_DOC)
                .define(MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, LONG, DEFAULT_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS,
                        atLeast(0), MEDIUM, MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC)
                .define(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, LONG, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DEFAULT,
                        atLeast(0), MEDIUM, TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DOC)
                .define(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, LONG, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_DEFAULT,
                        atLeast(0), MEDIUM, TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_DOC)
                .define(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, LIST, QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_DEFAULT,
                        ThrottledReplicaListValidator.INSTANCE, MEDIUM, QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_DOC)
                .define(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, LIST, QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_DEFAULT,
                        ThrottledReplicaListValidator.INSTANCE, MEDIUM, QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_DOC)
                .define(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, BOOLEAN, ServerLogConfigs.LOG_MESSAGE_DOWNCONVERSION_ENABLE_DEFAULT, LOW,
                        TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_DOC)
                .define(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, BOOLEAN, DEFAULT_REMOTE_STORAGE_ENABLE, null,
                        MEDIUM, TopicConfig.REMOTE_LOG_STORAGE_ENABLE_DOC)
                .define(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, LONG, DEFAULT_LOCAL_RETENTION_MS, atLeast(-2), MEDIUM,
                        TopicConfig.LOCAL_LOG_RETENTION_MS_DOC)
                .define(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, LONG, DEFAULT_LOCAL_RETENTION_BYTES, atLeast(-2), MEDIUM,
                        TopicConfig.LOCAL_LOG_RETENTION_BYTES_DOC)
                .define(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, BOOLEAN, false, MEDIUM, TopicConfig.REMOTE_LOG_COPY_DISABLE_DOC)
                .define(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, BOOLEAN, false, MEDIUM, TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_DOC);
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
    public final BrokerCompressionType compressionType;
    public final Optional<Compression> compression;
    public final boolean preallocate;

    /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details regarding the deprecation */
    @Deprecated
    public final MetadataVersion messageFormatVersion;

    public final TimestampType messageTimestampType;

    /* See `TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG` for details regarding the deprecation */
    @Deprecated
    public final long messageTimestampDifferenceMaxMs;
    public final long messageTimestampBeforeMaxMs;
    public final long messageTimestampAfterMaxMs;
    public final List<String> leaderReplicationThrottledReplicas;
    public final List<String> followerReplicationThrottledReplicas;
    public final boolean messageDownConversionEnable;

    private final RemoteLogConfig remoteLogConfig;
    private final int maxMessageSize;
    private final Map<?, ?> props;

    public LogConfig(Map<?, ?> props) {
        this(props, Collections.emptySet());
    }

    @SuppressWarnings({"deprecation", "this-escape"})
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
        this.compressionType = BrokerCompressionType.forName(getString(TopicConfig.COMPRESSION_TYPE_CONFIG));
        this.compression = getCompression();
        this.preallocate = getBoolean(TopicConfig.PREALLOCATE_CONFIG);
        this.messageFormatVersion = MetadataVersion.fromVersionString(getString(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG));
        this.messageTimestampType = TimestampType.forName(getString(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG));
        this.messageTimestampDifferenceMaxMs = getLong(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG);
        this.messageTimestampBeforeMaxMs = getMessageTimestampBeforeMaxMs();
        this.messageTimestampAfterMaxMs = getMessageTimestampAfterMaxMs();
        this.leaderReplicationThrottledReplicas = Collections.unmodifiableList(getList(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG));
        this.followerReplicationThrottledReplicas = Collections.unmodifiableList(getList(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG));
        this.messageDownConversionEnable = getBoolean(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG);

        remoteLogConfig = new RemoteLogConfig(this);
    }

    private Optional<Compression> getCompression() {
        switch (compressionType) {
            case GZIP:
                return Optional.of(Compression.gzip()
                        .level(getInt(TopicConfig.COMPRESSION_GZIP_LEVEL_CONFIG))
                        .build());
            case LZ4:
                return Optional.of(Compression.lz4()
                        .level(getInt(TopicConfig.COMPRESSION_LZ4_LEVEL_CONFIG))
                        .build());
            case ZSTD:
                return Optional.of(Compression.zstd()
                        .level(getInt(TopicConfig.COMPRESSION_ZSTD_LEVEL_CONFIG))
                        .build());
            case SNAPPY:
                return Optional.of(Compression.snappy().build());
            case UNCOMPRESSED:
                return Optional.of(Compression.NONE);
            case PRODUCER:
                return Optional.empty();
            default:
                throw new IllegalArgumentException("Invalid value for " + TopicConfig.COMPRESSION_TYPE_CONFIG);
        }
    }

    //In the transition period before messageTimestampDifferenceMaxMs is removed, to maintain backward compatibility,
    // we are using its value if messageTimestampBeforeMaxMs default value hasn't changed.
    private long getMessageTimestampBeforeMaxMs() {
        final Long messageTimestampBeforeMaxMs = getLong(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG);
        if (!messageTimestampBeforeMaxMs.equals(Long.MAX_VALUE)) {
            return messageTimestampBeforeMaxMs;
        } else {
            return messageTimestampDifferenceMaxMs;
        }
    }

    //In the transition period before messageTimestampDifferenceMaxMs is removed, to maintain backward compatibility,
    // we are using its value if messageTimestampAfterMaxMs default value hasn't changed.
    private long getMessageTimestampAfterMaxMs() {
        final Long messageTimestampAfterMaxMs = getLong(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG);
        if (!messageTimestampAfterMaxMs.equals(Long.MAX_VALUE)) {
            return messageTimestampAfterMaxMs;
        } else {
            return messageTimestampDifferenceMaxMs;
        }
    }

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

    public boolean remoteStorageEnable() {
        return remoteLogConfig.remoteStorageEnable;
    }

    public Boolean remoteLogDeleteOnDisable() {
        return remoteLogConfig.remoteLogDeleteOnDisable;
    }

    public Boolean remoteLogCopyDisable() {
        return remoteLogConfig.remoteLogCopyDisable;
    }

    public long localRetentionMs() {
        return remoteLogConfig.localRetentionMs == LogConfig.DEFAULT_LOCAL_RETENTION_MS ? retentionMs : remoteLogConfig.localRetentionMs;
    }

    public long localRetentionBytes() {
        return remoteLogConfig.localRetentionBytes == LogConfig.DEFAULT_LOCAL_RETENTION_BYTES ? retentionSize : remoteLogConfig.localRetentionBytes;
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
        props.putAll(defaults);
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

    /**
     * Validates the values of the given properties. Can be called by both client and server.
     * The `props` supplied should contain all the LogConfig properties and the default values are extracted from the
     * LogConfig class.
     * @param props The properties to be validated
     */
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
     * Validates the values of the given properties. Should be called only by the broker.
     * The `props` supplied doesn't contain any topic-level configs, only broker-level configs.
     * The default values should be extracted from the KafkaConfig.
     * @param props The properties to be validated
     */
    public static void validateBrokerLogConfigValues(Map<?, ?> props,
                                                     boolean isRemoteLogStorageSystemEnabled) {
        validateValues(props);
        if (isRemoteLogStorageSystemEnabled) {
            validateRemoteStorageRetentionSize(props);
            validateRemoteStorageRetentionTime(props);
        }
    }

    /**
     * Validates the values of the given properties. Should be called only by the broker.
     * The `newConfigs` supplied contains the topic-level configs,
     * The default values should be extracted from the KafkaConfig.
     * @param existingConfigs                   The existing properties
     * @param newConfigs                        The new properties to be validated
     * @param isRemoteLogStorageSystemEnabled   true if system wise remote log storage is enabled
     * @param fromZK                            true if this is a ZK cluster
     */
    private static void validateTopicLogConfigValues(Map<String, String> existingConfigs,
                                                     Map<?, ?> newConfigs,
                                                     boolean isRemoteLogStorageSystemEnabled,
                                                     boolean fromZK) {
        validateValues(newConfigs);

        if (fromZK) {
            validateNoInvalidRemoteStorageConfigsInZK(newConfigs);
        }
        boolean isRemoteLogStorageEnabled = (Boolean) newConfigs.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG);
        if (isRemoteLogStorageEnabled) {
            validateRemoteStorageOnlyIfSystemEnabled(newConfigs, isRemoteLogStorageSystemEnabled, false);
            validateNoRemoteStorageForCompactedTopic(newConfigs);
            validateRemoteStorageRetentionSize(newConfigs);
            validateRemoteStorageRetentionTime(newConfigs);
            validateRetentionConfigsWhenRemoteCopyDisabled(newConfigs, isRemoteLogStorageEnabled);
        } else {
            // The new config "remote.storage.enable" is false, validate if it's turning from true to false
            boolean wasRemoteLogEnabled = Boolean.parseBoolean(existingConfigs.getOrDefault(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"));
            validateTurningOffRemoteStorageWithDelete(newConfigs, wasRemoteLogEnabled, isRemoteLogStorageEnabled);
        }
    }

    public static void validateTurningOffRemoteStorageWithDelete(Map<?, ?> newConfigs, boolean wasRemoteLogEnabled, boolean isRemoteLogStorageEnabled) {
        boolean isRemoteLogDeleteOnDisable = (Boolean) Utils.castToStringObjectMap(newConfigs).getOrDefault(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, false);
        if (wasRemoteLogEnabled && !isRemoteLogStorageEnabled && !isRemoteLogDeleteOnDisable) {
            throw new InvalidConfigurationException("It is invalid to disable remote storage without deleting remote data. " +
                    "If you want to keep the remote data and turn to read only, please set `remote.storage.enable=true,remote.log.copy.disable=true`. " +
                    "If you want to disable remote storage and delete all remote data, please set `remote.storage.enable=false,remote.log.delete.on.disable=true`.");
        }
    }

    public static void validateRetentionConfigsWhenRemoteCopyDisabled(Map<?, ?> newConfigs, boolean isRemoteLogStorageEnabled) {
        boolean isRemoteLogCopyDisabled = (Boolean) Utils.castToStringObjectMap(newConfigs).getOrDefault(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, false);
        long retentionMs = (Long) newConfigs.get(TopicConfig.RETENTION_MS_CONFIG);
        long localRetentionMs = (Long) newConfigs.get(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG);
        long retentionBytes = (Long) newConfigs.get(TopicConfig.RETENTION_BYTES_CONFIG);
        long localRetentionBytes = (Long) newConfigs.get(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG);
        if (isRemoteLogStorageEnabled && isRemoteLogCopyDisabled) {
            if (localRetentionBytes != -2 && localRetentionBytes != retentionBytes) {
                throw new InvalidConfigurationException("When `remote.log.copy.disable` is set to true, the `local.retention.bytes` " +
                        "and `retention.bytes` must be set to the identical value because there will be no more logs copied to the remote storage.");
            }
            if (localRetentionMs != -2 && localRetentionMs != retentionMs) {
                throw new InvalidConfigurationException("When `remote.log.copy.disable` is set to true, the `local.retention.ms` " +
                        "and `retention.ms` must be set to the identical value because there will be no more logs copied to the remote storage.");
            }
        }
    }

    public static void validateNoInvalidRemoteStorageConfigsInZK(Map<?, ?> newConfigs) {
        boolean isRemoteLogDeleteOnDisable = (Boolean) Utils.castToStringObjectMap(newConfigs).getOrDefault(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, false);
        boolean isRemoteLogCopyDisabled = (Boolean) Utils.castToStringObjectMap(newConfigs).getOrDefault(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, false);
        if (isRemoteLogDeleteOnDisable || isRemoteLogCopyDisabled) {
            throw new InvalidConfigurationException("It is invalid to set `remote.log.delete.on.disable` or " +
                    "`remote.log.copy.disable` under Zookeeper's mode.");
        }
    }

    public static void validateRemoteStorageOnlyIfSystemEnabled(Map<?, ?> props, boolean isRemoteLogStorageSystemEnabled, boolean isReceivingConfigFromStore) {
        boolean isRemoteLogStorageEnabled = (Boolean) props.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG);
        if (isRemoteLogStorageEnabled && !isRemoteLogStorageSystemEnabled) {
            if (isReceivingConfigFromStore) {
                throw new ConfigException("You have to delete all topics with the property remote.storage.enable=true before disabling tiered storage cluster-wide");
            } else {
                throw new ConfigException("Tiered Storage functionality is disabled in the broker. " +
                        "Topic cannot be configured with remote log storage.");
            }
        }
    }

    private static void validateNoRemoteStorageForCompactedTopic(Map<?, ?> props) {
        String cleanupPolicy = props.get(TopicConfig.CLEANUP_POLICY_CONFIG).toString().toLowerCase(Locale.getDefault());
        if (cleanupPolicy.contains(TopicConfig.CLEANUP_POLICY_COMPACT)) {
            throw new ConfigException("Remote log storage is unsupported for the compacted topics");
        }
    }

    private static void validateRemoteStorageRetentionSize(Map<?, ?> props) {
        Long retentionBytes = (Long) props.get(TopicConfig.RETENTION_BYTES_CONFIG);
        Long localRetentionBytes = (Long) props.get(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG);
        if (retentionBytes > -1 && localRetentionBytes != -2) {
            if (localRetentionBytes == -1) {
                String message = String.format("Value must not be -1 as %s value is set as %d.",
                        TopicConfig.RETENTION_BYTES_CONFIG, retentionBytes);
                throw new ConfigException(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, localRetentionBytes, message);
            }
            if (localRetentionBytes > retentionBytes) {
                String message = String.format("Value must not be more than %s property value: %d",
                        TopicConfig.RETENTION_BYTES_CONFIG, retentionBytes);
                throw new ConfigException(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, localRetentionBytes, message);
            }
        }
    }

    private static void validateRemoteStorageRetentionTime(Map<?, ?> props) {
        Long retentionMs = (Long) props.get(TopicConfig.RETENTION_MS_CONFIG);
        Long localRetentionMs = (Long) props.get(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG);
        if (retentionMs != -1 && localRetentionMs != -2) {
            if (localRetentionMs == -1) {
                String message = String.format("Value must not be -1 as %s value is set as %d.",
                        TopicConfig.RETENTION_MS_CONFIG, retentionMs);
                throw new ConfigException(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, localRetentionMs, message);
            }
            if (localRetentionMs > retentionMs) {
                String message = String.format("Value must not be more than %s property value: %d",
                        TopicConfig.RETENTION_MS_CONFIG, retentionMs);
                throw new ConfigException(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, localRetentionMs, message);
            }
        }
    }

    /**
     * Check that the given properties contain only valid log config names and that all values can be parsed and are valid
     */
    public static void validate(Properties props) {
        validate(Collections.emptyMap(), props, Collections.emptyMap(), false, false);
    }

    public static void validate(Map<String, String> existingConfigs,
                                Properties props,
                                Map<?, ?> configuredProps,
                                boolean isRemoteLogStorageSystemEnabled,
                                boolean fromZK) {
        validateNames(props);
        if (configuredProps == null || configuredProps.isEmpty()) {
            Map<?, ?> valueMaps = CONFIG.parse(props);
            validateValues(valueMaps);
        } else {
            Map<Object, Object> combinedConfigs = new HashMap<>(configuredProps);
            combinedConfigs.putAll(props);
            Map<?, ?> valueMaps = CONFIG.parse(combinedConfigs);
            validateTopicLogConfigValues(existingConfigs, valueMaps, isRemoteLogStorageSystemEnabled, fromZK);
        }
    }

    @Override
    public String toString() {
        return "LogConfig{" +
                "segmentSize=" + segmentSize +
                ", segmentMs=" + segmentMs +
                ", segmentJitterMs=" + segmentJitterMs +
                ", maxIndexSize=" + maxIndexSize +
                ", flushInterval=" + flushInterval +
                ", flushMs=" + flushMs +
                ", retentionSize=" + retentionSize +
                ", retentionMs=" + retentionMs +
                ", indexInterval=" + indexInterval +
                ", fileDeleteDelayMs=" + fileDeleteDelayMs +
                ", deleteRetentionMs=" + deleteRetentionMs +
                ", compactionLagMs=" + compactionLagMs +
                ", maxCompactionLagMs=" + maxCompactionLagMs +
                ", minCleanableRatio=" + minCleanableRatio +
                ", compact=" + compact +
                ", delete=" + delete +
                ", uncleanLeaderElectionEnable=" + uncleanLeaderElectionEnable +
                ", minInSyncReplicas=" + minInSyncReplicas +
                ", compressionType='" + compressionType + '\'' +
                ", preallocate=" + preallocate +
                ", messageFormatVersion=" + messageFormatVersion +
                ", messageTimestampType=" + messageTimestampType +
                ", messageTimestampDifferenceMaxMs=" + messageTimestampDifferenceMaxMs +
                ", leaderReplicationThrottledReplicas=" + leaderReplicationThrottledReplicas +
                ", followerReplicationThrottledReplicas=" + followerReplicationThrottledReplicas +
                ", messageDownConversionEnable=" + messageDownConversionEnable +
                ", remoteLogConfig=" + remoteLogConfig +
                ", maxMessageSize=" + maxMessageSize +
                '}';
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtml(4, config -> "topicconfigs_" + config));
    }
}
