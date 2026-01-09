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
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ConfigUtils;
import org.apache.kafka.common.utils.Utils;
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

public class LogConfig extends AbstractConfig {

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
            return List.of("Name", "Description", "Type", "Default", "Valid Values", SERVER_DEFAULT_HEADER_NAME, "Importance");
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

    public static final int DEFAULT_SEGMENT_BYTES = 1024 * 1024 * 1024;
    public static final long DEFAULT_SEGMENT_MS = 24 * 7 * 60 * 60 * 1000L;
    public static final long DEFAULT_SEGMENT_JITTER_MS = 0;
    public static final long DEFAULT_RETENTION_MS = 24 * 7 * 60 * 60 * 1000L;
    public static final long DEFAULT_DELETE_RETENTION_MS = 24 * 60 * 60 * 1000L;
    public static final long DEFAULT_MIN_COMPACTION_LAG_MS = 0;
    public static final long DEFAULT_MAX_COMPACTION_LAG_MS = Long.MAX_VALUE;
    public static final double DEFAULT_MIN_CLEANABLE_DIRTY_RATIO = 0.5;
    public static final boolean DEFAULT_UNCLEAN_LEADER_ELECTION_ENABLE = false;
    public static final boolean DEFAULT_PREALLOCATE = false;

    public static final boolean DEFAULT_REMOTE_STORAGE_ENABLE = false;
    public static final boolean DEFAULT_REMOTE_LOG_COPY_DISABLE_CONFIG = false;
    public static final boolean DEFAULT_REMOTE_LOG_DELETE_ON_DISABLE_CONFIG = false;
    public static final long DEFAULT_LOCAL_RETENTION_BYTES = -2; // It indicates the value to be derived from RetentionBytes
    public static final long DEFAULT_LOCAL_RETENTION_MS = -2; // It indicates the value to be derived from RetentionMs

    public static final String INTERNAL_SEGMENT_BYTES_CONFIG = "internal.segment.bytes";
    public static final String INTERNAL_SEGMENT_BYTES_DOC = "The maximum size of a single log file. This should be used for testing only.";

    public static final ConfigDef SERVER_CONFIG_DEF = new ConfigDef()
            .define(ServerLogConfigs.NUM_PARTITIONS_CONFIG, INT, ServerLogConfigs.NUM_PARTITIONS_DEFAULT, atLeast(1), MEDIUM, ServerLogConfigs.NUM_PARTITIONS_DOC)
            .define(ServerLogConfigs.LOG_DIR_CONFIG, LIST, ServerLogConfigs.LOG_DIR_DEFAULT, ConfigDef.ValidList.anyNonDuplicateValues(false, false), HIGH, ServerLogConfigs.LOG_DIR_DOC)
            .define(ServerLogConfigs.LOG_DIRS_CONFIG, LIST, null, ConfigDef.ValidList.anyNonDuplicateValues(false, true), HIGH, ServerLogConfigs.LOG_DIRS_DOC)
            .define(ServerLogConfigs.LOG_SEGMENT_BYTES_CONFIG, INT, DEFAULT_SEGMENT_BYTES, atLeast(1024 * 1024), HIGH, ServerLogConfigs.LOG_SEGMENT_BYTES_DOC)

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
            .define(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG, STRING, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_DEFAULT, ConfigDef.ValidString.in("CreateTime", "LogAppendTime"), MEDIUM, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_DOC)
            .define(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, LONG, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DEFAULT, atLeast(0), MEDIUM, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DOC)
            .define(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, LONG, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_DEFAULT, atLeast(0), MEDIUM, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_DOC)
            .define(ServerLogConfigs.CREATE_TOPIC_POLICY_CLASS_NAME_CONFIG, CLASS, null, LOW, ServerLogConfigs.CREATE_TOPIC_POLICY_CLASS_NAME_DOC)
            .define(ServerLogConfigs.ALTER_CONFIG_POLICY_CLASS_NAME_CONFIG, CLASS, null, LOW, ServerLogConfigs.ALTER_CONFIG_POLICY_CLASS_NAME_DOC)
            .define(ServerLogConfigs.LOG_DIR_FAILURE_TIMEOUT_MS_CONFIG, LONG, ServerLogConfigs.LOG_DIR_FAILURE_TIMEOUT_MS_DEFAULT, atLeast(1), LOW, ServerLogConfigs.LOG_DIR_FAILURE_TIMEOUT_MS_DOC)
            .defineInternal(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG, LONG, ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_DEFAULT, atLeast(0), LOW, ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_DOC);

    private static final LogConfigDef CONFIG = new LogConfigDef();

    static {
        CONFIG.
                define(TopicConfig.SEGMENT_BYTES_CONFIG, INT, DEFAULT_SEGMENT_BYTES, atLeast(1024 * 1024), MEDIUM,
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
                .define(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, INT, ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT, atLeast(0), MEDIUM,
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
                .define(TopicConfig.COMPRESSION_TYPE_CONFIG, STRING, ServerLogConfigs.COMPRESSION_TYPE_DEFAULT, in(BrokerCompressionType.names().toArray(new String[0])),
                        MEDIUM, TopicConfig.COMPRESSION_TYPE_DOC)
                .define(TopicConfig.COMPRESSION_GZIP_LEVEL_CONFIG, INT, CompressionType.GZIP.defaultLevel(),
                        CompressionType.GZIP.levelValidator(), MEDIUM, TopicConfig.COMPRESSION_GZIP_LEVEL_DOC)
                .define(TopicConfig.COMPRESSION_LZ4_LEVEL_CONFIG, INT, CompressionType.LZ4.defaultLevel(),
                        CompressionType.LZ4.levelValidator(), MEDIUM, TopicConfig.COMPRESSION_LZ4_LEVEL_DOC)
                .define(TopicConfig.COMPRESSION_ZSTD_LEVEL_CONFIG, INT, CompressionType.ZSTD.defaultLevel(),
                        CompressionType.ZSTD.levelValidator(), MEDIUM, TopicConfig.COMPRESSION_ZSTD_LEVEL_DOC)
                .define(TopicConfig.PREALLOCATE_CONFIG, BOOLEAN, DEFAULT_PREALLOCATE, MEDIUM, TopicConfig.PREALLOCATE_DOC)
                .define(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, STRING, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_DEFAULT,
                        in("CreateTime", "LogAppendTime"), MEDIUM, TopicConfig.MESSAGE_TIMESTAMP_TYPE_DOC)
                .define(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, LONG, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DEFAULT,
                        atLeast(0), MEDIUM, TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DOC)
                .define(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, LONG, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_DEFAULT,
                        atLeast(0), MEDIUM, TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_DOC)
                .define(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, LIST, QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_DEFAULT,
                        ThrottledReplicaListValidator.INSTANCE, MEDIUM, QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_DOC)
                .define(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, LIST, QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_DEFAULT,
                        ThrottledReplicaListValidator.INSTANCE, MEDIUM, QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_DOC)
                .define(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, BOOLEAN, DEFAULT_REMOTE_STORAGE_ENABLE, null,
                        MEDIUM, TopicConfig.REMOTE_LOG_STORAGE_ENABLE_DOC)
                .define(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, LONG, DEFAULT_LOCAL_RETENTION_MS, atLeast(-2), MEDIUM,
                        TopicConfig.LOCAL_LOG_RETENTION_MS_DOC)
                .define(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, LONG, DEFAULT_LOCAL_RETENTION_BYTES, atLeast(-2), MEDIUM,
                        TopicConfig.LOCAL_LOG_RETENTION_BYTES_DOC)
                .define(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, BOOLEAN, false, MEDIUM, TopicConfig.REMOTE_LOG_COPY_DISABLE_DOC)
                .define(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, BOOLEAN, false, MEDIUM, TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_DOC)
                .defineInternal(INTERNAL_SEGMENT_BYTES_CONFIG, INT, null, null, MEDIUM, INTERNAL_SEGMENT_BYTES_DOC)
                // BookKeeper client configs
                .define(ServerLogConfigs.LOG_ASYNC_MODE, BOOLEAN, true, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_AUTHENTICATION_PLUGIN, STRING, null, null, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_AUTHENTICATION_PARAMETERS, STRING, null, null, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_AUTHENTICATION, BOOLEAN, false, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_CERTIFICATE_FILE_PATH, STRING, null, null, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_KEY_FILE_PATH, STRING, null, null, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_KEY_FILE_TYPE, STRING, "PEM", in("PEM", "JKS", "PKCS12"), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_KEY_STORE_PASSWORD_PATH, STRING, null, null, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_PROVIDER_FACTORY_CLASS, STRING, "org.apache.bookkeeper.tls.TLSContextFactory", null, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_TRUST_CERTS_FILE_PATH, STRING, null, null, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_TRUST_CERT_TYPES, STRING, "PEM", in("PEM", "JKS", "PKCS12"), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_TRUST_STORE_PASSWORD_PATH, STRING, null, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_CERT_FILES_REFRESH_DURATION_SECONDS, INT, 300, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_ENABLE_BUSY_WAIT, BOOLEAN, false, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_NUM_WORKER_THREADS, INT, Runtime.getRuntime().availableProcessors(), atLeast(1), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_THROTTLE_VALUE, INT, 0, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_METADATA_SESSION_TIMEOUT_MILLIS, INT, 30_000, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_TIMEOUT_SECONDS, INT, 30_000, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_SPECULATIVE_READ_TIMEOUT_MILLIS, INT, 0, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_NUMBER_OF_CHANNEL_PER_BOOKIE, INT, 16, atLeast(1), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_USE_V2_WIRE_PROTOCOL, BOOLEAN, true, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_ENABLE_STICKY_READS, BOOLEAN, true, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_NETTY_MAX_FRAME_SIZE_BYTES, INT, 5 * 1024 * 1024, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_DISK_WEIGHT_BASED_PLACEMENT_ENABLED, BOOLEAN, false, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_METADATA_SERVICE_URL, STRING, null, null, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_HEALTH_CHECK_ENABLED, BOOLEAN, true, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_HEALTH_CHECK_INTERVAL_SECONDS, INT, 60, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_HEALTH_CHECK_ERROR_THRESHOLD_PER_INTERVAL, INT, 5, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_HEALTH_CHECK_QUARANTINE_TIME_IN_SECONDS, INT, 1800, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_QUARANTINE_RATIO, DOUBLE, 1.0, atLeast(0.0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_REORDER_READ_SEQUENCE_ENABLED, BOOLEAN, true, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_EXPLICIT_LAC_INTERVAL_IN_MILLS, INT, 0, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_GET_BOOKIE_INFO_INTERVAL_SECONDS, INT, 24 * 60 * 60, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_GET_BOOKIE_INFO_RETRY_INTERVAL_SECONDS, INT, 60, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_NUM_IO_THREADS, INT, Runtime.getRuntime().availableProcessors() * 2, atLeast(1), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_LIMIT_STATS_LOGGING, BOOLEAN, true, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_CLIENT_AUTHENTICATION_PARAMETERS_NAME, STRING, null, null, HIGH, "")
                // Managed Ledger Factory configs
                .define(ServerLogConfigs.ML_MAX_CACHE_SIZE_MB, INT, 512, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.ML_CACHE_EVICTION_WATERMARK, DOUBLE, 0.9, atLeast(0.0), HIGH, "")
                .define(ServerLogConfigs.ML_NUM_SCHEDULER_THREADS, INT, Runtime.getRuntime().availableProcessors(), atLeast(1), HIGH, "")
                .define(ServerLogConfigs.ML_CACHE_EVICTION_INTERVAL_MS, INT, 10, atLeast(1), HIGH, "")
                .define(ServerLogConfigs.ML_CACHE_EVICTION_TIME_THRESHOLD_MS, INT, 1000, atLeast(1), HIGH, "")
                .define(ServerLogConfigs.ML_COPY_ENTRIES_IN_CACHE, BOOLEAN, false, HIGH, "")
                .define(ServerLogConfigs.ML_MAX_READ_IN_FLIGHT_SIZE_MB, INT, 0, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.ML_MAX_READS_INFLIGHT_SIZE, INT, 0, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.ML_MAX_READ_INFLIGHT_PERMITS_ACQUIRE_TIMEOUT_MS, INT, 60_000, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.ML_MAX_READ_INFLIGHT_PERMITS_ACQUIRE_QUEUE_SIZE, INT, 50000, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.ML_PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS, INT, 60, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.ML_TRACE_TASK_EXECUTION, BOOLEAN, true, HIGH, "")
                .define(ServerLogConfigs.ML_INFO_COMPRESSION_TYPE, STRING, "NONE", in("NONE", "LZ4", "ZLIB", "ZSTD", "SNAPPY"), HIGH, "")
                .define(ServerLogConfigs.ML_INFO_COMPRESSION_THRESHOLD_BYTES, INT, 16 * 1024, atLeast(1), HIGH, "")
                .define(ServerLogConfigs.ML_STATS_PERIOD_SECONDS, INT, 60, atLeast(1), HIGH, "")
                // Managed Ledger configs
                .define(ServerLogConfigs.BOOKKEEPER_ENSEMBLE_SIZE, INT, 2, between(1, 5), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_WRITE_QUORUM_SIZE, INT, 2, between(1, 5), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_ACK_QUORUM_SIZE, INT, 2, between(1, 5), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_LEDGER_DELETE_MAX_CONCURRENT_REQUESTS, INT, 1000, atLeast(1), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_LEDGER_DELETION_THREADS, INT, 1, between(1, 4), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_DIGEST_TYPE, STRING, "CRC32C", in("CRC32", "CRC32C", "MAC", "DUMMY"), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_PASSWORD, STRING, "", HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_MAX_ENTRIES_PER_LEDGER, INT, 50000, atLeast(1), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_MAX_BYTES_PER_LEDGER_MB, INT, 2048, atLeast(1), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_MIN_ROLLOVER_TIME_MINUTES, INT, 10, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_MAX_ROLLOVER_TIME_MINUTES, INT, 240, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_METADATA_OPERATION_TIMEOUT_SECONDS, INT, 60, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_READ_ENTRY_TIMEOUT_SECONDS, INT, 0, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_ADD_ENTRY_TIMEOUT_SECONDS, INT, 0, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_METADATA_ENSEMBLE_SIZE, INT, 2, between(1, 5), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_METADATA_WRITE_QUORUM_SIZE, INT, 2, between(1, 5), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_METADATA_ACK_QUORUM_SIZE, INT, 2, between(1, 5), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_METADATA_MAX_ENTRIES_PER_LEDGER, INT, 50000, atLeast(1), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_LEDGER_ROLLOVER_TIMEOUT_SECONDS, INT, 14400, atLeast(1), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_RETENTION_TIME_SECONDS, INT, 0, atLeast(-1), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_RETENTION_SIZE_MB, INT, 0, atLeast(-1), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_AUTO_SKIP_NONRECORVERABLE_DATA, BOOLEAN, false, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_LEDGER_FORCE_RECOVERY, BOOLEAN, false, HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_INACTIVE_LEDGER_ROLLOVER_TIME_SECONDS, INT, 0, atLeast(0), HIGH, "")
                .define(ServerLogConfigs.BOOKKEEPER_METADATA_STORE_URL, STRING, "", HIGH, "");
    }

    public final Set<String> overriddenConfigs;

    /*
     * Important note: Any configuration parameter that is passed along from KafkaConfig to LogConfig
     * should also be in `KafkaConfig#extractLogConfigMap`.
     */
    private final int segmentSize;
    private final Integer internalSegmentSize;
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

    public final TimestampType messageTimestampType;

    public final long messageTimestampBeforeMaxMs;
    public final long messageTimestampAfterMaxMs;
    public final List<String> leaderReplicationThrottledReplicas;
    public final List<String> followerReplicationThrottledReplicas;

    // Log IMPL config
    public final boolean asyncLogMode;
    // ML config
    public final int ensembleSize;
    public final int writeQuorumSize;
    public final int ackQuorumSize;
    public final int ledgerDeleteMaxConcurrentRequests;
    public final int ledgerDeletionThreads;
    public final String digestTypeName;
    public final String password;
    public final int maxEntriesPerLedger;
    public final int maxBytesPerLedgerMB;
    public final int minRolloverTimeMinutes;
    public final int maxRolloverTimeMinutes;
    public final int metadataOperationTimeoutSeconds;
    public final int readEntryTimeoutSeconds;
    public final int addEntryTimeoutSeconds;
    public final int defaultEnsembleSize;
    public final int defaultWriteQuorumSize;
    public final int defaultAckQuorumSize;
    public final int defaultMaxEntriesPerLedger;
    public final int ledgerRolloverTimeoutSeconds;
    public final int retentionTimeSeconds;
    public final int retentionSizeMb;
    public final boolean autoSkipNonRecoverableData;
    public final boolean ledgerForceRecovery;
    public final int inactiveLedgerRolloverTimeSeconds;
    public final String metadataStoreUrl;

    // ML Factory config
    public final int maxCacheSizeMb;
    public final double cacheEvictionWatermark;
    public final int numSchedulerThreads;
    public final int cacheEvictionIntervalMs;
    public final int cacheEvictionTimeThresholdMs;
    public final boolean copyEntriesInCache;
    public final int maxReadInflightSizeMb;
    public final int maxReadInflightSize;
    public final int maxReadInflightPermitsAcquireTimeoutMs;
    public final int maxReadInflightPermitsAcquireQueueSize;
    public final int prometheusStatsLatencyRolloverSeconds;
    public final boolean traceTaskExecutor;
    public final String managedLedgerInfoCompressionType;
    public final int managedLedgerInfoCompressionThresholdBytes;
    public final int infoStatsPeriodSeconds;

    // Bookkeeper Client Config
    public final String bookkeeperClientAuthenticationPlugin;
    public final String bookkeeperClientAuthenticationParameters;
    public final boolean bookkeeperTLSClientAuthentication;
    public final String bookkeeperTLSCertificateFilePath;
    public final String bookkeeperTLSKeyFilePath;
    public final String bookkeeperTLSKeyFileType;
    public final String bookkeeperTLSKeyStorePasswordPath;
    public final String bookkeeperTLSProviderFactoryClass;
    public final String bookkeeperTLSTrustCertsFilePath;
    public final String bookkeeperTLSTrustCertTypes;
    public final String bookkeeperTLSTrustStorePasswordPath;
    public final int bookkeeperTlsCertFilesRefreshDurationSeconds;
    public final boolean enableBusyWait;
    public final int bookkeeperClientNumWorkerThreads;
    public final int bookkeeperClientThrottleValue;
    public final int bookkeeperMetadataSessionTimeoutMillis;
    public final int bookkeeperClientTimeoutInSeconds;
    public final int bookkeeperClientSpeculativeReadTimeoutInMillis;
    public final int bookkeeperNumberOfChannelsPerBookie;
    public final boolean bookkeeperUseV2WireProtocol;
    public final boolean bookkeeperEnableStickyReads;
    public final int bookkeeperNettyMaxFrameSizeBytes;
    public final boolean bookkeeperDiskWeightBasedPlacementEnabled;
    public final String bookkeeperMetadataServiceUrl;
    public final boolean bookkeeperClientHealthCheckEnabled;
    public final int bookkeeperClientHealthCheckIntervalSeconds;
    public final int bookkeeperClientHealthCheckErrorThresholdPerInterval;
    public final int bookkeeperClientHealthCheckQuarantineTimeInSeconds;
    public final double bookkeeperClientQuarantineRatio;
    public final boolean bookkeeperClientReorderReadSequenceEnabled;
    public final int bookkeeperExplicitLacIntervalInMills;
    public final int bookkeeperClientGetBookieInfoIntervalSeconds;
    public final int bookkeeperClientGetBookieInfoRetryIntervalSeconds;
    public final int bookkeeperClientNumIoThreads;
    public final String bookkeeperClientAuthenticationParametersName;
    public final boolean bookkeeperClientLimitStatsLogging;

    private final RemoteLogConfig remoteLogConfig;
    private final int maxMessageSize;
    private final Map<?, ?> props;

    public LogConfig(Map<?, ?> props) {
        this(props, Set.of());
    }

    @SuppressWarnings({"this-escape"})
    public LogConfig(Map<?, ?> props, Set<String> overriddenConfigs) {
        super(CONFIG, props, false);
        this.props = Collections.unmodifiableMap(props);
        this.overriddenConfigs = Collections.unmodifiableSet(overriddenConfigs);

        this.segmentSize = getInt(TopicConfig.SEGMENT_BYTES_CONFIG);
        this.internalSegmentSize = getInt(INTERNAL_SEGMENT_BYTES_CONFIG);
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
                .toList()
                .contains(TopicConfig.CLEANUP_POLICY_COMPACT);
        this.delete = getList(TopicConfig.CLEANUP_POLICY_CONFIG).stream()
                .map(c -> c.toLowerCase(Locale.ROOT))
                .toList()
                .contains(TopicConfig.CLEANUP_POLICY_DELETE);
        this.uncleanLeaderElectionEnable = getBoolean(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG);
        this.minInSyncReplicas = getInt(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
        this.compressionType = BrokerCompressionType.forName(getString(TopicConfig.COMPRESSION_TYPE_CONFIG));
        this.compression = getCompression();
        this.preallocate = getBoolean(TopicConfig.PREALLOCATE_CONFIG);
        this.messageTimestampType = TimestampType.forName(getString(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG));
        this.messageTimestampBeforeMaxMs = getLong(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG);
        this.messageTimestampAfterMaxMs = getLong(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG);
        this.leaderReplicationThrottledReplicas = Collections.unmodifiableList(getList(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG));
        this.followerReplicationThrottledReplicas = Collections.unmodifiableList(getList(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG));
        this.asyncLogMode = getBoolean(ServerLogConfigs.LOG_ASYNC_MODE);
        // Bookkeeper client configs
        this.bookkeeperClientAuthenticationPlugin = getString(ServerLogConfigs.BOOKKEEPER_CLIENT_AUTHENTICATION_PLUGIN);
        this.bookkeeperClientAuthenticationParameters = getString(ServerLogConfigs.BOOKKEEPER_CLIENT_AUTHENTICATION_PARAMETERS);
        this.bookkeeperTLSClientAuthentication = getBoolean(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_AUTHENTICATION);
        this.bookkeeperTLSCertificateFilePath = getString(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_CERTIFICATE_FILE_PATH);
        this.bookkeeperTLSKeyFilePath = getString(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_KEY_FILE_PATH);
        this.bookkeeperTLSKeyFileType = getString(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_KEY_FILE_TYPE);
        this.bookkeeperTLSKeyStorePasswordPath = getString(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_KEY_STORE_PASSWORD_PATH);
        this.bookkeeperTLSProviderFactoryClass = getString(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_PROVIDER_FACTORY_CLASS);
        this.bookkeeperTLSTrustCertsFilePath = getString(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_TRUST_CERTS_FILE_PATH);
        this.bookkeeperTLSTrustCertTypes = getString(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_TRUST_CERT_TYPES);
        this.bookkeeperTLSTrustStorePasswordPath = getString(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_TRUST_STORE_PASSWORD_PATH);
        this.bookkeeperTlsCertFilesRefreshDurationSeconds = getInt(ServerLogConfigs.BOOKKEEPER_CLIENT_TLS_CERT_FILES_REFRESH_DURATION_SECONDS);
        this.enableBusyWait = getBoolean(ServerLogConfigs.BOOKKEEPER_CLIENT_ENABLE_BUSY_WAIT);
        this.bookkeeperClientNumWorkerThreads = getInt(ServerLogConfigs.BOOKKEEPER_CLIENT_NUM_WORKER_THREADS);
        this.bookkeeperClientThrottleValue = getInt(ServerLogConfigs.BOOKKEEPER_CLIENT_THROTTLE_VALUE);
        this.bookkeeperMetadataSessionTimeoutMillis = getInt(ServerLogConfigs.BOOKKEEPER_CLIENT_METADATA_SESSION_TIMEOUT_MILLIS);
        this.bookkeeperClientTimeoutInSeconds = getInt(ServerLogConfigs.BOOKKEEPER_CLIENT_TIMEOUT_SECONDS);
        this.bookkeeperClientSpeculativeReadTimeoutInMillis = getInt(ServerLogConfigs.BOOKKEEPER_CLIENT_SPECULATIVE_READ_TIMEOUT_MILLIS);
        this.bookkeeperNumberOfChannelsPerBookie = getInt(ServerLogConfigs.BOOKKEEPER_CLIENT_NUMBER_OF_CHANNEL_PER_BOOKIE);
        this.bookkeeperUseV2WireProtocol = getBoolean(ServerLogConfigs.BOOKKEEPER_CLIENT_USE_V2_WIRE_PROTOCOL);
        this.bookkeeperEnableStickyReads = getBoolean(ServerLogConfigs.BOOKKEEPER_CLIENT_ENABLE_STICKY_READS);
        this.bookkeeperNettyMaxFrameSizeBytes = getInt(ServerLogConfigs.BOOKKEEPER_CLIENT_NETTY_MAX_FRAME_SIZE_BYTES);
        this.bookkeeperDiskWeightBasedPlacementEnabled = getBoolean(ServerLogConfigs.BOOKKEEPER_CLIENT_DISK_WEIGHT_BASED_PLACEMENT_ENABLED);
        this.bookkeeperMetadataServiceUrl = getString(ServerLogConfigs.BOOKKEEPER_CLIENT_METADATA_SERVICE_URL);
        this.bookkeeperClientHealthCheckEnabled = getBoolean(ServerLogConfigs.BOOKKEEPER_CLIENT_HEALTH_CHECK_ENABLED);
        this.bookkeeperClientHealthCheckIntervalSeconds = getInt(ServerLogConfigs.BOOKKEEPER_CLIENT_HEALTH_CHECK_INTERVAL_SECONDS);
        this.bookkeeperClientHealthCheckErrorThresholdPerInterval = getInt(ServerLogConfigs.BOOKKEEPER_CLIENT_HEALTH_CHECK_ERROR_THRESHOLD_PER_INTERVAL);
        this.bookkeeperClientHealthCheckQuarantineTimeInSeconds = getInt(ServerLogConfigs.BOOKKEEPER_CLIENT_HEALTH_CHECK_QUARANTINE_TIME_IN_SECONDS);
        this.bookkeeperClientQuarantineRatio = getDouble(ServerLogConfigs.BOOKKEEPER_CLIENT_QUARANTINE_RATIO);
        this.bookkeeperClientReorderReadSequenceEnabled = getBoolean(ServerLogConfigs.BOOKKEEPER_CLIENT_REORDER_READ_SEQUENCE_ENABLED);
        this.bookkeeperExplicitLacIntervalInMills = getInt(ServerLogConfigs.BOOKKEEPER_CLIENT_EXPLICIT_LAC_INTERVAL_IN_MILLS);
        this.bookkeeperClientGetBookieInfoIntervalSeconds = getInt(ServerLogConfigs.BOOKKEEPER_CLIENT_GET_BOOKIE_INFO_INTERVAL_SECONDS);
        this.bookkeeperClientGetBookieInfoRetryIntervalSeconds = getInt(ServerLogConfigs.BOOKKEEPER_CLIENT_GET_BOOKIE_INFO_RETRY_INTERVAL_SECONDS);
        this.bookkeeperClientNumIoThreads = getInt(ServerLogConfigs.BOOKKEEPER_CLIENT_NUM_IO_THREADS);
        this.bookkeeperClientAuthenticationParametersName = getString(ServerLogConfigs.BOOKKEEPER_CLIENT_AUTHENTICATION_PARAMETERS_NAME);
        this.bookkeeperClientLimitStatsLogging = getBoolean(ServerLogConfigs.BOOKKEEPER_CLIENT_LIMIT_STATS_LOGGING);
        // Managed Ledger Factory Configs
        this.maxCacheSizeMb = getInt(ServerLogConfigs.ML_MAX_CACHE_SIZE_MB);
        this.cacheEvictionWatermark = getDouble(ServerLogConfigs.ML_CACHE_EVICTION_WATERMARK);
        this.numSchedulerThreads = getInt(ServerLogConfigs.ML_NUM_SCHEDULER_THREADS);
        this.cacheEvictionIntervalMs = getInt(ServerLogConfigs.ML_CACHE_EVICTION_INTERVAL_MS);
        this.cacheEvictionTimeThresholdMs = getInt(ServerLogConfigs.ML_CACHE_EVICTION_TIME_THRESHOLD_MS);
        this.copyEntriesInCache = getBoolean(ServerLogConfigs.ML_COPY_ENTRIES_IN_CACHE);
        this.maxReadInflightSizeMb = getInt(ServerLogConfigs.ML_MAX_READ_IN_FLIGHT_SIZE_MB);
        this.maxReadInflightSize = getInt(ServerLogConfigs.ML_MAX_READS_INFLIGHT_SIZE);
        this.maxReadInflightPermitsAcquireTimeoutMs = getInt(ServerLogConfigs.ML_MAX_READ_INFLIGHT_PERMITS_ACQUIRE_TIMEOUT_MS);
        this.maxReadInflightPermitsAcquireQueueSize = getInt(ServerLogConfigs.ML_MAX_READ_INFLIGHT_PERMITS_ACQUIRE_QUEUE_SIZE);
        this.prometheusStatsLatencyRolloverSeconds = getInt(ServerLogConfigs.ML_PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS);
        this.traceTaskExecutor = getBoolean(ServerLogConfigs.ML_TRACE_TASK_EXECUTION);
        this.managedLedgerInfoCompressionType = getString(ServerLogConfigs.ML_INFO_COMPRESSION_TYPE);
        this.managedLedgerInfoCompressionThresholdBytes = getInt(ServerLogConfigs.ML_INFO_COMPRESSION_THRESHOLD_BYTES);
        this.infoStatsPeriodSeconds = getInt(ServerLogConfigs.ML_STATS_PERIOD_SECONDS);
        // Managed Ledger Configs
        this.ensembleSize = getInt(ServerLogConfigs.BOOKKEEPER_ENSEMBLE_SIZE);
        this.writeQuorumSize = getInt(ServerLogConfigs.BOOKKEEPER_WRITE_QUORUM_SIZE);
        this.ackQuorumSize = getInt(ServerLogConfigs.BOOKKEEPER_ACK_QUORUM_SIZE);
        this.ledgerDeleteMaxConcurrentRequests = getInt(ServerLogConfigs.BOOKKEEPER_LEDGER_DELETE_MAX_CONCURRENT_REQUESTS);
        this.ledgerDeletionThreads = getInt(ServerLogConfigs.BOOKKEEPER_LEDGER_DELETION_THREADS);
        this.digestTypeName = getString(ServerLogConfigs.BOOKKEEPER_DIGEST_TYPE);
        this.password = getString(ServerLogConfigs.BOOKKEEPER_PASSWORD);
        this.maxEntriesPerLedger = getInt(ServerLogConfigs.BOOKKEEPER_MAX_ENTRIES_PER_LEDGER);
        this.maxBytesPerLedgerMB = getInt(ServerLogConfigs.BOOKKEEPER_MAX_BYTES_PER_LEDGER_MB);
        this.minRolloverTimeMinutes = getInt(ServerLogConfigs.BOOKKEEPER_MIN_ROLLOVER_TIME_MINUTES);
        this.maxRolloverTimeMinutes = getInt(ServerLogConfigs.BOOKKEEPER_MAX_ROLLOVER_TIME_MINUTES);
        this.metadataOperationTimeoutSeconds = getInt(ServerLogConfigs.BOOKKEEPER_METADATA_OPERATION_TIMEOUT_SECONDS);
        this.readEntryTimeoutSeconds = getInt(ServerLogConfigs.BOOKKEEPER_READ_ENTRY_TIMEOUT_SECONDS);
        this.addEntryTimeoutSeconds = getInt(ServerLogConfigs.BOOKKEEPER_ADD_ENTRY_TIMEOUT_SECONDS);
        this.defaultEnsembleSize = getInt(ServerLogConfigs.BOOKKEEPER_METADATA_ENSEMBLE_SIZE);
        this.defaultWriteQuorumSize = getInt(ServerLogConfigs.BOOKKEEPER_METADATA_WRITE_QUORUM_SIZE);
        this.defaultAckQuorumSize = getInt(ServerLogConfigs.BOOKKEEPER_METADATA_ACK_QUORUM_SIZE);
        this.defaultMaxEntriesPerLedger = getInt(ServerLogConfigs.BOOKKEEPER_METADATA_MAX_ENTRIES_PER_LEDGER);
        this.ledgerRolloverTimeoutSeconds = getInt(ServerLogConfigs.BOOKKEEPER_LEDGER_ROLLOVER_TIMEOUT_SECONDS);
        this.retentionTimeSeconds = getInt(ServerLogConfigs.BOOKKEEPER_RETENTION_TIME_SECONDS);
        this.retentionSizeMb = getInt(ServerLogConfigs.BOOKKEEPER_RETENTION_SIZE_MB);
        this.autoSkipNonRecoverableData = getBoolean(ServerLogConfigs.BOOKKEEPER_AUTO_SKIP_NONRECORVERABLE_DATA);
        this.ledgerForceRecovery = getBoolean(ServerLogConfigs.BOOKKEEPER_LEDGER_FORCE_RECOVERY);
        this.inactiveLedgerRolloverTimeSeconds = getInt(ServerLogConfigs.BOOKKEEPER_INACTIVE_LEDGER_ROLLOVER_TIME_SECONDS);
        this.metadataStoreUrl = getString(ServerLogConfigs.BOOKKEEPER_METADATA_STORE_URL);
        remoteLogConfig = new RemoteLogConfig(this);
    }

    private Optional<Compression> getCompression() {
        return switch (compressionType) {
            case GZIP -> Optional.of(Compression.gzip()
                    .level(getInt(TopicConfig.COMPRESSION_GZIP_LEVEL_CONFIG))
                    .build());
            case LZ4 -> Optional.of(Compression.lz4()
                    .level(getInt(TopicConfig.COMPRESSION_LZ4_LEVEL_CONFIG))
                    .build());
            case ZSTD -> Optional.of(Compression.zstd()
                    .level(getInt(TopicConfig.COMPRESSION_ZSTD_LEVEL_CONFIG))
                    .build());
            case SNAPPY -> Optional.of(Compression.snappy().build());
            case UNCOMPRESSED -> Optional.of(Compression.NONE);
            case PRODUCER -> Optional.empty();
        };
    }

    public int segmentSize() {
        if (internalSegmentSize == null) return segmentSize;
        return internalSegmentSize;
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
            return segmentSize();
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

    public static Optional<Type> configType(String configName) {
        return Optional.ofNullable(CONFIG.configKeys().get(configName)).map(c -> c.type);
    }

    public static List<String> configNames() {
        return CONFIG.names().stream().sorted().toList();
    }

    public static List<String> nonInternalConfigNames() {
        return CONFIG.configKeys().entrySet()
                .stream()
                .filter(entry -> !entry.getValue().internalConfig)
                .map(Map.Entry::getKey)
                .sorted().toList();
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
     *
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
     *
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
     *
     * @param existingConfigs                 The existing properties
     * @param newConfigs                      The new properties to be validated
     * @param isRemoteLogStorageSystemEnabled true if system wise remote log storage is enabled
     */
    private static void validateTopicLogConfigValues(Map<String, String> existingConfigs,
                                                     Map<?, ?> newConfigs,
                                                     boolean isRemoteLogStorageSystemEnabled) {
        validateValues(newConfigs);

        boolean isRemoteLogStorageEnabled = (Boolean) newConfigs.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG);
        if (isRemoteLogStorageEnabled) {
            validateRemoteStorageOnlyIfSystemEnabled(newConfigs, isRemoteLogStorageSystemEnabled, false);
            validateRemoteStorageRequiresDeleteCleanupPolicy(newConfigs);
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

    @SuppressWarnings("unchecked")
    private static void validateRemoteStorageRequiresDeleteCleanupPolicy(Map<?, ?> props) {
        List<String> cleanupPolicy = (List<String>) props.get(TopicConfig.CLEANUP_POLICY_CONFIG);
        if (!cleanupPolicy.isEmpty() && (cleanupPolicy.size() != 1 || !TopicConfig.CLEANUP_POLICY_DELETE.equals(cleanupPolicy.get(0)))) {
            throw new ConfigException("Remote log storage only supports topics with cleanup.policy=delete or cleanup.policy being an empty list.");
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
        validate(Map.of(), props, Map.of(), false);
    }

    public static void validate(Map<String, String> existingConfigs,
                                Properties props,
                                Map<?, ?> configuredProps,
                                boolean isRemoteLogStorageSystemEnabled) {
        validateNames(props);
        if (configuredProps == null || configuredProps.isEmpty()) {
            Map<?, ?> valueMaps = CONFIG.parse(props);
            validateValues(valueMaps);
        } else {
            Map<Object, Object> combinedConfigs = new HashMap<>(configuredProps);
            combinedConfigs.putAll(props);
            Map<?, ?> valueMaps = CONFIG.parse(combinedConfigs);
            validateTopicLogConfigValues(existingConfigs, valueMaps, isRemoteLogStorageSystemEnabled);
        }
    }

    @Override
    public String toString() {
        return "LogConfig{" +
                "segmentSize=" + segmentSize() +
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
                ", messageTimestampType=" + messageTimestampType +
                ", leaderReplicationThrottledReplicas=" + leaderReplicationThrottledReplicas +
                ", followerReplicationThrottledReplicas=" + followerReplicationThrottledReplicas +
                ", remoteLogConfig=" + remoteLogConfig +
                ", maxMessageSize=" + maxMessageSize +
                '}';
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtml(4, config -> "topicconfigs_" + config));
    }
}
