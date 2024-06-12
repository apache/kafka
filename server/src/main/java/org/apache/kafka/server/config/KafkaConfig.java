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
package org.apache.kafka.server.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.compress.GzipCompression;
import org.apache.kafka.common.compress.Lz4Compression;
import org.apache.kafka.common.compress.ZstdCompression;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.ConsumerGroupMigrationPolicy;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.transaction.TransactionLogConfigs;
import org.apache.kafka.coordinator.transaction.TransactionStateManagerConfigs;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.security.PasswordEncoderConfigs;
import org.apache.kafka.security.authorizer.AuthorizerUtils;
import org.apache.kafka.server.ProcessRole;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.MetadataVersionValidator;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.metrics.MetricConfigs;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.server.util.Csv;
import org.apache.kafka.server.utils.EndpointUtils;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.storage.internals.log.LogConfig;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import static org.apache.kafka.common.config.ConfigDef.Type.PASSWORD;
import static org.apache.kafka.common.config.ConfigDef.Type.SHORT;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public abstract class KafkaConfig extends AbstractConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);
    public final Set<ProcessRole> processRoles = parseProcessRoles();
    protected final KafkaConfigValidator configValidator = new KafkaConfigValidator(this, log);

    @SuppressWarnings("deprecation")
    public final static ConfigDef CONFIG_DEF =  new ConfigDef()
        /** ********* Zookeeper Configuration ***********/
        .define(ZkConfigs.ZK_CONNECT_CONFIG, STRING, null, HIGH, ZkConfigs.ZK_CONNECT_DOC)
        .define(ZkConfigs.ZK_SESSION_TIMEOUT_MS_CONFIG, INT, ZkConfigs.ZK_SESSION_TIMEOUT_MS, HIGH, ZkConfigs.ZK_SESSION_TIMEOUT_MS_DOC)
        .define(ZkConfigs.ZK_CONNECTION_TIMEOUT_MS_CONFIG, INT, null, HIGH, ZkConfigs.ZK_CONNECTION_TIMEOUT_MS_DOC)
        .define(ZkConfigs.ZK_ENABLE_SECURE_ACLS_CONFIG, BOOLEAN, ZkConfigs.ZK_ENABLE_SECURE_ACLS, HIGH, ZkConfigs.ZK_ENABLE_SECURE_ACLS_DOC)
        .define(ZkConfigs.ZK_MAX_IN_FLIGHT_REQUESTS_CONFIG, INT, ZkConfigs.ZK_MAX_IN_FLIGHT_REQUESTS, atLeast(1), HIGH, ZkConfigs.ZK_MAX_IN_FLIGHT_REQUESTS_DOC)
        .define(ZkConfigs.ZK_SSL_CLIENT_ENABLE_CONFIG, BOOLEAN, ZkConfigs.ZK_SSL_CLIENT_ENABLE, MEDIUM, ZkConfigs.ZK_SSL_CLIENT_ENABLE_DOC)
        .define(ZkConfigs.ZK_CLIENT_CNXN_SOCKET_CONFIG, STRING, null, MEDIUM, ZkConfigs.ZK_CLIENT_CNXN_SOCKET_DOC)
        .define(ZkConfigs.ZK_SSL_KEY_STORE_LOCATION_CONFIG, STRING, null, MEDIUM, ZkConfigs.ZK_SSL_KEY_STORE_LOCATION_DOC)
        .define(ZkConfigs.ZK_SSL_KEY_STORE_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, ZkConfigs.ZK_SSL_KEY_STORE_PASSWORD_DOC)
        .define(ZkConfigs.ZK_SSL_KEY_STORE_TYPE_CONFIG, STRING, null, MEDIUM, ZkConfigs.ZK_SSL_KEY_STORE_TYPE_DOC)
        .define(ZkConfigs.ZK_SSL_TRUST_STORE_LOCATION_CONFIG, STRING, null, MEDIUM, ZkConfigs.ZK_SSL_TRUST_STORE_LOCATION_DOC)
        .define(ZkConfigs.ZK_SSL_TRUST_STORE_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, ZkConfigs.ZK_SSL_TRUST_STORE_PASSWORD_DOC)
        .define(ZkConfigs.ZK_SSL_TRUST_STORE_TYPE_CONFIG, STRING, null, MEDIUM, ZkConfigs.ZK_SSL_TRUST_STORE_TYPE_DOC)
        .define(ZkConfigs.ZK_SSL_PROTOCOL_CONFIG, STRING, ZkConfigs.ZK_SSL_PROTOCOL, LOW, ZkConfigs.ZK_SSL_PROTOCOL_DOC)
        .define(ZkConfigs.ZK_SSL_ENABLED_PROTOCOLS_CONFIG, LIST, null, LOW, ZkConfigs.ZK_SSL_ENABLED_PROTOCOLS_DOC)
        .define(ZkConfigs.ZK_SSL_CIPHER_SUITES_CONFIG, LIST, null, LOW, ZkConfigs.ZK_SSL_CIPHER_SUITES_DOC)
        .define(ZkConfigs.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, STRING, ZkConfigs.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, LOW, ZkConfigs.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
        .define(ZkConfigs.ZK_SSL_CRL_ENABLE_CONFIG, BOOLEAN, ZkConfigs.ZK_SSL_CRL_ENABLE, LOW, ZkConfigs.ZK_SSL_CRL_ENABLE_DOC)
        .define(ZkConfigs.ZK_SSL_OCSP_ENABLE_CONFIG, BOOLEAN, ZkConfigs.ZK_SSL_OCSP_ENABLE, LOW, ZkConfigs.ZK_SSL_OCSP_ENABLE_DOC)

        /** ********* General Configuration ***********/
        .define(ServerConfigs.BROKER_ID_GENERATION_ENABLE_CONFIG, BOOLEAN, ServerConfigs.BROKER_ID_GENERATION_ENABLE_DEFAULT, MEDIUM, ServerConfigs.BROKER_ID_GENERATION_ENABLE_DOC)
        .define(ServerConfigs.RESERVED_BROKER_MAX_ID_CONFIG, INT, ServerConfigs.RESERVED_BROKER_MAX_ID_DEFAULT, atLeast(0), MEDIUM, ServerConfigs.RESERVED_BROKER_MAX_ID_DOC)
        .define(ServerConfigs.BROKER_ID_CONFIG, INT, ServerConfigs.BROKER_ID_DEFAULT, HIGH, ServerConfigs.BROKER_ID_DOC)
        .define(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG, INT, LogConfig.DEFAULT_MAX_MESSAGE_BYTES, atLeast(0), HIGH, ServerConfigs.MESSAGE_MAX_BYTES_DOC)
        .define(ServerConfigs.NUM_NETWORK_THREADS_CONFIG, INT, ServerConfigs.NUM_NETWORK_THREADS_DEFAULT, atLeast(1), HIGH, ServerConfigs.NUM_NETWORK_THREADS_DOC)
        .define(ServerConfigs.NUM_IO_THREADS_CONFIG, INT, ServerConfigs.NUM_IO_THREADS_DEFAULT, atLeast(1), HIGH, ServerConfigs.NUM_IO_THREADS_DOC)
        .define(ServerConfigs.NUM_REPLICA_ALTER_LOG_DIRS_THREADS_CONFIG, INT, null, HIGH, ServerConfigs.NUM_REPLICA_ALTER_LOG_DIRS_THREADS_DOC)
        .define(ServerConfigs.BACKGROUND_THREADS_CONFIG, INT, ServerConfigs.BACKGROUND_THREADS_DEFAULT, atLeast(1), HIGH, ServerConfigs.BACKGROUND_THREADS_DOC)
        .define(ServerConfigs.QUEUED_MAX_REQUESTS_CONFIG, INT, ServerConfigs.QUEUED_MAX_REQUESTS_DEFAULT, atLeast(1), HIGH, ServerConfigs.QUEUED_MAX_REQUESTS_DOC)
        .define(ServerConfigs.QUEUED_MAX_BYTES_CONFIG, LONG, ServerConfigs.QUEUED_MAX_REQUEST_BYTES_DEFAULT, MEDIUM, ServerConfigs.QUEUED_MAX_REQUEST_BYTES_DOC)
        .define(ServerConfigs.REQUEST_TIMEOUT_MS_CONFIG, INT, ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT, HIGH, ServerConfigs.REQUEST_TIMEOUT_MS_DOC)
        .define(ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, LONG, ServerConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS, MEDIUM, ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
        .define(ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, LONG, ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS, MEDIUM, ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)

        /*
         * KRaft mode configs.
         */
        .define(KRaftConfigs.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_CONFIG, LONG, KRaftConfigs.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES, atLeast(1), HIGH, KRaftConfigs.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_DOC)
        .define(KRaftConfigs.METADATA_SNAPSHOT_MAX_INTERVAL_MS_CONFIG, LONG, KRaftConfigs.METADATA_SNAPSHOT_MAX_INTERVAL_MS_DEFAULT, atLeast(0), HIGH, KRaftConfigs.METADATA_SNAPSHOT_MAX_INTERVAL_MS_DOC)
        .define(KRaftConfigs.PROCESS_ROLES_CONFIG, LIST, Collections.emptyList(), ConfigDef.ValidList.in("broker", "controller"), HIGH, KRaftConfigs.PROCESS_ROLES_DOC)
        .define(KRaftConfigs.NODE_ID_CONFIG, INT, KRaftConfigs.EMPTY_NODE_ID, null, HIGH, KRaftConfigs.NODE_ID_DOC)
        .define(KRaftConfigs.INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_CONFIG, INT, KRaftConfigs.INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_DEFAULT, null, MEDIUM, KRaftConfigs.INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_DOC)
        .define(KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG, INT, KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_DEFAULT, null, MEDIUM, KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_DOC)
        .define(KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG, INT, KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_DEFAULT, null, MEDIUM, KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_DOC)
        .define(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, STRING, null, null, HIGH, KRaftConfigs.CONTROLLER_LISTENER_NAMES_DOC)
        .define(KRaftConfigs.SASL_MECHANISM_CONTROLLER_PROTOCOL_CONFIG, STRING, SaslConfigs.DEFAULT_SASL_MECHANISM, null, HIGH, KRaftConfigs.SASL_MECHANISM_CONTROLLER_PROTOCOL_DOC)
        .define(KRaftConfigs.METADATA_LOG_DIR_CONFIG, STRING, null, null, HIGH, KRaftConfigs.METADATA_LOG_DIR_DOC)
        .define(KRaftConfigs.METADATA_LOG_SEGMENT_BYTES_CONFIG, INT, LogConfig.DEFAULT_SEGMENT_BYTES, atLeast(Records.LOG_OVERHEAD), HIGH, KRaftConfigs.METADATA_LOG_SEGMENT_BYTES_DOC)
        .defineInternal(KRaftConfigs.METADATA_LOG_SEGMENT_MIN_BYTES_CONFIG, INT, 8 * 1024 * 1024, atLeast(Records.LOG_OVERHEAD), HIGH, KRaftConfigs.METADATA_LOG_SEGMENT_MIN_BYTES_DOC)
        .define(KRaftConfigs.METADATA_LOG_SEGMENT_MILLIS_CONFIG, LONG, LogConfig.DEFAULT_SEGMENT_MS, null, HIGH, KRaftConfigs.METADATA_LOG_SEGMENT_MILLIS_DOC)
        .define(KRaftConfigs.METADATA_MAX_RETENTION_BYTES_CONFIG, LONG, KRaftConfigs.METADATA_MAX_RETENTION_BYTES_DEFAULT, null, HIGH, KRaftConfigs.METADATA_MAX_RETENTION_BYTES_DOC)
        .define(KRaftConfigs.METADATA_MAX_RETENTION_MILLIS_CONFIG, LONG, LogConfig.DEFAULT_RETENTION_MS, null, HIGH, KRaftConfigs.METADATA_MAX_RETENTION_MILLIS_DOC)
        .define(KRaftConfigs.METADATA_MAX_IDLE_INTERVAL_MS_CONFIG, INT, KRaftConfigs.METADATA_MAX_IDLE_INTERVAL_MS_DEFAULT, atLeast(0), LOW, KRaftConfigs.METADATA_MAX_IDLE_INTERVAL_MS_DOC)
        .defineInternal(KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_CONFIG, LONG, KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_DEFAULT, atLeast(0), MEDIUM, KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_DOC)
        .define(KRaftConfigs.MIGRATION_ENABLED_CONFIG, BOOLEAN, false, HIGH, KRaftConfigs.MIGRATION_ENABLED_DOC)
        .define(KRaftConfigs.ELR_ENABLED_CONFIG, BOOLEAN, false, HIGH, KRaftConfigs.ELR_ENABLED_DOC)
        .defineInternal(KRaftConfigs.MIGRATION_METADATA_MIN_BATCH_SIZE_CONFIG, INT, KRaftConfigs.MIGRATION_METADATA_MIN_BATCH_SIZE_DEFAULT, atLeast(1),
                MEDIUM, KRaftConfigs.MIGRATION_METADATA_MIN_BATCH_SIZE_DOC)

        /************* Authorizer Configuration ***********/
        .define(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, STRING, ServerConfigs.AUTHORIZER_CLASS_NAME_DEFAULT, new ConfigDef.NonNullValidator(), LOW, ServerConfigs.AUTHORIZER_CLASS_NAME_DOC)
        .define(ServerConfigs.EARLY_START_LISTENERS_CONFIG, STRING, null,  HIGH, ServerConfigs.EARLY_START_LISTENERS_DOC)

        /** ********* Socket Server Configuration ***********/
        .define(SocketServerConfigs.LISTENERS_CONFIG, STRING, SocketServerConfigs.LISTENERS_DEFAULT, HIGH, SocketServerConfigs.LISTENERS_DOC)
        .define(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, STRING, null, HIGH, SocketServerConfigs.ADVERTISED_LISTENERS_DOC)
        .define(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, STRING, SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_DEFAULT, LOW, SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_DOC)
        .define(SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG, STRING, null, HIGH, SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_DOC)
        .define(SocketServerConfigs.SOCKET_SEND_BUFFER_BYTES_CONFIG, INT, SocketServerConfigs.SOCKET_SEND_BUFFER_BYTES_DEFAULT, HIGH, SocketServerConfigs.SOCKET_SEND_BUFFER_BYTES_DOC)
        .define(SocketServerConfigs.SOCKET_RECEIVE_BUFFER_BYTES_CONFIG, INT, SocketServerConfigs.SOCKET_RECEIVE_BUFFER_BYTES_DEFAULT, HIGH, SocketServerConfigs.SOCKET_RECEIVE_BUFFER_BYTES_DOC)
        .define(SocketServerConfigs.SOCKET_REQUEST_MAX_BYTES_CONFIG, INT, SocketServerConfigs.SOCKET_REQUEST_MAX_BYTES_DEFAULT, atLeast(1), HIGH, SocketServerConfigs.SOCKET_REQUEST_MAX_BYTES_DOC)
        .define(SocketServerConfigs.SOCKET_LISTEN_BACKLOG_SIZE_CONFIG, INT, SocketServerConfigs.SOCKET_LISTEN_BACKLOG_SIZE_DEFAULT, atLeast(1), MEDIUM, SocketServerConfigs.SOCKET_LISTEN_BACKLOG_SIZE_DOC)
        .define(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG, INT, SocketServerConfigs.MAX_CONNECTIONS_PER_IP_DEFAULT, atLeast(0), MEDIUM, SocketServerConfigs.MAX_CONNECTIONS_PER_IP_DOC)
        .define(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG, STRING, SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_DEFAULT, MEDIUM, SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_DOC)
        .define(SocketServerConfigs.MAX_CONNECTIONS_CONFIG, INT, SocketServerConfigs.MAX_CONNECTIONS_DEFAULT, atLeast(0), MEDIUM, SocketServerConfigs.MAX_CONNECTIONS_DOC)
        .define(SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG, INT, SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_DEFAULT, atLeast(0), MEDIUM, SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_DOC)
        .define(SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, LONG, SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_DEFAULT, MEDIUM, SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
        .define(SocketServerConfigs.FAILED_AUTHENTICATION_DELAY_MS_CONFIG, INT, SocketServerConfigs.FAILED_AUTHENTICATION_DELAY_MS_DEFAULT, atLeast(0), LOW, SocketServerConfigs.FAILED_AUTHENTICATION_DELAY_MS_DOC)

        /************ Rack Configuration ******************/
        .define(ServerConfigs.BROKER_RACK_CONFIG, STRING, null, MEDIUM, ServerConfigs.BROKER_RACK_DOC)

        /** ********* Log Configuration ***********/
        .define(ServerLogConfigs.NUM_PARTITIONS_CONFIG, INT, ServerLogConfigs.NUM_PARTITIONS_DEFAULT, atLeast(1), MEDIUM, ServerLogConfigs.NUM_PARTITIONS_DOC)
        .define(ServerLogConfigs.LOG_DIR_CONFIG, STRING, ServerLogConfigs.LOG_DIR_DEFAULT, HIGH, ServerLogConfigs.LOG_DIR_DOC)
        .define(ServerLogConfigs.LOG_DIRS_CONFIG, STRING, null, HIGH, ServerLogConfigs.LOG_DIRS_DOC)
        .define(ServerLogConfigs.LOG_SEGMENT_BYTES_CONFIG, INT, LogConfig.DEFAULT_SEGMENT_BYTES, atLeast(LegacyRecord.RECORD_OVERHEAD_V0), HIGH, ServerLogConfigs.LOG_SEGMENT_BYTES_DOC)

        .define(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG, LONG, null, HIGH, ServerLogConfigs.LOG_ROLL_TIME_MILLIS_DOC)
        .define(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG, INT, (int) TimeUnit.MILLISECONDS.toHours(LogConfig.DEFAULT_SEGMENT_MS), atLeast(1), HIGH, ServerLogConfigs.LOG_ROLL_TIME_HOURS_DOC)

        .define(ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_CONFIG, LONG, null, HIGH, ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_DOC)
        .define(ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_CONFIG, INT, (int) TimeUnit.MILLISECONDS.toHours(LogConfig.DEFAULT_SEGMENT_JITTER_MS), atLeast(0), HIGH, ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_DOC)

        .define(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, LONG, null, HIGH, ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_DOC)
        .define(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG, INT, null, HIGH, ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_DOC)
        .define(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, INT, (int) TimeUnit.MILLISECONDS.toHours(LogConfig.DEFAULT_RETENTION_MS), HIGH, ServerLogConfigs.LOG_RETENTION_TIME_HOURS_DOC)

        .define(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG, LONG, ServerLogConfigs.LOG_RETENTION_BYTES_DEFAULT, HIGH, ServerLogConfigs.LOG_RETENTION_BYTES_DOC)
        .define(ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG, LONG, ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_DEFAULT, atLeast(1), MEDIUM, ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_DOC)
        .define(ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG, LIST, ServerLogConfigs.LOG_CLEANUP_POLICY_DEFAULT, ConfigDef.ValidList.in(TopicConfig.CLEANUP_POLICY_COMPACT, TopicConfig.CLEANUP_POLICY_DELETE), MEDIUM, ServerLogConfigs.LOG_CLEANUP_POLICY_DOC)
        .define(CleanerConfig.LOG_CLEANER_THREADS_PROP, INT, CleanerConfig.LOG_CLEANER_THREADS, atLeast(0), MEDIUM, CleanerConfig.LOG_CLEANER_THREADS_DOC)
        .define(CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP, DOUBLE, CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND, MEDIUM, CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_DOC)
        .define(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, LONG, CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE, MEDIUM, CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_DOC)
        .define(CleanerConfig.LOG_CLEANER_IO_BUFFER_SIZE_PROP, INT, CleanerConfig.LOG_CLEANER_IO_BUFFER_SIZE, atLeast(0), MEDIUM, CleanerConfig.LOG_CLEANER_IO_BUFFER_SIZE_DOC)
        .define(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP, DOUBLE, CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR, MEDIUM, CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_DOC)
        .define(CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP, LONG, CleanerConfig.LOG_CLEANER_BACKOFF_MS, atLeast(0), MEDIUM, CleanerConfig.LOG_CLEANER_BACKOFF_MS_DOC)
        .define(CleanerConfig.LOG_CLEANER_MIN_CLEAN_RATIO_PROP, DOUBLE, LogConfig.DEFAULT_MIN_CLEANABLE_DIRTY_RATIO, between(0, 1), MEDIUM, CleanerConfig.LOG_CLEANER_MIN_CLEAN_RATIO_DOC)
        .define(CleanerConfig.LOG_CLEANER_ENABLE_PROP, BOOLEAN, CleanerConfig.LOG_CLEANER_ENABLE, MEDIUM, CleanerConfig.LOG_CLEANER_ENABLE_DOC)
        .define(CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP, LONG, LogConfig.DEFAULT_DELETE_RETENTION_MS, atLeast(0), MEDIUM, CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_DOC)
        .define(CleanerConfig.LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP, LONG, LogConfig.DEFAULT_MIN_COMPACTION_LAG_MS, atLeast(0), MEDIUM, CleanerConfig.LOG_CLEANER_MIN_COMPACTION_LAG_MS_DOC)
        .define(CleanerConfig.LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP, LONG, LogConfig.DEFAULT_MAX_COMPACTION_LAG_MS, atLeast(1), MEDIUM, CleanerConfig.LOG_CLEANER_MAX_COMPACTION_LAG_MS_DOC)
        .define(ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_CONFIG, INT, ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_DEFAULT, atLeast(4), MEDIUM, ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_DOC)
        .define(ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_CONFIG, INT, ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_DEFAULT, atLeast(0), MEDIUM, ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_DOC)
        .define(ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG, LONG, ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_DEFAULT, atLeast(1), HIGH, ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_DOC)
        .define(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG, LONG, ServerLogConfigs.LOG_DELETE_DELAY_MS_DEFAULT, atLeast(0), HIGH, ServerLogConfigs.LOG_DELETE_DELAY_MS_DOC)
        .define(ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG, LONG, ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_DEFAULT, HIGH, ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_DOC)
        .define(ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG, LONG, null, HIGH, ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_DOC)
        .define(ServerLogConfigs.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG, INT, ServerLogConfigs.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_DEFAULT, atLeast(0), HIGH, ServerLogConfigs.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_DOC)
        .define(ServerLogConfigs.LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG, INT, ServerLogConfigs.LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_DEFAULT, atLeast(0), HIGH, ServerLogConfigs.LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_DOC)
        .define(ServerLogConfigs.LOG_PRE_ALLOCATE_CONFIG, BOOLEAN, LogConfig.DEFAULT_PREALLOCATE, MEDIUM, ServerLogConfigs.LOG_PRE_ALLOCATE_ENABLE_DOC)
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
        .defineInternal(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG, LONG, ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_DEFAULT, atLeast(0), LOW, ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_DOC)

        /** ********* Replication configuration ***********/
        .define(ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_CONFIG, INT, ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_DEFAULT, MEDIUM, ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_DOC)
        .define(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, INT, ReplicationConfigs.REPLICATION_FACTOR_DEFAULT, MEDIUM, ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_DOC)
        .define(ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_CONFIG, LONG, ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT, HIGH, ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DOC)
        .define(ReplicationConfigs.REPLICA_SOCKET_TIMEOUT_MS_CONFIG, INT, ReplicationConfigs.REPLICA_SOCKET_TIMEOUT_MS_DEFAULT, HIGH, ReplicationConfigs.REPLICA_SOCKET_TIMEOUT_MS_DOC)
        .define(ReplicationConfigs.REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_CONFIG, INT, ReplicationConfigs.REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_DEFAULT, HIGH, ReplicationConfigs.REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_DOC)
        .define(ReplicationConfigs.REPLICA_FETCH_MAX_BYTES_CONFIG, INT, ReplicationConfigs.REPLICA_FETCH_MAX_BYTES_DEFAULT, atLeast(0), MEDIUM, ReplicationConfigs.REPLICA_FETCH_MAX_BYTES_DOC)
        .define(ReplicationConfigs.REPLICA_FETCH_WAIT_MAX_MS_CONFIG, INT, ReplicationConfigs.REPLICA_FETCH_WAIT_MAX_MS_DEFAULT, HIGH, ReplicationConfigs.REPLICA_FETCH_WAIT_MAX_MS_DOC)
        .define(ReplicationConfigs.REPLICA_FETCH_BACKOFF_MS_CONFIG, INT, ReplicationConfigs.REPLICA_FETCH_BACKOFF_MS_DEFAULT, atLeast(0), MEDIUM, ReplicationConfigs.REPLICA_FETCH_BACKOFF_MS_DOC)
        .define(ReplicationConfigs.REPLICA_FETCH_MIN_BYTES_CONFIG, INT, ReplicationConfigs.REPLICA_FETCH_MIN_BYTES_DEFAULT, HIGH, ReplicationConfigs.REPLICA_FETCH_MIN_BYTES_DOC)
        .define(ReplicationConfigs.REPLICA_FETCH_RESPONSE_MAX_BYTES_CONFIG, INT, ReplicationConfigs.REPLICA_FETCH_RESPONSE_MAX_BYTES_DEFAULT, atLeast(0), MEDIUM, ReplicationConfigs.REPLICA_FETCH_RESPONSE_MAX_BYTES_DOC)
        .define(ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG, INT, ReplicationConfigs.NUM_REPLICA_FETCHERS_DEFAULT, HIGH, ReplicationConfigs.NUM_REPLICA_FETCHERS_DOC)
        .define(ReplicationConfigs.REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_CONFIG, LONG, ReplicationConfigs.REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_DEFAULT, HIGH, ReplicationConfigs.REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_DOC)
        .define(ReplicationConfigs.FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG, INT, ReplicationConfigs.FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT, MEDIUM, ReplicationConfigs.FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC)
        .define(ReplicationConfigs.PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG, INT, ReplicationConfigs.PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT, MEDIUM, ReplicationConfigs.PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC)
        .define(ReplicationConfigs.DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG, INT, ReplicationConfigs.DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT, MEDIUM, ReplicationConfigs.DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC)
        .define(ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG, BOOLEAN, ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_DEFAULT, HIGH, ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_DOC)
        .define(ReplicationConfigs.LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_CONFIG, INT, ReplicationConfigs.LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_DEFAULT, HIGH, ReplicationConfigs.LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_DOC)
        .define(ReplicationConfigs.LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_CONFIG, LONG, ReplicationConfigs.LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_DEFAULT, atLeast(1), HIGH, ReplicationConfigs.LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_DOC)
        .define(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, BOOLEAN, LogConfig.DEFAULT_UNCLEAN_LEADER_ELECTION_ENABLE, HIGH, ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_DOC)
        .define(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG, STRING, ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_DEFAULT, ConfigDef.ValidString.in(Utils.enumOptions(SecurityProtocol.class)), MEDIUM, ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_DOC)
        .define(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG, STRING, ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_DEFAULT, new MetadataVersionValidator(), MEDIUM, ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_DOC)
        .define(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, STRING, null, MEDIUM, ReplicationConfigs.INTER_BROKER_LISTENER_NAME_DOC)
        .define(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG, STRING, null, MEDIUM, ReplicationConfigs.REPLICA_SELECTOR_CLASS_DOC)


        /** ********* Controlled shutdown configuration ***********/
        .define(ServerConfigs.CONTROLLED_SHUTDOWN_MAX_RETRIES_CONFIG, INT, ServerConfigs.CONTROLLED_SHUTDOWN_MAX_RETRIES_DEFAULT, MEDIUM, ServerConfigs.CONTROLLED_SHUTDOWN_MAX_RETRIES_DOC)
        .define(ServerConfigs.CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_CONFIG, LONG, ServerConfigs.CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_DEFAULT, MEDIUM, ServerConfigs.CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_DOC)
        .define(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, BOOLEAN, ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_DEFAULT, MEDIUM, ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_DOC)

        /** ********* Group coordinator configuration ***********/
        .define(GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, INT, GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT, MEDIUM, GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_DOC)
        .define(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, INT, GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT, MEDIUM, GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_DOC)
        .define(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, INT, GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT, MEDIUM, GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_DOC)
        .define(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, INT, GroupCoordinatorConfig.GROUP_MAX_SIZE_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.GROUP_MAX_SIZE_DOC)

        /** New group coordinator configs */
        .define(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, LIST, GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DEFAULT,
                ConfigDef.ValidList.in(Utils.enumOptions(Group.GroupType.class)), MEDIUM, GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DOC)
        .define(GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_CONFIG, INT, GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_DOC)
        // Internal configuration used by integration and system tests.
        .defineInternal(GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG, BOOLEAN, GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_DEFAULT, null, MEDIUM, GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_DOC)

        /** Consumer groups configs */
        .define(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, INT, GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_DOC)
        .define(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, INT, GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DOC)
        .define(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, INT, GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DOC)
        .define(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, INT, GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DOC)
        .define(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, INT, GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DOC)
        .define(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, INT, GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DOC)
        .define(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG, INT, GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_DOC)
        .define(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, LIST, GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_DEFAULT, null, MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_DOC)
        .defineInternal(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, STRING, GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_DEFAULT, ConfigDef.CaseInsensitiveValidString.in(Utils.enumOptions(ConsumerGroupMigrationPolicy.class)), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_DOC)

        /** ********* Offset management configuration ***********/
        .define(GroupCoordinatorConfig.OFFSET_METADATA_MAX_SIZE_CONFIG, INT, GroupCoordinatorConfig.OFFSET_METADATA_MAX_SIZE_DEFAULT, HIGH, GroupCoordinatorConfig.OFFSET_METADATA_MAX_SIZE_DOC)
        .define(GroupCoordinatorConfig.OFFSETS_LOAD_BUFFER_SIZE_CONFIG, INT, GroupCoordinatorConfig.OFFSETS_LOAD_BUFFER_SIZE_DEFAULT, atLeast(1), HIGH, GroupCoordinatorConfig.OFFSETS_LOAD_BUFFER_SIZE_DOC)
        .define(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, SHORT, GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_DEFAULT, atLeast(1), HIGH, GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_DOC)
        .define(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, INT, GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_DEFAULT, atLeast(1), HIGH, GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_DOC)
        .define(GroupCoordinatorConfig.OFFSETS_TOPIC_SEGMENT_BYTES_CONFIG, INT, GroupCoordinatorConfig.OFFSETS_TOPIC_SEGMENT_BYTES_DEFAULT, atLeast(1), HIGH, GroupCoordinatorConfig.OFFSETS_TOPIC_SEGMENT_BYTES_DOC)
        .define(GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_CONFIG, INT, (int) GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_DEFAULT.id, HIGH, GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_DOC)
        .define(GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_CONFIG, INT, GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_DEFAULT, atLeast(1), HIGH, GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_DOC)
        .define(GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG, LONG, GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_DEFAULT, atLeast(1), HIGH, GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_DOC)
        .define(GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG, INT, GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT, atLeast(1), HIGH, GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_DOC)
        .define(GroupCoordinatorConfig.OFFSET_COMMIT_REQUIRED_ACKS_CONFIG, SHORT, GroupCoordinatorConfig.OFFSET_COMMIT_REQUIRED_ACKS_DEFAULT, HIGH, GroupCoordinatorConfig.OFFSET_COMMIT_REQUIRED_ACKS_DOC)
        .define(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, BOOLEAN, ServerConfigs.DELETE_TOPIC_ENABLE_DEFAULT, HIGH, ServerConfigs.DELETE_TOPIC_ENABLE_DOC)
        .define(ServerConfigs.COMPRESSION_TYPE_CONFIG, STRING, LogConfig.DEFAULT_COMPRESSION_TYPE, ConfigDef.ValidString.in(BrokerCompressionType.names().toArray(new String[0])), HIGH, ServerConfigs.COMPRESSION_TYPE_DOC)
        .define(ServerConfigs.COMPRESSION_GZIP_LEVEL_CONFIG, INT, GzipCompression.DEFAULT_LEVEL, new GzipCompression.LevelValidator(), MEDIUM, ServerConfigs.COMPRESSION_GZIP_LEVEL_DOC)
        .define(ServerConfigs.COMPRESSION_LZ4_LEVEL_CONFIG, INT, Lz4Compression.DEFAULT_LEVEL, between(Lz4Compression.MIN_LEVEL, Lz4Compression.MAX_LEVEL), MEDIUM, ServerConfigs.COMPRESSION_LZ4_LEVEL_DOC)
        .define(ServerConfigs.COMPRESSION_ZSTD_LEVEL_CONFIG, INT, ZstdCompression.DEFAULT_LEVEL, between(ZstdCompression.MIN_LEVEL, ZstdCompression.MAX_LEVEL), MEDIUM, ServerConfigs.COMPRESSION_ZSTD_LEVEL_DOC)

        /** ********* Transaction management configuration ***********/
        .define(TransactionStateManagerConfigs.TRANSACTIONAL_ID_EXPIRATION_MS_CONFIG, INT, TransactionStateManagerConfigs.TRANSACTIONAL_ID_EXPIRATION_MS_DEFAULT, atLeast(1), HIGH, TransactionStateManagerConfigs.TRANSACTIONAL_ID_EXPIRATION_MS_DOC)
        .define(TransactionStateManagerConfigs.TRANSACTIONS_MAX_TIMEOUT_MS_CONFIG, INT, TransactionStateManagerConfigs.TRANSACTIONS_MAX_TIMEOUT_MS_DEFAULT, atLeast(1), HIGH, TransactionStateManagerConfigs.TRANSACTIONS_MAX_TIMEOUT_MS_DOC)
        .define(TransactionLogConfigs.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, INT, TransactionLogConfigs.TRANSACTIONS_TOPIC_MIN_ISR_DEFAULT, atLeast(1), HIGH, TransactionLogConfigs.TRANSACTIONS_TOPIC_MIN_ISR_DOC)
        .define(TransactionLogConfigs.TRANSACTIONS_LOAD_BUFFER_SIZE_CONFIG, INT, TransactionLogConfigs.TRANSACTIONS_LOAD_BUFFER_SIZE_DEFAULT, atLeast(1), HIGH, TransactionLogConfigs.TRANSACTIONS_LOAD_BUFFER_SIZE_DOC)
        .define(TransactionLogConfigs.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, SHORT, TransactionLogConfigs.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_DEFAULT, atLeast(1), HIGH, TransactionLogConfigs.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_DOC)
        .define(TransactionLogConfigs.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, INT, TransactionLogConfigs.TRANSACTIONS_TOPIC_PARTITIONS_DEFAULT, atLeast(1), HIGH, TransactionLogConfigs.TRANSACTIONS_TOPIC_PARTITIONS_DOC)
        .define(TransactionLogConfigs.TRANSACTIONS_TOPIC_SEGMENT_BYTES_CONFIG, INT, TransactionLogConfigs.TRANSACTIONS_TOPIC_SEGMENT_BYTES_DEFAULT, atLeast(1), HIGH, TransactionLogConfigs.TRANSACTIONS_TOPIC_SEGMENT_BYTES_DOC)
        .define(TransactionStateManagerConfigs.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, INT, TransactionStateManagerConfigs.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_DEFAULT, atLeast(1), LOW, TransactionStateManagerConfigs.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTIONS_INTERVAL_MS_DOC)
        .define(TransactionStateManagerConfigs.TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_CONFIG, INT, TransactionStateManagerConfigs.TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_DEFAULT, atLeast(1), LOW, TransactionStateManagerConfigs.TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONS_INTERVAL_MS_DOC)

        .define(TransactionLogConfigs.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG, BOOLEAN, TransactionLogConfigs.TRANSACTION_PARTITION_VERIFICATION_ENABLE_DEFAULT, LOW, TransactionLogConfigs.TRANSACTION_PARTITION_VERIFICATION_ENABLE_DOC)

        .define(TransactionLogConfigs.PRODUCER_ID_EXPIRATION_MS_CONFIG, INT, TransactionLogConfigs.PRODUCER_ID_EXPIRATION_MS_DEFAULT, atLeast(1), LOW, TransactionLogConfigs.PRODUCER_ID_EXPIRATION_MS_DOC)
        // Configuration for testing only as default value should be sufficient for typical usage
        .defineInternal(TransactionLogConfigs.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_CONFIG, INT, TransactionLogConfigs.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT, atLeast(1), LOW, TransactionLogConfigs.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DOC)

        /** ********* Fetch Configuration **************/
        .define(ServerConfigs.MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_CONFIG, INT, ServerConfigs.MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_DEFAULT, atLeast(0), MEDIUM, ServerConfigs.MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_DOC)
        .define(ServerConfigs.FETCH_MAX_BYTES_CONFIG, INT, ServerConfigs.FETCH_MAX_BYTES_DEFAULT, atLeast(1024), MEDIUM, ServerConfigs.FETCH_MAX_BYTES_DOC)

        /** ********* Request Limit Configuration ***********/
        .define(ServerConfigs.MAX_REQUEST_PARTITION_SIZE_LIMIT_CONFIG, INT, ServerConfigs.MAX_REQUEST_PARTITION_SIZE_LIMIT_DEFAULT, atLeast(1), MEDIUM, ServerConfigs.MAX_REQUEST_PARTITION_SIZE_LIMIT_DOC)

        /** ********* Kafka Metrics Configuration ***********/
        .define(MetricConfigs.METRIC_NUM_SAMPLES_CONFIG, INT, MetricConfigs.METRIC_NUM_SAMPLES_DEFAULT, atLeast(1), LOW, MetricConfigs.METRIC_NUM_SAMPLES_DOC)
        .define(MetricConfigs.METRIC_SAMPLE_WINDOW_MS_CONFIG, LONG, MetricConfigs.METRIC_SAMPLE_WINDOW_MS_DEFAULT, atLeast(1), LOW, MetricConfigs.METRIC_SAMPLE_WINDOW_MS_DOC)
        .define(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, LIST, MetricConfigs.METRIC_REPORTER_CLASSES_DEFAULT, LOW, MetricConfigs.METRIC_REPORTER_CLASSES_DOC)
        .define(MetricConfigs.METRIC_RECORDING_LEVEL_CONFIG, STRING, MetricConfigs.METRIC_RECORDING_LEVEL_DEFAULT, LOW, MetricConfigs.METRIC_RECORDING_LEVEL_DOC)
        .define(MetricConfigs.AUTO_INCLUDE_JMX_REPORTER_CONFIG, BOOLEAN, MetricConfigs.AUTO_INCLUDE_JMX_REPORTER_DEFAULT, LOW, MetricConfigs.AUTO_INCLUDE_JMX_REPORTER_DOC)

        /** ********* Kafka Yammer Metrics Reporter Configuration for docs ***********/
        .define(MetricConfigs.KAFKA_METRICS_REPORTER_CLASSES_CONFIG, LIST, MetricConfigs.KAFKA_METRIC_REPORTER_CLASSES_DEFAULT, LOW, MetricConfigs.KAFKA_METRICS_REPORTER_CLASSES_DOC)
        .define(MetricConfigs.KAFKA_METRICS_POLLING_INTERVAL_SECONDS_CONFIG, INT, MetricConfigs.KAFKA_METRICS_POLLING_INTERVAL_SECONDS_DEFAULT, atLeast(1), LOW, MetricConfigs.KAFKA_METRICS_POLLING_INTERVAL_SECONDS_DOC)

        /** ********* Kafka Client Telemetry Metrics Configuration ***********/
        .define(MetricConfigs.CLIENT_TELEMETRY_MAX_BYTES_CONFIG, INT, MetricConfigs.CLIENT_TELEMETRY_MAX_BYTES_DEFAULT, atLeast(1), LOW, MetricConfigs.CLIENT_TELEMETRY_MAX_BYTES_DOC)

        /** ********* Quota configuration ***********/
        .define(QuotaConfigs.NUM_QUOTA_SAMPLES_CONFIG, INT, QuotaConfigs.NUM_QUOTA_SAMPLES_DEFAULT, atLeast(1), LOW, QuotaConfigs.NUM_QUOTA_SAMPLES_DOC)
        .define(QuotaConfigs.NUM_REPLICATION_QUOTA_SAMPLES_CONFIG, INT, QuotaConfigs.NUM_QUOTA_SAMPLES_DEFAULT, atLeast(1), LOW, QuotaConfigs.NUM_REPLICATION_QUOTA_SAMPLES_DOC)
        .define(QuotaConfigs.NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_CONFIG, INT, QuotaConfigs.NUM_QUOTA_SAMPLES_DEFAULT, atLeast(1), LOW, QuotaConfigs.NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_DOC)
        .define(QuotaConfigs.NUM_CONTROLLER_QUOTA_SAMPLES_CONFIG, INT, QuotaConfigs.NUM_QUOTA_SAMPLES_DEFAULT, atLeast(1), LOW, QuotaConfigs.NUM_CONTROLLER_QUOTA_SAMPLES_DOC)
        .define(QuotaConfigs.QUOTA_WINDOW_SIZE_SECONDS_CONFIG, INT, QuotaConfigs.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT, atLeast(1), LOW, QuotaConfigs.QUOTA_WINDOW_SIZE_SECONDS_DOC)
        .define(QuotaConfigs.REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_CONFIG, INT, QuotaConfigs.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT, atLeast(1), LOW, QuotaConfigs.REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_DOC)
        .define(QuotaConfigs.ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_CONFIG, INT, QuotaConfigs.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT, atLeast(1), LOW, QuotaConfigs.ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_DOC)
        .define(QuotaConfigs.CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_CONFIG, INT, QuotaConfigs.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT, atLeast(1), LOW, QuotaConfigs.CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_DOC)
        .define(QuotaConfigs.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG, CLASS, null, LOW, QuotaConfigs.CLIENT_QUOTA_CALLBACK_CLASS_DOC)

        /** ********* General Security Configuration ****************/
        .define(KafkaSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_CONFIG, LONG, KafkaSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_DEFAULT, MEDIUM, KafkaSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_DOC)
        .define(KafkaSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG, INT, KafkaSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_DOC)
        .define(KafkaSecurityConfigs.SECURITY_PROVIDER_CLASS_CONFIG, STRING, null, LOW, KafkaSecurityConfigs.SECURITY_PROVIDERS_DOC)

        /** ********* SSL Configuration ****************/
        .define(KafkaSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, CLASS, KafkaSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DEFAULT, MEDIUM, KafkaSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DOC)
        .define(KafkaSecurityConfigs.SSL_PROTOCOL_CONFIG, STRING, KafkaSecurityConfigs.SSL_PROTOCOL_DEFAULT, MEDIUM, KafkaSecurityConfigs.SSL_PROTOCOL_DOC)
        .define(KafkaSecurityConfigs.SSL_PROVIDER_CONFIG, STRING, null, MEDIUM, KafkaSecurityConfigs.SSL_PROVIDER_DOC)
        .define(KafkaSecurityConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, LIST, KafkaSecurityConfigs.SSL_ENABLED_PROTOCOLS_DEFAULTS, MEDIUM, KafkaSecurityConfigs.SSL_ENABLED_PROTOCOLS_DOC)
        .define(KafkaSecurityConfigs.SSL_KEYSTORE_TYPE_CONFIG, STRING, KafkaSecurityConfigs.SSL_KEYSTORE_TYPE_DEFAULT, MEDIUM, KafkaSecurityConfigs.SSL_KEYSTORE_TYPE_DOC)
        .define(KafkaSecurityConfigs.SSL_KEYSTORE_LOCATION_CONFIG, STRING, null, MEDIUM, KafkaSecurityConfigs.SSL_KEYSTORE_LOCATION_DOC)
        .define(KafkaSecurityConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, KafkaSecurityConfigs.SSL_KEYSTORE_PASSWORD_DOC)
        .define(KafkaSecurityConfigs.SSL_KEY_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, KafkaSecurityConfigs.SSL_KEY_PASSWORD_DOC)
        .define(KafkaSecurityConfigs.SSL_KEYSTORE_KEY_CONFIG, PASSWORD, null, MEDIUM, KafkaSecurityConfigs.SSL_KEYSTORE_KEY_DOC)
        .define(KafkaSecurityConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, PASSWORD, null, MEDIUM, KafkaSecurityConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC)
        .define(KafkaSecurityConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, STRING, KafkaSecurityConfigs.SSL_TRUSTSTORE_TYPE_DEFAULT, MEDIUM, KafkaSecurityConfigs.SSL_TRUSTSTORE_TYPE_DOC)
        .define(KafkaSecurityConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, STRING, null, MEDIUM, KafkaSecurityConfigs.SSL_TRUSTSTORE_LOCATION_DOC)
        .define(KafkaSecurityConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, KafkaSecurityConfigs.SSL_TRUSTSTORE_PASSWORD_DOC)
        .define(KafkaSecurityConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, PASSWORD, null, MEDIUM, KafkaSecurityConfigs.SSL_TRUSTSTORE_CERTIFICATES_DOC)
        .define(KafkaSecurityConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, STRING, KafkaSecurityConfigs.SSL_KEYMANAGER_ALGORITHM_DEFAULT, MEDIUM, KafkaSecurityConfigs.SSL_KEYMANAGER_ALGORITHM_DOC)
        .define(KafkaSecurityConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, STRING, KafkaSecurityConfigs.SSL_TRUSTMANAGER_ALGORITHM_DEFAULT, MEDIUM, KafkaSecurityConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC)
        .define(KafkaSecurityConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, STRING, KafkaSecurityConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DEFAULT, LOW, KafkaSecurityConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
        .define(KafkaSecurityConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, STRING, null, LOW, KafkaSecurityConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC)
        .define(KafkaSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, STRING, KafkaSecurityConfigs.SSL_CLIENT_AUTH_DEFAULT, ConfigDef.ValidString.in(KafkaSecurityConfigs.SSL_CLIENT_AUTHENTICATION_VALID_VALUES), MEDIUM, KafkaSecurityConfigs.SSL_CLIENT_AUTH_DOC)
        .define(KafkaSecurityConfigs.SSL_CIPHER_SUITES_CONFIG, LIST, Collections.emptyList(), MEDIUM, KafkaSecurityConfigs.SSL_CIPHER_SUITES_DOC)
        .define(KafkaSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG, STRING, KafkaSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_DEFAULT, LOW, KafkaSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_DOC)
        .define(KafkaSecurityConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, CLASS, null, LOW, KafkaSecurityConfigs.SSL_ENGINE_FACTORY_CLASS_DOC)
        .define(KafkaSecurityConfigs.SSL_ALLOW_DN_CHANGES_CONFIG, BOOLEAN, KafkaSecurityConfigs.SSL_ALLOW_DN_CHANGES_DEFAULT, LOW, KafkaSecurityConfigs.SSL_ALLOW_DN_CHANGES_DOC)
        .define(KafkaSecurityConfigs.SSL_ALLOW_SAN_CHANGES_CONFIG, BOOLEAN, KafkaSecurityConfigs.SSL_ALLOW_SAN_CHANGES_DEFAULT, LOW, KafkaSecurityConfigs.SSL_ALLOW_SAN_CHANGES_DOC)

        /** ********* Sasl Configuration ****************/
        .define(KafkaSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, STRING, KafkaSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_DOC)
        .define(KafkaSecurityConfigs.SASL_JAAS_CONFIG, PASSWORD, null, MEDIUM, KafkaSecurityConfigs.SASL_JAAS_CONFIG_DOC)
        .define(KafkaSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, LIST, KafkaSecurityConfigs.SASL_ENABLED_MECHANISMS_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_ENABLED_MECHANISMS_DOC)
        .define(KafkaSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG, CLASS, null, MEDIUM, KafkaSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC)
        .define(KafkaSecurityConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_CONFIG, CLASS, null, MEDIUM, KafkaSecurityConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC)
        .define(KafkaSecurityConfigs.SASL_LOGIN_CLASS_CONFIG, CLASS, null, MEDIUM, KafkaSecurityConfigs.SASL_LOGIN_CLASS_DOC)
        .define(KafkaSecurityConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_CONFIG, CLASS, null, MEDIUM, KafkaSecurityConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC)
        .define(KafkaSecurityConfigs.SASL_KERBEROS_SERVICE_NAME_CONFIG, STRING, null, MEDIUM, KafkaSecurityConfigs.SASL_KERBEROS_SERVICE_NAME_DOC)
        .define(KafkaSecurityConfigs.SASL_KERBEROS_KINIT_CMD_CONFIG, STRING, KafkaSecurityConfigs.SASL_KERBEROS_KINIT_CMD_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_KERBEROS_KINIT_CMD_DOC)
        .define(KafkaSecurityConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_CONFIG, DOUBLE, KafkaSecurityConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC)
        .define(KafkaSecurityConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_CONFIG, DOUBLE, KafkaSecurityConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DOC)
        .define(KafkaSecurityConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_CONFIG, LONG, KafkaSecurityConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC)
        .define(KafkaSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG, LIST, KafkaSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC)
        .define(KafkaSecurityConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_CONFIG, DOUBLE, KafkaSecurityConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC)
        .define(KafkaSecurityConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_CONFIG, DOUBLE, KafkaSecurityConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC)
        .define(KafkaSecurityConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_CONFIG, SHORT, KafkaSecurityConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC)
        .define(KafkaSecurityConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_CONFIG, SHORT, KafkaSecurityConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC)
        .define(KafkaSecurityConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS_CONFIG, INT, null, LOW, KafkaSecurityConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC)
        .define(KafkaSecurityConfigs.SASL_LOGIN_READ_TIMEOUT_MS_CONFIG, INT, null, LOW, KafkaSecurityConfigs.SASL_LOGIN_READ_TIMEOUT_MS_DOC)
        .define(KafkaSecurityConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS_CONFIG, LONG, KafkaSecurityConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DEFAULT, LOW, KafkaSecurityConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC)
        .define(KafkaSecurityConfigs.SASL_LOGIN_RETRY_BACKOFF_MS_CONFIG, LONG, KafkaSecurityConfigs.SASL_LOGIN_RETRY_BACKOFF_MS_DEFAULT, LOW, KafkaSecurityConfigs.SASL_LOGIN_RETRY_BACKOFF_MS_DOC)
        .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_CONFIG, STRING, KafkaSecurityConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DEFAULT, LOW, KafkaSecurityConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC)
        .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME_CONFIG, STRING, KafkaSecurityConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME_DEFAULT, LOW, KafkaSecurityConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC)
        .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_CONFIG, STRING, null, MEDIUM, KafkaSecurityConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC)
        .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_CONFIG, STRING, null, MEDIUM, KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC)
        .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_CONFIG, LONG, KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DEFAULT, LOW, KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC)
        .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_CONFIG, LONG, KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DEFAULT, LOW, KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC)
        .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_CONFIG, LONG, KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DEFAULT, LOW, KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC)
        .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_CONFIG, INT, KafkaSecurityConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DEFAULT, LOW, KafkaSecurityConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC)
        .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE_CONFIG, LIST, null, LOW, KafkaSecurityConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC)
        .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER_CONFIG, STRING, null, LOW, KafkaSecurityConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC)

        /** ********* Delegation Token Configuration ****************/
        .define(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_ALIAS_CONFIG, PASSWORD, null, MEDIUM, DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_ALIAS_DOC)
        .define(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_CONFIG, PASSWORD, null, MEDIUM, DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_DOC)
        .define(DelegationTokenManagerConfigs.DELEGATION_TOKEN_MAX_LIFETIME_CONFIG, LONG, DelegationTokenManagerConfigs.DELEGATION_TOKEN_MAX_LIFE_TIME_MS_DEFAULT, atLeast(1), MEDIUM, DelegationTokenManagerConfigs.DELEGATION_TOKEN_MAX_LIFE_TIME_DOC)
        .define(DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_TIME_MS_CONFIG, LONG, DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_TIME_MS_DEFAULT, atLeast(1), MEDIUM, DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_TIME_MS_DOC)
        .define(DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_CONFIG, LONG, DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_DEFAULT, atLeast(1), LOW, DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_DOC)

        /** ********* Password encryption configuration for dynamic configs *********/
        .define(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG, PASSWORD, null, MEDIUM, PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_DOC)
        .define(PasswordEncoderConfigs.PASSWORD_ENCODER_OLD_SECRET_CONFIG, PASSWORD, null, MEDIUM, PasswordEncoderConfigs.PASSWORD_ENCODER_OLD_SECRET_DOC)
        .define(PasswordEncoderConfigs.PASSWORD_ENCODER_KEYFACTORY_ALGORITHM_CONFIG, STRING, null, LOW, PasswordEncoderConfigs.PASSWORD_ENCODER_KEYFACTORY_ALGORITHM_DOC)
        .define(PasswordEncoderConfigs.PASSWORD_ENCODER_CIPHER_ALGORITHM_CONFIG, STRING, PasswordEncoderConfigs.PASSWORD_ENCODER_CIPHER_ALGORITHM_DEFAULT, LOW, PasswordEncoderConfigs.PASSWORD_ENCODER_CIPHER_ALGORITHM_DOC)
        .define(PasswordEncoderConfigs.PASSWORD_ENCODER_KEY_LENGTH_CONFIG, INT, PasswordEncoderConfigs.PASSWORD_ENCODER_KEY_LENGTH_DEFAULT, atLeast(8), LOW, PasswordEncoderConfigs.PASSWORD_ENCODER_KEY_LENGTH_DOC)
        .define(PasswordEncoderConfigs.PASSWORD_ENCODER_ITERATIONS_CONFIG, INT, PasswordEncoderConfigs.PASSWORD_ENCODER_ITERATIONS_DEFAULT, atLeast(1024), LOW, PasswordEncoderConfigs.PASSWORD_ENCODER_ITERATIONS_DOC)

        /** ********* Raft Quorum Configuration *********/
        .define(QuorumConfig.QUORUM_VOTERS_CONFIG, LIST, QuorumConfig.DEFAULT_QUORUM_VOTERS, new QuorumConfig.ControllerQuorumVotersValidator(), HIGH, QuorumConfig.QUORUM_VOTERS_DOC)
        .define(QuorumConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_ELECTION_TIMEOUT_MS, null, HIGH, QuorumConfig.QUORUM_ELECTION_TIMEOUT_MS_DOC)
        .define(QuorumConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_FETCH_TIMEOUT_MS, null, HIGH, QuorumConfig.QUORUM_FETCH_TIMEOUT_MS_DOC)
        .define(QuorumConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_ELECTION_BACKOFF_MAX_MS, null, HIGH, QuorumConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_DOC)
        .define(QuorumConfig.QUORUM_LINGER_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_LINGER_MS, null, MEDIUM, QuorumConfig.QUORUM_LINGER_MS_DOC)
        .define(QuorumConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_REQUEST_TIMEOUT_MS, null, MEDIUM, QuorumConfig.QUORUM_REQUEST_TIMEOUT_MS_DOC)
        .define(QuorumConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_RETRY_BACKOFF_MS, null, LOW, QuorumConfig.QUORUM_RETRY_BACKOFF_MS_DOC)

        /** Internal Configurations **/
        // This indicates whether unreleased APIs should be advertised by this node.
        .defineInternal(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, BOOLEAN, false, HIGH)
        // This indicates whether unreleased MetadataVersions should be enabled on this node.
        .defineInternal(ServerConfigs.UNSTABLE_METADATA_VERSIONS_ENABLE_CONFIG, BOOLEAN, false, HIGH);

    public KafkaConfig(ConfigDef definition, Map<?, ?> originals, Map<String, ?> configProviderProps, boolean doLog) {
        super(definition, originals, configProviderProps, doLog);
    }

    public KafkaConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }

    public KafkaConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
        super(definition, originals, doLog);
    }

    // Cache the current config to avoid acquiring read lock to access from dynamicConfig
    protected volatile KafkaConfig currentConfig = this;

    @Override
    public Map<String, Object> originals() {
        return (this == currentConfig) ? super.originals() : currentConfig.originals();
    }

    @Override
    public Map<String, ?> values() {
        return (this == currentConfig) ? super.values() : currentConfig.values();
    }

    @Override
    public Map<String, ?> nonInternalValues() {
        return (this == currentConfig) ? super.nonInternalValues() : currentConfig.nonInternalValues();
    }

    @Override
    public Map<String, String> originalsStrings() {
        return (this == currentConfig) ? super.originalsStrings() : currentConfig.originalsStrings();
    }

    @Override
    public Map<String, Object> originalsWithPrefix(String prefix) {
        return (this == currentConfig) ? super.originalsWithPrefix(prefix) : currentConfig.originalsWithPrefix(prefix);
    }

    @Override
    public Map<String, Object> valuesWithPrefixOverride(String prefix) {
        return (this == currentConfig) ? super.valuesWithPrefixOverride(prefix) : currentConfig.valuesWithPrefixOverride(prefix);
    }

    @Override
    public Object get(String key) {
        return (this == currentConfig) ? super.get(key) : currentConfig.get(key);
    }

    private final RemoteLogManagerConfig _remoteLogManagerConfig = new RemoteLogManagerConfig(this);

    public final RemoteLogManagerConfig remoteLogManagerConfig() {
        return _remoteLogManagerConfig;
    }

    /** ********* Socket Server Configuration ***********/
    public final int socketSendBufferBytes() {
        return getInt(SocketServerConfigs.SOCKET_SEND_BUFFER_BYTES_CONFIG);
    }
    public final int socketReceiveBufferBytes() {
        return getInt(SocketServerConfigs.SOCKET_RECEIVE_BUFFER_BYTES_CONFIG);
    }
    public final int socketRequestMaxBytes() {
        return getInt(SocketServerConfigs.SOCKET_REQUEST_MAX_BYTES_CONFIG);
    }
    public final int socketListenBacklogSize() {
        return getInt(SocketServerConfigs.SOCKET_LISTEN_BACKLOG_SIZE_CONFIG);
    }
    public final int maxConnectionsPerIp(){
        return getInt(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG);
    }
    public final Map<String, Integer> maxConnectionsPerIpOverrides() {
        return getMap(
                SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG,
                getString(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG)
        ).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> Integer.parseInt(entry.getValue())));
    }
    public final int maxConnections(){
        return getInt(SocketServerConfigs.MAX_CONNECTIONS_CONFIG);
    }
    public final int  maxConnectionCreationRate() {
        return getInt(SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG);
    }
    public final long connectionsMaxIdleMs() {
        return getLong(SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG);
    }
    public final int failedAuthenticationDelayMs() {
        return getInt(SocketServerConfigs.FAILED_AUTHENTICATION_DELAY_MS_CONFIG);
    }

    /***************** rack configuration **************/
    public final Optional<String> rack() {
        return Optional.ofNullable(getString(ServerConfigs.BROKER_RACK_CONFIG));
    }
    public final Optional<String> replicaSelectorClassName(){
        return Optional.ofNullable(getString(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG));
    }

    /** ********* Log Configuration ***********/
    public final Boolean autoCreateTopicsEnable() {
        return getBoolean(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG);
    }
    public final int numPartitions() {
        return getInt(ServerLogConfigs.NUM_PARTITIONS_CONFIG);
    }
    public final List<String> logDirs() {
        return Csv.parseCsvList(Optional.ofNullable(getString(ServerLogConfigs.LOG_DIRS_CONFIG)).orElse(getString(ServerLogConfigs.LOG_DIR_CONFIG)));
    }
    public final int logSegmentBytes() {
        return getInt(ServerLogConfigs.LOG_SEGMENT_BYTES_CONFIG);
    }
    public final long logFlushIntervalMessages() {
        return getLong(ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG);
    }
    public final int  logCleanerThreads() {
        return getInt(CleanerConfig.LOG_CLEANER_THREADS_PROP);
    }
    public final int  numRecoveryThreadsPerDataDir() {
        return getInt(ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG);
    }
    public final long logFlushSchedulerIntervalMs() {
        return getLong(ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG);
    }
    public final long logFlushOffsetCheckpointIntervalMs() {
        return (long) getInt(ServerLogConfigs.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG);
    }
    public final int logFlushStartOffsetCheckpointIntervalMs() {
        return getInt(ServerLogConfigs.LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG);
    }
    public final long logCleanupIntervalMs() {
        return getLong(ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG);
    }
    public final List<String> logCleanupPolicy() {
        return getList(ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG);
    }

    public final int  offsetsRetentionMinutes() {
        return getInt(GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_CONFIG);
    }
    public final long offsetsRetentionCheckIntervalMs() {
        return getLong(GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG);
    }
    public final long logRetentionBytes() {
        return getLong(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG);
    }
    public final long logCleanerDedupeBufferSize() {
        return getLong(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP);
    }
    public final double logCleanerDedupeBufferLoadFactor() {
        return getDouble(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP);
    }
    public final int  logCleanerIoBufferSize() {
        return getInt(CleanerConfig.LOG_CLEANER_IO_BUFFER_SIZE_PROP);
    }
    public final double logCleanerIoMaxBytesPerSecond() {
        return getDouble(CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP);
    }
    public final long logCleanerDeleteRetentionMs() {
        return getLong(CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP);
    }
    public final long logCleanerMinCompactionLagMs() {
        return getLong(CleanerConfig.LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP);
    }
    public final long logCleanerMaxCompactionLagMs() {
        return getLong(CleanerConfig.LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP);
    }
    public final long logCleanerBackoffMs() {
        return getLong(CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP);
    }
    public final double logCleanerMinCleanRatio() {
        return getDouble(CleanerConfig.LOG_CLEANER_MIN_CLEAN_RATIO_PROP);
    }
    public final boolean logCleanerEnable() {
        return getBoolean(CleanerConfig.LOG_CLEANER_ENABLE_PROP);
    }
    public final int  logIndexSizeMaxBytes() {
        return getInt(ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_CONFIG);
    }
    public final int  logIndexIntervalBytes() {
        return getInt(ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_CONFIG);
    }
    public final long logDeleteDelayMs() {
        return getLong(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG);
    }
    public final Long logRollTimeMillis() {
        return Optional.ofNullable(getLong(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG)).orElse(60 * 60 * 1000L * getInt(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG));
    }
    public final Long logRollTimeJitterMillis() {
        return Optional.ofNullable(getLong(ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_CONFIG)).orElse(60 * 60 * 1000L * getInt(ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_CONFIG));
    }
    public final Long logFlushIntervalMs() {
        return Optional.ofNullable(getLong(ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG)).orElse(getLong(ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG));
    }
    public final int  minInSyncReplicas() {
        return getInt(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG);
    }

    public final boolean logPreAllocateEnable() {
        return getBoolean(ServerLogConfigs.LOG_PRE_ALLOCATE_CONFIG);
    }

    public final Long logInitialTaskDelayMs() {
        return Optional.ofNullable(getLong(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG)).orElse(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_DEFAULT);
    }

    // We keep the user-provided String as `MetadataVersion.fromVersionString` can choose a slightly different version (eg if `0.10.0`
    // is passed, `0.10.0-IV0` may be picked)
    @Deprecated
    public final String logMessageFormatVersionString() {
        return getString(ServerLogConfigs.LOG_MESSAGE_FORMAT_VERSION_CONFIG);
    }

    /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details
    deprecated since 3.0
     */
    @Deprecated
    public final MetadataVersion logMessageFormatVersion(){
        if (LogConfig.shouldIgnoreMessageFormatVersion(interBrokerProtocolVersion()))
            return MetadataVersion.fromVersionString(ServerLogConfigs.LOG_MESSAGE_FORMAT_VERSION_DEFAULT);
        else return MetadataVersion.fromVersionString(logMessageFormatVersionString());
    }

    public final TimestampType logMessageTimestampType() {
        return TimestampType.forName(getString(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG));
    }

    /* See `TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG` for details
    deprecated since 3.6
     */
    @Deprecated
    public final long logMessageTimestampDifferenceMaxMs() {
        return getLong(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG);
    }

    // In the transition period before logMessageTimestampDifferenceMaxMs is removed, to maintain backward compatibility,
    // we are using its value if logMessageTimestampBeforeMaxMs default value hasn't changed.
    // See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for deprecation details
    @Deprecated
    public final long logMessageTimestampBeforeMaxMs() {
        long messageTimestampBeforeMaxMs = getLong(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG);
        if (messageTimestampBeforeMaxMs != ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DEFAULT) {
            return messageTimestampBeforeMaxMs;
        } else {
            return logMessageTimestampDifferenceMaxMs();
        }
    }
    // In the transition period before logMessageTimestampDifferenceMaxMs is removed, to maintain backward compatibility,
    // we are using its value if logMessageTimestampAfterMaxMs default value hasn't changed.
    // See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for deprecation details
    @Deprecated
    public final long  logMessageTimestampAfterMaxMs() {
        long messageTimestampAfterMaxMs = getLong(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG);
        if (messageTimestampAfterMaxMs != Long.MAX_VALUE) {
            return messageTimestampAfterMaxMs;
        } else {
            return logMessageTimestampDifferenceMaxMs();
        }
    }
    public final boolean logMessageDownConversionEnable() {
        return getBoolean(ServerLogConfigs.LOG_MESSAGE_DOWNCONVERSION_ENABLE_CONFIG);
    }

    public final long logDirFailureTimeoutMs() {
        return getLong(ServerLogConfigs.LOG_DIR_FAILURE_TIMEOUT_MS_CONFIG);
    }

    /** ********* Replication configuration ***********/
    public final int controllerSocketTimeoutMs() {
        return getInt(ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_CONFIG);
    }
    public final int defaultReplicationFactor() {
        return getInt(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG);
    }
    public final long replicaLagTimeMaxMs() {
        return getLong(ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_CONFIG);
    }
    public final int replicaSocketTimeoutMs() {
        return getInt(ReplicationConfigs.REPLICA_SOCKET_TIMEOUT_MS_CONFIG);
    }
    public final int replicaSocketReceiveBufferBytes() {
        return getInt(ReplicationConfigs.REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_CONFIG);
    }
    public final int replicaFetchMaxBytes() {
        return getInt(ReplicationConfigs.REPLICA_FETCH_MAX_BYTES_CONFIG);
    }
    public final int replicaFetchWaitMaxMs() {
        return getInt(ReplicationConfigs.REPLICA_FETCH_WAIT_MAX_MS_CONFIG);
    }
    public final int replicaFetchMinBytes() {
        return getInt(ReplicationConfigs.REPLICA_FETCH_MIN_BYTES_CONFIG);
    }
    public final int replicaFetchResponseMaxBytes() {
        return getInt(ReplicationConfigs.REPLICA_FETCH_RESPONSE_MAX_BYTES_CONFIG);
    }
    public final int replicaFetchBackoffMs() {
        return getInt(ReplicationConfigs.REPLICA_FETCH_BACKOFF_MS_CONFIG);
    }
    public final int numReplicaFetchers() {
        return getInt(ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG);
    }
    public final long replicaHighWatermarkCheckpointIntervalMs() {
        return getLong(ReplicationConfigs.REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_CONFIG);
    }
    public final int fetchPurgatoryPurgeIntervalRequests() {
        return getInt(ReplicationConfigs.FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG);
    }
    public final int producerPurgatoryPurgeIntervalRequests() {
        return getInt(ReplicationConfigs.PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG);
    }
    public final int deleteRecordsPurgatoryPurgeIntervalRequests() {
        return getInt(ReplicationConfigs.DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG);
    }
    public final boolean autoLeaderRebalanceEnable() {
        return getBoolean(ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG);
    }
    public final int leaderImbalancePerBrokerPercentage() {
        return getInt(ReplicationConfigs.LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_CONFIG);
    }
    public final long leaderImbalanceCheckIntervalSeconds(){
        return getLong(ReplicationConfigs.LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_CONFIG);
    }
    public final boolean uncleanLeaderElectionEnable() {
        return getBoolean(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG);
    }

    // We keep the user-provided String as `MetadataVersion.fromVersionString` can choose a slightly different version (eg if `0.10.0`
    // is passed, `0.10.0-IV0` may be picked)
    public final String interBrokerProtocolVersionString() {
        return getString(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG);
    }
    public final MetadataVersion interBrokerProtocolVersion() {
        if (processRoles.isEmpty()) {
            return MetadataVersion.fromVersionString(interBrokerProtocolVersionString());
        } else {
            if (originals().containsKey(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG)) {
                // A user-supplied IBP was given
                MetadataVersion configuredVersion = MetadataVersion.fromVersionString(interBrokerProtocolVersionString());
                if (!configuredVersion.isKRaftSupported()) {
                    throw new ConfigException(String.format("A non-KRaft version %s given for %s. The minimum version is %s",
                            interBrokerProtocolVersionString(), ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG, MetadataVersion.MINIMUM_KRAFT_VERSION));
                } else {
                    log.warn(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG + " is deprecated in KRaft mode as of 3.3 and will only " +
                            "be read when first upgrading from a KRaft prior to 3.3. See kafka-storage.sh help for details on setting " +
                            "the metadata.version for a new KRaft cluster.");
                }
            }
            // In KRaft mode, we pin this value to the minimum KRaft-supported version. This prevents inadvertent usage of
            // the static IBP config in broker components running in KRaft mode
            return MetadataVersion.MINIMUM_KRAFT_VERSION;
        }
    }

    /** ********* Controlled shutdown configuration ***********/
    public final int controlledShutdownMaxRetries() {
        return getInt(ServerConfigs.CONTROLLED_SHUTDOWN_MAX_RETRIES_CONFIG);
    }
    public final long controlledShutdownRetryBackoffMs() {
        return getLong(ServerConfigs.CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_CONFIG);
    }
    public final boolean controlledShutdownEnable() {
        return getBoolean(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG);
    }

    /** ********* Feature configuration ***********/
    public final boolean  isFeatureVersioningSupported() {
        return interBrokerProtocolVersion().isFeatureVersioningSupported();
    }

    /** ********* Group coordinator configuration ***********/
    public final int groupMinSessionTimeoutMs() {
        return getInt(GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG);
    }
    public final int groupMaxSessionTimeoutMs() {
        return getInt(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG);
    }
    public final int groupInitialRebalanceDelay() {
        return getInt(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG);
    }
    public final int groupMaxSize() {
        return getInt(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG);
    }

    /** New group coordinator configs */
    public final Set<Group.GroupType> groupCoordinatorRebalanceProtocols() {
        Set<Group.GroupType> protocols = getList(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG).stream()
                .map(String::toUpperCase)
                .map(Group.GroupType::valueOf)
                .collect(Collectors.toSet());
        if (!protocols.contains(Group.GroupType.CLASSIC)) {
            throw new ConfigException(String.format("Disabling the '%s' protocol is not supported.", Group.GroupType.CLASSIC));
        }
        if (protocols.contains(Group.GroupType.CONSUMER)) {
            log.warn(String.format("The new '%s' rebalance protocol is enabled along with the new group coordinator. " +
                    "This is part of the early access of KIP-848 and MUST NOT be used in production.", Group.GroupType.CONSUMER));
        }
        return protocols;
    }
    // The new group coordinator is enabled in two cases: 1) The internal configuration to enable
    // it is explicitly set; or 2) the consumer rebalance protocol is enabled.
    public boolean  isNewGroupCoordinatorEnabled() {
        return getBoolean(GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG) ||
                groupCoordinatorRebalanceProtocols().contains(Group.GroupType.CONSUMER);
    }
    public final int groupCoordinatorNumThreads() {
        return getInt(GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_CONFIG);
    }

    /** Consumer group configs */
    public final int consumerGroupSessionTimeoutMs() {
        return getInt(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG);
    }
    public final int consumerGroupMinSessionTimeoutMs() {
        return getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG);
    }
    public final int consumerGroupMaxSessionTimeoutMs() {
        return getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG);
    }
    public final int consumerGroupHeartbeatIntervalMs() {
        return getInt(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG);
    }
    public final int consumerGroupMinHeartbeatIntervalMs() {
        return getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG);
    }
    public final int consumerGroupMaxHeartbeatIntervalMs() {
        return getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG);
    }
    public final int consumerGroupMaxSize() {
        return getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG);
    }
    public final List<PartitionAssignor> consumerGroupAssignors(){
        return getConfiguredInstances(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, PartitionAssignor.class);
    }
    public final ConsumerGroupMigrationPolicy consumerGroupMigrationPolicy() {
        return ConsumerGroupMigrationPolicy.parse(getString(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG));
    }

    /** ********* Offset management configuration ***********/
    public final int offsetMetadataMaxSize() {
        return getInt(GroupCoordinatorConfig.OFFSET_METADATA_MAX_SIZE_CONFIG);
    }
    public final int offsetsLoadBufferSize() {
        return getInt(GroupCoordinatorConfig.OFFSETS_LOAD_BUFFER_SIZE_CONFIG);
    }
    public short offsetsTopicReplicationFactor() {
        return getShort(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG);
    }
    public final int offsetsTopicPartitions() {
        return getInt(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG);
    }
    public final int offsetCommitTimeoutMs() {
        return getInt(GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG);
    }

    // Deprecated since 3.8
    @Deprecated
    public short offsetCommitRequiredAcks() {
        return getShort(GroupCoordinatorConfig.OFFSET_COMMIT_REQUIRED_ACKS_CONFIG);
    }
    public final int offsetsTopicSegmentBytes() {
        return getInt(GroupCoordinatorConfig.OFFSETS_TOPIC_SEGMENT_BYTES_CONFIG);
    }
    public CompressionType offsetsTopicCompressionType(){
        return Optional.ofNullable(getInt(GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_CONFIG))
                .map(CompressionType::forId).orElse(null);
    }

    /** ********* Transaction management configuration ***********/
    public final int transactionalIdExpirationMs() {
        return getInt(TransactionStateManagerConfigs.TRANSACTIONAL_ID_EXPIRATION_MS_CONFIG);
    }
    public final int transactionMaxTimeoutMs() {
        return getInt(TransactionStateManagerConfigs.TRANSACTIONS_MAX_TIMEOUT_MS_CONFIG);
    }
    public final int transactionTopicMinISR() {
        return getInt(TransactionLogConfigs.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG);
    }
    public final int transactionsLoadBufferSize() {
        return getInt(TransactionLogConfigs.TRANSACTIONS_LOAD_BUFFER_SIZE_CONFIG);
    }
    public short transactionTopicReplicationFactor() {
        return getShort(TransactionLogConfigs.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG);
    }
    public final int transactionTopicPartitions() {
        return getInt(TransactionLogConfigs.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG);
    }
    public final int transactionTopicSegmentBytes() {
        return getInt(TransactionLogConfigs.TRANSACTIONS_TOPIC_SEGMENT_BYTES_CONFIG);
    }
    public final int transactionAbortTimedOutTransactionCleanupIntervalMs() {
        return getInt(TransactionStateManagerConfigs.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG);
    }
    public final int transactionRemoveExpiredTransactionalIdCleanupIntervalMs() {
        return getInt(TransactionStateManagerConfigs.TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_CONFIG);
    }

    public boolean transactionPartitionVerificationEnable() {
        return getBoolean(TransactionLogConfigs.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG);
    }

    public final int producerIdExpirationMs() {
        return getInt(TransactionLogConfigs.PRODUCER_ID_EXPIRATION_MS_CONFIG);
    }
    public final int producerIdExpirationCheckIntervalMs() {
        return getInt(TransactionLogConfigs.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_CONFIG);
    }

    /** ********* Metric Configuration **************/
    public final int metricNumSamples() {
        return getInt(MetricConfigs.METRIC_NUM_SAMPLES_CONFIG);
    }
    public final long metricSampleWindowMs() {
        return getLong(MetricConfigs.METRIC_SAMPLE_WINDOW_MS_CONFIG);
    }
    public final String metricRecordingLevel() {
        return getString(MetricConfigs.METRIC_RECORDING_LEVEL_CONFIG);
    }

    /** ********* Kafka Client Telemetry Metrics Configuration ***********/
    public final int clientTelemetryMaxBytes() {
        return getInt(MetricConfigs.CLIENT_TELEMETRY_MAX_BYTES_CONFIG);
    }

    /** ********* SSL/SASL Configuration **************/
    // Security configs may be overridden for listeners, so it is not safe to use the base values
    // Hence the base SSL/SASL configs are not fields of KafkaConfig, listener configs should be
    // retrieved using KafkaConfig#valuesWithPrefixOverride
    @SuppressWarnings("unchecked")
    protected final Set<String> saslEnabledMechanisms(ListenerName listenerName) {
        return Optional.ofNullable(valuesWithPrefixOverride(listenerName.configPrefix()).get(KafkaSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG))
                .map(value -> new HashSet<>((List<String>) value))
                .orElse(new HashSet<>());
    }

    public final ListenerName interBrokerListenerName() {
        return getInterBrokerListenerNameAndSecurityProtocol().getKey();
    }
    public final SecurityProtocol interBrokerSecurityProtocol() {
        return getInterBrokerListenerNameAndSecurityProtocol().getValue();
    }
    public final Optional<ListenerName> controlPlaneListenerName() {
        return getControlPlaneListenerNameAndSecurityProtocol().map(Map.Entry::getKey);
    }
    public final Optional<SecurityProtocol> controlPlaneSecurityProtocol() {
        return getControlPlaneListenerNameAndSecurityProtocol().map(Map.Entry::getValue);
    }

    public final String saslMechanismInterBrokerProtocol(){
        return getString(KafkaSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG);
    }
    public final boolean saslInterBrokerHandshakeRequestEnable() {
        return interBrokerProtocolVersion().isSaslInterBrokerHandshakeRequestEnabled();
    }

    /** ********* DelegationToken Configuration **************/
    @SuppressWarnings("deprecation")
    public final Password delegationTokenSecretKey() {
        return Optional.ofNullable(getPassword(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_CONFIG))
                .orElse(getPassword(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_ALIAS_CONFIG));
    }
    public final boolean tokenAuthEnabled() {
        return delegationTokenSecretKey() != null && !delegationTokenSecretKey().value().isEmpty();
    }
    public final long delegationTokenMaxLifeMs() {
        return getLong(DelegationTokenManagerConfigs.DELEGATION_TOKEN_MAX_LIFETIME_CONFIG);
    }
    public final long delegationTokenExpiryTimeMs() {
        return getLong(DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_TIME_MS_CONFIG);
    }
    public final long delegationTokenExpiryCheckIntervalMs() {
        return getLong(DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_CONFIG);
    }

    /** ********* Password encryption configuration for dynamic configs *********/
    public final Optional<Password> passwordEncoderSecret() {
        return Optional.ofNullable(getPassword(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG));
    }
    public final Optional<Password> passwordEncoderOldSecret() {
        return Optional.ofNullable(getPassword(PasswordEncoderConfigs.PASSWORD_ENCODER_OLD_SECRET_CONFIG));
    }

    public final String passwordEncoderCipherAlgorithm() {
        return getString(PasswordEncoderConfigs.PASSWORD_ENCODER_CIPHER_ALGORITHM_CONFIG);
    }
    public final String passwordEncoderKeyFactoryAlgorithm() {
        return getString(PasswordEncoderConfigs.PASSWORD_ENCODER_KEYFACTORY_ALGORITHM_CONFIG);
    }
    public final int passwordEncoderKeyLength() {
        return getInt(PasswordEncoderConfigs.PASSWORD_ENCODER_KEY_LENGTH_CONFIG);
    }
    public final int passwordEncoderIterations() {
        return getInt(PasswordEncoderConfigs.PASSWORD_ENCODER_ITERATIONS_CONFIG);
    }

    /** ********* Quota Configuration **************/
    public final int numQuotaSamples() {
        return getInt(QuotaConfigs.NUM_QUOTA_SAMPLES_CONFIG);
    }
    public final int quotaWindowSizeSeconds() {
        return getInt(QuotaConfigs.QUOTA_WINDOW_SIZE_SECONDS_CONFIG);
    }
    public final int numReplicationQuotaSamples() {
        return getInt(QuotaConfigs.NUM_REPLICATION_QUOTA_SAMPLES_CONFIG);
    }
    public final int replicationQuotaWindowSizeSeconds() {
        return getInt(QuotaConfigs.REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_CONFIG);
    }
    public final int numAlterLogDirsReplicationQuotaSamples() {
        return getInt(QuotaConfigs.NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_CONFIG);
    }
    public final int alterLogDirsReplicationQuotaWindowSizeSeconds() {
        return getInt(QuotaConfigs.ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_CONFIG);
    }
    public final int numControllerQuotaSamples() {
        return getInt(QuotaConfigs.NUM_CONTROLLER_QUOTA_SAMPLES_CONFIG);
    }
    public final int controllerQuotaWindowSizeSeconds() {
        return getInt(QuotaConfigs.CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_CONFIG);
    }

    /** ********* Fetch Configuration **************/
    public final int maxIncrementalFetchSessionCacheSlots() {
        return getInt(ServerConfigs.MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_CONFIG);
    }
    public final int fetchMaxBytes() {
        return getInt(ServerConfigs.FETCH_MAX_BYTES_CONFIG);
    }

    /** ********* Request Limit Configuration ***********/
    public final int maxRequestPartitionSizeLimit() {
        return getInt(ServerConfigs.MAX_REQUEST_PARTITION_SIZE_LIMIT_CONFIG);
    }

    public final boolean deleteTopicEnable() {
        return getBoolean(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG);
    }
    public final String compressionType() {
        return getString(ServerConfigs.COMPRESSION_TYPE_CONFIG);
    }

    public final int gzipCompressionLevel() {
        return getInt(ServerConfigs.COMPRESSION_GZIP_LEVEL_CONFIG);
    }
    public final int lz4CompressionLevel() {
        return getInt(ServerConfigs.COMPRESSION_LZ4_LEVEL_CONFIG);
    }
    public final int zstdCompressionLevel() {
        return getInt(ServerConfigs.COMPRESSION_ZSTD_LEVEL_CONFIG);
    }

    /** ********* Raft Quorum Configuration *********/
    public final List<String> quorumVoters() {
        return getList(QuorumConfig.QUORUM_VOTERS_CONFIG);
    }
    public final int quorumElectionTimeoutMs() {
        return getInt(QuorumConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG);
    }
    public final int quorumFetchTimeoutMs() {
        return getInt(QuorumConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG);
    }
    public final int quorumElectionBackoffMs() {
        return getInt(QuorumConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG);
    }
    public final int quorumLingerMs() {
        return getInt(QuorumConfig.QUORUM_LINGER_MS_CONFIG);
    }
    public final int quorumRequestTimeoutMs() {
        return getInt(QuorumConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG);
    }
    public final int quorumRetryBackoffMs() {
        return getInt(QuorumConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG);
    }

    /** Internal Configurations **/
    public final boolean unstableApiVersionsEnabled() {
        return getBoolean(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG);
    }
    public final boolean unstableMetadataVersionsEnabled() {
        return getBoolean(ServerConfigs.UNSTABLE_METADATA_VERSIONS_ENABLE_CONFIG);
    }

    /** ********* General Configuration ***********/
    public Boolean brokerIdGenerationEnable() {
        return getBoolean(ServerConfigs.BROKER_ID_GENERATION_ENABLE_CONFIG);
    }
    public final int maxReservedBrokerId() {
        return getInt(ServerConfigs.RESERVED_BROKER_MAX_ID_CONFIG);
    }
    public final int brokerId() {
        return getInt(ServerConfigs.BROKER_ID_CONFIG);
    }
    public final int nodeId() {
        return getInt(KRaftConfigs.NODE_ID_CONFIG);
    }
    public final int initialRegistrationTimeoutMs() {
        return getInt(KRaftConfigs.INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_CONFIG);
    }
    public final int brokerHeartbeatIntervalMs() {
        return getInt(KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG);
    }
    public final int brokerSessionTimeoutMs() {
        return getInt(KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG);
    }

    public final int metadataLogSegmentBytes() {
        return getInt(KRaftConfigs.METADATA_LOG_SEGMENT_BYTES_CONFIG);
    }
    public final long metadataLogSegmentMillis() {
        return getLong(KRaftConfigs.METADATA_LOG_SEGMENT_MILLIS_CONFIG);
    }
    public final long metadataRetentionBytes() {
        return getLong(KRaftConfigs.METADATA_MAX_RETENTION_BYTES_CONFIG);
    }

    public final long metadataRetentionMillis() {
        return getLong(KRaftConfigs.METADATA_MAX_RETENTION_MILLIS_CONFIG);
    }

    public final int metadataNodeIDConfig() {
        return getInt(KRaftConfigs.NODE_ID_CONFIG);
    }

    public final int metadataLogSegmentMinBytes() {
        return getInt(KRaftConfigs.METADATA_LOG_SEGMENT_MIN_BYTES_CONFIG);
    }

    public final Boolean isKRaftCombinedMode() {
        return processRoles.equals(new HashSet<>(Arrays.asList(ProcessRole.BrokerRole, ProcessRole.ControllerRole)));
    }

    public final String metadataLogDir() {
        return Optional.ofNullable(getString(KRaftConfigs.METADATA_LOG_DIR_CONFIG)).orElse(logDirs().get(0));
    }

    public final boolean requiresZookeeper() {
        return processRoles.isEmpty();
    }

    public final boolean usesSelfManagedQuorum() {
        return !processRoles.isEmpty();
    }

    public final boolean migrationEnabled() {
        return getBoolean(KRaftConfigs.MIGRATION_ENABLED_CONFIG);
    }

    public final int migrationMetadataMinBatchSize() {
        return getInt(KRaftConfigs.MIGRATION_METADATA_MIN_BATCH_SIZE_CONFIG);
    }

    public final boolean elrEnabled() {
        return getBoolean(KRaftConfigs.ELR_ENABLED_CONFIG);
    }

    public final long logRetentionTimeMillis() {
        long millisInMinute = 60L * 1000L;
        long millisInHour = 60L * millisInMinute;

        long millis =
                Optional.ofNullable(getLong(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG)).orElse(
                        Optional.ofNullable(getInt(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG))
                                .map(mins -> millisInMinute * mins)
                                .orElse(getInt(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG) * millisInHour)
                );

        if (millis < 0) return -1L;
        return millis;
    }

    public final long serverMaxStartupTimeMs() {
        return getLong(KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_CONFIG);
    }

    public final int numNetworkThreads() {
        return getInt(ServerConfigs.NUM_NETWORK_THREADS_CONFIG);
    }
    public final int backgroundThreads() {
        return getInt(ServerConfigs.BACKGROUND_THREADS_CONFIG);
    }
    public final int queuedMaxRequests() {
        return getInt(ServerConfigs.QUEUED_MAX_REQUESTS_CONFIG);
    }
    public final long queuedMaxBytes() {
        return getLong(ServerConfigs.QUEUED_MAX_BYTES_CONFIG);
    }
    public final int numIoThreads() {
        return getInt(ServerConfigs.NUM_IO_THREADS_CONFIG);
    }
    public final int messageMaxBytes() {
        return getInt(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG);
    }
    public final int requestTimeoutMs() {
        return getInt(ServerConfigs.REQUEST_TIMEOUT_MS_CONFIG);
    }
    public final long connectionSetupTimeoutMs() {
        return getLong(ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG);
    }
    public final long connectionSetupTimeoutMaxMs() {
        return getLong(ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG);
    }

    public final int getNumReplicaAlterLogDirsThreads() {
        return Optional.ofNullable(getInt(ServerConfigs.NUM_REPLICA_ALTER_LOG_DIRS_THREADS_CONFIG)).orElse(logDirs().size());
    }

    /************* Metadata Configuration ***********/
    public final long metadataSnapshotMaxNewRecordBytes() {
        return getLong(KRaftConfigs.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_CONFIG);
    }

    public final long metadataSnapshotMaxIntervalMs() {
        return getLong(KRaftConfigs.METADATA_SNAPSHOT_MAX_INTERVAL_MS_CONFIG);
    }

    public final Optional<Long> metadataMaxIdleIntervalNs() {
        long value = TimeUnit.NANOSECONDS.convert((long) getInt(KRaftConfigs.METADATA_MAX_IDLE_INTERVAL_MS_CONFIG), TimeUnit.MILLISECONDS);
        return (value > 0) ? Optional.of(value) : Optional.empty();
    }


    private Set<ProcessRole> parseProcessRoles() {
        List<String> rolesList = getList(KRaftConfigs.PROCESS_ROLES_CONFIG);
        Set<ProcessRole> roles = rolesList.stream()
                .map(role -> {
                    switch (role) {
                        case "broker":
                            return ProcessRole.BrokerRole;
                        case "controller":
                            return ProcessRole.ControllerRole;
                        default:
                            throw new ConfigException(String.format("Unknown process role '%s' " +
                                    "(only 'broker' and 'controller' are allowed roles)", role));
                    }
                })
                .collect(Collectors.toSet());

        Set<ProcessRole> distinctRoles = new HashSet<>(roles);

        if (distinctRoles.size() != roles.size()) {
            throw new ConfigException(String.format("Duplicate role names found in `%s`: %s",
                    KRaftConfigs.PROCESS_ROLES_CONFIG, roles));
        }

        return distinctRoles;
    }

    /************* Authorizer Configuration ***********/
    public final Optional<Authorizer> createNewAuthorizer() throws ClassNotFoundException {
        String className = getString(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG);
        if (className == null || className.isEmpty())
            return Optional.empty();
        else {
            return Optional.of(AuthorizerUtils.createAuthorizer(className));
        }
    }

    public final Set<ListenerName> earlyStartListeners() {
        Set<ListenerName> listenersSet = extractListenerNames(listeners());
        Set<ListenerName> controllerListenersSet = extractListenerNames(controllerListeners());
        return Optional.ofNullable(getString(ServerConfigs.EARLY_START_LISTENERS_CONFIG))
                .map(listener -> {
                    String[] listenerNames = listener.split(",");
                    Set<ListenerName> result = new HashSet<>();
                    for (String listenerNameStr : listenerNames) {
                        String trimmedName = listenerNameStr.trim();
                        if (!trimmedName.isEmpty()) {
                            ListenerName listenerName = ListenerName.normalised(trimmedName);
                            if (!listenersSet.contains(listenerName) && !controllerListenersSet.contains(listenerName)) {
                                throw new ConfigException(
                                        String.format("%s contains listener %s, but this is not contained in %s or %s",
                                                ServerConfigs.EARLY_START_LISTENERS_CONFIG,
                                                listenerName.value(),
                                                SocketServerConfigs.LISTENERS_CONFIG,
                                                KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG));
                            }
                            result.add(listenerName);
                        }
                    }
                    return result;
                }).orElse(controllerListenersSet);
    }

    public final List<Endpoint> listeners() {
        return EndpointUtils.listenerListToEndpoints(getString(SocketServerConfigs.LISTENERS_CONFIG), effectiveListenerSecurityProtocolMap());
    }

    public final List<Endpoint> controllerListeners() {
        return listeners().stream()
                .filter(l -> l.listenerName().isPresent() && controllerListenerNames().contains(l.listenerName().get()))
                .collect(Collectors.toList());
    }

    public final List<String> controllerListenerNames() {
        String value = Optional.ofNullable(getString(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG)).orElse("");
        if (value.isEmpty()) {
            return Collections.emptyList();
        } else {
           return Arrays.asList(value.split(","));
        }
    }

    public final String saslMechanismControllerProtocol() {
        return getString(KRaftConfigs.SASL_MECHANISM_CONTROLLER_PROTOCOL_CONFIG);
    }

    public final Optional<Endpoint> controlPlaneListener() {
        return controlPlaneListenerName().flatMap(listenerName ->
                listeners().stream()
                        .filter(endpoint -> endpoint.listenerName().isPresent())
                        .filter(endpoint -> endpoint.listenerName().get().equals(listenerName.value()))
                        .findFirst()
        );
    }

    public final List<Endpoint> dataPlaneListeners() {
        return listeners().stream().filter(listener ->
                listener.listenerName().filter(name ->
                        !name.equals(getString(SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG)) ||
                                !controllerListenerNames().contains(name))
                        .isPresent()
        ).collect(Collectors.toList());
    }

    // Topic IDs are used with all self-managed quorum clusters and ZK cluster with IBP greater than or equal to 2.8
    public final Boolean usesTopicId() {
        return usesSelfManagedQuorum() || interBrokerProtocolVersion().isTopicIdsSupported();
    }
    public final boolean isRemoteLogStorageSystemEnabled() {
        return getBoolean(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP);
    }

    public final long logLocalRetentionBytes() {
        return getLong(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP);
    }

    public final long logLocalRetentionMs() {
        return getLong(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP);
    }


    // Use advertised listeners if defined, fallback to listeners otherwise
    public final List<Endpoint> effectiveAdvertisedListeners() {
        String advertisedListenersProp = getString(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG);
        if (advertisedListenersProp != null) {
            return EndpointUtils.listenerListToEndpoints(advertisedListenersProp, effectiveListenerSecurityProtocolMap(), false);
        } else {
            return listeners().stream()
                    .filter(endpoint -> endpoint.listenerName().isPresent())
                    .filter(l -> !controllerListenerNames().contains(l.listenerName().get())).collect(Collectors.toList());
        }
    }

    protected Set<ListenerName> extractListenerNames(Collection<Endpoint> listeners) {
        return listeners.stream()
                .filter(l -> l.listenerName().isPresent())
                .map(listener -> ListenerName.normalised(listener.listenerName().get()))
                .collect(Collectors.toSet());
    }

    protected Set<ListenerName> extractListenerNames() {
        return extractListenerNames(listeners());
    }

    protected Map<String, String> getMap(String propName, String propValue) {
        try {
            return Csv.parseCsvMap(propValue);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Error parsing configuration property '%s': %s", propName, e.getMessage()));
        }
    }

    protected Optional<Map.Entry<ListenerName, SecurityProtocol>> getControlPlaneListenerNameAndSecurityProtocol() {
        return Optional.ofNullable(getString(SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG)).map(name -> {
            ListenerName listenerName = ListenerName.normalised(name);
            SecurityProtocol securityProtocol = Optional.ofNullable(effectiveListenerSecurityProtocolMap().get(listenerName))
                    .orElseThrow(() -> new ConfigException("Listener with " + listenerName.value() + " defined in " +
                            SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG + " not found in " + SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG  + "."));
            return new AbstractMap.SimpleEntry<>(listenerName, securityProtocol);
        });
    }

    public final Map<ListenerName, SecurityProtocol> effectiveListenerSecurityProtocolMap() {
        Map<ListenerName, SecurityProtocol> mapValue = getMap(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, getString(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG))
                .entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> ListenerName.normalised(entry.getKey()),
                        entry -> getSecurityProtocol(entry.getValue(), SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG)));

        if (usesSelfManagedQuorum() && !originals().containsKey(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG)) {
            // check controller listener names (they won't appear in listeners when process.roles=broker)
            // as well as listeners for occurrences of SSL or SASL_*
            boolean listenerIsSslOrSasl = Csv.parseCsvList(getString(SocketServerConfigs.LISTENERS_CONFIG)).stream()
                    .noneMatch(listenerValue -> isSslOrSasl(EndpointUtils.parseListenerName(listenerValue)));
            if (controllerListenerNames().stream().noneMatch(this::isSslOrSasl) && listenerIsSslOrSasl) {
                // add the PLAINTEXT mappings for all controller listener names that are not explicitly PLAINTEXT
                mapValue.putAll(
                        controllerListenerNames().stream().filter(name -> !name.equals(SecurityProtocol.PLAINTEXT.name))
                                .collect(Collectors.toMap(ListenerName::normalised, name -> SecurityProtocol.PLAINTEXT)));
            }
            return mapValue; // don't add default mappings since we found something that is SSL or SASL_*
        } else {
            return mapValue;
        }
    }

    protected final Map.Entry<ListenerName, SecurityProtocol> getInterBrokerListenerNameAndSecurityProtocol() throws ConfigException {
        Optional<String> internalBrokerListener = Optional.ofNullable(getString(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG));
        return internalBrokerListener.map(name -> {
            if (originals().containsKey(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG)) {
                throw new ConfigException("Only one of " + ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG + " and " +
                        ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG + " should be set.");
            }
            ListenerName listenerName = ListenerName.normalised(name);
            SecurityProtocol securityProtocol = Optional.ofNullable(effectiveListenerSecurityProtocolMap().get(listenerName)).orElseThrow(
                    () -> new ConfigException("Listener with name " + listenerName.value() + " defined in " +
                            ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG + " not found in " + SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG + ".")
            );
            return new AbstractMap.SimpleEntry<>(listenerName, securityProtocol);
        }).orElseGet(() -> {
            SecurityProtocol securityProtocol = getSecurityProtocol(getString(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG),
                    ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG);
            return new AbstractMap.SimpleEntry<>(ListenerName.forSecurityProtocol(securityProtocol), securityProtocol);
        });
    }

    protected final SecurityProtocol getSecurityProtocol(String protocolName, String configName) {
        try {
            return SecurityProtocol.forName(protocolName);
        } catch (IllegalArgumentException e) {
            throw new ConfigException(String.format("Invalid security protocol `%s` defined in %s", protocolName, configName));
        }
    }

    // Nothing was specified explicitly for listener.security.protocol.map, so we are using the default value,
    // and we are using KRaft.
    // Add PLAINTEXT mappings for controller listeners as long as there is no SSL or SASL_{PLAINTEXT,SSL} in use
    private boolean isSslOrSasl(String name) {
        return name.equals(SecurityProtocol.SSL.name) || name.equals(SecurityProtocol.SASL_SSL.name) || name.equals(SecurityProtocol.SASL_PLAINTEXT.name);
    }

    /**
     * Copy the subset of properties that are relevant to Logs. The individual properties
     * are listed here since the names are slightly different in each Config class...
     */
    @SuppressWarnings("deprecation")
    public final Map<String, Object> extractLogConfigMap() {
        return Utils.mkMap(
                Utils.mkEntry(TopicConfig.SEGMENT_BYTES_CONFIG, logSegmentBytes()),
                Utils.mkEntry(TopicConfig.SEGMENT_MS_CONFIG, logRollTimeMillis()),
                Utils.mkEntry(TopicConfig.SEGMENT_JITTER_MS_CONFIG, logRollTimeJitterMillis()),
                Utils.mkEntry(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, logIndexSizeMaxBytes()),
                Utils.mkEntry(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, logFlushIntervalMessages()),
                Utils.mkEntry(TopicConfig.FLUSH_MS_CONFIG, logFlushIntervalMs()),
                Utils.mkEntry(TopicConfig.RETENTION_BYTES_CONFIG, logRetentionBytes()),
                Utils.mkEntry(TopicConfig.RETENTION_MS_CONFIG, logRetentionTimeMillis()),
                Utils.mkEntry(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, messageMaxBytes()),
                Utils.mkEntry(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, logIndexIntervalBytes()),
                Utils.mkEntry(TopicConfig.DELETE_RETENTION_MS_CONFIG, logCleanerDeleteRetentionMs()),
                Utils.mkEntry(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, logCleanerMinCompactionLagMs()),
                Utils.mkEntry(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, logCleanerMaxCompactionLagMs()),
                Utils.mkEntry(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, logDeleteDelayMs()),
                Utils.mkEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, logCleanerMinCleanRatio()),
                Utils.mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, logCleanupPolicy()),
                Utils.mkEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInSyncReplicas()),
                Utils.mkEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, compressionType()),
                Utils.mkEntry(TopicConfig.COMPRESSION_GZIP_LEVEL_CONFIG, gzipCompressionLevel()),
                Utils.mkEntry(TopicConfig.COMPRESSION_LZ4_LEVEL_CONFIG, lz4CompressionLevel()),
                Utils.mkEntry(TopicConfig.COMPRESSION_ZSTD_LEVEL_CONFIG, zstdCompressionLevel()),
                Utils.mkEntry(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, uncleanLeaderElectionEnable()),
                Utils.mkEntry(TopicConfig.PREALLOCATE_CONFIG, logPreAllocateEnable()),
                Utils.mkEntry(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG, logMessageFormatVersion().version()),
                Utils.mkEntry(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, logMessageTimestampType().name()),
                Utils.mkEntry(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, logMessageTimestampDifferenceMaxMs()),
                Utils.mkEntry(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, logMessageTimestampBeforeMaxMs()),
                Utils.mkEntry(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, logMessageTimestampAfterMaxMs()),
                Utils.mkEntry(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, logMessageDownConversionEnable()),
                Utils.mkEntry(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, logLocalRetentionMs()),
                Utils.mkEntry(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, logLocalRetentionBytes())
        );
    }
}
