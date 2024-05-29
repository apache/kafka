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

import org.apache.kafka.common.compress.GzipCompression;
import org.apache.kafka.common.compress.Lz4Compression;
import org.apache.kafka.common.compress.ZstdCompression;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslClientAuth;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.ConsumerGroupMigrationPolicy;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfigs;
import org.apache.kafka.coordinator.transaction.TransactionStateManagerConfigs;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.security.PasswordEncoderConfigs;
import org.apache.kafka.server.common.MetadataVersionValidator;
import org.apache.kafka.server.metrics.MetricConfigs;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.storage.internals.log.LogConfig;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

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

public class KafkaConfig {
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
        .define(BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_CONFIG, LONG, BrokerSecurityConfigs.DEFAULT_CONNECTIONS_MAX_REAUTH_MS, MEDIUM, BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_DOC)
        .define(BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG, INT, BrokerSecurityConfigs.DEFAULT_SASL_SERVER_MAX_RECEIVE_SIZE, MEDIUM, BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_DOC)
        .define(SecurityConfig.SECURITY_PROVIDERS_CONFIG, STRING, null, LOW, SecurityConfig.SECURITY_PROVIDERS_DOC)

        /** ********* SSL Configuration ****************/
        .define(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, CLASS, BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DEFAULT, MEDIUM, BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DOC)
        .define(SslConfigs.SSL_PROTOCOL_CONFIG, STRING, SslConfigs.DEFAULT_SSL_PROTOCOL, MEDIUM, SslConfigs.SSL_PROTOCOL_DOC)
        .define(SslConfigs.SSL_PROVIDER_CONFIG, STRING, null, MEDIUM, SslConfigs.SSL_PROVIDER_DOC)
        .define(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, LIST, SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS, MEDIUM, SslConfigs.SSL_ENABLED_PROTOCOLS_DOC)
        .define(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, STRING, SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE, MEDIUM, SslConfigs.SSL_KEYSTORE_TYPE_DOC)
        .define(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, STRING, null, MEDIUM, SslConfigs.SSL_KEYSTORE_LOCATION_DOC)
        .define(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_KEYSTORE_PASSWORD_DOC)
        .define(SslConfigs.SSL_KEY_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_KEY_PASSWORD_DOC)
        .define(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_KEYSTORE_KEY_DOC)
        .define(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC)
        .define(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, STRING, SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE, MEDIUM, SslConfigs.SSL_TRUSTSTORE_TYPE_DOC)
        .define(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, STRING, null, MEDIUM, SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC)
        .define(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC)
        .define(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_DOC)
        .define(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, STRING, SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM, MEDIUM, SslConfigs.SSL_KEYMANAGER_ALGORITHM_DOC)
        .define(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, STRING, SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM, MEDIUM, SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC)
        .define(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, STRING, SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, LOW, SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
        .define(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, STRING, null, LOW, SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC)
        .define(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, STRING, BrokerSecurityConfigs.SSL_CLIENT_AUTH_DEFAULT, ConfigDef.ValidString.in(Utils.enumOptions(SslClientAuth.class)), MEDIUM, BrokerSecurityConfigs.SSL_CLIENT_AUTH_DOC)
        .define(SslConfigs.SSL_CIPHER_SUITES_CONFIG, LIST, Collections.emptyList(), MEDIUM, SslConfigs.SSL_CIPHER_SUITES_DOC)
        .define(BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG, STRING, BrokerSecurityConfigs.DEFAULT_SSL_PRINCIPAL_MAPPING_RULES, LOW, BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_DOC)
        .define(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, CLASS, null, LOW, SslConfigs.SSL_ENGINE_FACTORY_CLASS_DOC)
        .define(BrokerSecurityConfigs.SSL_ALLOW_DN_CHANGES_CONFIG, BOOLEAN, BrokerSecurityConfigs.DEFAULT_SSL_ALLOW_DN_CHANGES_VALUE, LOW, BrokerSecurityConfigs.SSL_ALLOW_DN_CHANGES_DOC)
        .define(BrokerSecurityConfigs.SSL_ALLOW_SAN_CHANGES_CONFIG, BOOLEAN, BrokerSecurityConfigs.DEFAULT_SSL_ALLOW_SAN_CHANGES_VALUE, LOW, BrokerSecurityConfigs.SSL_ALLOW_SAN_CHANGES_DOC)

        /** ********* Sasl Configuration ****************/
        .define(BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, STRING, SaslConfigs.DEFAULT_SASL_MECHANISM, MEDIUM, BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_DOC)
        .define(SaslConfigs.SASL_JAAS_CONFIG, PASSWORD, null, MEDIUM, SaslConfigs.SASL_JAAS_CONFIG_DOC)
        .define(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, LIST, BrokerSecurityConfigs.DEFAULT_SASL_ENABLED_MECHANISMS, MEDIUM, BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_DOC)
        .define(BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG, CLASS, null, MEDIUM, BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC)
        .define(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, CLASS, null, MEDIUM, SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC)
        .define(SaslConfigs.SASL_LOGIN_CLASS, CLASS, null, MEDIUM, SaslConfigs.SASL_LOGIN_CLASS_DOC)
        .define(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, CLASS, null, MEDIUM, SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC)
        .define(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, STRING, null, MEDIUM, SaslConfigs.SASL_KERBEROS_SERVICE_NAME_DOC)
        .define(SaslConfigs.SASL_KERBEROS_KINIT_CMD, STRING, SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD, MEDIUM, SaslConfigs.SASL_KERBEROS_KINIT_CMD_DOC)
        .define(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, DOUBLE, SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, MEDIUM, SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC)
        .define(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER, DOUBLE, SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER, MEDIUM, SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DOC)
        .define(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN, LONG, SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN, MEDIUM, SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC)
        .define(BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG, LIST, BrokerSecurityConfigs.DEFAULT_SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES, MEDIUM, BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC)
        .define(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR, DOUBLE, SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR, MEDIUM, SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC)
        .define(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER, DOUBLE, SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER, MEDIUM, SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC)
        .define(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, SHORT, SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS, MEDIUM, SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC)
        .define(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, SHORT, SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS, MEDIUM, SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC)
        .define(SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS, INT, null, LOW, SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC)
        .define(SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS, INT, null, LOW, SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS_DOC)
        .define(SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS, LONG, SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MAX_MS, LOW, SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC)
        .define(SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS, LONG, SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MS, LOW, SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS_DOC)
        .define(SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME, STRING, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME, LOW, SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC)
        .define(SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME, STRING, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME, LOW, SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC)
        .define(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, STRING, null, MEDIUM, SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC)
        .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, STRING, null, MEDIUM, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC)
        .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, LONG, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, LOW, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC)
        .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS, LONG, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS, LOW, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC)
        .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS, LONG, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS, LOW, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC)
        .define(SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, INT, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, LOW, SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC)
        .define(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE, LIST, null, LOW, SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC)
        .define(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER, STRING, null, LOW, SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC)

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

}
