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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.assignor.RangeAssignor;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslClientAuth;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.coordinator.group.OffsetConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig;
import org.apache.kafka.raft.RaftConfig;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class KafkaConfig {
    /** ********* Zookeeper Configuration *********/
    public static final int ZK_SESSION_TIMEOUT_MS_DEFAULT = 18000;
    public static final boolean ZK_ENABLE_SECURE_ACLS_DEFAULT = false;
    public static final int ZK_MAX_IN_FLIGHT_REQUESTS_DEFAULT = 10;
    public static final boolean ZK_SSL_CLIENT_ENABLE_DEFAULT = false;
    public static final String ZK_SSL_PROTOCOL_DEFAULT = "TLSv1.2";
    public static final String ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DEFAULT = "HTTPS";
    public static final boolean ZK_SSL_CRL_ENABLE_DEFAULT = false;
    public static final boolean ZK_SSL_OCSP_ENABLE_DEFAULT = false;

    /** ********* General Configuration *********/
    public static final boolean BROKER_ID_GENERATION_ENABLE_DEFAULT = true;
    public static final int MAX_RESERVED_BROKER_ID_DEFAULT = 1000;
    public static final int BROKER_ID_DEFAULT = -1;
    public static final int NUM_NETWORK_THREADS_DEFAULT = 3;
    public static final int NUM_IO_THREADS_DEFAULT = 8;
    public static final int BACKGROUND_THREADS_DEFAULT = 10;
    public static final int QUEUED_MAX_REQUESTS_DEFAULT = 500;
    public static final int QUEUED_MAX_REQUEST_BYTES_DEFAULT = -1;
    public static final int INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_DEFAULT = 60000;
    public static final int BROKER_HEARTBEAT_INTERVAL_MS_DEFAULT = 2000;
    public static final int BROKER_SESSION_TIMEOUT_MS_DEFAULT = 9000;
    public static final int METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_DEFAULT = 20 * 1024 * 1024;
    public static final long METADATA_SNAPSHOT_MAX_INTERVAL_MS_DEFAULT = TimeUnit.HOURS.toMillis(1);
    public static final int METADATA_MAX_IDLE_INTERVAL_MS_DEFAULT = 500;
    public static final int METADATA_MAX_RETENTION_BYTES_DEFAULT = 100 * 1024 * 1024;
    public static final boolean DELETE_TOPIC_ENABLE_DEFAULT = true;
    /** ********* KRaft mode configs *********/
    public static final int EMPTY_NODE_ID_DEFAULT = -1;
    public static final long SERVER_MAX_STARTUP_TIME_MS_DEFAULT = Long.MAX_VALUE;

    /** ********* Authorizer Configuration *********/
    public static final String AUTHORIZER_CLASS_NAME_DEFAULT = "";

    /** ********* Socket Server Configuration *********/
    public static final String LISTENERS_DEFAULT = "PLAINTEXT://:9092";
    //TODO: Replace this with EndPoint.DefaultSecurityProtocolMap once EndPoint is out of core.
    public static final String LISTENER_SECURITY_PROTOCOL_MAP_DEFAULT = Arrays.stream(SecurityProtocol.values())
            .collect(Collectors.toMap(sp -> ListenerName.forSecurityProtocol(sp), sp -> sp))
            .entrySet()
            .stream()
            .map(entry -> entry.getKey().value() + ":" + entry.getValue().name())
            .collect(Collectors.joining(","));
    public static final int SOCKET_SEND_BUFFER_BYTES_DEFAULT = 100 * 1024;
    public static final int SOCKET_RECEIVE_BUFFER_BYTES_DEFAULT = 100 * 1024;
    public static final int SOCKET_REQUEST_MAX_BYTES_DEFAULT = 100 * 1024 * 1024;
    public static final int SOCKET_LISTEN_BACKLOG_SIZE_DEFAULT = 50;
    public static final int MAX_CONNECTIONS_PER_IP_DEFAULT = Integer.MAX_VALUE;
    public static final String MAX_CONNECTIONS_PER_IP_OVERRIDES_DEFAULT = "";
    public static final int MAX_CONNECTIONS_DEFAULT = Integer.MAX_VALUE;
    public static final int MAX_CONNECTION_CREATION_RATE_DEFAULT = Integer.MAX_VALUE;
    public static final long CONNECTIONS_MAX_IDLE_MS_DEFAULT = 10 * 60 * 1000L;
    public static final int REQUEST_TIMEOUT_MS_DEFAULT = 30000;
    public static final long CONNECTION_SETUP_TIMEOUT_MS_DEFAULT = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS;
    public static final long CONNECTION_SETUP_TIMEOUT_MAX_MS_DEFAULT = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS;
    public static final int FAILED_AUTHENTICATION_DELAY_MS_DEFAULT = 100;

    /** ********* Log Configuration *********/
    public static final int NUM_PARTITIONS_DEFAULT = 1;
    public static final String LOG_DIR_DEFAULT = "/tmp/kafka-logs";
    public static final long LOG_CLEANUP_INTERVAL_MS_DEFAULT = 5 * 60 * 1000L;
    public static final int LOG_CLEANER_THREADS_DEFAULT = 1;
    public static final double LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_DEFAULT = Double.MAX_VALUE;
    public static final long LOG_CLEANER_DEDUPE_BUFFER_SIZE_DEFAULT = 128 * 1024 * 1024L;
    public static final int LOG_CLEANER_IO_BUFFER_SIZE_DEFAULT = 512 * 1024;
    public static final double LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_DEFAULT = 0.9d;
    public static final int LOG_CLEANER_BACKOFF_MS_DEFAULT = 15 * 1000;
    public static final boolean LOG_CLEANER_ENABLE_DEFAULT = true;
    public static final int LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_DEFAULT = 60000;
    public static final int LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_DEFAULT = 60000;
    public static final int NUM_RECOVERY_THREADS_PER_DATA_DIR_DEFAULT = 1;
    public static final boolean AUTO_CREATE_TOPICS_ENABLE_DEFAULT = true;

    /** ********* Replication configuration *********/
    public static final int CONTROLLER_SOCKET_TIMEOUT_MS_DEFAULT = REQUEST_TIMEOUT_MS_DEFAULT;
    public static final int REPLICATION_FACTOR_DEFAULT = 1;
    public static final long REPLICA_LAG_TIME_MAX_MS_DEFAULT = 30000L;
    public static final int REPLICA_SOCKET_TIMEOUT_MS_DEFAULT = 30 * 1000;
    public static final int REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_DEFAULT = 64 * 1024;
    public static final int REPLICA_FETCH_MAX_BYTES_DEFAULT = 1024 * 1024;
    public static final int REPLICA_FETCH_WAIT_MAX_MS_DEFAULT = 500;
    public static final int REPLICA_FETCH_MIN_BYTES_DEFAULT = 1;
    public static final int REPLICA_FETCH_RESPONSE_MAX_BYTES_DEFAULT = 10 * 1024 * 1024;
    public static final int NUM_REPLICA_FETCHERS_DEFAULT = 1;
    public static final int REPLICA_FETCH_BACKOFF_MS_DEFAULT = 1000;
    public static final long REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_DEFAULT = 5000L;
    public static final int FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT = 1000;
    public static final int PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT = 1000;
    public static final int DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT = 1;
    public static final boolean AUTO_LEADER_REBALANCE_ENABLE_DEFAULT = true;
    public static final int LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_DEFAULT = 10;
    public static final int LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_DEFAULT = 300;
    public static final String INTER_BROKER_SECURITY_PROTOCOL_DEFAULT = SecurityProtocol.PLAINTEXT.toString();
    public static final String INTER_BROKER_PROTOCOL_VERSION_DEFAULT = MetadataVersion.latestProduction().version();

    /** ********* Controlled shutdown configuration *********/
    public static final int CONTROLLED_SHUTDOWN_MAX_RETRIES_DEFAULT = 3;
    public static final int CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_DEFAULT = 5000;
    public static final boolean CONTROLLED_SHUTDOWN_ENABLE_DEFAULT = true;

    /** ********* Group coordinator configuration *********/
    public static final int GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT = 6000;
    public static final int GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT = 1800000;
    public static final int GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT = 3000;
    public static final int GROUP_MAX_SIZE_DEFAULT = Integer.MAX_VALUE;

    /** ********* New group coordinator configs *********/
    public static final boolean NEW_GROUP_COORDINATOR_ENABLE_DEFAULT = false;
    public static final List<String> GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DEFAULT = Collections.singletonList(Group.GroupType.CLASSIC.toString());
    public static final int GROUP_COORDINATOR_NUM_THREADS_DEFAULT = 1;

    /** ********* Consumer group configs *********/
    public static final int CONSUMER_GROUP_SESSION_TIMEOUT_MS_DEFAULT = 45000;
    public static final int CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT = 45000;
    public static final int CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT = 60000;
    public static final int CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT = 5000;
    public static final int CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT = 5000;
    public static final int CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT = 15000;
    public static final int CONSUMER_GROUP_MAX_SIZE_DEFAULT = Integer.MAX_VALUE;
    public static final List<String> CONSUMER_GROUP_ASSIGNORS_DEFAULT = Collections.singletonList(RangeAssignor.class.getName());

    /** ********* Offset management configuration *********/
    public static final int OFFSET_METADATA_MAX_SIZE_DEFAULT = OffsetConfig.DEFAULT_MAX_METADATA_SIZE;
    public static final int OFFSETS_LOAD_BUFFER_SIZE_DEFAULT = OffsetConfig.DEFAULT_LOAD_BUFFER_SIZE;
    public static final short OFFSETS_TOPIC_REPLICATION_FACTOR_DEFAULT = OffsetConfig.DEFAULT_OFFSETS_TOPIC_REPLICATION_FACTOR;
    public static final int OFFSETS_TOPIC_PARTITIONS_DEFAULT = OffsetConfig.DEFAULT_OFFSETS_TOPIC_NUM_PARTITIONS;
    public static final int OFFSETS_TOPIC_SEGMENT_BYTES_DEFAULT = OffsetConfig.DEFAULT_OFFSETS_TOPIC_SEGMENT_BYTES;
    public static final int OFFSETS_TOPIC_COMPRESSION_CODEC_DEFAULT = OffsetConfig.DEFAULT_OFFSETS_TOPIC_COMPRESSION_TYPE.id;
    public static final int OFFSETS_RETENTION_MINUTES_DEFAULT = 7 * 24 * 60;
    public static final long OFFSETS_RETENTION_CHECK_INTERVAL_MS_DEFAULT = OffsetConfig.DEFAULT_OFFSETS_RETENTION_CHECK_INTERVAL_MS;
    public static final int OFFSET_COMMIT_TIMEOUT_MS_DEFAULT = OffsetConfig.DEFAULT_OFFSET_COMMIT_TIMEOUT_MS;
    public static final short OFFSET_COMMIT_REQUIRED_ACKS_DEFAULT = OffsetConfig.DEFAULT_OFFSET_COMMIT_REQUIRED_ACKS;

    /** ********* Transaction management configuration *********/
    public static final int TRANSACTIONAL_ID_EXPIRATION_MS_DEFAULT = TransactionStateManagerConfig.DEFAULT_TRANSACTIONAL_ID_EXPIRATION_MS;
    public static final int TRANSACTIONS_MAX_TIMEOUT_MS_DEFAULT = TransactionStateManagerConfig.DEFAULT_TRANSACTIONS_MAX_TIMEOUT_MS;
    public static final int TRANSACTIONS_TOPIC_MIN_ISR_DEFAULT = TransactionLogConfig.DEFAULT_MIN_IN_SYNC_REPLICAS;
    public static final int TRANSACTIONS_LOAD_BUFFER_SIZE_DEFAULT = TransactionLogConfig.DEFAULT_LOAD_BUFFER_SIZE;
    public static final short TRANSACTIONS_TOPIC_REPLICATION_FACTOR_DEFAULT = TransactionLogConfig.DEFAULT_REPLICATION_FACTOR;
    public static final int TRANSACTIONS_TOPIC_PARTITIONS_DEFAULT = TransactionLogConfig.DEFAULT_NUM_PARTITIONS;
    public static final int TRANSACTIONS_TOPIC_SEGMENT_BYTES_DEFAULT = TransactionLogConfig.DEFAULT_SEGMENT_BYTES;
    public static final int TRANSACTIONS_ABORT_TIMED_OUT_CLEANUP_INTERVAL_MS_DEFAULT = TransactionStateManagerConfig.DEFAULT_ABORT_TIMED_OUT_TRANSACTIONS_INTERVAL_MS;
    public static final int TRANSACTIONS_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS_DEFAULT = TransactionStateManagerConfig.DEFAULT_REMOVE_EXPIRED_TRANSACTIONAL_IDS_INTERVAL_MS;
    public static final boolean TRANSACTION_PARTITION_VERIFICATION_ENABLE_DEFAULT = true;
    public static final int PRODUCER_ID_EXPIRATION_MS_DEFAULT = 86400000;
    public static final int PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT = 600000;

    /** ********* Fetch Configuration *********/
    public static final int MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_DEFAULT = 1000;
    public static final int FETCH_MAX_BYTES_DEFAULT = 55 * 1024 * 1024;

    /** ********* Request Limit Configuration ***********/
    public static final int MAX_REQUEST_PARTITION_SIZE_LIMIT_DEFAULT = 2000;

    /** ********* Quota Configuration *********/
    public static final int NUM_QUOTA_SAMPLES_DEFAULT = ClientQuotaManagerConfig.DEFAULT_NUM_QUOTA_SAMPLES;
    public static final int QUOTA_WINDOW_SIZE_SECONDS_DEFAULT = ClientQuotaManagerConfig.DEFAULT_QUOTA_WINDOW_SIZE_SECONDS;
    public static final int NUM_REPLICATION_QUOTA_SAMPLES_DEFAULT = ReplicationQuotaManagerConfig.DEFAULT_NUM_QUOTA_SAMPLES;
    public static final int REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_DEFAULT = ReplicationQuotaManagerConfig.DEFAULT_QUOTA_WINDOW_SIZE_SECONDS;
    public static final int NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_DEFAULT = ReplicationQuotaManagerConfig.DEFAULT_NUM_QUOTA_SAMPLES;
    public static final int ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_DEFAULT = ReplicationQuotaManagerConfig.DEFAULT_QUOTA_WINDOW_SIZE_SECONDS;
    public static final int NUM_CONTROLLER_QUOTA_SAMPLES_DEFAULT = ClientQuotaManagerConfig.DEFAULT_NUM_QUOTA_SAMPLES;
    public static final int CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_DEFAULT = ClientQuotaManagerConfig.DEFAULT_QUOTA_WINDOW_SIZE_SECONDS;

    /** ********* Kafka Metrics Configuration *********/
    public static final int METRIC_NUM_SAMPLES_DEFAULT = 2;
    public static final int METRIC_SAMPLE_WINDOW_MS_DEFAULT = 30000;
    public static final String METRIC_REPORTER_CLASSES_DEFAULT = "";
    public static final String METRIC_RECORDING_LEVEL_DEFAULT = Sensor.RecordingLevel.INFO.toString();
    public static final boolean AUTO_INCLUDE_JMX_REPORTER_DEFAULT = true;

    /**  ********* Kafka Yammer Metrics Reporter Configuration *********/
    public static final String KAFKA_METRIC_REPORTER_CLASSES_DEFAULT = "";
    public static final int KAFKA_METRICS_POLLING_INTERVAL_SECONDS_DEFAULT = 10;


    /** ********* Kafka Client Telemetry Metrics Configuration *********/
    public static final int CLIENT_TELEMETRY_MAX_BYTES_DEFAULT = 1024 * 1024;

    /**  ********* SSL configuration *********/
    public static final String SSL_PROTOCOL_DEFAULT = SslConfigs.DEFAULT_SSL_PROTOCOL;
    public static final String SSL_ENABLED_PROTOCOLS_DEFAULT = SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS;
    public static final String SSL_KEYSTORE_TYPE_DEFAULT = SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE;
    public static final String SSL_TRUSTSTORE_TYPE_DEFAULT = SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE;
    public static final String SSL_KEY_MANAGER_ALGORITHM_DEFAULT = SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM;
    public static final String SSL_TRUST_MANAGER_ALGORITHM_DEFAULT = SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM;
    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DEFAULT = SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM;
    public static final String SSL_CLIENT_AUTHENTICATION_DEFAULT = SslClientAuth.NONE.name().toLowerCase(Locale.ROOT);
    public static final String[] SSL_CLIENT_AUTHENTICATION_VALID_VALUES_DEFAULT = SslClientAuth.VALUES.stream()
            .map(v -> v.toString().toLowerCase(Locale.ROOT)).toArray(String[]::new);
    public static final String SSL_PRINCIPAL_MAPPING_RULES_DEFAULT = BrokerSecurityConfigs.DEFAULT_SSL_PRINCIPAL_MAPPING_RULES;

    /**  ********* General Security Configuration *********/
    public static final long CONNECTIONS_MAX_REAUTH_MS_DEFAULT = 0L;
    public static final int SERVER_MAX_RECEIVE_SIZE_DEFAULT = BrokerSecurityConfigs.DEFAULT_SASL_SERVER_MAX_RECEIVE_SIZE;
    public static final Class<? extends KafkaPrincipalBuilder> PRINCIPAL_BUILDER_DEFAULT = DefaultKafkaPrincipalBuilder.class;

    /**  ********* Sasl configuration *********/
    public static final String SASL_MECHANISM_INTER_BROKER_PROTOCOL_DEFAULT = SaslConfigs.DEFAULT_SASL_MECHANISM;
    public static final List<String> SASL_ENABLED_MECHANISMS_DEFAULT = BrokerSecurityConfigs.DEFAULT_SASL_ENABLED_MECHANISMS;
    public static final String SASL_KERBEROS_KINIT_CMD_DEFAULT = SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD;
    public static final double SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DEFAULT = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR;
    public static final double SASL_KERBEROS_TICKET_RENEW_JITTER_DEFAULT = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER;
    public static final long SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DEFAULT = SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN;
    public static final List<String> SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DEFAULT = BrokerSecurityConfigs.DEFAULT_SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES;
    public static final double SASL_LOGIN_REFRESH_WINDOW_FACTOR_DEFAULT = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR;
    public static final double SASL_LOGIN_REFRESH_WINDOW_JITTER_DEFAULT = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER;
    public static final short SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DEFAULT = SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS;
    public static final short SASL_LOGIN_REFRESH_BUFFER_SECONDS_DEFAULT = SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS;
    public static final long SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DEFAULT = SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
    public static final long SASL_LOGIN_RETRY_BACKOFF_MS_DEFAULT = SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MS;
    public static final String SASL_OAUTH_BEARER_SCOPE_CLAIM_NAME_DEFAULT = SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
    public static final String SASL_OAUTH_BEARER_SUB_CLAIM_NAME_DEFAULT = SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME;
    public static final long SASL_OAUTH_BEARER_JWKS_ENDPOINT_REFRESH_MS_DEFAULT = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS;
    public static final long SASL_OAUTH_BEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DEFAULT = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS;
    public static final long SASL_OAUTH_BEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DEFAULT = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS;
    public static final int SASL_OAUTH_BEARER_CLOCK_SKEW_SECONDS_DEFAULT = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS;

    /**  ********* Delegation Token Configuration *********/
    public static final long DELEGATION_TOKEN_MAX_LIFE_TIME_MS_DEFAULT = 7 * 24 * 60 * 60 * 1000L;
    public static final long DELEGATION_TOKEN_EXPIRY_TIME_MS_DEFAULT = 24 * 60 * 60 * 1000L;
    public static final long DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_DEFAULT = 1 * 60 * 60 * 1000L;

    /**  ********* Password Encryption Configuration for Dynamic Configs *********/
    public static final String PASSWORD_ENCODER_CIPHER_ALGORITHM_DEFAULT = "AES/CBC/PKCS5Padding";
    public static final int PASSWORD_ENCODER_KEY_LENGTH_DEFAULT = 128;
    public static final int PASSWORD_ENCODER_ITERATIONS_DEFAULT = 4096;

    /**  ********* Raft Quorum Configuration *********/
    public static final List<String> QUORUM_VOTERS_DEFAULT = RaftConfig.DEFAULT_QUORUM_VOTERS;
    public static final int QUORUM_ELECTION_TIMEOUT_MS_DEFAULT = RaftConfig.DEFAULT_QUORUM_ELECTION_TIMEOUT_MS;
    public static final int QUORUM_FETCH_TIMEOUT_MS_DEFAULT = RaftConfig.DEFAULT_QUORUM_FETCH_TIMEOUT_MS;
    public static final int QUORUM_ELECTION_BACKOFF_MS_DEFAULT = RaftConfig.DEFAULT_QUORUM_ELECTION_BACKOFF_MAX_MS;
    public static final int QUORUM_LINGER_MS_DEFAULT = RaftConfig.DEFAULT_QUORUM_LINGER_MS;
    public static final int QUORUM_REQUEST_TIMEOUT_MS_DEFAULT = RaftConfig.DEFAULT_QUORUM_REQUEST_TIMEOUT_MS;
    public static final int QUORUM_RETRY_BACKOFF_MS_DEFAULT = RaftConfig.DEFAULT_QUORUM_RETRY_BACKOFF_MS;
}
