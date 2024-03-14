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
import org.apache.kafka.coordinator.group.Group.GroupType;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.RaftConfig;
import org.apache.kafka.security.PasswordEncoderConfigs;
import org.apache.kafka.server.common.MetadataVersionValidator;
import org.apache.kafka.server.config.dynamic.BrokerDynamicConfigs;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.zookeeper.client.ZKClientConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import static org.apache.kafka.server.config.DynamicBrokerConfig.brokerConfigSynonyms;

public class KafkaConfig {
    private final static String LOG_CONFIG_PREFIX = "log.";

    /** ********* Zookeeper Configuration ***********/
    public final static String ZK_CONNECT_PROP = "zookeeper.connect";
    public final static String ZK_SESSION_TIMEOUT_MS_PROP = "zookeeper.session.timeout.ms";
    public final static String ZK_CONNECTION_TIMEOUT_MS_PROP = "zookeeper.connection.timeout.ms";
    public final static String ZK_ENABLE_SECURE_ACLS_PROP = "zookeeper.set.acl";
    public final static String ZK_MAX_IN_FLIGHT_REQUESTS_PROP = "zookeeper.max.in.flight.requests";
    public final static String ZK_SSL_CLIENT_ENABLE_PROP = "zookeeper.ssl.client.enable";
    public final static String ZK_CLIENT_CNXN_SOCKET_PROP = "zookeeper.clientCnxnSocket";
    public final static String ZK_SSL_KEYSTORE_LOCATION_PROP = "zookeeper.ssl.keystore.location";
    public final static String ZK_SSL_KEYSTORE_PASSWORD_PROP = "zookeeper.ssl.keystore.password";
    public final static String ZK_SSL_KEYSTORE_TYPE_PROP = "zookeeper.ssl.keystore.type";
    public final static String ZK_SSL_TRUSTSTORE_LOCATION_PROP = "zookeeper.ssl.truststore.location";
    public final static String ZK_SSL_TRUSTSTORE_PASSWORD_PROP = "zookeeper.ssl.truststore.password";
    public final static String ZK_SSL_TRUSTSTORE_TYPE_PROP = "zookeeper.ssl.truststore.type";
    public final static String ZK_SSL_PROTOCOL_PROP = "zookeeper.ssl.protocol";
    public final static String ZK_SSL_ENABLED_PROTOCOLS_PROP = "zookeeper.ssl.enabled.protocols";
    public final static String ZK_SSL_CIPHER_SUITES_PROP = "zookeeper.ssl.cipher.suites";
    public final static String ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP = "zookeeper.ssl.endpoint.identification.algorithm";
    public final static String ZK_SSL_CRL_ENABLE_PROP = "zookeeper.ssl.crl.enable";
    public final static String ZK_SSL_OCSP_ENABLE_PROP = "zookeeper.ssl.ocsp.enable";

    // a map from the Kafka config to the corresponding ZooKeeper Java system property
    private final static Map<String, String> ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP = new HashMap<String, String>() {{
            put(ZK_SSL_CLIENT_ENABLE_PROP, ZKClientConfig.SECURE_CLIENT);
            put(ZK_CLIENT_CNXN_SOCKET_PROP, ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET);
            put(ZK_SSL_KEYSTORE_LOCATION_PROP, "zookeeper.ssl.keyStore.location");
            put(ZK_SSL_KEYSTORE_PASSWORD_PROP, "zookeeper.ssl.keyStore.password");
            put(ZK_SSL_KEYSTORE_TYPE_PROP, "zookeeper.ssl.keyStore.type");
            put(ZK_SSL_TRUSTSTORE_LOCATION_PROP, "zookeeper.ssl.trustStore.location");
            put(ZK_SSL_TRUSTSTORE_PASSWORD_PROP, "zookeeper.ssl.trustStore.password");
            put(ZK_SSL_TRUSTSTORE_TYPE_PROP, "zookeeper.ssl.trustStore.type");
            put(ZK_SSL_PROTOCOL_PROP, "zookeeper.ssl.protocol");
            put(ZK_SSL_ENABLED_PROTOCOLS_PROP, "zookeeper.ssl.enabledProtocols");
            put(ZK_SSL_CIPHER_SUITES_PROP, "zookeeper.ssl.ciphersuites");
            put(ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP, "zookeeper.ssl.hostnameVerification");
            put(ZK_SSL_CRL_ENABLE_PROP, "zookeeper.ssl.crl");
            put(ZK_SSL_OCSP_ENABLE_PROP, "zookeeper.ssl.ocsp");
        }};

    public final static Map<String, String> zkSslConfigToSystemPropertyMap() {
        return ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP;
    }

    /** ********* General Configuration ***********/
    public final static String BROKER_ID_GENERATION_ENABLE_PROP = "broker.id.generation.enable";
    public final static String MAX_RESERVED_BROKER_ID_PROP = "reserved.broker.max.id";
    public final static String BROKER_ID_PROP = "broker.id";
    public final static String MESSAGE_MAX_BYTES_PROP = "message.max.bytes";
    public final static String NUM_NETWORK_THREADS_PROP = "num.network.threads";
    public final static String NUM_IO_THREADS_PROP = "num.io.threads";
    public final static String BACKGROUND_THREADS_PROP = "background.threads";
    public final static String NUM_REPLICA_ALTER_LOG_DIRS_THREADS_PROP = "num.replica.alter.log.dirs.threads";
    public final static String QUEUED_MAX_REQUESTS_PROP = "queued.max.requests";
    public final static String QUEUED_MAX_BYTES_PROP = "queued.max.request.bytes";
    public final static String REQUEST_TIMEOUT_MS_PROP = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    public final static String CONNECTION_SETUP_TIMEOUT_MS_PROP = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;
    public final static String CONNECTION_SETUP_TIMEOUT_MAX_MS_PROP = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;

    /** KRaft mode configs */
    public final static String PROCESS_ROLES_PROP = "process.roles";
    public final static String INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_PROP = "initial.broker.registration.timeout.ms";
    public final static String BROKER_HEARTBEAT_INTERVAL_MS_PROP = "broker.heartbeat.interval.ms";
    public final static String BROKER_SESSION_TIMEOUT_MS_PROP = "broker.session.timeout.ms";
    public final static String NODE_ID_PROP = "node.id";
    public final static String METADATA_LOG_DIR_PROP = "metadata.log.dir";
    public final static String METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_PROP = "metadata.log.max.record.bytes.between.snapshots";
    public final static String METADATA_SNAPSHOT_MAX_INTERVAL_MS_PROP = "metadata.log.max.snapshot.interval.ms";
    public final static String CONTROLLER_LISTENER_NAMES_PROP = "controller.listener.names";
    public final static String SASL_MECHANISM_CONTROLLER_PROTOCOL_PROP = "sasl.mechanism.controller.protocol";
    public final static String METADATA_LOG_SEGMENT_MIN_BYTES_PROP = "metadata.log.segment.min.bytes";
    public final static String METADATA_LOG_SEGMENT_BYTES_PROP = "metadata.log.segment.bytes";
    public final static String METADATA_LOG_SEGMENT_MILLIS_PROP = "metadata.log.segment.ms";
    public final static String METADATA_MAX_RETENTION_BYTES_PROP = "metadata.max.retention.bytes";
    public final static String METADATA_MAX_RETENTION_MILLIS_PROP = "metadata.max.retention.ms";
    public final static String QUORUM_VOTERS_PROP = RaftConfig.QUORUM_VOTERS_CONFIG;
    public final static String METADATA_MAX_IDLE_INTERVAL_MS_PROP = "metadata.max.idle.interval.ms";
    public final static String SERVER_MAX_STARTUP_TIME_MS_PROP = "server.max.startup.time.ms";

    /** ZK to KRaft Migration configs */
    public final static String MIGRATION_ENABLED_PROP = "zookeeper.metadata.migration.enable";
    public final static String MIGRATION_METADATA_MIN_BATCH_SIZE_PROP = "zookeeper.metadata.migration.min.batch.size";

    /** Enable eligible leader replicas configs */
    public final static String ELR_ENABLED_PROP = "eligible.leader.replicas.enable";

    /************* Authorizer Configuration ***********/
    public final static String AUTHORIZER_CLASS_NAME_PROP = "authorizer.class.name";
    public final static String EARLY_START_LISTENERS_PROP = "early.start.listeners";

    /** ********* Socket Server Configuration ***********/
    public final static String LISTENERS_PROP = "listeners";
    public final static String ADVERTISED_LISTENERS_PROP = "advertised.listeners";
    public final static String LISTENER_SECURITY_PROTOCOL_MAP_PROP = "listener.security.protocol.map";
    public final static String CONTROL_PLANE_LISTENER_NAME_PROP = "control.plane.listener.name";
    public final static String SOCKET_SEND_BUFFER_BYTES_PROP = "socket.send.buffer.bytes";
    public final static String SOCKET_RECEIVE_BUFFER_BYTES_PROP = "socket.receive.buffer.bytes";
    public final static String SOCKET_REQUEST_MAX_BYTES_PROP = "socket.request.max.bytes";
    public final static String SOCKET_LISTEN_BACKLOG_SIZE_PROP = "socket.listen.backlog.size";
    public final static String MAX_CONNECTIONS_PER_IP_PROP = "max.connections.per.ip";
    public final static String MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP = "max.connections.per.ip.overrides";
    public final static String MAX_CONNECTIONS_PROP = "max.connections";
    public final static String MAX_CONNECTION_CREATION_RATE_PROP = "max.connection.creation.rate";
    public final static String CONNECTIONS_MAX_IDLE_MS_PROP = "connections.max.idle.ms";
    public final static String FAILED_AUTHENTICATION_DELAY_MS_PROP = "connection.failed.authentication.delay.ms";

    /***************** rack configuration *************/
    public final static String RACK_PROP = "broker.rack";

    /** ********* Log Configuration ***********/
    public final static String NUM_PARTITIONS_PROP = "num.partitions";
    public final static String LOG_DIRS_PROP = LOG_CONFIG_PREFIX + "dirs";
    public final static String LOG_DIR_PROP = LOG_CONFIG_PREFIX + "dir";
    public final static String LOG_SEGMENT_BYTES_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.SEGMENT_BYTES_CONFIG);

    public final static String LOG_ROLL_TIME_MILLIS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.SEGMENT_MS_CONFIG);
    public final static String LOG_ROLL_TIME_HOURS_PROP = LOG_CONFIG_PREFIX + "roll.hours";

    public final static String LOG_ROLL_TIME_JITTER_MILLIS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.SEGMENT_JITTER_MS_CONFIG);
    public final static String LOG_ROLL_TIME_JITTER_HOURS_PROP = LOG_CONFIG_PREFIX + "roll.jitter.hours";

    public final static String LOG_RETENTION_TIME_MILLIS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.RETENTION_MS_CONFIG);
    public final static String LOG_RETENTION_TIME_MINUTES_PROP = LOG_CONFIG_PREFIX + "retention.minutes";
    public final static String LOG_RETENTION_TIME_HOURS_PROP = LOG_CONFIG_PREFIX + "retention.hours";

    public final static String LOG_RETENTION_BYTES_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.RETENTION_BYTES_CONFIG);
    public final static String LOG_CLEANUP_INTERVAL_MS_PROP = LOG_CONFIG_PREFIX + "retention.check.interval.ms";
    public final static String LOG_CLEANUP_POLICY_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.CLEANUP_POLICY_CONFIG);
    public final static String LOG_INDEX_SIZE_MAX_BYTES_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG);
    public final static String LOG_INDEX_INTERVAL_BYTES_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG);
    public final static String LOG_FLUSH_INTERVAL_MESSAGES_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG);
    public final static String LOG_DELETE_DELAY_MS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG);
    public final static String LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP = LOG_CONFIG_PREFIX + "flush.scheduler.interval.ms";
    public final static String LOG_FLUSH_INTERVAL_MS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.FLUSH_MS_CONFIG);
    public final static String LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_PROP = LOG_CONFIG_PREFIX + "flush.offset.checkpoint.interval.ms";
    public final static String LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_PROP = LOG_CONFIG_PREFIX + "flush.start.offset.checkpoint.interval.ms";
    public final static String LOG_PRE_ALLOCATE_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.PREALLOCATE_CONFIG);

    /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
    /**
     * @deprecated since "3.0"
     */
    @Deprecated
    public final static String LOG_MESSAGE_FORMAT_VERSION_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG);

    public final static String LOG_MESSAGE_TIMESTAMP_TYPE_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG);

    /* See `TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG` for details */
    /**
     * @deprecated since "3.6"
     */
    @Deprecated
    public final static String LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG);

    public final static String LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG);
    public final static String LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG);

    public final static String NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP = "num.recovery.threads.per.data.dir";
    public final static String AUTO_CREATE_TOPICS_ENABLE_PROP = "auto.create.topics.enable";
    public final static String MIN_IN_SYNC_REPLICAS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
    public final static String CREATE_TOPIC_POLICY_CLASS_NAME_PROP = "create.topic.policy.class.name";
    public final static String ALTER_CONFIG_POLICY_CLASS_NAME_PROP = "alter.config.policy.class.name";
    public final static String LOG_MESSAGE_DOWN_CONVERSION_ENABLE_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG);
    /** ********* Replication configuration ***********/
    public final static String CONTROLLER_SOCKET_TIMEOUT_MS_PROP = "controller.socket.timeout.ms";
    public final static String DEFAULT_REPLICATION_FACTOR_PROP = "default.replication.factor";
    public final static String REPLICA_LAG_TIME_MAX_MS_PROP = "replica.lag.time.max.ms";
    public final static String REPLICA_SOCKET_TIMEOUT_MS_PROP = "replica.socket.timeout.ms";
    public final static String REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_PROP = "replica.socket.receive.buffer.bytes";
    public final static String REPLICA_FETCH_MAX_BYTES_PROP = "replica.fetch.max.bytes";
    public final static String REPLICA_FETCH_WAIT_MAX_MS_PROP = "replica.fetch.wait.max.ms";
    public final static String REPLICA_FETCH_MIN_BYTES_PROP = "replica.fetch.min.bytes";
    public final static String REPLICA_FETCH_RESPONSE_MAX_BYTES_PROP = "replica.fetch.response.max.bytes";
    public final static String REPLICA_FETCH_BACKOFF_MS_PROP = "replica.fetch.backoff.ms";
    public final static String NUM_REPLICA_FETCHERS_PROP = "num.replica.fetchers";
    public final static String REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_PROP = "replica.high.watermark.checkpoint.interval.ms";
    public final static String FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP = "fetch.purgatory.purge.interval.requests";
    public final static String PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP = "producer.purgatory.purge.interval.requests";
    public final static String DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP = "delete.records.purgatory.purge.interval.requests";
    public final static String AUTO_LEADER_REBALANCE_ENABLE_PROP = "auto.leader.rebalance.enable";
    public final static String LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_PROP = "leader.imbalance.per.broker.percentage";
    public final static String LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_PROP = "leader.imbalance.check.interval.seconds";
    public final static String UNCLEAN_LEADER_ELECTION_ENABLE_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG);
    public final static String INTER_BROKER_SECURITY_PROTOCOL_PROP = "security.inter.broker.protocol";
    public final static String INTER_BROKER_PROTOCOL_VERSION_PROP = "inter.broker.protocol.version";
    public final static String INTER_BROKER_LISTENER_NAME_PROP = "inter.broker.listener.name";
    public final static String REPLICA_SELECTOR_CLASS_PROP = "replica.selector.class";
    /** ********* Controlled shutdown configuration ***********/
    public final static String CONTROLLED_SHUTDOWN_MAX_RETRIES_PROP = "controlled.shutdown.max.retries";
    public final static String CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_PROP = "controlled.shutdown.retry.backoff.ms";
    public final static String CONTROLLED_SHUTDOWN_ENABLE_PROP = "controlled.shutdown.enable";

    /** ********* Group coordinator configuration ***********/
    public final static String GROUP_MIN_SESSION_TIMEOUT_MS_PROP = "group.min.session.timeout.ms";
    public final static String GROUP_MAX_SESSION_TIMEOUT_MS_PROP = "group.max.session.timeout.ms";
    public final static String GROUP_INITIAL_REBALANCE_DELAY_MS_PROP = "group.initial.rebalance.delay.ms";
    public final static String GROUP_MAX_SIZE_PROP = "group.max.size";

    /** New group coordinator configs */
    public final static String NEW_GROUP_COORDINATOR_ENABLE_PROP = "group.coordinator.new.enable";
    public final static String GROUP_COORDINATOR_REBALANCE_PROTOCOLS_PROP = "group.coordinator.rebalance.protocols";
    public final static String GROUP_COORDINATOR_NUM_THREADS_PROP = "group.coordinator.threads";

    /** Consumer group configs */
    public final static String CONSUMER_GROUP_SESSION_TIMEOUT_MS_PROP = "group.consumer.session.timeout.ms";
    public final static String CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_PROP = "group.consumer.min.session.timeout.ms";
    public final static String CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_PROP = "group.consumer.max.session.timeout.ms";
    public final static String CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_PROP = "group.consumer.heartbeat.interval.ms";
    public final static String CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_PROP = "group.consumer.min.heartbeat.interval.ms";
    public final static String CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_PROP = "group.consumer.max.heartbeat.interval.ms";
    public final static String CONSUMER_GROUP_MAX_SIZE_PROP = "group.consumer.max.size";
    public final static String CONSUMER_GROUP_ASSIGNORS_PROP = "group.consumer.assignors";

    /** ********* Offset management configuration ***********/
    public final static String OFFSET_METADATA_MAX_SIZE_PROP = "offset.metadata.max.bytes";
    public final static String OFFSETS_LOAD_BUFFER_SIZE_PROP = "offsets.load.buffer.size";
    public final static String OFFSETS_TOPIC_REPLICATION_FACTOR_PROP = "offsets.topic.replication.factor";
    public final static String OFFSETS_TOPIC_PARTITIONS_PROP = "offsets.topic.num.partitions";
    public final static String OFFSETS_TOPIC_SEGMENT_BYTES_PROP = "offsets.topic.segment.bytes";
    public final static String OFFSETS_TOPIC_COMPRESSION_CODEC_PROP = "offsets.topic.compression.codec";
    public final static String OFFSETS_RETENTION_MINUTES_PROP = "offsets.retention.minutes";
    public final static String OFFSETS_RETENTION_CHECK_INTERVAL_MS_PROP = "offsets.retention.check.interval.ms";
    public final static String OFFSET_COMMIT_TIMEOUT_MS_PROP = "offsets.commit.timeout.ms";
    public final static String OFFSET_COMMIT_REQUIRED_ACKS_PROP = "offsets.commit.required.acks";

    /** ********* Transaction management configuration ***********/
    public final static String TRANSACTIONAL_ID_EXPIRATION_MS_PROP = "transactional.id.expiration.ms";
    public final static String TRANSACTIONS_MAX_TIMEOUT_MS_PROP = "transaction.max.timeout.ms";
    public final static String TRANSACTIONS_TOPIC_MIN_ISR_PROP = "transaction.state.log.min.isr";
    public final static String TRANSACTIONS_LOAD_BUFFER_SIZE_PROP = "transaction.state.log.load.buffer.size";
    public final static String TRANSACTIONS_TOPIC_PARTITIONS_PROP = "transaction.state.log.num.partitions";
    public final static String TRANSACTIONS_TOPIC_SEGMENT_BYTES_PROP = "transaction.state.log.segment.bytes";
    public final static String TRANSACTIONS_TOPIC_REPLICATION_FACTOR_PROP = "transaction.state.log.replication.factor";
    public final static String TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_PROP = "transaction.abort.timed.out.transaction.cleanup.interval.ms";
    public final static String TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_PROP = "transaction.remove.expired.transaction.cleanup.interval.ms";

    public final static String TRANSACTION_PARTITION_VERIFICATION_ENABLE_PROP = "transaction.partition.verification.enable";

    public final static String PRODUCER_ID_EXPIRATION_MS_PROP = ProducerStateManagerConfig.PRODUCER_ID_EXPIRATION_MS;
    public final static String PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_PROP = "producer.id.expiration.check.interval.ms";

    /** ********* Fetch Configuration **************/
    public final static String MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_PROP = "max.incremental.fetch.session.cache.slots";
    public final static String FETCH_MAX_BYTES_PROP = "fetch.max.bytes";

    /** ********* Request Limit Configuration **************/
    public final static String MAX_REQUEST_PARTITION_SIZE_LIMIT_PROP = "max.request.partition.size.limit";

    /** ********* Quota Configuration ***********/
    public final static String NUM_QUOTA_SAMPLES_PROP = "quota.window.num";
    public final static String NUM_REPLICATION_QUOTA_SAMPLES_PROP = "replication.quota.window.num";
    public final static String NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_PROP = "alter.log.dirs.replication.quota.window.num";
    public final static String NUM_CONTROLLER_QUOTA_SAMPLES_PROP = "controller.quota.window.num";
    public final static String QUOTA_WINDOW_SIZE_SECONDS_PROP = "quota.window.size.seconds";
    public final static String REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_PROP = "replication.quota.window.size.seconds";
    public final static String ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_PROP = "alter.log.dirs.replication.quota.window.size.seconds";
    public final static String CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_PROP = "controller.quota.window.size.seconds";
    public final static String CLIENT_QUOTA_CALLBACK_CLASS_PROP = "client.quota.callback.class";

    public final static String DELETE_TOPIC_ENABLE_PROP = "delete.topic.enable";
    public final static String COMPRESSION_TYPE_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.COMPRESSION_TYPE_CONFIG);

    /** ********* Kafka Metrics Configuration ***********/
    public final static String METRIC_SAMPLE_WINDOW_MS_PROP = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;
    public final static String METRIC_NUM_SAMPLES_PROP = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;
    public final static String METRIC_REPORTER_CLASSES_PROP = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;
    public final static String METRIC_RECORDING_LEVEL_PROP = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;
    @Deprecated
    public final static String AUTO_INCLUDE_JMX_REPORTER_PROP = CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_CONFIG;

    /** ********* Kafka Yammer Metrics Reporters Configuration ***********/
    public final static String KAFKA_METRICS_REPORTER_CLASSES_PROP = "kafka.metrics.reporters";
    public final static String KAFKA_METRICS_POLLING_INTERVAL_SECONDS_PROP = "kafka.metrics.polling.interval.secs";

    /** ********* Kafka Client Telemetry Metrics Configuration ***********/
    public final static String CLIENT_TELEMETRY_MAX_BYTES_PROP = "telemetry.max.bytes";

    /** ******** Common Security Configuration *************/
    public final static String PRINCIPAL_BUILDER_CLASS_PROP = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG;
    public final static String CONNECTIONS_MAX_REAUTH_MS_PROP = BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS;
    public final static String SASL_SERVER_MAX_RECEIVE_SIZE_PROP = BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG;
    public final static String SECURITY_PROVIDER_CLASS_PROP = SecurityConfig.SECURITY_PROVIDERS_CONFIG;

    /** ********* SSL Configuration ****************/
    public final static String SSL_PROTOCOL_PROP = SslConfigs.SSL_PROTOCOL_CONFIG;
    public final static String SSL_PROVIDER_PROP = SslConfigs.SSL_PROVIDER_CONFIG;
    public final static String SSL_CIPHER_SUITES_PROP = SslConfigs.SSL_CIPHER_SUITES_CONFIG;
    public final static String SSL_ENABLED_PROTOCOLS_PROP = SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG;
    public final static String SSL_KEYSTORE_TYPE_PROP = SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
    public final static String SSL_KEYSTORE_LOCATION_PROP = SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
    public final static String SSL_KEYSTORE_PASSWORD_PROP = SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
    public final static String SSL_KEY_PASSWORD_PROP = SslConfigs.SSL_KEY_PASSWORD_CONFIG;
    public final static String SSL_KEYSTORE_KEY_PROP = SslConfigs.SSL_KEYSTORE_KEY_CONFIG;
    public final static String SSL_KEYSTORE_CERTIFICATE_CHAIN_PROP = SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG;
    public final static String SSL_TRUSTSTORE_TYPE_PROP = SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;
    public final static String SSL_TRUSTSTORE_LOCATION_PROP = SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
    public final static String SSL_TRUSTSTORE_PASSWORD_PROP = SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
    public final static String SSL_TRUSTSTORE_CERTIFICATES_PROP = SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG;
    public final static String SSL_KEY_MANAGER_ALGORITHM_PROP = SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG;
    public final static String SSL_TRUST_MANAGER_ALGORITHM_PROP = SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG;
    public final static String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP = SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
    public final static String SSL_SECURE_RANDOM_IMPLEMENTATION_PROP = SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG;
    public final static String SSL_CLIENT_AUTH_PROP = BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG;
    public final static String SSL_PRINCIPAL_MAPPING_RULES_PROP = BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG;
    public final static String SSL_ENGINE_FACTORY_CLASS_PROP = SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG;
    public final static String SSL_ALLOW_DN_CHANGES_PROP = BrokerSecurityConfigs.SSL_ALLOW_DN_CHANGES_CONFIG;
    public final static String SSL_ALLOW_SAN_CHANGES_PROP = BrokerSecurityConfigs.SSL_ALLOW_SAN_CHANGES_CONFIG;

    /** ********* SASL Configuration ****************/
    public final static String SASL_MECHANISM_INTER_BROKER_PROTOCOL_PROP = "sasl.mechanism.inter.broker.protocol";
    public final static String SASL_JAAS_CONFIG_PROP = SaslConfigs.SASL_JAAS_CONFIG;
    public final static String SASL_ENABLED_MECHANISMS_PROP = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG;
    public final static String SASL_SERVER_CALLBACK_HANDLER_CLASS_PROP = BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS;
    public final static String SASL_CLIENT_CALLBACK_HANDLER_CLASS_PROP = SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS;
    public final static String SASL_LOGIN_CLASS_PROP = SaslConfigs.SASL_LOGIN_CLASS;
    public final static String SASL_LOGIN_CALLBACK_HANDLER_CLASS_PROP = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS;
    public final static String SASL_KERBEROS_SERVICE_NAME_PROP = SaslConfigs.SASL_KERBEROS_SERVICE_NAME;
    public final static String SASL_KERBEROS_KINIT_CMD_PROP = SaslConfigs.SASL_KERBEROS_KINIT_CMD;
    public final static String SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_PROP = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR;
    public final static String SASL_KERBEROS_TICKET_RENEW_JITTER_PROP = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER;
    public final static String SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_PROP = SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN;
    public final static String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_PROP = BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG;
    public final static String SASL_LOGIN_REFRESH_WINDOW_FACTOR_PROP = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR;
    public final static String SASL_LOGIN_REFRESH_WINDOW_JITTER_PROP = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER;
    public final static String SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_PROP = SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS;
    public final static String SASL_LOGIN_REFRESH_BUFFER_SECONDS_PROP = SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS;

    public final static String SASL_LOGIN_CONNECT_TIMEOUT_MS_PROP = SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
    public final static String SASL_LOGIN_READ_TIMEOUT_MS_PROP = SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS;
    public final static String SASL_LOGIN_RETRY_BACKOFF_MAX_MS_PROP = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
    public final static String SASL_LOGIN_RETRY_BACKOFF_MS_PROP = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
    public final static String SASL_O_AUTH_BEARER_SCOPE_CLAIM_NAME_PROP = SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
    public final static String SASL_O_AUTH_BEARER_SUB_CLAIM_NAME_PROP = SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME;
    public final static String SASL_O_AUTH_BEARER_TOKEN_ENDPOINT_URL_PROP = SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
    public final static String SASL_O_AUTH_BEARER_JWKS_ENDPOINT_URL_PROP = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL;
    public final static String SASL_O_AUTH_BEARER_JWKS_ENDPOINT_REFRESH_MS_PROP = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS;
    public final static String SASL_O_AUTH_BEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_PROP = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS;
    public final static String SASL_O_AUTH_BEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_PROP = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS;
    public final static String SASL_O_AUTH_BEARER_CLOCK_SKEW_SECONDS_PROP = SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS;
    public final static String SASL_O_AUTH_BEARER_EXPECTED_AUDIENCE_PROP = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
    public final static String SASL_O_AUTH_BEARER_EXPECTED_ISSUER_PROP = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER;

    /** ********* Delegation Token Configuration ****************/
    public final static String DELEGATION_TOKEN_SECRET_KEY_ALIAS_PROP = "delegation.token.master.key";
    public final static String DELEGATION_TOKEN_SECRET_KEY_PROP = "delegation.token.secret.key";
    public final static String DELEGATION_TOKEN_MAX_LIFE_TIME_PROP = "delegation.token.max.lifetime.ms";
    public final static String DELEGATION_TOKEN_EXPIRY_TIME_MS_PROP = "delegation.token.expiry.time.ms";
    public final static String DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_PROP = "delegation.token.expiry.check.interval.ms";

    /** ********* Password encryption configuration for dynamic configs *********/
    public final static String PASSWORD_ENCODER_SECRET_PROP = PasswordEncoderConfigs.SECRET;
    public final static String PASSWORD_ENCODER_OLD_SECRET_PROP = PasswordEncoderConfigs.OLD_SECRET;
    public final static String PASSWORD_ENCODER_KEY_FACTORY_ALGORITHM_PROP = PasswordEncoderConfigs.KEYFACTORY_ALGORITHM;
    public final static String PASSWORD_ENCODER_CIPHER_ALGORITHM_PROP = PasswordEncoderConfigs.CIPHER_ALGORITHM;
    public final static String PASSWORD_ENCODER_KEY_LENGTH_PROP = PasswordEncoderConfigs.KEY_LENGTH;
    public final static String PASSWORD_ENCODER_ITERATIONS_PROP = PasswordEncoderConfigs.ITERATIONS;

    /** Internal Configurations **/
    public final static String UNSTABLE_API_VERSIONS_ENABLE_PROP = "unstable.api.versions.enable";
    public final static String UNSTABLE_METADATA_VERSIONS_ENABLE_PROP = "unstable.metadata.versions.enable";

    /** ******** Common Security Configuration ************ */
    public final static String PRINCIPAL_BUILDER_CLASS_DOC = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DOC;
    public final static String CONNECTIONS_MAX_REAUTH_MS_DOC = BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_DOC;
    public final static String SASL_SERVER_MAX_RECEIVE_SIZE_DOC = BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_DOC;
    public final static String SECURITY_PROVIDERS_DOC = SecurityConfig.SECURITY_PROVIDERS_DOC;

    /** ********* SSL Configuration ****************/
    public final static String SSL_PROTOCOL_DOC = SslConfigs.SSL_PROTOCOL_DOC;
    public final static String SSL_PROVIDER_DOC = SslConfigs.SSL_PROVIDER_DOC;
    public final static String SSL_CIPHER_SUITES_DOC = SslConfigs.SSL_CIPHER_SUITES_DOC;
    public final static String SSL_ENABLED_PROTOCOLS_DOC = SslConfigs.SSL_ENABLED_PROTOCOLS_DOC;
    public final static String SSL_KEYSTORE_TYPE_DOC = SslConfigs.SSL_KEYSTORE_TYPE_DOC;
    public final static String SSL_KEYSTORE_LOCATION_DOC = SslConfigs.SSL_KEYSTORE_LOCATION_DOC;
    public final static String SSL_KEYSTORE_PASSWORD_DOC = SslConfigs.SSL_KEYSTORE_PASSWORD_DOC;
    public final static String SSL_KEY_PASSWORD_DOC = SslConfigs.SSL_KEY_PASSWORD_DOC;
    public final static String SSL_KEYSTORE_KEY_DOC = SslConfigs.SSL_KEYSTORE_KEY_DOC;
    public final static String SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC = SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC;
    public final static String SSL_TRUSTSTORE_TYPE_DOC = SslConfigs.SSL_TRUSTSTORE_TYPE_DOC;
    public final static String SSL_TRUSTSTORE_PASSWORD_DOC = SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC;
    public final static String SSL_TRUSTSTORE_LOCATION_DOC = SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC;
    public final static String SSL_TRUSTSTORE_CERTIFICATES_DOC = SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_DOC;
    public final static String SSL_KEYMANAGER_ALGORITHM_DOC = SslConfigs.SSL_KEYMANAGER_ALGORITHM_DOC;
    public final static String SSL_TRUSTMANAGER_ALGORITHM_DOC = SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC;
    public final static String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC = SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC;
    public final static String SSL_SECURE_RANDOM_IMPLEMENTATION_DOC = SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC;
    public final static String SSL_CLIENT_AUTH_DOC = BrokerSecurityConfigs.SSL_CLIENT_AUTH_DOC;
    public final static String SSL_PRINCIPAL_MAPPING_RULES_DOC = BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_DOC;
    public final static String SSL_ENGINE_FACTORY_CLASS_DOC = SslConfigs.SSL_ENGINE_FACTORY_CLASS_DOC;
    public final static String SSL_ALLOW_DN_CHANGES_DOC = BrokerSecurityConfigs.SSL_ALLOW_DN_CHANGES_DOC;
    public final static String SSL_ALLOW_SAN_CHANGES_DOC = BrokerSecurityConfigs.SSL_ALLOW_SAN_CHANGES_DOC;

    /** ********* Sasl Configuration ****************/
    public final static String SASL_MECHANISM_INTER_BROKER_PROTOCOL_DOC = "SASL mechanism used for inter-broker communication. Default is GSSAPI.";
    public final static String SASL_JAAS_CONFIG_DOC = SaslConfigs.SASL_JAAS_CONFIG_DOC;
    public final static String SASL_ENABLED_MECHANISMS_DOC = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_DOC;
    public final static String SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC = BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC;
    public final static String SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC = SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC;
    public final static String SASL_LOGIN_CLASS_DOC = SaslConfigs.SASL_LOGIN_CLASS_DOC;
    public final static String SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC;
    public final static String SASL_KERBEROS_SERVICE_NAME_DOC = SaslConfigs.SASL_KERBEROS_SERVICE_NAME_DOC;
    public final static String SASL_KERBEROS_KINIT_CMD_DOC = SaslConfigs.SASL_KERBEROS_KINIT_CMD_DOC;
    public final static String SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC;
    public final static String SASL_KERBEROS_TICKET_RENEW_JITTER_DOC = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DOC;
    public final static String SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC = SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC;
    public final static String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC = BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC;
    public final static String SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC;
    public final static String SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC;
    public final static String SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC = SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC;
    public final static String SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC = SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC;
    public final static String SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC = SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC;
    public final static String SASL_LOGIN_READ_TIMEOUT_MS_DOC = SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS_DOC;
    public final static String SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC;
    public final static String SASL_LOGIN_RETRY_BACKOFF_MS_DOC = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS_DOC;
    public final static String SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC = SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC;
    public final static String SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC = SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC;
    public final static String SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC = SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC;
    public final static String SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC;
    public final static String SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC;
    public final static String SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC;
    public final static String SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC;
    public final static String SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC = SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC;
    public final static String SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC;
    public final static String SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC;

    /* Documentation */
    /** ********* Zookeeper Configuration ********** */
    public final static String ZK_CONNECT_DOC = "Specifies the ZooKeeper connection string in the form <code>hostname:port</code> where host and port are the " +
            "host and port of a ZooKeeper server. To allow connecting through other ZooKeeper nodes when that ZooKeeper machine is " +
            "down you can also specify multiple hosts in the form <code>hostname1:port1,hostname2:port2,hostname3:port3</code>.\n" +
            "The server can also have a ZooKeeper chroot path as part of its ZooKeeper connection string which puts its data under some path in the global ZooKeeper namespace. " +
            "For example to give a chroot path of <code>/chroot/path</code> you would give the connection string as <code>hostname1:port1,hostname2:port2,hostname3:port3/chroot/path</code>.";
    public final static String ZK_SESSION_TIMEOUT_MS_DOC = "Zookeeper session timeout";
    public final static String ZK_CONNECTION_TIMEOUT_MS_DOC = "The max time that the client waits to establish a connection to ZooKeeper. If not set, the value in " + ZK_SESSION_TIMEOUT_MS_PROP + " is used";
    public final static String ZK_ENABLE_SECURE_ACLS_DOC = "Set client to use secure ACLs";
    public final static String ZK_MAX_INFLIGHT_REQUESTS_DOC = "The maximum number of unacknowledged requests the client will send to ZooKeeper before blocking.";
    public final static String ZK_SSL_CLIENT_ENABLE_DOC = String.format("Set client to use TLS when connecting to ZooKeeper." +
            " An explicit value overrides any value set via the <code>zookeeper.client.secure</code> system property (note the different name)." +
            " Defaults to false if neither is set; when true, <code>%s</code> must be set (typically to <code>org.apache.zookeeper.ClientCnxnSocketNetty</code>); other values to set may include ", ZK_CLIENT_CNXN_SOCKET_PROP) +
            ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.keySet().stream().filter(x -> !x.equals(ZK_SSL_CLIENT_ENABLE_PROP) && !x.equals(ZK_CLIENT_CNXN_SOCKET_PROP)).sorted().collect(Collectors.joining("<code>", "</code>, <code>", "</code>"));
    public final static String ZK_CLIENT_CNXN_SOCKET_DOC = String.format("Typically set to <code>org.apache.zookeeper.ClientCnxnSocketNetty</code> when using TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the same-named <code>%s</code> system property.", ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_CLIENT_CNXN_SOCKET_PROP));
    public final static String ZK_SSL_KEYSTORE_LOCATION_DOC = String.format("Keystore location when using a client-side certificate with TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>%s</code> system property (note the camelCase).", ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_KEYSTORE_LOCATION_PROP));
    public final static String ZK_SSL_KEYSTORE_PASSWORD_DOC = String.format("Keystore password when using a client-side certificate with TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>%s</code> system property (note the camelCase)." +
            " Note that ZooKeeper does not support a key password different from the keystore password, so be sure to set the key password in the keystore to be identical to the keystore password; otherwise the connection attempt to Zookeeper will fail.",
            ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_KEYSTORE_PASSWORD_PROP));
    public final static String ZK_SSL_KEYSTORE_TYPE_DOC = String.format("Keystore type when using a client-side certificate with TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>%s</code> system property (note the camelCase)." +
            " The default value of <code>null</code> means the type will be auto-detected based on the filename extension of the keystore.",
            ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_KEYSTORE_TYPE_PROP));
    public final static String ZK_SSL_TRUSTSTORE_LOCATION_DOC = String.format("Truststore location when using TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>%s</code> system property (note the camelCase).",
            ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_TRUSTSTORE_LOCATION_PROP));
    public final static String ZK_SSL_TRUSTSTORE_PASSWORD_DOC = String.format("Truststore password when using TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>%s</code> system property (note the camelCase).",
            ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_TRUSTSTORE_PASSWORD_PROP));
    public final static String ZK_SSL_TRUSTSTORE_TYPE_DOC = String.format("Truststore type when using TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>%s</code> system property (note the camelCase)." +
            " The default value of <code>null</code> means the type will be auto-detected based on the filename extension of the truststore.",
            ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_TRUSTSTORE_TYPE_PROP));
    public final static String ZK_SSL_PROTOCOL_DOC = String.format("Specifies the protocol to be used in ZooKeeper TLS negotiation." +
            " An explicit value overrides any value set via the same-named <code>%s</code> system property.",
            ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_PROTOCOL_PROP));
    public final static String ZK_SSL_ENABLED_PROTOCOLS_DOC = String.format("Specifies the enabled protocol(s) in ZooKeeper TLS negotiation (csv)." +
            " Overrides any explicit value set via the <code>%s</code> system property (note the camelCase)." +
            " The default value of <code>null</code> means the enabled protocol will be the value of the <code>%s</code> configuration property.",
            ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_ENABLED_PROTOCOLS_PROP), ZK_SSL_PROTOCOL_PROP);
    public final static String ZK_SSL_CIPHER_SUITES_DOC = String.format("Specifies the enabled cipher suites to be used in ZooKeeper TLS negotiation (csv)." +
            " Overrides any explicit value set via the <code>%s</code> system property (note the single word \"ciphersuites\")." +
            " The default value of <code>null</code> means the list of enabled cipher suites is determined by the Java runtime being used.",
            ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_CIPHER_SUITES_PROP));
    public final static String ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC = String.format("Specifies whether to enable hostname verification in the ZooKeeper TLS negotiation process, with (case-insensitively) \"https\" meaning ZooKeeper hostname verification is enabled and an explicit blank value meaning it is disabled (disabling it is only recommended for testing purposes)." +
            " An explicit value overrides any \"true\" or \"false\" value set via the <code>%s</code> system property (note the different name and values; true implies https and false implies blank).",
            ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP));
    public final static String ZK_SSL_CRL_ENABLE_DOC = String.format("Specifies whether to enable Certificate Revocation List in the ZooKeeper TLS protocols." +
            " Overrides any explicit value set via the <code>%s</code> system property (note the shorter name).",
            ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_CRL_ENABLE_PROP));
    public final static String ZK_SSL_OCSP_ENABLE_DOC = String.format("Specifies whether to enable Online Certificate Status Protocol in the ZooKeeper TLS protocols." +
            " Overrides any explicit value set via the <code>%s</code> system property (note the shorter name).",
            ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_OCSP_ENABLE_PROP));

    /** ********* General Configuration ********** */
    public final static String BROKER_ID_GENERATION_ENABLE_DOC = String.format("Enable automatic broker id generation on the server. When enabled the value configured for %s should be reviewed.", MAX_RESERVED_BROKER_ID_PROP);
    public final static String MAX_RESERVED_BROKER_ID_DOC = "Max number that can be used for a broker.id";
    public final static String BROKER_ID_DOC = "The broker id for this server. If unset, a unique broker id will be generated." +
            "To avoid conflicts between ZooKeeper generated broker id's and user configured broker id's, generated broker ids " +
            "start from " + MAX_RESERVED_BROKER_ID_PROP + " + 1.";
    public final static String MESSAGE_MAX_BYTES_DOC = TopicConfig.MAX_MESSAGE_BYTES_DOC +
            String.format("This can be set per topic with the topic level <code>%s</code> config.", TopicConfig.MAX_MESSAGE_BYTES_CONFIG);
    public final static String NUM_NETWORK_THREADS_DOC = "The number of threads that the server uses for receiving requests from the network and sending responses to the network. Noted: each listener (except for controller listener) creates its own thread pool.";
    public final static String NUM_IO_THREADS_DOC = "The number of threads that the server uses for processing requests, which may include disk I/O";
    public final static String NUM_REPLICA_ALTER_LOG_DIRS_THREADS_DOC = "The number of threads that can move replicas between log directories, which may include disk I/O";
    public final static String BACKGROUND_THREADS_DOC = "The number of threads to use for various background processing tasks";
    public final static String QUEUED_MAX_REQUESTS_DOC = "The number of queued requests allowed for data-plane, before blocking the network threads";
    public final static String QUEUED_MAX_REQUEST_BYTES_DOC = "The number of queued bytes allowed before no more requests are read";
    public final static String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;
    public final static String CONNECTION_SETUP_TIMEOUT_MS_DOC = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC;
    public final static String CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC;

    /** KRaft mode configs */
    public final static String PROCESS_ROLES_DOC = "The roles that this process plays: 'broker', 'controller', or 'broker,controller' if it is both. " +
            "This configuration is only applicable for clusters in KRaft (Kafka Raft) mode (instead of ZooKeeper). Leave this config undefined or empty for ZooKeeper clusters.";
    public final static String INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_DOC = "When initially registering with the controller quorum, the number of milliseconds to wait before declaring failure and exiting the broker process.";
    public final static String BROKER_HEARTBEAT_INTERVAL_MS_DOC = "The length of time in milliseconds between broker heartbeats. Used when running in KRaft mode.";
    public final static String BROKER_SESSION_TIMEOUT_MS_DOC = "The length of time in milliseconds that a broker lease lasts if no heartbeats are made. Used when running in KRaft mode.";
    public final static String NODE_ID_DOC = "The node ID associated with the roles this process is playing when <code>process.roles</code> is non-empty. " +
            "This is required configuration when running in KRaft mode.";
    public final static String METADATA_LOG_DIR_DOC = "This configuration determines where we put the metadata log for clusters in KRaft mode. " +
            "If it is not set, the metadata log is placed in the first log directory from log.dirs.";
    public final static String METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_DOC = "This is the maximum number of bytes in the log between the latest " +
            "snapshot and the high-watermark needed before generating a new snapshot. The default value is " +
            Defaults.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES + ". To generate snapshots based on the time elapsed, see " +
            String.format("the <code>%s</code> configuration. The Kafka node will generate a snapshot when ", METADATA_SNAPSHOT_MAX_INTERVAL_MS_PROP) +
            "either the maximum time interval is reached or the maximum bytes limit is reached.";
    public final static String METADATA_SNAPSHOT_MAX_INTERVAL_MS_DOC = String.format("This is the maximum number of milliseconds to wait to generate a snapshot " +
            "if there are committed records in the log that are not included in the latest snapshot. A value of zero disables " +
            "time based snapshot generation. The default value is %s. To generate " +
            "snapshots based on the number of metadata bytes, see the <code>%s</code> " +
            "configuration. The Kafka node will generate a snapshot when either the maximum time interval is reached or the " +
            "maximum bytes limit is reached.",
            Defaults.METADATA_SNAPSHOT_MAX_INTERVAL_MS, METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_PROP
            );
    public final static String METADATA_MAX_IDLE_INTERVAL_MS_DOC = "This configuration controls how often the active " +
            "controller should write no-op records to the metadata partition. If the value is 0, no-op records " +
            "are not appended to the metadata partition. The default value is " + Defaults.METADATA_MAX_IDLE_INTERVAL_MS;
    public final static String CONTROLLER_LISTENER_NAMES_DOC = "A comma-separated list of the names of the listeners used by the controller. This is required " +
            "if running in KRaft mode. When communicating with the controller quorum, the broker will always use the first listener in this list.\n " +
            "Note: The ZooKeeper-based controller should not set this configuration.";
    public final static String SASL_MECHANISM_CONTROLLER_PROTOCOL_DOC = "SASL mechanism used for communication with controllers. Default is GSSAPI.";
    public final static String METADATA_LOG_SEGMENT_BYTES_DOC = "The maximum size of a single metadata log file.";
    public final static String METADATA_LOG_SEGMENT_MIN_BYTES_DOC = "Override the minimum size for a single metadata log file. This should be used for testing only.";
    public final static String SERVER_MAX_STARTUP_TIME_MS_DOC = "The maximum number of milliseconds we will wait for the server to come up. " +
            "By default there is no limit. This should be used for testing only.";

    public final static String METADATA_LOG_SEGMENT_MILLIS_DOC = "The maximum time before a new metadata log file is rolled out (in milliseconds).";
    public final static String METADATA_MAX_RETENTION_BYTES_DOC = "The maximum combined size of the metadata log and snapshots before deleting old " +
            "snapshots and log files. Since at least one snapshot must exist before any logs can be deleted, this is a soft limit.";
    public final static String METADATA_MAX_RETENTION_MILLIS_DOC = "The number of milliseconds to keep a metadata log file or snapshot before " +
            "deleting it. Since at least one snapshot must exist before any logs can be deleted, this is a soft limit.";

    /** *********** Authorizer Configuration ********** */
    public final static String AUTHORIZER_CLASS_NAME_DOC = "The fully qualified name of a class that implements <code>" + Authorizer.class.getName() + "</code>" +
            " interface, which is used by the broker for authorization.";
    public final static String EARLY_START_LISTENERS_DOC = "A comma-separated list of listener names which may be started before the authorizer has finished " +
            "initialization. This is useful when the authorizer is dependent on the cluster itself for bootstrapping, as is the case for " +
            "the StandardAuthorizer (which stores ACLs in the metadata log.) By default, all listeners included in controller.listener.names " +
            "will also be early start listeners. A listener should not appear in this list if it accepts external traffic.";

    /** ********* Socket Server Configuration ********** */
    public final static String LISTENERS_DOC = "Listener List - Comma-separated list of URIs we will listen on and the listener names." +
            " If the listener name is not a security protocol, <code>" + LISTENER_SECURITY_PROTOCOL_MAP_PROP + "</code> must also be set.\n" +
            " Listener names and port numbers must be unique unless \n" +
            " one listener is an IPv4 address and the other listener is \n" +
            " an IPv6 address (for the same port).\n" +
            " Specify hostname as 0.0.0.0 to bind to all interfaces.\n" +
            " Leave hostname empty to bind to default interface.\n" +
            " Examples of legal listener lists:\n" +
            " <code>PLAINTEXT://myhost:9092,SSL://:9091</code>\n" +
            " <code>CLIENT://0.0.0.0:9092,REPLICATION://localhost:9093</code>\n" +
            " <code>PLAINTEXT://127.0.0.1:9092,SSL://[::1]:9092</code>\n";
    public final static String ADVERTISED_LISTENERS_DOC = "Listeners to publish to ZooKeeper for clients to use, if different than the <code>" + LISTENERS_PROP + "</code> config property." +
            " In IaaS environments, this may need to be different from the interface to which the broker binds." +
            " If this is not set, the value for <code>" + LISTENERS_PROP + "</code> will be used." +
            " Unlike <code>" + LISTENERS_PROP + "</code>, it is not valid to advertise the 0.0.0.0 meta-address.\n" +
            " Also unlike <code>" + LISTENERS_PROP + "</code>, there can be duplicated ports in this property," +
            " so that one listener can be configured to advertise another listener's address." +
            " This can be useful in some cases where external load balancers are used.";
    public final static String LISTENER_SECURITY_PROTOCOL_MAP_DOC = "Map between listener names and security protocols. This must be defined for " +
            "the same security protocol to be usable in more than one port or IP. For example, internal and " +
            "external traffic can be separated even if SSL is required for both. Concretely, the user could define listeners " +
            "with names INTERNAL and EXTERNAL and this property as: <code>INTERNAL:SSL,EXTERNAL:SSL</code>. As shown, key and value are " +
            "separated by a colon and map entries are separated by commas. Each listener name should only appear once in the map. " +
            "Different security (SSL and SASL) settings can be configured for each listener by adding a normalised " +
            "prefix (the listener name is lowercased) to the config name. For example, to set a different keystore for the " +
            "INTERNAL listener, a config with name <code>listener.name.internal.ssl.keystore.location</code> would be set. " +
            "If the config for the listener name is not set, the config will fallback to the generic config (i.e. <code>ssl.keystore.location</code>). " +
            "Note that in KRaft a default mapping from the listener names defined by <code>controller.listener.names</code> to PLAINTEXT " +
            "is assumed if no explicit mapping is provided and no other security protocol is in use.";
    public final static String CONTROL_PLANE_LISTENER_NAME_DOC = "Name of listener used for communication between controller and brokers. " +
            "A broker will use the <code>" + CONTROL_PLANE_LISTENER_NAME_PROP + "</code> to locate the endpoint in " + LISTENERS_PROP + " list, to listen for connections from the controller. " +
            "For example, if a broker's config is:\n" +
            "<code>listeners = INTERNAL://192.1.1.8:9092, EXTERNAL://10.1.1.5:9093, CONTROLLER://192.1.1.8:9094" +
            "listener.security.protocol.map = INTERNAL:PLAINTEXT, EXTERNAL:SSL, CONTROLLER:SSL" +
            "control.plane.listener.name = CONTROLLER</code>\n" +
            "On startup, the broker will start listening on \"192.1.1.8:9094\" with security protocol \"SSL\".\n" +
            "On the controller side, when it discovers a broker's published endpoints through ZooKeeper, it will use the <code>%s</code> " +
            "to find the endpoint, which it will use to establish connection to the broker.\n" +
            "For example, if the broker's published endpoints on ZooKeeper are:\n" +
            " <code>\"endpoints\" : [\"INTERNAL://broker1.example.com:9092\",\"EXTERNAL://broker1.example.com:9093\",\"CONTROLLER://broker1.example.com:9094\"]</code>\n" +
            " and the controller's config is:\n" +
            "<code>listener.security.protocol.map = INTERNAL:PLAINTEXT, EXTERNAL:SSL, CONTROLLER:SSL" +
            "control.plane.listener.name = CONTROLLER</code>\n" +
            "then the controller will use \"broker1.example.com:9094\" with security protocol \"SSL\" to connect to the broker.\n" +
            "If not explicitly configured, the default value will be null and there will be no dedicated endpoints for controller connections.\n" +
            "If explicitly configured, the value cannot be the same as the value of <code>" + INTER_BROKER_LISTENER_NAME_PROP + "</code>.";

    public final static String SOCKET_SEND_BUFFER_BYTES_DOC = "The SO_SNDBUF buffer of the socket server sockets. If the value is -1, the OS default will be used.";
    public final static String SOCKET_RECEIVE_BUFFER_BYTES_DOC = "The SO_RCVBUF buffer of the socket server sockets. If the value is -1, the OS default will be used.";
    public final static String SOCKET_REQUEST_MAX_BYTES_DOC = "The maximum number of bytes in a socket request";
    public final static String SOCKET_LISTEN_BACKLOG_SIZE_DOC = "The maximum number of pending connections on the socket. " +
            "In Linux, you may also need to configure <code>somaxconn</code> and <code>tcp_max_syn_backlog</code> kernel parameters " +
            "accordingly to make the configuration takes effect.";
    public final static String MAX_CONNECTIONS_PER_IP_DOC = "The maximum number of connections we allow from each ip address. This can be set to 0 if there are overrides " +
            "configured using " + MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP + " property. New connections from the ip address are dropped if the limit is reached.";
    public final static String MAX_CONNECTIONS_PER_IP_OVERRIDES_DOC = "A comma-separated list of per-ip or hostname overrides to the default maximum number of connections. " +
            "An example value is \"hostName:100,127.0.0.1:200\"";
    public final static String MAX_CONNECTIONS_DOC = String.format("The maximum number of connections we allow in the broker at any time. This limit is applied in addition " +
            "to any per-ip limits configured using %s. Listener-level limits may also be configured by prefixing the " +
            "config name with the listener prefix, for example, <code>listener.name.internal.%s</code>. Broker-wide limit " +
            "should be configured based on broker capacity while listener limits should be configured based on application requirements. " +
            "New connections are blocked if either the listener or broker limit is reached. Connections on the inter-broker listener are " +
            "permitted even if broker-wide limit is reached. The least recently used connection on another listener will be closed in this case.",
            MAX_CONNECTIONS_PER_IP_PROP, MAX_CONNECTIONS_PROP
            );
    public final static String MAX_CONNECTION_CREATION_RATE_DOC = "The maximum connection creation rate we allow in the broker at any time. Listener-level limits " +
            String.format("may also be configured by prefixing the config name with the listener prefix, for example, <code>listener.name.internal.%s</code>.", MAX_CONNECTION_CREATION_RATE_PROP) +
            "Broker-wide connection rate limit should be configured based on broker capacity while listener limits should be configured based on " +
            "application requirements. New connections will be throttled if either the listener or the broker limit is reached, with the exception " +
            "of inter-broker listener. Connections on the inter-broker listener will be throttled only when the listener-level rate limit is reached.";
    public final static String CONNECTIONS_MAX_IDLE_MS_DOC = "Idle connections timeout: the server socket processor threads close the connections that idle more than this";
    public final static String FAILED_AUTHENTICATION_DELAY_MS_DOC = "Connection close delay on failed authentication: this is the time (in milliseconds) by which connection close will be delayed on authentication failure. " +
            String.format("This must be configured to be less than %s to prevent connection timeout.", CONNECTIONS_MAX_IDLE_MS_PROP);
    /** *********** Rack Configuration ************* */
    public final static String RACK_DOC = "Rack of the broker. This will be used in rack aware replication assignment for fault tolerance. Examples: <code>RACK1</code>, <code>us-east-1d</code>";
    /** ********* Log Configuration ********** */
    public final static String NUM_PARTITIONS_DOC = "The default number of log partitions per topic";
    public final static String LOG_DIR_DOC = "The directory in which the log data is kept (supplemental for " + LOG_DIRS_PROP + " property)";
    public final static String LOG_DIRS_DOC = "A comma-separated list of the directories where the log data is stored. If not set, the value in " + LOG_DIR_PROP + " is used.";
    public final static String LOG_SEGMENT_BYTES_DOC = "The maximum size of a single log file";
    public final static String LOG_ROLL_TIME_MILLIS_DOC = "The maximum time before a new log segment is rolled out (in milliseconds). If not set, the value in " + LOG_ROLL_TIME_HOURS_PROP + " is used";
    public final static String LOG_ROLL_TIME_HOURS_DOC = "The maximum time before a new log segment is rolled out (in hours), secondary to " + LOG_ROLL_TIME_MILLIS_PROP + " property";

    public final static String LOG_ROLL_TIME_JITTER_MILLIS_DOC = "The maximum jitter to subtract from logRollTimeMillis (in milliseconds). If not set, the value in " + LOG_ROLL_TIME_JITTER_HOURS_PROP + " is used";
    public final static String LOG_ROLL_TIME_JITTER_HOURS_DOC = "The maximum jitter to subtract from logRollTimeMillis (in hours), secondary to " + LOG_ROLL_TIME_JITTER_MILLIS_PROP + " property";

    public final static String LOG_RETENTION_TIME_MILLIS_DOC = "The number of milliseconds to keep a log file before deleting it (in milliseconds), If not set, the value in " + LOG_RETENTION_TIME_MINUTES_PROP + " is used. If set to -1, no time limit is applied.";
    public final static String LOG_RETENTION_TIME_MINS_DOC = "The number of minutes to keep a log file before deleting it (in minutes), secondary to " + LOG_RETENTION_TIME_MILLIS_PROP + " property. If not set, the value in " + LOG_RETENTION_TIME_HOURS_PROP + " is used";
    public final static String LOG_RETENTION_TIME_HOURS_DOC = "The number of hours to keep a log file before deleting it (in hours), tertiary to " + LOG_RETENTION_TIME_MILLIS_PROP + " property";

    public final static String LOG_RETENTION_BYTES_DOC = "The maximum size of the log before deleting it";
    public final static String LOG_CLEANUP_INTERVAL_MS_DOC = "The frequency in milliseconds that the log cleaner checks whether any log is eligible for deletion";
    public final static String LOG_CLEANUP_POLICY_DOC = "The default cleanup policy for segments beyond the retention window. A comma separated list of valid policies. Valid policies are: \"delete\" and \"compact\"";
    public final static String LOG_INDEX_SIZE_MAX_BYTES_DOC = "The maximum size in bytes of the offset index";
    public final static String LOG_INDEX_INTERVAL_BYTES_DOC = "The interval with which we add an entry to the offset index.";
    public final static String LOG_FLUSH_INTERVAL_MESSAGES_DOC = "The number of messages accumulated on a log partition before messages are flushed to disk.";
    public final static String LOG_DELETE_DELAY_MS_DOC = "The amount of time to wait before deleting a file from the filesystem";
    public final static String LOG_FLUSH_SCHEDULER_INTERVAL_MS_DOC = "The frequency in ms that the log flusher checks whether any log needs to be flushed to disk";
    public final static String LOG_FLUSH_INTERVAL_MS_DOC = "The maximum time in ms that a message in any topic is kept in memory before flushed to disk. If not set, the value in " + LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP + " is used";
    public final static String LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_DOC = "The frequency with which we update the persistent record of the last flush which acts as the log recovery point.";
    public final static String LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_DOC = "The frequency with which we update the persistent record of log start offset";
    public final static String LOG_PRE_ALLOCATE_ENABLE_DOC = "Should pre allocate file when create new segment? If you are using Kafka on Windows, you probably need to set it to true.";
    public final static String LOG_MESSAGE_FORMAT_VERSION_DOC = "Specify the message format version the broker will use to append messages to the logs. The value should be a valid MetadataVersion. " +
            "Some examples are: 0.8.2, 0.9.0.0, 0.10.0, check MetadataVersion for more details. By setting a particular message format version, the " +
            "user is certifying that all the existing messages on disk are smaller or equal than the specified version. Setting this value incorrectly " +
            "will cause consumers with older versions to break as they will receive messages with a format that they don't understand.";
    public final static String LOG_MESSAGE_TIMESTAMP_TYPE_DOC = "Define whether the timestamp in the message is message create time or log append time. The value should be either " +
            "<code>CreateTime</code> or <code>LogAppendTime</code>.";
    public final static String LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC = "[DEPRECATED] The maximum difference allowed between the timestamp when a broker receives " +
            "a message and the timestamp specified in the message. If log.message.timestamp.type=CreateTime, a message will be rejected " +
            "if the difference in timestamp exceeds this threshold. This configuration is ignored if log.message.timestamp.type=LogAppendTime." +
            "The maximum timestamp difference allowed should be no greater than log.retention.ms to avoid unnecessarily frequent log rolling.";
    public final static String LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DOC = "This configuration sets the allowable timestamp difference between the " +
            "broker's timestamp and the message timestamp. The message timestamp can be earlier than or equal to the broker's " +
            "timestamp, with the maximum allowable difference determined by the value set in this configuration. " +
            "If log.message.timestamp.type=CreateTime, the message will be rejected if the difference in timestamps exceeds " +
            "this specified threshold. This configuration is ignored if log.message.timestamp.type=LogAppendTime.";
    public final static String LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_DOC = "This configuration sets the allowable timestamp difference between the " +
            "message timestamp and the broker's timestamp. The message timestamp can be later than or equal to the broker's " +
            "timestamp, with the maximum allowable difference determined by the value set in this configuration. " +
            "If log.message.timestamp.type=CreateTime, the message will be rejected if the difference in timestamps exceeds " +
            "this specified threshold. This configuration is ignored if log.message.timestamp.type=LogAppendTime.";
    public final static String NUM_RECOVERY_THREADS_PER_DATA_DIR_DOC = "The number of threads per data directory to be used for log recovery at startup and flushing at shutdown";
    public final static String AUTO_CREATE_TOPICS_ENABLE_DOC = "Enable auto creation of topic on the server.";
    public final static String MIN_IN_SYNC_REPLICAS_DOC = "When a producer sets acks to \"all\" (or \"-1\"), " +
            "<code>min.insync.replicas</code> specifies the minimum number of replicas that must acknowledge " +
            "a write for the write to be considered successful. If this minimum cannot be met, " +
            "then the producer will raise an exception (either <code>NotEnoughReplicas</code> or " +
            "<code>NotEnoughReplicasAfterAppend</code>).<br>When used together, <code>min.insync.replicas</code> and acks " +
            "allow you to enforce greater durability guarantees. A typical scenario would be to " +
            "create a topic with a replication factor of 3, set <code>min.insync.replicas</code> to 2, and " +
            "produce with acks of \"all\". This will ensure that the producer raises an exception " +
            "if a majority of replicas do not receive a write.";
    public final static String CREATE_TOPIC_POLICY_CLASS_NAME_DOC = "The create topic policy class that should be used for validation. The class should " +
            "implement the <code>org.apache.kafka.server.policy.CreateTopicPolicy</code> interface.";
    public final static String ALTER_CONFIG_POLICY_CLASS_NAME_DOC = "The alter configs policy class that should be used for validation. The class should " +
            "implement the <code>org.apache.kafka.server.policy.AlterConfigPolicy</code> interface.";
    public final static String LOG_MESSAGE_DOWN_CONVERSION_ENABLE_DOC = TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_DOC;

    /** ********* Replication configuration ********** */
    public final static String CONTROLLER_SOCKET_TIMEOUT_MS_DOC = "The socket timeout for controller-to-broker channels.";
    public final static String DEFAULT_REPLICATION_FACTOR_DOC = "The default replication factors for automatically created topics.";
    public final static String REPLICA_LAG_TIME_MAX_MS_DOC = "If a follower hasn't sent any fetch requests or hasn't consumed up to the leaders log end offset for at least this time," +
            " the leader will remove the follower from isr";
    public final static String REPLICA_SOCKET_TIMEOUT_MS_DOC = "The socket timeout for network requests. Its value should be at least replica.fetch.wait.max.ms";
    public final static String REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_DOC = "The socket receive buffer for network requests to the leader for replicating data";
    public final static String REPLICA_FETCH_MAX_BYTES_DOC = "The number of bytes of messages to attempt to fetch for each partition. This is not an absolute maximum, " +
            "if the first record batch in the first non-empty partition of the fetch is larger than this value, the record batch will still be returned " +
            "to ensure that progress can be made. The maximum record batch size accepted by the broker is defined via " +
            "<code>message.max.bytes</code> (broker config) or <code>max.message.bytes</code> (topic config).";
    public final static String REPLICA_FETCH_WAIT_MAX_MS_DOC = "The maximum wait time for each fetcher request issued by follower replicas. This value should always be less than the " +
            "replica.lag.time.max.ms at all times to prevent frequent shrinking of ISR for low throughput topics";
    public final static String REPLICA_FETCH_MIN_BYTES_DOC = "Minimum bytes expected for each fetch response. If not enough bytes, wait up to <code>replica.fetch.wait.max.ms</code> (broker config).";
    public final static String REPLICA_FETCH_RESPONSE_MAX_BYTES_DOC = "Maximum bytes expected for the entire fetch response. Records are fetched in batches, " +
            "and if the first record batch in the first non-empty partition of the fetch is larger than this value, the record batch " +
            "will still be returned to ensure that progress can be made. As such, this is not an absolute maximum. The maximum " +
            "record batch size accepted by the broker is defined via <code>message.max.bytes</code> (broker config) or " +
            "<code>max.message.bytes</code> (topic config).";
    public final static String NUM_REPLICA_FETCHERS_DOC = "Number of fetcher threads used to replicate records from each source broker. The total number of fetchers " +
            "on each broker is bound by <code>num.replica.fetchers</code> multiplied by the number of brokers in the cluster." +
            "Increasing this value can increase the degree of I/O parallelism in the follower and leader broker at the cost " +
            "of higher CPU and memory utilization.";
    public final static String REPLICA_FETCH_BACKOFF_MS_DOC = "The amount of time to sleep when fetch partition error occurs.";
    public final static String REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_DOC = "The frequency with which the high watermark is saved out to disk";
    public final static String FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC = "The purge interval (in number of requests) of the fetch request purgatory";
    public final static String PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC = "The purge interval (in number of requests) of the producer request purgatory";
    public final static String DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC = "The purge interval (in number of requests) of the delete records request purgatory";
    public final static String AUTO_LEADER_REBALANCE_ENABLE_DOC = String.format("Enables auto leader balancing. A background thread checks the distribution of partition leaders at regular intervals, configurable by %s. If the leader imbalance exceeds %s, leader rebalance to the preferred leader for partitions is triggered.",
            LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_PROP, LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_PROP);
    public final static String LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_DOC = "The ratio of leader imbalance allowed per broker. The controller would trigger a leader balance if it goes above this value per broker. The value is specified in percentage.";
    public final static String LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_DOC = "The frequency with which the partition rebalance check is triggered by the controller";
    public final static String UNCLEAN_LEADER_ELECTION_ENABLE_DOC = "Indicates whether to enable replicas not in the ISR set to be elected as leader as a last resort, even though doing so may result in data loss";
    public final static String INTER_BROKER_SECURITY_PROTOCOL_DOC = "Security protocol used to communicate between brokers. Valid values are: " +
            Utils.join(SecurityProtocol.names(), ", ") + ". It is an error to set this and " + INTER_BROKER_LISTENER_NAME_PROP +
            " properties at the same time.";
    public final static String INTER_BROKER_PROTOCOL_VERSION_DOC = "Specify which version of the inter-broker protocol will be used.\n" +
            " This is typically bumped after all brokers were upgraded to a new version.\n" +
            " Example of some valid values are: 0.8.0, 0.8.1, 0.8.1.1, 0.8.2, 0.8.2.0, 0.8.2.1, 0.9.0.0, 0.9.0.1 Check MetadataVersion for the full list.";
    public final static String INTER_BROKER_LISTENER_NAME_DOC = "Name of listener used for communication between brokers. If this is unset, the listener name is defined by $InterBrokerSecurityProtocolProp. " +
            "It is an error to set this and " + INTER_BROKER_SECURITY_PROTOCOL_PROP + " properties at the same time.";
    public final static String REPLICA_SELECTOR_CLASS_DOC = "The fully qualified class name that implements ReplicaSelector. This is used by the broker to find the preferred read replica. By default, we use an implementation that returns the leader.";
    /** ********* Controlled shutdown configuration ********** */
    public final static String CONTROLLED_SHUTDOWN_MAX_RETRIES_DOC = "Controlled shutdown can fail for multiple reasons. This determines the number of retries when such failure happens";
    public final static String CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_DOC = "Before each retry, the system needs time to recover from the state that caused the previous failure (Controller fail over, replica lag etc). This config determines the amount of time to wait before retrying.";
    public final static String CONTROLLED_SHUTDOWN_ENABLE_DOC = "Enable controlled shutdown of the server.";

    /** ********* Group coordinator configuration ********** */
    public final static String GROUP_MIN_SESSION_TIMEOUT_MS_DOC = "The minimum allowed session timeout for registered consumers. Shorter timeouts result in quicker failure detection at the cost of more frequent consumer heartbeating, which can overwhelm broker resources.";
    public final static String GROUP_MAX_SESSION_TIMEOUT_MS_DOC = "The maximum allowed session timeout for registered consumers. Longer timeouts give consumers more time to process messages in between heartbeats at the cost of a longer time to detect failures.";
    public final static String GROUP_INITIAL_REBALANCE_DELAY_MS_DOC = "The amount of time the group coordinator will wait for more consumers to join a new group before performing the first rebalance. A longer delay means potentially fewer rebalances, but increases the time until processing begins.";
    public final static String GROUP_MAX_SIZE_DOC = "The maximum number of consumers that a single consumer group can accommodate.";

    /** New group coordinator configs */
    public final static String NEW_GROUP_COORDINATOR_ENABLE_DOC = "Enable the new group coordinator.";
    public final static String GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DOC = String.format("The list of enabled rebalance protocols. Supported protocols: %s. " +
            "The %s rebalance protocol is in early access and therefore must not be used in production.",
            Utils.join(Stream.of(GroupType.values()).map(GroupType::toString).collect(Collectors.toList()), ","),
            GroupType.CONSUMER);
    public final static String GROUP_COORDINATOR_NUM_THREADS_DOC = "The number of threads used by the group coordinator.";

    /** Consumer group configs */
    public final static String CONSUMER_GROUP_SESSION_TIMEOUT_MS_DOC = "The timeout to detect client failures when using the consumer group protocol.";
    public final static String CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DOC = "The minimum allowed session timeout for registered consumers.";
    public final static String CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DOC = "The maximum allowed session timeout for registered consumers.";
    public final static String CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DOC = "The heartbeat interval given to the members of a consumer group.";
    public final static String CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DOC = "The minimum heartbeat interval for registered consumers.";
    public final static String CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DOC = "The maximum heartbeat interval for registered consumers.";
    public final static String CONSUMER_GROUP_MAX_SIZE_DOC = "The maximum number of consumers that a single consumer group can accommodate.";
    public final static String CONSUMER_GROUP_ASSIGNORS_DOC = "The server side assignors as a list of full class names. The first one in the list is considered as the default assignor to be used in the case where the consumer does not specify an assignor.";

    /** ********* Offset management configuration ********** */
    public final static String OFFSET_METADATA_MAX_SIZE_DOC = "The maximum size for a metadata entry associated with an offset commit.";
    public final static String OFFSETS_LOAD_BUFFER_SIZE_DOC = "Batch size for reading from the offsets segments when loading offsets into the cache (soft-limit, overridden if records are too large).";
    public final static String OFFSETS_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor for the offsets topic (set higher to ensure availability). " +
            "Internal topic creation will fail until the cluster size meets this replication factor requirement.";
    public final static String OFFSETS_TOPIC_PARTITIONS_DOC = "The number of partitions for the offset commit topic (should not change after deployment).";
    public final static String OFFSETS_TOPIC_SEGMENT_BYTES_DOC = "The offsets topic segment bytes should be kept relatively small in order to facilitate faster log compaction and cache loads.";
    public final static String OFFSETS_TOPIC_COMPRESSION_CODEC_DOC = "Compression codec for the offsets topic - compression may be used to achieve \"atomic\" commits.";
    public final static String OFFSETS_RETENTION_MINUTES_DOC = "For subscribed consumers, committed offset of a specific partition will be expired and discarded when 1) this retention period has elapsed after the consumer group loses all its consumers (i.e. becomes empty); " +
            "2) this retention period has elapsed since the last time an offset is committed for the partition and the group is no longer subscribed to the corresponding topic. " +
            "For standalone consumers (using manual assignment), offsets will be expired after this retention period has elapsed since the time of last commit. " +
            "Note that when a group is deleted via the delete-group request, its committed offsets will also be deleted without extra retention period; " +
            "also when a topic is deleted via the delete-topic request, upon propagated metadata update any group's committed offsets for that topic will also be deleted without extra retention period.";
    public final static String OFFSETS_RETENTION_CHECK_INTERVAL_MS_DOC = "Frequency at which to check for stale offsets";
    public final static String OFFSET_COMMIT_TIMEOUT_MS_DOC = "Offset commit will be delayed until all replicas for the offsets topic receive the commit " +
            "or this timeout is reached. This is similar to the producer request timeout.";
    public final static String OFFSET_COMMIT_REQUIRED_ACKS_DOC = "The required acks before the commit can be accepted. In general, the default (-1) should not be overridden.";
    /** ********* Transaction management configuration ********** */
    public final static String TRANSACTIONAL_ID_EXPIRATION_MS_DOC = "The time in ms that the transaction coordinator will wait without receiving any transaction status updates " +
            "for the current transaction before expiring its transactional id. Transactional IDs will not expire while a the transaction is still ongoing.";
    public final static String TRANSACTIONS_MAX_TIMEOUT_MS_DOC = "The maximum allowed timeout for transactions. " +
            "If a client’s requested transaction time exceed this, then the broker will return an error in InitProducerIdRequest. This prevents a client from too large of a timeout, which can stall consumers reading from topics included in the transaction.";
    public final static String TRANSACTIONS_TOPIC_MIN_ISR_DOC = "Overridden " + MIN_IN_SYNC_REPLICAS_PROP + " config for the transaction topic.";
    public final static String TRANSACTIONS_LOAD_BUFFER_SIZE_DOC = "Batch size for reading from the transaction log segments when loading producer ids and transactions into the cache (soft-limit, overridden if records are too large).";
    public final static String TRANSACTIONS_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor for the transaction topic (set higher to ensure availability). " +
            "Internal topic creation will fail until the cluster size meets this replication factor requirement.";
    public final static String TRANSACTIONS_TOPIC_PARTITIONS_DOC = "The number of partitions for the transaction topic (should not change after deployment).";
    public final static String TRANSACTIONS_TOPIC_SEGMENT_BYTES_DOC = "The transaction topic segment bytes should be kept relatively small in order to facilitate faster log compaction and cache loads";
    public final static String TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTIONS_INTERVAL_MS_DOC = "The interval at which to rollback transactions that have timed out";
    public final static String TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONS_INTERVAL_MS_DOC = "The interval at which to remove transactions that have expired due to <code>transactional.id.expiration.ms</code> passing";

    public final static String TRANSACTION_PARTITION_VERIFICATION_ENABLE_DOC = "Enable verification that checks that the partition has been added to the transaction before writing transactional records to the partition";

    public final static String PRODUCER_ID_EXPIRATION_MS_DOC = "The time in ms that a topic partition leader will wait before expiring producer IDs. Producer IDs will not expire while a transaction associated to them is still ongoing. " +
            "Note that producer IDs may expire sooner if the last write from the producer ID is deleted due to the topic's retention settings. Setting this value the same or higher than " +
            "<code>delivery.timeout.ms</code> can help prevent expiration during retries and protect against message duplication, but the default should be reasonable for most use cases.";
    public final static String PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DOC = "The interval at which to remove producer IDs that have expired due to <code>producer.id.expiration.ms</code> passing.";

    /** ********* Fetch Configuration ************* */
    public final static String MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_DOC = "The maximum number of incremental fetch sessions that we will maintain.";
    public final static String FETCH_MAX_BYTES_DOC = "The maximum number of bytes we will return for a fetch request. Must be at least 1024.";

    /** ********* Request Limit Configuration ************* */
    public final static String MAX_REQUEST_PARTITION_SIZE_LIMIT_DOC = "The maximum number of partitions can be served in one request.";

    /** ********* Quota Configuration ********** */
    public final static String NUM_QUOTA_SAMPLES_DOC = "The number of samples to retain in memory for client quotas";
    public final static String NUM_REPLICATION_QUOTA_SAMPLES_DOC = "The number of samples to retain in memory for replication quotas";
    public final static String NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_DOC = "The number of samples to retain in memory for alter log dirs replication quotas";
    public final static String NUM_CONTROLLER_QUOTA_SAMPLES_DOC = "The number of samples to retain in memory for controller mutation quotas";
    public final static String QUOTA_WINDOW_SIZE_SECONDS_DOC = "The time span of each sample for client quotas";
    public final static String REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_DOC = "The time span of each sample for replication quotas";
    public final static String ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_DOC = "The time span of each sample for alter log dirs replication quotas";
    public final static String CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_DOC = "The time span of each sample for controller mutations quotas";

    public final static String CLIENT_QUOTA_CALLBACK_CLASS_DOC = "The fully qualified name of a class that implements the ClientQuotaCallback interface, " +
            "which is used to determine quota limits applied to client requests. By default, the &lt;user&gt; and &lt;client-id&gt; " +
            "quotas that are stored in ZooKeeper are applied. For any given request, the most specific quota that matches the user principal " +
            "of the session and the client-id of the request is applied.";

    public final static String DELETE_TOPIC_ENABLE_DOC = "Enables delete topic. Delete topic through the admin tool will have no effect if this config is turned off";
    public final static String COMPRESSION_TYPE_DOC = "Specify the final compression type for a given topic. This configuration accepts the standard compression codecs " +
            "('gzip', 'snappy', 'lz4', 'zstd'). It additionally accepts 'uncompressed' which is equivalent to no compression; and " +
            "'producer' which means retain the original compression codec set by the producer.";

    /** ********* Kafka Metrics Configuration ********** */
    public final static String METRIC_SAMPLE_WINDOW_MS_DOC = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC;
    public final static String METRIC_NUM_SAMPLES_DOC = CommonClientConfigs.METRICS_NUM_SAMPLES_DOC;
    public final static String METRIC_REPORTER_CLASSES_DOC = CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC;
    public final static String METRICS_RECORDING_LEVEL_DOC = CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC;
    public final static String AUTO_INCLUDE_JMX_REPORTER_DOC = CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_DOC;


    /** ********* Kafka Yammer Metrics Reporter Configuration ********** */
    public final static String KAFKA_METRICS_REPORTER_CLASSES_DOC = "A list of classes to use as Yammer metrics custom reporters." +
            " The reporters should implement <code>kafka.metrics.KafkaMetricsReporter</code> trait. If a client wants" +
            " to expose JMX operations on a custom reporter, the custom reporter needs to additionally implement an MBean" +
            " trait that extends <code>kafka.metrics.KafkaMetricsReporterMBean</code> trait so that the registered MBean is compliant with" +
            " the standard MBean convention.";

    public final static String KAFKA_METRICS_POLLING_INTERVAL_SECONDS_DOC = "The metrics polling interval (in seconds) which can be used in " +
            KAFKA_METRICS_REPORTER_CLASSES_PROP + " implementations.";

    /** ********* Kafka Client Telemetry Metrics Configuration ********** */
    public final static String CLIENT_TELEMETRY_MAX_BYTES_DOC = "The maximum size (after compression if compression is used) of" +
            " telemetry metrics pushed from a client to the broker. The default value is 1048576 (1 MB).";


    /** ********* Delegation Token Configuration ****************/
    public final static String DELEGATION_TOKEN_SECRET_KEY_ALIAS_DOC = "DEPRECATED: An alias for " + DELEGATION_TOKEN_SECRET_KEY_PROP + ", which should be used instead of this config.";
    public final static String DELEGATION_TOKEN_SECRET_KEY_DOC = "Secret key to generate and verify delegation tokens. The same key must be configured across all the brokers. " +
            " If using Kafka with KRaft, the key must also be set across all controllers. " +
            " If the key is not set or set to empty string, brokers will disable the delegation token support.";
    public final static String DELEGATION_TOKEN_MAX_LIFE_TIME_DOC = "The token has a maximum lifetime beyond which it cannot be renewed anymore. Default value 7 days.";
    public final static String DELEGATION_TOKEN_EXPIRY_TIME_MS_DOC = "The token validity time in milliseconds before the token needs to be renewed. Default value 1 day.";
    public final static String DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_DOC = "Scan interval to remove expired delegation tokens.";

    /** ********* Password encryption configuration for dynamic configs *********/
    public final static String PASSWORD_ENCODER_SECRET_DOC = "The secret used for encoding dynamically configured passwords for this broker.";
    public final static String PASSWORD_ENCODER_OLD_SECRET_DOC = "The old secret that was used for encoding dynamically configured passwords. " +
            "This is required only when the secret is updated. If specified, all dynamically encoded passwords are " +
            "decoded using this old secret and re-encoded using " + PASSWORD_ENCODER_SECRET_PROP + " when broker starts up.";
    public final static String PASSWORD_ENCODER_KEY_FACTORY_ALGORITHM_DOC = "The SecretKeyFactory algorithm used for encoding dynamically configured passwords. " +
            "Default is PBKDF2WithHmacSHA512 if available and PBKDF2WithHmacSHA1 otherwise.";
    public final static String PASSWORD_ENCODER_CIPHER_ALGORITHM_DOC = "The Cipher algorithm used for encoding dynamically configured passwords.";
    public final static String PASSWORD_ENCODER_KEY_LENGTH_DOC =  "The key length used for encoding dynamically configured passwords.";
    public final static String PASSWORD_ENCODER_ITERATIONS_DOC =  "The iteration count used for encoding dynamically configured passwords.";

    @SuppressWarnings("deprecation")
    public final static ConfigDef CONFIG_DEF = new ConfigDef()
        /** ********* Zookeeper Configuration ***********/
        .define(ZK_CONNECT_PROP, STRING, null, HIGH, ZK_CONNECT_DOC)
        .define(ZK_SESSION_TIMEOUT_MS_PROP, INT, Defaults.ZK_SESSION_TIMEOUT_MS, HIGH, ZK_SESSION_TIMEOUT_MS_DOC)
        .define(ZK_CONNECTION_TIMEOUT_MS_PROP, INT, null, HIGH, ZK_CONNECTION_TIMEOUT_MS_DOC)
        .define(ZK_ENABLE_SECURE_ACLS_PROP, BOOLEAN, Defaults.ZK_ENABLE_SECURE_ACLS, HIGH, ZK_ENABLE_SECURE_ACLS_DOC)
        .define(ZK_MAX_IN_FLIGHT_REQUESTS_PROP, INT, Defaults.ZK_MAX_IN_FLIGHT_REQUESTS, atLeast(1), HIGH, ZK_MAX_INFLIGHT_REQUESTS_DOC)
        .define(ZK_SSL_CLIENT_ENABLE_PROP, BOOLEAN, Defaults.ZK_SSL_CLIENT_ENABLE, MEDIUM, ZK_SSL_CLIENT_ENABLE_DOC)
        .define(ZK_CLIENT_CNXN_SOCKET_PROP, STRING, null, MEDIUM, ZK_CLIENT_CNXN_SOCKET_DOC)
        .define(ZK_SSL_KEYSTORE_LOCATION_PROP, STRING, null, MEDIUM, ZK_SSL_KEYSTORE_LOCATION_DOC)
        .define(ZK_SSL_KEYSTORE_PASSWORD_PROP, PASSWORD, null, MEDIUM, ZK_SSL_KEYSTORE_PASSWORD_DOC)
        .define(ZK_SSL_KEYSTORE_TYPE_PROP, STRING, null, MEDIUM, ZK_SSL_KEYSTORE_TYPE_DOC)
        .define(ZK_SSL_TRUSTSTORE_LOCATION_PROP, STRING, null, MEDIUM, ZK_SSL_TRUSTSTORE_LOCATION_DOC)
        .define(ZK_SSL_TRUSTSTORE_PASSWORD_PROP, PASSWORD, null, MEDIUM, ZK_SSL_TRUSTSTORE_PASSWORD_DOC)
        .define(ZK_SSL_TRUSTSTORE_TYPE_PROP, STRING, null, MEDIUM, ZK_SSL_TRUSTSTORE_TYPE_DOC)
        .define(ZK_SSL_PROTOCOL_PROP, STRING, Defaults.ZK_SSL_PROTOCOL, LOW, ZK_SSL_PROTOCOL_DOC)
        .define(ZK_SSL_ENABLED_PROTOCOLS_PROP, LIST, null, LOW, ZK_SSL_ENABLED_PROTOCOLS_DOC)
        .define(ZK_SSL_CIPHER_SUITES_PROP, LIST, null, LOW, ZK_SSL_CIPHER_SUITES_DOC)
        .define(ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP, STRING, Defaults.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, LOW, ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
        .define(ZK_SSL_CRL_ENABLE_PROP, BOOLEAN, Defaults.ZK_SSL_CRL_ENABLE, LOW, ZK_SSL_CRL_ENABLE_DOC)
        .define(ZK_SSL_OCSP_ENABLE_PROP, BOOLEAN, Defaults.ZK_SSL_OCSP_ENABLE, LOW, ZK_SSL_OCSP_ENABLE_DOC)

        /** ********* General Configuration ***********/
        .define(BROKER_ID_GENERATION_ENABLE_PROP, BOOLEAN, Defaults.BROKER_ID_GENERATION_ENABLE, MEDIUM, BROKER_ID_GENERATION_ENABLE_DOC)
        .define(MAX_RESERVED_BROKER_ID_PROP, INT, Defaults.MAX_RESERVED_BROKER_ID, atLeast(0), MEDIUM, MAX_RESERVED_BROKER_ID_DOC)
        .define(BROKER_ID_PROP, INT, Defaults.BROKER_ID, HIGH, BROKER_ID_DOC)
        .define(MESSAGE_MAX_BYTES_PROP, INT, LogConfig.DEFAULT_MAX_MESSAGE_BYTES, atLeast(0), HIGH, MESSAGE_MAX_BYTES_DOC)
        .define(NUM_NETWORK_THREADS_PROP, INT, Defaults.NUM_NETWORK_THREADS, atLeast(1), HIGH, NUM_NETWORK_THREADS_DOC)
        .define(NUM_IO_THREADS_PROP, INT, Defaults.NUM_IO_THREADS, atLeast(1), HIGH, NUM_IO_THREADS_DOC)
        .define(NUM_REPLICA_ALTER_LOG_DIRS_THREADS_PROP, INT, null, HIGH, NUM_REPLICA_ALTER_LOG_DIRS_THREADS_DOC)
        .define(BACKGROUND_THREADS_PROP, INT, Defaults.BACKGROUND_THREADS, atLeast(1), HIGH, BACKGROUND_THREADS_DOC)
        .define(QUEUED_MAX_REQUESTS_PROP, INT, Defaults.QUEUED_MAX_REQUESTS, atLeast(1), HIGH, QUEUED_MAX_REQUESTS_DOC)
        .define(QUEUED_MAX_BYTES_PROP, LONG, Defaults.QUEUED_MAX_REQUEST_BYTES, MEDIUM, QUEUED_MAX_REQUEST_BYTES_DOC)
        .define(REQUEST_TIMEOUT_MS_PROP, INT, Defaults.REQUEST_TIMEOUT_MS, HIGH, REQUEST_TIMEOUT_MS_DOC)
        .define(CONNECTION_SETUP_TIMEOUT_MS_PROP, LONG, Defaults.CONNECTION_SETUP_TIMEOUT_MS, MEDIUM, CONNECTION_SETUP_TIMEOUT_MS_DOC)
        .define(CONNECTION_SETUP_TIMEOUT_MAX_MS_PROP, LONG, Defaults.CONNECTION_SETUP_TIMEOUT_MAX_MS, MEDIUM, CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)

        /*
        * KRaft mode configs.
        */
        .define(METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_PROP, LONG, Defaults.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES, atLeast(1), HIGH, METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_DOC)
        .define(METADATA_SNAPSHOT_MAX_INTERVAL_MS_PROP, LONG, Defaults.METADATA_SNAPSHOT_MAX_INTERVAL_MS, atLeast(0), HIGH, METADATA_SNAPSHOT_MAX_INTERVAL_MS_DOC)
        .define(PROCESS_ROLES_PROP, LIST, Collections.emptyList(), ConfigDef.ValidList.in("broker", "controller"), HIGH, PROCESS_ROLES_DOC)
        .define(NODE_ID_PROP, INT, Defaults.EMPTY_NODE_ID, null, HIGH, NODE_ID_DOC)
        .define(INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_PROP, INT, Defaults.INITIAL_BROKER_REGISTRATION_TIMEOUT_MS, null, MEDIUM, INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_DOC)
        .define(BROKER_HEARTBEAT_INTERVAL_MS_PROP, INT, Defaults.BROKER_HEARTBEAT_INTERVAL_MS, null, MEDIUM, BROKER_HEARTBEAT_INTERVAL_MS_DOC)
        .define(BROKER_SESSION_TIMEOUT_MS_PROP, INT, Defaults.BROKER_SESSION_TIMEOUT_MS, null, MEDIUM, BROKER_SESSION_TIMEOUT_MS_DOC)
        .define(CONTROLLER_LISTENER_NAMES_PROP, STRING, null, null, HIGH, CONTROLLER_LISTENER_NAMES_DOC)
        .define(SASL_MECHANISM_CONTROLLER_PROTOCOL_PROP, STRING, SaslConfigs.DEFAULT_SASL_MECHANISM, null, HIGH, SASL_MECHANISM_CONTROLLER_PROTOCOL_DOC)
        .define(METADATA_LOG_DIR_PROP, STRING, null, null, HIGH, METADATA_LOG_DIR_DOC)
        .define(METADATA_LOG_SEGMENT_BYTES_PROP, INT, LogConfig.DEFAULT_SEGMENT_BYTES, atLeast(Records.LOG_OVERHEAD), HIGH, METADATA_LOG_SEGMENT_BYTES_DOC)
        .defineInternal(METADATA_LOG_SEGMENT_MIN_BYTES_PROP, INT, 8 * 1024 * 1024, atLeast(Records.LOG_OVERHEAD), HIGH, METADATA_LOG_SEGMENT_MIN_BYTES_DOC)
        .define(METADATA_LOG_SEGMENT_MILLIS_PROP, LONG, LogConfig.DEFAULT_SEGMENT_MS, null, HIGH, METADATA_LOG_SEGMENT_MILLIS_DOC)
        .define(METADATA_MAX_RETENTION_BYTES_PROP, LONG, Defaults.METADATA_MAX_RETENTION_BYTES, null, HIGH, METADATA_MAX_RETENTION_BYTES_DOC)
        .define(METADATA_MAX_RETENTION_MILLIS_PROP, LONG, LogConfig.DEFAULT_RETENTION_MS, null, HIGH, METADATA_MAX_RETENTION_MILLIS_DOC)
        .define(METADATA_MAX_IDLE_INTERVAL_MS_PROP, INT, Defaults.METADATA_MAX_IDLE_INTERVAL_MS, atLeast(0), LOW, METADATA_MAX_IDLE_INTERVAL_MS_DOC)
        .defineInternal(SERVER_MAX_STARTUP_TIME_MS_PROP, LONG, Defaults.SERVER_MAX_STARTUP_TIME_MS, atLeast(0), MEDIUM, SERVER_MAX_STARTUP_TIME_MS_DOC)
        .define(MIGRATION_ENABLED_PROP, BOOLEAN, false, HIGH, "Enable ZK to KRaft migration")
        .define(ELR_ENABLED_PROP, BOOLEAN, false, HIGH, "Enable the Eligible leader replicas")
        .defineInternal(MIGRATION_METADATA_MIN_BATCH_SIZE_PROP, INT, Defaults.MIGRATION_METADATA_MIN_BATCH_SIZE, atLeast(1),
        MEDIUM, "Soft minimum batch size to use when migrating metadata from ZooKeeper to KRaft")

        /************* Authorizer Configuration ***********/
        .define(AUTHORIZER_CLASS_NAME_PROP, STRING, Defaults.AUTHORIZER_CLASS_NAME, new ConfigDef.NonNullValidator(), LOW, AUTHORIZER_CLASS_NAME_DOC)
        .define(EARLY_START_LISTENERS_PROP, STRING, null,  HIGH, EARLY_START_LISTENERS_DOC)

        /** ********* Socket Server Configuration ***********/
        .define(LISTENERS_PROP, STRING, Defaults.LISTENERS, HIGH, LISTENERS_DOC)
        .define(ADVERTISED_LISTENERS_PROP, STRING, null, HIGH, ADVERTISED_LISTENERS_DOC)
        .define(LISTENER_SECURITY_PROTOCOL_MAP_PROP, STRING, Defaults.LISTENER_SECURITY_PROTOCOL_MAP, LOW, LISTENER_SECURITY_PROTOCOL_MAP_DOC)
        .define(CONTROL_PLANE_LISTENER_NAME_PROP, STRING, null, HIGH, CONTROL_PLANE_LISTENER_NAME_DOC)
        .define(SOCKET_SEND_BUFFER_BYTES_PROP, INT, Defaults.SOCKET_SEND_BUFFER_BYTES, HIGH, SOCKET_SEND_BUFFER_BYTES_DOC)
        .define(SOCKET_RECEIVE_BUFFER_BYTES_PROP, INT, Defaults.SOCKET_RECEIVE_BUFFER_BYTES, HIGH, SOCKET_RECEIVE_BUFFER_BYTES_DOC)
        .define(SOCKET_REQUEST_MAX_BYTES_PROP, INT, Defaults.SOCKET_REQUEST_MAX_BYTES, atLeast(1), HIGH, SOCKET_REQUEST_MAX_BYTES_DOC)
        .define(SOCKET_LISTEN_BACKLOG_SIZE_PROP, INT, Defaults.SOCKET_LISTEN_BACKLOG_SIZE, atLeast(1), MEDIUM, SOCKET_LISTEN_BACKLOG_SIZE_DOC)
        .define(MAX_CONNECTIONS_PER_IP_PROP, INT, Defaults.MAX_CONNECTIONS_PER_IP, atLeast(0), MEDIUM, MAX_CONNECTIONS_PER_IP_DOC)
        .define(MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP, STRING, Defaults.MAX_CONNECTIONS_PER_IP_OVERRIDES, MEDIUM, MAX_CONNECTIONS_PER_IP_OVERRIDES_DOC)
        .define(MAX_CONNECTIONS_PROP, INT, Defaults.MAX_CONNECTIONS, atLeast(0), MEDIUM, MAX_CONNECTIONS_DOC)
        .define(MAX_CONNECTION_CREATION_RATE_PROP, INT, Defaults.MAX_CONNECTION_CREATION_RATE, atLeast(0), MEDIUM, MAX_CONNECTION_CREATION_RATE_DOC)
        .define(CONNECTIONS_MAX_IDLE_MS_PROP, LONG, Defaults.CONNECTIONS_MAX_IDLE_MS, MEDIUM, CONNECTIONS_MAX_IDLE_MS_DOC)
        .define(FAILED_AUTHENTICATION_DELAY_MS_PROP, INT, Defaults.FAILED_AUTHENTICATION_DELAY_MS, atLeast(0), LOW, FAILED_AUTHENTICATION_DELAY_MS_DOC)

        /************ Rack Configuration ******************/
        .define(RACK_PROP, STRING, null, MEDIUM, RACK_DOC)

        /** ********* Log Configuration ***********/
        .define(NUM_PARTITIONS_PROP, INT, Defaults.NUM_PARTITIONS, atLeast(1), MEDIUM, NUM_PARTITIONS_DOC)
        .define(LOG_DIR_PROP, STRING, Defaults.LOG_DIR, HIGH, LOG_DIR_DOC)
        .define(LOG_DIRS_PROP, STRING, null, HIGH, LOG_DIRS_DOC)
        .define(LOG_SEGMENT_BYTES_PROP, INT, LogConfig.DEFAULT_SEGMENT_BYTES, atLeast(LegacyRecord.RECORD_OVERHEAD_V0), HIGH, LOG_SEGMENT_BYTES_DOC)

        .define(LOG_ROLL_TIME_MILLIS_PROP, LONG, null, HIGH, LOG_ROLL_TIME_MILLIS_DOC)
        .define(LOG_ROLL_TIME_HOURS_PROP, INT, (int) TimeUnit.MILLISECONDS.toHours(LogConfig.DEFAULT_SEGMENT_MS), atLeast(1), HIGH, LOG_ROLL_TIME_HOURS_DOC)

        .define(LOG_ROLL_TIME_JITTER_MILLIS_PROP, LONG, null, HIGH, LOG_ROLL_TIME_JITTER_MILLIS_DOC)
        .define(LOG_ROLL_TIME_JITTER_HOURS_PROP, INT, (int) TimeUnit.MILLISECONDS.toHours(LogConfig.DEFAULT_SEGMENT_JITTER_MS), atLeast(0), HIGH, LOG_ROLL_TIME_JITTER_HOURS_DOC)

        .define(LOG_RETENTION_TIME_MILLIS_PROP, LONG, null, HIGH, LOG_RETENTION_TIME_MILLIS_DOC)
        .define(LOG_RETENTION_TIME_MINUTES_PROP, INT, null, HIGH, LOG_RETENTION_TIME_MINS_DOC)
        .define(LOG_RETENTION_TIME_HOURS_PROP, INT, (int) TimeUnit.MILLISECONDS.toHours(LogConfig.DEFAULT_RETENTION_MS), HIGH, LOG_RETENTION_TIME_HOURS_DOC)

        .define(LOG_RETENTION_BYTES_PROP, LONG, LogConfig.DEFAULT_RETENTION_BYTES, HIGH, LOG_RETENTION_BYTES_DOC)
        .define(LOG_CLEANUP_INTERVAL_MS_PROP, LONG, Defaults.LOG_CLEANUP_INTERVAL_MS, atLeast(1), MEDIUM, LOG_CLEANUP_INTERVAL_MS_DOC)
        .define(LOG_CLEANUP_POLICY_PROP, LIST, LogConfig.DEFAULT_CLEANUP_POLICY, ConfigDef.ValidList.in(TopicConfig.CLEANUP_POLICY_COMPACT, TopicConfig.CLEANUP_POLICY_DELETE), MEDIUM, LOG_CLEANUP_POLICY_DOC)
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
        .define(LOG_INDEX_SIZE_MAX_BYTES_PROP, INT, LogConfig.DEFAULT_SEGMENT_INDEX_BYTES, atLeast(4), MEDIUM, LOG_INDEX_SIZE_MAX_BYTES_DOC)
        .define(LOG_INDEX_INTERVAL_BYTES_PROP, INT, LogConfig.DEFAULT_INDEX_INTERVAL_BYTES, atLeast(0), MEDIUM, LOG_INDEX_INTERVAL_BYTES_DOC)
        .define(LOG_FLUSH_INTERVAL_MESSAGES_PROP, LONG, LogConfig.DEFAULT_FLUSH_MESSAGES_INTERVAL, atLeast(1), HIGH, LOG_FLUSH_INTERVAL_MESSAGES_DOC)
        .define(LOG_DELETE_DELAY_MS_PROP, LONG, LogConfig.DEFAULT_FILE_DELETE_DELAY_MS, atLeast(0), HIGH, LOG_DELETE_DELAY_MS_DOC)
        .define(LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP, LONG, LogConfig.DEFAULT_FLUSH_MS, HIGH, LOG_FLUSH_SCHEDULER_INTERVAL_MS_DOC)
        .define(LOG_FLUSH_INTERVAL_MS_PROP, LONG, null, HIGH, LOG_FLUSH_INTERVAL_MS_DOC)
        .define(LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_PROP, INT, Defaults.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS, atLeast(0), HIGH, LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_DOC)
        .define(LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_PROP, INT, Defaults.LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS, atLeast(0), HIGH, LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_DOC)
        .define(LOG_PRE_ALLOCATE_PROP, BOOLEAN, LogConfig.DEFAULT_PREALLOCATE, MEDIUM, LOG_PRE_ALLOCATE_ENABLE_DOC)
        .define(NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP, INT, Defaults.NUM_RECOVERY_THREADS_PER_DATA_DIR, atLeast(1), HIGH, NUM_RECOVERY_THREADS_PER_DATA_DIR_DOC)
        .define(AUTO_CREATE_TOPICS_ENABLE_PROP, BOOLEAN, Defaults.AUTO_CREATE_TOPICS_ENABLE, HIGH, AUTO_CREATE_TOPICS_ENABLE_DOC)
        .define(MIN_IN_SYNC_REPLICAS_PROP, INT, LogConfig.DEFAULT_MIN_IN_SYNC_REPLICAS, atLeast(1), HIGH, MIN_IN_SYNC_REPLICAS_DOC)
        .define(LOG_MESSAGE_FORMAT_VERSION_PROP, STRING, LogConfig.DEFAULT_MESSAGE_FORMAT_VERSION, new MetadataVersionValidator(), MEDIUM, LOG_MESSAGE_FORMAT_VERSION_DOC)
        .define(LOG_MESSAGE_TIMESTAMP_TYPE_PROP, STRING, LogConfig.DEFAULT_MESSAGE_TIMESTAMP_TYPE, ConfigDef.ValidString.in("CreateTime", "LogAppendTime"), MEDIUM, LOG_MESSAGE_TIMESTAMP_TYPE_DOC)
        .define(LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_PROP, LONG, LogConfig.DEFAULT_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS, atLeast(0), MEDIUM, LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC)
        .define(LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_PROP, LONG, LogConfig.DEFAULT_MESSAGE_TIMESTAMP_BEFORE_MAX_MS, atLeast(0), MEDIUM, LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DOC)
        .define(LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_PROP, LONG, LogConfig.DEFAULT_MESSAGE_TIMESTAMP_AFTER_MAX_MS, atLeast(0), MEDIUM, LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_DOC)
        .define(CREATE_TOPIC_POLICY_CLASS_NAME_PROP, CLASS, null, LOW, CREATE_TOPIC_POLICY_CLASS_NAME_DOC)
        .define(ALTER_CONFIG_POLICY_CLASS_NAME_PROP, CLASS, null, LOW, ALTER_CONFIG_POLICY_CLASS_NAME_DOC)
        .define(LOG_MESSAGE_DOWN_CONVERSION_ENABLE_PROP, BOOLEAN, LogConfig.DEFAULT_MESSAGE_DOWNCONVERSION_ENABLE, LOW, LOG_MESSAGE_DOWN_CONVERSION_ENABLE_DOC)

        /** ********* Replication configuration ***********/
        .define(CONTROLLER_SOCKET_TIMEOUT_MS_PROP, INT, Defaults.CONTROLLER_SOCKET_TIMEOUT_MS, MEDIUM, CONTROLLER_SOCKET_TIMEOUT_MS_DOC)
        .define(DEFAULT_REPLICATION_FACTOR_PROP, INT, Defaults.REPLICATION_FACTOR, MEDIUM, DEFAULT_REPLICATION_FACTOR_DOC)
        .define(REPLICA_LAG_TIME_MAX_MS_PROP, LONG, Defaults.REPLICA_LAG_TIME_MAX_MS, HIGH, REPLICA_LAG_TIME_MAX_MS_DOC)
        .define(REPLICA_SOCKET_TIMEOUT_MS_PROP, INT, Defaults.REPLICA_SOCKET_TIMEOUT_MS, HIGH, REPLICA_SOCKET_TIMEOUT_MS_DOC)
        .define(REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_PROP, INT, Defaults.REPLICA_SOCKET_RECEIVE_BUFFER_BYTES, HIGH, REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_DOC)
        .define(REPLICA_FETCH_MAX_BYTES_PROP, INT, Defaults.REPLICA_FETCH_MAX_BYTES, atLeast(0), MEDIUM, REPLICA_FETCH_MAX_BYTES_DOC)
        .define(REPLICA_FETCH_WAIT_MAX_MS_PROP, INT, Defaults.REPLICA_FETCH_WAIT_MAX_MS, HIGH, REPLICA_FETCH_WAIT_MAX_MS_DOC)
        .define(REPLICA_FETCH_BACKOFF_MS_PROP, INT, Defaults.REPLICA_FETCH_BACKOFF_MS, atLeast(0), MEDIUM, REPLICA_FETCH_BACKOFF_MS_DOC)
        .define(REPLICA_FETCH_MIN_BYTES_PROP, INT, Defaults.REPLICA_FETCH_MIN_BYTES, HIGH, REPLICA_FETCH_MIN_BYTES_DOC)
        .define(REPLICA_FETCH_RESPONSE_MAX_BYTES_PROP, INT, Defaults.REPLICA_FETCH_RESPONSE_MAX_BYTES, atLeast(0), MEDIUM, REPLICA_FETCH_RESPONSE_MAX_BYTES_DOC)
        .define(NUM_REPLICA_FETCHERS_PROP, INT, Defaults.NUM_REPLICA_FETCHERS, HIGH, NUM_REPLICA_FETCHERS_DOC)
        .define(REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_PROP, LONG, Defaults.REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS, HIGH, REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_DOC)
        .define(FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP, INT, Defaults.FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS, MEDIUM, FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC)
        .define(PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP, INT, Defaults.PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS, MEDIUM, PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC)
        .define(DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP, INT, Defaults.DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS, MEDIUM, DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC)
        .define(AUTO_LEADER_REBALANCE_ENABLE_PROP, BOOLEAN, Defaults.AUTO_LEADER_REBALANCE_ENABLE, HIGH, AUTO_LEADER_REBALANCE_ENABLE_DOC)
        .define(LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_PROP, INT, Defaults.LEADER_IMBALANCE_PER_BROKER_PERCENTAGE, HIGH, LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_DOC)
        .define(LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_PROP, LONG, Defaults.LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS, atLeast(1), HIGH, LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_DOC)
        .define(UNCLEAN_LEADER_ELECTION_ENABLE_PROP, BOOLEAN, LogConfig.DEFAULT_UNCLEAN_LEADER_ELECTION_ENABLE, HIGH, UNCLEAN_LEADER_ELECTION_ENABLE_DOC)
        .define(INTER_BROKER_SECURITY_PROTOCOL_PROP, STRING, Defaults.INTER_BROKER_SECURITY_PROTOCOL, ConfigDef.ValidString.in(Utils.enumOptions(SecurityProtocol.class)), MEDIUM, INTER_BROKER_SECURITY_PROTOCOL_DOC)
        .define(INTER_BROKER_PROTOCOL_VERSION_PROP, STRING, Defaults.INTER_BROKER_PROTOCOL_VERSION, new MetadataVersionValidator(), MEDIUM, INTER_BROKER_PROTOCOL_VERSION_DOC)
        .define(INTER_BROKER_LISTENER_NAME_PROP, STRING, null, MEDIUM, INTER_BROKER_LISTENER_NAME_DOC)
        .define(REPLICA_SELECTOR_CLASS_PROP, STRING, null, MEDIUM, REPLICA_SELECTOR_CLASS_DOC)

        /** ********* Controlled shutdown configuration ***********/
        .define(CONTROLLED_SHUTDOWN_MAX_RETRIES_PROP, INT, Defaults.CONTROLLED_SHUTDOWN_MAX_RETRIES, MEDIUM, CONTROLLED_SHUTDOWN_MAX_RETRIES_DOC)
        .define(CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_PROP, LONG, Defaults.CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS, MEDIUM, CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_DOC)
        .define(CONTROLLED_SHUTDOWN_ENABLE_PROP, BOOLEAN, Defaults.CONTROLLED_SHUTDOWN_ENABLE, MEDIUM, CONTROLLED_SHUTDOWN_ENABLE_DOC)

        /** ********* Group coordinator configuration ***********/
        .define(GROUP_MIN_SESSION_TIMEOUT_MS_PROP, INT, Defaults.GROUP_MIN_SESSION_TIMEOUT_MS, MEDIUM, GROUP_MIN_SESSION_TIMEOUT_MS_DOC)
        .define(GROUP_MAX_SESSION_TIMEOUT_MS_PROP, INT, Defaults.GROUP_MAX_SESSION_TIMEOUT_MS, MEDIUM, GROUP_MAX_SESSION_TIMEOUT_MS_DOC)
        .define(GROUP_INITIAL_REBALANCE_DELAY_MS_PROP, INT, Defaults.GROUP_INITIAL_REBALANCE_DELAY_MS, MEDIUM, GROUP_INITIAL_REBALANCE_DELAY_MS_DOC)
        .define(GROUP_MAX_SIZE_PROP, INT, Defaults.GROUP_MAX_SIZE, atLeast(1), MEDIUM, GROUP_MAX_SIZE_DOC)

        /** New group coordinator configs */
        .define(GROUP_COORDINATOR_REBALANCE_PROTOCOLS_PROP, LIST, Defaults.GROUP_COORDINATOR_REBALANCE_PROTOCOLS, ConfigDef.ValidList.in(Utils.enumOptions(GroupType.class)), MEDIUM, GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DOC)
        .define(GROUP_COORDINATOR_NUM_THREADS_PROP, INT, Defaults.GROUP_COORDINATOR_NUM_THREADS, atLeast(1), MEDIUM, GROUP_COORDINATOR_NUM_THREADS_DOC)
        // Internal configuration used by integration and system tests.
        .defineInternal(NEW_GROUP_COORDINATOR_ENABLE_PROP, BOOLEAN, Defaults.NEW_GROUP_COORDINATOR_ENABLE, null, MEDIUM, NEW_GROUP_COORDINATOR_ENABLE_DOC)

        /** Consumer groups configs */
        .define(CONSUMER_GROUP_SESSION_TIMEOUT_MS_PROP, INT, Defaults.CONSUMER_GROUP_SESSION_TIMEOUT_MS, atLeast(1), MEDIUM, CONSUMER_GROUP_SESSION_TIMEOUT_MS_DOC)
        .define(CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_PROP, INT, Defaults.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS, atLeast(1), MEDIUM, CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DOC)
        .define(CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_PROP, INT, Defaults.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS, atLeast(1), MEDIUM, CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DOC)
        .define(CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_PROP, INT, Defaults.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS, atLeast(1), MEDIUM, CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DOC)
        .define(CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_PROP, INT, Defaults.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS, atLeast(1), MEDIUM, CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DOC)
        .define(CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_PROP, INT, Defaults.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS, atLeast(1), MEDIUM, CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DOC)
        .define(CONSUMER_GROUP_MAX_SIZE_PROP, INT, Defaults.CONSUMER_GROUP_MAX_SIZE, atLeast(1), MEDIUM, CONSUMER_GROUP_MAX_SIZE_DOC)
        .define(CONSUMER_GROUP_ASSIGNORS_PROP, LIST, Defaults.CONSUMER_GROUP_ASSIGNORS, null, MEDIUM, CONSUMER_GROUP_ASSIGNORS_DOC)

        /** ********* Offset management configuration ***********/
        .define(OFFSET_METADATA_MAX_SIZE_PROP, INT, Defaults.OFFSET_METADATA_MAX_SIZE, HIGH, OFFSET_METADATA_MAX_SIZE_DOC)
        .define(OFFSETS_LOAD_BUFFER_SIZE_PROP, INT, Defaults.OFFSETS_LOAD_BUFFER_SIZE, atLeast(1), HIGH, OFFSETS_LOAD_BUFFER_SIZE_DOC)
        .define(OFFSETS_TOPIC_REPLICATION_FACTOR_PROP, SHORT, Defaults.OFFSETS_TOPIC_REPLICATION_FACTOR, atLeast(1), HIGH, OFFSETS_TOPIC_REPLICATION_FACTOR_DOC)
        .define(OFFSETS_TOPIC_PARTITIONS_PROP, INT, Defaults.OFFSETS_TOPIC_PARTITIONS, atLeast(1), HIGH, OFFSETS_TOPIC_PARTITIONS_DOC)
        .define(OFFSETS_TOPIC_SEGMENT_BYTES_PROP, INT, Defaults.OFFSETS_TOPIC_SEGMENT_BYTES, atLeast(1), HIGH, OFFSETS_TOPIC_SEGMENT_BYTES_DOC)
        .define(OFFSETS_TOPIC_COMPRESSION_CODEC_PROP, INT, Defaults.OFFSETS_TOPIC_COMPRESSION_CODEC, HIGH, OFFSETS_TOPIC_COMPRESSION_CODEC_DOC)
        .define(OFFSETS_RETENTION_MINUTES_PROP, INT, Defaults.OFFSETS_RETENTION_MINUTES, atLeast(1), HIGH, OFFSETS_RETENTION_MINUTES_DOC)
        .define(OFFSETS_RETENTION_CHECK_INTERVAL_MS_PROP, LONG, Defaults.OFFSETS_RETENTION_CHECK_INTERVAL_MS, atLeast(1), HIGH, OFFSETS_RETENTION_CHECK_INTERVAL_MS_DOC)
        .define(OFFSET_COMMIT_TIMEOUT_MS_PROP, INT, Defaults.OFFSET_COMMIT_TIMEOUT_MS, atLeast(1), HIGH, OFFSET_COMMIT_TIMEOUT_MS_DOC)
        .define(OFFSET_COMMIT_REQUIRED_ACKS_PROP, SHORT, Defaults.OFFSET_COMMIT_REQUIRED_ACKS, HIGH, OFFSET_COMMIT_REQUIRED_ACKS_DOC)
        .define(DELETE_TOPIC_ENABLE_PROP, BOOLEAN, Defaults.DELETE_TOPIC_ENABLE, HIGH, DELETE_TOPIC_ENABLE_DOC)
        .define(COMPRESSION_TYPE_PROP, STRING, LogConfig.DEFAULT_COMPRESSION_TYPE, ConfigDef.ValidString.in(BrokerCompressionType.names().toArray(new String[0])), HIGH, COMPRESSION_TYPE_DOC)

        /** ********* Transaction management configuration ***********/
        .define(TRANSACTIONAL_ID_EXPIRATION_MS_PROP, INT, Defaults.TRANSACTIONAL_ID_EXPIRATION_MS, atLeast(1), HIGH, TRANSACTIONAL_ID_EXPIRATION_MS_DOC)
        .define(TRANSACTIONS_MAX_TIMEOUT_MS_PROP, INT, Defaults.TRANSACTIONS_MAX_TIMEOUT_MS, atLeast(1), HIGH, TRANSACTIONS_MAX_TIMEOUT_MS_DOC)
        .define(TRANSACTIONS_TOPIC_MIN_ISR_PROP, INT, Defaults.TRANSACTIONS_TOPIC_MIN_ISR, atLeast(1), HIGH, TRANSACTIONS_TOPIC_MIN_ISR_DOC)
        .define(TRANSACTIONS_LOAD_BUFFER_SIZE_PROP, INT, Defaults.TRANSACTIONS_LOAD_BUFFER_SIZE, atLeast(1), HIGH, TRANSACTIONS_LOAD_BUFFER_SIZE_DOC)
        .define(TRANSACTIONS_TOPIC_REPLICATION_FACTOR_PROP, SHORT, Defaults.TRANSACTIONS_TOPIC_REPLICATION_FACTOR, atLeast(1), HIGH, TRANSACTIONS_TOPIC_REPLICATION_FACTOR_DOC)
        .define(TRANSACTIONS_TOPIC_PARTITIONS_PROP, INT, Defaults.TRANSACTIONS_TOPIC_PARTITIONS, atLeast(1), HIGH, TRANSACTIONS_TOPIC_PARTITIONS_DOC)
        .define(TRANSACTIONS_TOPIC_SEGMENT_BYTES_PROP, INT, Defaults.TRANSACTIONS_TOPIC_SEGMENT_BYTES, atLeast(1), HIGH, TRANSACTIONS_TOPIC_SEGMENT_BYTES_DOC)
        .define(TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_PROP, INT, Defaults.TRANSACTIONS_ABORT_TIMED_OUT_CLEANUP_INTERVAL_MS, atLeast(1), LOW, TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTIONS_INTERVAL_MS_DOC)
        .define(TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_PROP, INT, Defaults.TRANSACTIONS_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS, atLeast(1), LOW, TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONS_INTERVAL_MS_DOC)

        .define(TRANSACTION_PARTITION_VERIFICATION_ENABLE_PROP, BOOLEAN, Defaults.TRANSACTION_PARTITION_VERIFICATION_ENABLE, LOW, TRANSACTION_PARTITION_VERIFICATION_ENABLE_DOC)

        .define(PRODUCER_ID_EXPIRATION_MS_PROP, INT, Defaults.PRODUCER_ID_EXPIRATION_MS, atLeast(1), LOW, PRODUCER_ID_EXPIRATION_MS_DOC)
        // Configuration for testing only as default value should be sufficient for typical usage
        .defineInternal(PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_PROP, INT, Defaults.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS, atLeast(1), LOW, PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DOC)

        /** ********* Fetch Configuration **************/
        .define(MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_PROP, INT, Defaults.MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS, atLeast(0), MEDIUM, MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_DOC)
        .define(FETCH_MAX_BYTES_PROP, INT, Defaults.FETCH_MAX_BYTES, atLeast(1024), MEDIUM, FETCH_MAX_BYTES_DOC)

        /** ********* Request Limit Configuration ***********/
        .define(MAX_REQUEST_PARTITION_SIZE_LIMIT_PROP, INT, Defaults.MAX_REQUEST_PARTITION_SIZE_LIMIT, atLeast(1), MEDIUM, MAX_REQUEST_PARTITION_SIZE_LIMIT_DOC)

        /** ********* Kafka Metrics Configuration ***********/
        .define(METRIC_NUM_SAMPLES_PROP, INT, Defaults.METRIC_NUM_SAMPLES, atLeast(1), LOW, METRIC_NUM_SAMPLES_DOC)
        .define(METRIC_SAMPLE_WINDOW_MS_PROP, LONG, Defaults.METRIC_SAMPLE_WINDOW_MS, atLeast(1), LOW, METRIC_SAMPLE_WINDOW_MS_DOC)
        .define(METRIC_REPORTER_CLASSES_PROP, LIST, Defaults.METRIC_REPORTER_CLASSES, LOW, METRIC_REPORTER_CLASSES_DOC)
        .define(METRIC_RECORDING_LEVEL_PROP, STRING, Defaults.METRIC_RECORDING_LEVEL, LOW, METRICS_RECORDING_LEVEL_DOC)
        .define(AUTO_INCLUDE_JMX_REPORTER_PROP, BOOLEAN, Defaults.AUTO_INCLUDE_JMX_REPORTER, LOW, AUTO_INCLUDE_JMX_REPORTER_DOC)

        /** ********* Kafka Yammer Metrics Reporter Configuration for docs ***********/
        .define(KAFKA_METRICS_REPORTER_CLASSES_PROP, LIST, Defaults.KAFKA_METRIC_REPORTER_CLASSES, LOW, KAFKA_METRICS_REPORTER_CLASSES_DOC)
        .define(KAFKA_METRICS_POLLING_INTERVAL_SECONDS_PROP, INT, Defaults.KAFKA_METRICS_POLLING_INTERVAL_SECONDS, atLeast(1), LOW, KAFKA_METRICS_POLLING_INTERVAL_SECONDS_DOC)

        /** ********* Kafka Client Telemetry Metrics Configuration ***********/
        .define(CLIENT_TELEMETRY_MAX_BYTES_PROP, INT, Defaults.CLIENT_TELEMETRY_MAX_BYTES, atLeast(1), LOW, CLIENT_TELEMETRY_MAX_BYTES_DOC)

        /** ********* Quota configuration ***********/
        .define(NUM_QUOTA_SAMPLES_PROP, INT, Defaults.NUM_QUOTA_SAMPLES, atLeast(1), LOW, NUM_QUOTA_SAMPLES_DOC)
        .define(NUM_REPLICATION_QUOTA_SAMPLES_PROP, INT, Defaults.NUM_REPLICATION_QUOTA_SAMPLES, atLeast(1), LOW, NUM_REPLICATION_QUOTA_SAMPLES_DOC)
        .define(NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_PROP, INT, Defaults.NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES, atLeast(1), LOW, NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_DOC)
        .define(NUM_CONTROLLER_QUOTA_SAMPLES_PROP, INT, Defaults.NUM_CONTROLLER_QUOTA_SAMPLES, atLeast(1), LOW, NUM_CONTROLLER_QUOTA_SAMPLES_DOC)
        .define(QUOTA_WINDOW_SIZE_SECONDS_PROP, INT, Defaults.QUOTA_WINDOW_SIZE_SECONDS, atLeast(1), LOW, QUOTA_WINDOW_SIZE_SECONDS_DOC)
        .define(REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_PROP, INT, Defaults.REPLICATION_QUOTA_WINDOW_SIZE_SECONDS, atLeast(1), LOW, REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_DOC)
        .define(ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_PROP, INT, Defaults.ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS, atLeast(1), LOW, ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_DOC)
        .define(CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_PROP, INT, Defaults.CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS, atLeast(1), LOW, CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_DOC)
        .define(CLIENT_QUOTA_CALLBACK_CLASS_PROP, CLASS, null, LOW, CLIENT_QUOTA_CALLBACK_CLASS_DOC)

        /** ********* General Security Configuration ****************/
        .define(CONNECTIONS_MAX_REAUTH_MS_PROP, LONG, Defaults.CONNECTIONS_MAX_REAUTH_MS, MEDIUM, CONNECTIONS_MAX_REAUTH_MS_DOC)
        .define(SASL_SERVER_MAX_RECEIVE_SIZE_PROP, INT, Defaults.SERVER_MAX_RECEIVE_SIZE, MEDIUM, SASL_SERVER_MAX_RECEIVE_SIZE_DOC)
        .define(SECURITY_PROVIDER_CLASS_PROP, STRING, null, LOW, SECURITY_PROVIDERS_DOC)

        /** ********* SSL Configuration ****************/
        .define(PRINCIPAL_BUILDER_CLASS_PROP, CLASS, Defaults.PRINCIPAL_BUILDER, MEDIUM, PRINCIPAL_BUILDER_CLASS_DOC)
        .define(SSL_PROTOCOL_PROP, STRING, Defaults.SSL_PROTOCOL, MEDIUM, SSL_PROTOCOL_DOC)
        .define(SSL_PROVIDER_PROP, STRING, null, MEDIUM, SSL_PROVIDER_DOC)
        .define(SSL_ENABLED_PROTOCOLS_PROP, LIST, Defaults.SSL_ENABLED_PROTOCOLS, MEDIUM, SSL_ENABLED_PROTOCOLS_DOC)
        .define(SSL_KEYSTORE_TYPE_PROP, STRING, Defaults.SSL_KEYSTORE_TYPE, MEDIUM, SSL_KEYSTORE_TYPE_DOC)
        .define(SSL_KEYSTORE_LOCATION_PROP, STRING, null, MEDIUM, SSL_KEYSTORE_LOCATION_DOC)
        .define(SSL_KEYSTORE_PASSWORD_PROP, PASSWORD, null, MEDIUM, SSL_KEYSTORE_PASSWORD_DOC)
        .define(SSL_KEY_PASSWORD_PROP, PASSWORD, null, MEDIUM, SSL_KEY_PASSWORD_DOC)
        .define(SSL_KEYSTORE_KEY_PROP, PASSWORD, null, MEDIUM, SSL_KEYSTORE_KEY_DOC)
        .define(SSL_KEYSTORE_CERTIFICATE_CHAIN_PROP, PASSWORD, null, MEDIUM, SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC)
        .define(SSL_TRUSTSTORE_TYPE_PROP, STRING, Defaults.SSL_TRUSTSTORE_TYPE, MEDIUM, SSL_TRUSTSTORE_TYPE_DOC)
        .define(SSL_TRUSTSTORE_LOCATION_PROP, STRING, null, MEDIUM, SSL_TRUSTSTORE_LOCATION_DOC)
        .define(SSL_TRUSTSTORE_PASSWORD_PROP, PASSWORD, null, MEDIUM, SSL_TRUSTSTORE_PASSWORD_DOC)
        .define(SSL_TRUSTSTORE_CERTIFICATES_PROP, PASSWORD, null, MEDIUM, SSL_TRUSTSTORE_CERTIFICATES_DOC)
        .define(SSL_KEY_MANAGER_ALGORITHM_PROP, STRING, Defaults.SSL_KEY_MANAGER_ALGORITHM, MEDIUM, SSL_KEYMANAGER_ALGORITHM_DOC)
        .define(SSL_TRUST_MANAGER_ALGORITHM_PROP, STRING, Defaults.SSL_TRUST_MANAGER_ALGORITHM, MEDIUM, SSL_TRUSTMANAGER_ALGORITHM_DOC)
        .define(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP, STRING, Defaults.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, LOW, SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
        .define(SSL_SECURE_RANDOM_IMPLEMENTATION_PROP, STRING, null, LOW, SSL_SECURE_RANDOM_IMPLEMENTATION_DOC)
        .define(SSL_CLIENT_AUTH_PROP, STRING, Defaults.SSL_CLIENT_AUTHENTICATION, ConfigDef.ValidString.in(Defaults.SSL_CLIENT_AUTHENTICATION_VALID_VALUES), MEDIUM, SSL_CLIENT_AUTH_DOC)
        .define(SSL_CIPHER_SUITES_PROP, LIST, Collections.emptyList(), MEDIUM, SSL_CIPHER_SUITES_DOC)
        .define(SSL_PRINCIPAL_MAPPING_RULES_PROP, STRING, Defaults.SSL_PRINCIPAL_MAPPING_RULES, LOW, SSL_PRINCIPAL_MAPPING_RULES_DOC)
        .define(SSL_ENGINE_FACTORY_CLASS_PROP, CLASS, null, LOW, SSL_ENGINE_FACTORY_CLASS_DOC)
        .define(SSL_ALLOW_DN_CHANGES_PROP, BOOLEAN, BrokerSecurityConfigs.DEFAULT_SSL_ALLOW_DN_CHANGES_VALUE, LOW, SSL_ALLOW_DN_CHANGES_DOC)
        .define(SSL_ALLOW_SAN_CHANGES_PROP, BOOLEAN, BrokerSecurityConfigs.DEFAULT_SSL_ALLOW_SAN_CHANGES_VALUE, LOW, SSL_ALLOW_SAN_CHANGES_DOC)

        /** ********* Sasl Configuration ****************/
        .define(SASL_MECHANISM_INTER_BROKER_PROTOCOL_PROP, STRING, Defaults.SASL_MECHANISM_INTER_BROKER_PROTOCOL, MEDIUM, SASL_MECHANISM_INTER_BROKER_PROTOCOL_DOC)
        .define(SASL_JAAS_CONFIG_PROP, PASSWORD, null, MEDIUM, SASL_JAAS_CONFIG_DOC)
        .define(SASL_ENABLED_MECHANISMS_PROP, LIST, Defaults.SASL_ENABLED_MECHANISMS, MEDIUM, SASL_ENABLED_MECHANISMS_DOC)
        .define(SASL_SERVER_CALLBACK_HANDLER_CLASS_PROP, CLASS, null, MEDIUM, SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC)
        .define(SASL_CLIENT_CALLBACK_HANDLER_CLASS_PROP, CLASS, null, MEDIUM, SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC)
        .define(SASL_LOGIN_CLASS_PROP, CLASS, null, MEDIUM, SASL_LOGIN_CLASS_DOC)
        .define(SASL_LOGIN_CALLBACK_HANDLER_CLASS_PROP, CLASS, null, MEDIUM, SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC)
        .define(SASL_KERBEROS_SERVICE_NAME_PROP, STRING, null, MEDIUM, SASL_KERBEROS_SERVICE_NAME_DOC)
        .define(SASL_KERBEROS_KINIT_CMD_PROP, STRING, Defaults.SASL_KERBEROS_KINIT_CMD, MEDIUM, SASL_KERBEROS_KINIT_CMD_DOC)
        .define(SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_PROP, DOUBLE, Defaults.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, MEDIUM, SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC)
        .define(SASL_KERBEROS_TICKET_RENEW_JITTER_PROP, DOUBLE, Defaults.SASL_KERBEROS_TICKET_RENEW_JITTER, MEDIUM, SASL_KERBEROS_TICKET_RENEW_JITTER_DOC)
        .define(SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_PROP, LONG, Defaults.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN, MEDIUM, SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC)
        .define(SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_PROP, LIST, Defaults.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES, MEDIUM, SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC)
        .define(SASL_LOGIN_REFRESH_WINDOW_FACTOR_PROP, DOUBLE, Defaults.SASL_LOGIN_REFRESH_WINDOW_FACTOR, MEDIUM, SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC)
        .define(SASL_LOGIN_REFRESH_WINDOW_JITTER_PROP, DOUBLE, Defaults.SASL_LOGIN_REFRESH_WINDOW_JITTER, MEDIUM, SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC)
        .define(SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_PROP, SHORT, Defaults.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, MEDIUM, SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC)
        .define(SASL_LOGIN_REFRESH_BUFFER_SECONDS_PROP, SHORT, Defaults.SASL_LOGIN_REFRESH_BUFFER_SECONDS, MEDIUM, SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC)
        .define(SASL_LOGIN_CONNECT_TIMEOUT_MS_PROP, INT, null, LOW, SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC)
        .define(SASL_LOGIN_READ_TIMEOUT_MS_PROP, INT, null, LOW, SASL_LOGIN_READ_TIMEOUT_MS_DOC)
        .define(SASL_LOGIN_RETRY_BACKOFF_MAX_MS_PROP, LONG, Defaults.SASL_LOGIN_RETRY_BACKOFF_MAX_MS, LOW, SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC)
        .define(SASL_LOGIN_RETRY_BACKOFF_MS_PROP, LONG, Defaults.SASL_LOGIN_RETRY_BACKOFF_MS, LOW, SASL_LOGIN_RETRY_BACKOFF_MS_DOC)
        .define(SASL_O_AUTH_BEARER_SCOPE_CLAIM_NAME_PROP, STRING, Defaults.SASL_OAUTH_BEARER_SCOPE_CLAIM_NAME, LOW, SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC)
        .define(SASL_O_AUTH_BEARER_SUB_CLAIM_NAME_PROP, STRING, Defaults.SASL_OAUTH_BEARER_SUB_CLAIM_NAME, LOW, SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC)
        .define(SASL_O_AUTH_BEARER_TOKEN_ENDPOINT_URL_PROP, STRING, null, MEDIUM, SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC)
        .define(SASL_O_AUTH_BEARER_JWKS_ENDPOINT_URL_PROP, STRING, null, MEDIUM, SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC)
        .define(SASL_O_AUTH_BEARER_JWKS_ENDPOINT_REFRESH_MS_PROP, LONG, Defaults.SASL_OAUTH_BEARER_JWKS_ENDPOINT_REFRESH_MS, LOW, SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC)
        .define(SASL_O_AUTH_BEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_PROP, LONG, Defaults.SASL_OAUTH_BEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS, LOW, SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC)
        .define(SASL_O_AUTH_BEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_PROP, LONG, Defaults.SASL_OAUTH_BEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS, LOW, SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC)
        .define(SASL_O_AUTH_BEARER_CLOCK_SKEW_SECONDS_PROP, INT, Defaults.SASL_OAUTH_BEARER_CLOCK_SKEW_SECONDS, LOW, SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC)
        .define(SASL_O_AUTH_BEARER_EXPECTED_AUDIENCE_PROP, LIST, null, LOW, SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC)
        .define(SASL_O_AUTH_BEARER_EXPECTED_ISSUER_PROP, STRING, null, LOW, SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC)

        /** ********* Delegation Token Configuration ****************/
        .define(DELEGATION_TOKEN_SECRET_KEY_ALIAS_PROP, PASSWORD, null, MEDIUM, DELEGATION_TOKEN_SECRET_KEY_ALIAS_DOC)
        .define(DELEGATION_TOKEN_SECRET_KEY_PROP, PASSWORD, null, MEDIUM, DELEGATION_TOKEN_SECRET_KEY_DOC)
        .define(DELEGATION_TOKEN_MAX_LIFE_TIME_PROP, LONG, Defaults.DELEGATION_TOKEN_MAX_LIFE_TIME_MS, atLeast(1), MEDIUM, DELEGATION_TOKEN_MAX_LIFE_TIME_DOC)
        .define(DELEGATION_TOKEN_EXPIRY_TIME_MS_PROP, LONG, Defaults.DELEGATION_TOKEN_EXPIRY_TIME_MS, atLeast(1), MEDIUM, DELEGATION_TOKEN_EXPIRY_TIME_MS_DOC)
        .define(DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_PROP, LONG, Defaults.DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS, atLeast(1), LOW, DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_DOC)

        /** ********* Password encryption configuration for dynamic configs *********/
        .define(PASSWORD_ENCODER_SECRET_PROP, PASSWORD, null, MEDIUM, PASSWORD_ENCODER_SECRET_DOC)
        .define(PASSWORD_ENCODER_OLD_SECRET_PROP, PASSWORD, null, MEDIUM, PASSWORD_ENCODER_OLD_SECRET_DOC)
        .define(PASSWORD_ENCODER_KEY_FACTORY_ALGORITHM_PROP, STRING, null, LOW, PASSWORD_ENCODER_KEY_FACTORY_ALGORITHM_DOC)
        .define(PASSWORD_ENCODER_CIPHER_ALGORITHM_PROP, STRING, Defaults.PASSWORD_ENCODER_CIPHER_ALGORITHM, LOW, PASSWORD_ENCODER_CIPHER_ALGORITHM_DOC)
        .define(PASSWORD_ENCODER_KEY_LENGTH_PROP, INT, Defaults.PASSWORD_ENCODER_KEY_LENGTH, atLeast(8), LOW, PASSWORD_ENCODER_KEY_LENGTH_DOC)
        .define(PASSWORD_ENCODER_ITERATIONS_PROP, INT, Defaults.PASSWORD_ENCODER_ITERATIONS, atLeast(1024), LOW, PASSWORD_ENCODER_ITERATIONS_DOC)

        /** ********* Raft Quorum Configuration *********/
        .define(RaftConfig.QUORUM_VOTERS_CONFIG, LIST, Defaults.QUORUM_VOTERS, new RaftConfig.ControllerQuorumVotersValidator(), HIGH, RaftConfig.QUORUM_VOTERS_DOC)
        .define(RaftConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG, INT, Defaults.QUORUM_ELECTION_TIMEOUT_MS, null, HIGH, RaftConfig.QUORUM_ELECTION_TIMEOUT_MS_DOC)
        .define(RaftConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG, INT, Defaults.QUORUM_FETCH_TIMEOUT_MS, null, HIGH, RaftConfig.QUORUM_FETCH_TIMEOUT_MS_DOC)
        .define(RaftConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG, INT, Defaults.QUORUM_ELECTION_BACKOFF_MS, null, HIGH, RaftConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_DOC)
        .define(RaftConfig.QUORUM_LINGER_MS_CONFIG, INT, Defaults.QUORUM_LINGER_MS, null, MEDIUM, RaftConfig.QUORUM_LINGER_MS_DOC)
        .define(RaftConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG, INT, Defaults.QUORUM_REQUEST_TIMEOUT_MS, null, MEDIUM, RaftConfig.QUORUM_REQUEST_TIMEOUT_MS_DOC)
        .define(RaftConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG, INT, Defaults.QUORUM_RETRY_BACKOFF_MS, null, LOW, RaftConfig.QUORUM_RETRY_BACKOFF_MS_DOC)
        /** Internal Configurations **/
        // This indicates whether unreleased APIs should be advertised by this node.
        .defineInternal(UNSTABLE_API_VERSIONS_ENABLE_PROP, BOOLEAN, false, HIGH)
        // This indicates whether unreleased MetadataVersions should be enabled on this node.
        .defineInternal(UNSTABLE_METADATA_VERSIONS_ENABLE_PROP, BOOLEAN, false, HIGH);


    public static List<String> configNames() {
        return new ArrayList<>(CONFIG_DEF.names());
    }

    public static Map<String, Object> defaultValues() {
        return CONFIG_DEF.defaultValues();
    }

    public static Map<String, ConfigDef.ConfigKey> configKeys() {
        return CONFIG_DEF.configKeys();
    }

    static {
        /** ********* Remote Log Management Configuration *********/
        RemoteLogManagerConfig.CONFIG_DEF.configKeys().values().forEach(key -> CONFIG_DEF.define(key));
    }

    public static Optional<String> zooKeeperClientProperty(ZKClientConfig clientConfig, String kafkaPropName) {
        return Optional.ofNullable(clientConfig.getProperty(ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(kafkaPropName)));
    }

    public static void setZooKeeperClientProperty(ZKClientConfig clientConfig, String kafkaPropName, Object kafkaPropValue) {
        clientConfig.setProperty(ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(kafkaPropName),
                (kafkaPropName.equals(ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP) && kafkaPropValue.toString().equalsIgnoreCase("HTTPS"))
                        ? String.valueOf(kafkaPropValue.toString().equalsIgnoreCase("HTTPS"))
                        : (kafkaPropName.equals(ZK_SSL_ENABLED_PROTOCOLS_PROP) || kafkaPropName.equals(ZK_SSL_CIPHER_SUITES_PROP))
                        ? ((List<?>) kafkaPropValue).stream().map(Object::toString).collect(Collectors.joining(","))
                        : kafkaPropValue.toString());
    }

    // For ZooKeeper TLS client authentication to be enabled, the client must (at a minimum)
    // configure itself as using TLS with both a client connection socket and a key store location explicitly set.
    public static boolean zkTlsClientAuthEnabled(ZKClientConfig zkClientConfig) {
        return zooKeeperClientProperty(zkClientConfig, ZK_SSL_CLIENT_ENABLE_PROP).filter(value -> value.equals("true")).isPresent() &&
                zooKeeperClientProperty(zkClientConfig, ZK_CLIENT_CNXN_SOCKET_PROP).isPresent() &&
                zooKeeperClientProperty(zkClientConfig, ZK_SSL_KEYSTORE_LOCATION_PROP).isPresent();
    }

    public static Optional<ConfigDef.Type> configType(String configName) {
        Optional<ConfigDef.Type> configType = configTypeExact(configName);
        if (configType.isPresent()) {
            return configType;
        }

        Optional<ConfigDef.Type> typeFromTypeOf = typeOf(configName);
        if (typeFromTypeOf.isPresent()) {
            return typeFromTypeOf;
        }

        return brokerConfigSynonyms(configName, true)
                .stream()
                .findFirst()
                .flatMap(conf -> typeOf(conf));
    }

    private static Optional<ConfigDef.Type> configTypeExact(String exactName) {
        return typeOf(exactName)
                .map(Optional::of)
                .orElseGet(() -> {
                    Map<String, ConfigDef.ConfigKey> configKeys = BrokerDynamicConfigs.BROKER_CONFIG_DEF.configKeys();
                    return Optional.ofNullable(configKeys.get(exactName)).map(key -> key.type);
                });
    }

    private static Optional<ConfigDef.Type> typeOf(String name) {
        Map<String, ConfigDef.ConfigKey> configKeys = CONFIG_DEF.configKeys();
        return Optional.ofNullable(configKeys.get(name)).map(configKey -> configKey.type);
    }

    public static boolean maybeSensitive(Optional<ConfigDef.Type> configType) {
        // If we can't determine the config entry type, treat it as a sensitive config to be safe
        return !configType.isPresent() || configType.get().equals(ConfigDef.Type.PASSWORD);
    }

    public static String loggableValue(ConfigResource.Type resourceType, String name, String value) {
        boolean maybeSensitive;
        switch (resourceType) {
            case BROKER:
                maybeSensitive = maybeSensitive(configType(name));
                break;
            case TOPIC:
                maybeSensitive = maybeSensitive(LogConfig.configType(name));
                break;
            case BROKER_LOGGER:
                maybeSensitive = false;
                break;
            default:
                maybeSensitive = true;
        }

        return maybeSensitive ? Password.HIDDEN : value;
    }

    /**
     * Copy a configuration map, populating some keys that we want to treat as synonyms.
     */
    public static Map<Object, Object> populateSynonyms(Map<?, ?> input) {
        Map<Object, Object> output = new HashMap<>(input);
        Object brokerId = output.get(BROKER_ID_PROP);
        Object nodeId = output.get(NODE_ID_PROP);

        if (brokerId == null && nodeId != null) {
            output.put(BROKER_ID_PROP, nodeId);
        } else if (brokerId != null && nodeId == null) {
            output.put(NODE_ID_PROP, brokerId);
        }

        return output;
    }
}
