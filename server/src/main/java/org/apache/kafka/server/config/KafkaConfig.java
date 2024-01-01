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
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.raft.RaftConfig;
import org.apache.kafka.server.KafkaRaftServer;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.MetadataVersionValidator;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.utils.CoreUtils;
import org.apache.zookeeper.client.ZKClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
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
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;
import static org.apache.kafka.server.KafkaRaftServer.ProcessRole.BrokerRole;
import static org.apache.kafka.server.KafkaRaftServer.ProcessRole.ControllerRole;
import static org.apache.kafka.server.common.MetadataVersion.IBP_2_1_IV0;

final public class KafkaConfig extends AbstractConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);
    private final boolean doLog;
    private DynamicBrokerConfigBaseManager dynamicConfigOverride;
    private final DynamicConfigProvider dynamicConfigOverrideProvider;
    public final Map<?, ?> props;
    public KafkaConfig(boolean doLog, Map<?, ?> props, DynamicConfigProvider dynamicConfigOverrideProvider) {
        super(CONFIG_DEF, props);
        this.props = props;
        this.doLog = doLog;
        this.dynamicConfigOverrideProvider = dynamicConfigOverrideProvider;
        this.validateValues();
    }

    private static final String LOG_CONFIG_PREFIX = "log.";
    public static final String ZK_CONNECT_PROP = "zookeeper.connect";
    public static final String ZK_SESSION_TIMEOUT_MS_PROP = "zookeeper.session.timeout.ms";
    public static final String ZK_CONNECTION_TIMEOUT_MS_PROP = "zookeeper.connection.timeout.ms";
    public static final String ZK_ENABLE_SECURE_ACLS_PROP = "zookeeper.set.acl";
    public static final String ZK_MAX_IN_FLIGHT_REQUESTS_PROP = "zookeeper.max.in.flight.requests";
    public static final String ZK_SSL_CLIENT_ENABLE_PROP = "zookeeper.ssl.client.enable";
    public static final String ZK_CLIENT_CNXN_SOCKET_PROP = "zookeeper.clientCnxnSocket";
    public static final String ZK_SSL_KEY_STORE_LOCATION_PROP = "zookeeper.ssl.keystore.location";
    public static final String ZK_SSL_KEY_STORE_PASSWORD_PROP = "zookeeper.ssl.keystore.password";
    public static final String ZK_SSL_KEY_STORE_TYPE_PROP = "zookeeper.ssl.keystore.type";
    public static final String ZK_SSL_TRUST_STORE_LOCATION_PROP = "zookeeper.ssl.truststore.location";
    public static final String ZK_SSL_TRUST_STORE_PASSWORD_PROP = "zookeeper.ssl.truststore.password";
    public static final String ZK_SSL_TRUST_STORE_TYPE_PROP = "zookeeper.ssl.truststore.type";
    public static final String ZK_SSL_PROTOCOL_PROP = "zookeeper.ssl.protocol";
    public static final String ZK_SSL_ENABLED_PROTOCOLS_PROP = "zookeeper.ssl.enabled.protocols";
    public static final String ZK_SSL_CIPHER_SUITES_PROP = "zookeeper.ssl.cipher.suites";
    public static final String ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP = "zookeeper.ssl.endpoint.identification.algorithm";
    public static final String ZK_SSL_CRL_ENABLE_PROP = "zookeeper.ssl.crl.enable";
    public static final String ZK_SSL_OCSP_ENABLE_PROP = "zookeeper.ssl.ocsp.enable";

    // a map from the Kafka config to the corresponding ZooKeeper Java system property
    public static final Map<String, String> ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP = Utils.mkMap(
            Utils.mkEntry(ZK_SSL_CLIENT_ENABLE_PROP, ZKClientConfig.SECURE_CLIENT),
            Utils.mkEntry(ZK_CLIENT_CNXN_SOCKET_PROP, ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET),
            Utils.mkEntry(ZK_SSL_KEY_STORE_LOCATION_PROP, "zookeeper.ssl.keyStore.location"),
            Utils.mkEntry(ZK_SSL_KEY_STORE_PASSWORD_PROP, "zookeeper.ssl.keyStore.password"),
            Utils.mkEntry(ZK_SSL_KEY_STORE_TYPE_PROP, "zookeeper.ssl.keyStore.type"),
            Utils.mkEntry(ZK_SSL_TRUST_STORE_LOCATION_PROP, "zookeeper.ssl.trustStore.location"),
            Utils.mkEntry(ZK_SSL_TRUST_STORE_PASSWORD_PROP, "zookeeper.ssl.trustStore.password"),
            Utils.mkEntry(ZK_SSL_TRUST_STORE_TYPE_PROP, "zookeeper.ssl.trustStore.type"),
            Utils.mkEntry(ZK_SSL_PROTOCOL_PROP, "zookeeper.ssl.protocol"),
            Utils.mkEntry(ZK_SSL_ENABLED_PROTOCOLS_PROP, "zookeeper.ssl.enabledProtocols"),
            Utils.mkEntry(ZK_SSL_CIPHER_SUITES_PROP, "zookeeper.ssl.ciphersuites"),
            Utils.mkEntry(ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP, "zookeeper.ssl.hostnameVerification"),
            Utils.mkEntry(ZK_SSL_CRL_ENABLE_PROP, "zookeeper.ssl.crl"),
            Utils.mkEntry(ZK_SSL_OCSP_ENABLE_PROP, "zookeeper.ssl.ocsp")
        );

    /** ********* General Configuration *********/
    public static final String BROKER_ID_GENERATION_ENABLE_PROP = "broker.id.generation.enable";
    public static final String MAX_RESERVED_BROKER_ID_PROP = "reserved.broker.max.id";
    public static final String BROKER_ID_PROP = "broker.id";
    public static final String MESSAGE_MAX_BYTES_PROP = "message.max.bytes";
    public static final String NUM_NETWORK_THREADS_PROP = "num.network.threads";
    public static final String NUM_IO_THREADS_PROP = "num.io.threads";
    public static final String BACKGROUND_THREADS_PROP = "background.threads";
    public static final String NUM_REPLICA_ALTER_LOG_DIRS_THREADS_PROP = "num.replica.alter.log.dirs.threads";
    public static final String QUEUED_MAX_REQUESTS_PROP = "queued.max.requests";
    public static final String QUEUED_MAX_BYTES_PROP = "queued.max.request.bytes";
    public static final String REQUEST_TIMEOUT_MS_PROP = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    public static final String CONNECTION_SETUP_TIMEOUT_MS_PROP = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;
    public static final String CONNECTION_SETUP_TIMEOUT_MAX_MS_PROP = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;


    /** ********* KRaft mode configs *********/
    public static final String PROCESS_ROLES_PROP = "process.roles";
    public static final String INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_PROP = "initial.broker.registration.timeout.ms";
    public static final String BROKER_HEARTBEAT_INTERVAL_MS_PROP = "broker.heartbeat.interval.ms";
    public static final String BROKER_SESSION_TIMEOUT_MS_PROP = "broker.session.timeout.ms";
    public static final String NODE_ID_PROP = "node.id";
    public static final String METADATA_LOG_DIR_PROP = "metadata.log.dir";
    public static final String METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_PROP = "metadata.log.max.record.bytes.between.snapshots";
    public static final String METADATA_SNAPSHOT_MAX_INTERVAL_MS_PROP = "metadata.log.max.snapshot.interval.ms";
    public static final String CONTROLLER_LISTENER_NAMES_PROP = "controller.listener.names";
    public static final String SASL_MECHANISM_CONTROLLER_PROTOCOL_PROP = "sasl.mechanism.controller.protocol";
    public static final String METADATA_LOG_SEGMENT_MIN_BYTES_PROP = "metadata.log.segment.min.bytes";
    public static final String METADATA_LOG_SEGMENT_BYTES_PROP = "metadata.log.segment.bytes";
    public static final String METADATA_LOG_SEGMENT_MILLIS_PROP = "metadata.log.segment.ms";
    public static final String METADATA_MAX_RETENTION_BYTES_PROP = "metadata.max.retention.bytes";
    public static final String METADATA_MAX_RETENTION_MILLIS_PROP = "metadata.max.retention.ms";
    public static final String QUORUM_VOTERS_PROP = RaftConfig.QUORUM_VOTERS_CONFIG;
    public static final String METADATA_MAX_IDLE_INTERVAL_MS_PROP = "metadata.max.idle.interval.ms";
    public static final String SERVER_MAX_STARTUP_TIME_MS_PROP = "server.max.startup.time.ms";

    /** ********* ZK to KRaft Migration configs *********/
    public static final String MIGRATION_ENABLED_PROP = "zookeeper.metadata.migration.enable";

    /** ********* Enable eligible leader replicas configs *********/
    public static final String ELR_ENABLED_PROP = "eligible.leader.replicas.enable";

    /************* Authorizer Configuration ***********/
    public static final String AUTHORIZER_CLASS_NAME_PROP = "authorizer.class.name";
    public static final String EARLY_START_LISTENERS_PROP = "early.start.listeners";

    /** ********* Socket Server Configuration *********/
    public static final String LISTENERS_PROP = "listeners";
    public static final String ADVERTISED_LISTENERS_PROP = "advertised.listeners";
    public static final String LISTENER_SECURITY_PROTOCOL_MAP_PROP = "listener.security.protocol.map";
    public static final String CONTROL_PLANE_LISTENER_NAME_PROP = "control.plane.listener.name";
    public static final String SOCKET_SEND_BUFFER_BYTES_PROP = "socket.send.buffer.bytes";
    public static final String SOCKET_RECEIVE_BUFFER_BYTES_PROP = "socket.receive.buffer.bytes";
    public static final String SOCKET_REQUEST_MAX_BYTES_PROP = "socket.request.max.bytes";
    public static final String SOCKET_LISTEN_BACKLOG_SIZE_PROP = "socket.listen.backlog.size";
    public static final String MAX_CONNECTIONS_PER_IP_PROP = "max.connections.per.ip";
    public static final String MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP = "max.connections.per.ip.overrides";
    public static final String MAX_CONNECTIONS_PROP = "max.connections";
    public static final String MAX_CONNECTION_CREATION_RATE_PROP = "max.connection.creation.rate";
    public static final String CONNECTIONS_MAX_IDLE_MS_PROP = "connections.max.idle.ms";
    public static final String FAILED_AUTHENTICATION_DELAY_MS_PROP = "connection.failed.authentication.delay.ms";
    /***************** rack configuration *************/
    public static final String RACK_PROP = "broker.rack";
    /** ********* Log Configuration *********/
    public static final String NUM_PARTITIONS_PROP = "num.partitions";
    public static final String LOG_DIRS_PROP = LOG_CONFIG_PREFIX + "dirs";
    public static final String LOG_DIR_PROP = LOG_CONFIG_PREFIX + "dir";
    public static final String LOG_SEGMENT_BYTES_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.SEGMENT_BYTES_CONFIG);

    public static final String LOG_ROLL_TIME_MILLIS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.SEGMENT_MS_CONFIG);
    public static final String LOG_ROLL_TIME_HOURS_PROP = LOG_CONFIG_PREFIX + "roll.hours";

    public static final String LOG_ROLL_TIME_JITTER_MILLIS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.SEGMENT_JITTER_MS_CONFIG);
    public static final String LOG_ROLL_TIME_JITTER_HOURS_PROP = LOG_CONFIG_PREFIX + "roll.jitter.hours";

    public static final String LOG_RETENTION_TIME_MILLIS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.RETENTION_MS_CONFIG);
    public static final String LOG_RETENTION_TIME_MINUTES_PROP = LOG_CONFIG_PREFIX + "retention.minutes";
    public static final String LOG_RETENTION_TIME_HOURS_PROP = LOG_CONFIG_PREFIX + "retention.hours";

    public static final String LOG_RETENTION_BYTES_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.RETENTION_BYTES_CONFIG);
    public static final String LOG_CLEANUP_INTERVAL_MS_PROP = LOG_CONFIG_PREFIX + "retention.check.interval.ms";
    public static final String LOG_CLEANUP_POLICY_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.CLEANUP_POLICY_CONFIG);
    public static final String LOG_CLEANER_THREADS_PROP = LOG_CONFIG_PREFIX + "cleaner.threads";
    public static final String LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP = LOG_CONFIG_PREFIX + "cleaner.io.max.bytes.per.second";
    public static final String LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP = LOG_CONFIG_PREFIX + "cleaner.dedupe.buffer.size";
    public static final String LOG_CLEANER_IO_BUFFER_SIZE_PROP = LOG_CONFIG_PREFIX + "cleaner.io.buffer.size";
    public static final String LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP = LOG_CONFIG_PREFIX + "cleaner.io.buffer.load.factor";
    public static final String LOG_CLEANER_BACKOFF_MS_PROP = LOG_CONFIG_PREFIX + "cleaner.backoff.ms";
    public static final String LOG_CLEANER_MIN_CLEAN_RATIO_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG);
    public static final String LOG_CLEANER_ENABLE_PROP = LOG_CONFIG_PREFIX + "cleaner.enable";
    public static final String LOG_CLEANER_DELETE_RETENTION_MS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.DELETE_RETENTION_MS_CONFIG);
    public static final String LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG);
    public static final String LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG);
    public static final String LOG_INDEX_SIZE_MAX_BYTES_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG);
    public static final String LOG_INDEX_INTERVAL_BYTES_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG);
    public static final String LOG_FLUSH_INTERVAL_MESSAGES_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG);
    public static final String LOG_DELETE_DELAY_MS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG);
    public static final String LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP = LOG_CONFIG_PREFIX + "flush.scheduler.interval.ms";
    public static final String LOG_FLUSH_INTERVAL_MS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.FLUSH_MS_CONFIG);
    public static final String LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_PROP = LOG_CONFIG_PREFIX + "flush.offset.checkpoint.interval.ms";
    public static final String LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_PROP = LOG_CONFIG_PREFIX + "flush.start.offset.checkpoint.interval.ms";
    public static final String LOG_PRE_ALLOCATE_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.PREALLOCATE_CONFIG);

    /**
     * See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details
     */
    @Deprecated
    public static final String LOG_MESSAGE_FORMAT_VERSION_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG);

    public static final String LOG_MESSAGE_TIMESTAMP_TYPE_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG);

    /**
     * See `TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG` for details
     */
    @Deprecated
    public static final String LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG);
    public static final String LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG);
    public static final String LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG);

    public static final String NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP = "num.recovery.threads.per.data.dir";
    public static final String AUTO_CREATE_TOPICS_ENABLE_PROP = "auto.create.topics.enable";
    public static final String MIN_IN_SYNC_REPLICAS_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
    public static final String CREATE_TOPIC_POLICY_CLASS_NAME_PROP = "create.topic.policy.class.name";
    public static final String ALTER_CONFIG_POLICY_CLASS_NAME_PROP = "alter.config.policy.class.name";
    public static final String LOG_MESSAGE_DOWN_CONVERSION_ENABLE_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG);
    /** ********* Replication configuration ***********/
    public static final String CONTROLLER_SOCKET_TIMEOUT_MS_PROP = "controller.socket.timeout.ms";
    public static final String DEFAULT_REPLICATION_FACTOR_PROP = "default.replication.factor";
    public static final String REPLICA_LAG_TIME_MAX_MS_PROP = "replica.lag.time.max.ms";
    public static final String REPLICA_SOCKET_TIMEOUT_MS_PROP = "replica.socket.timeout.ms";
    public static final String REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_PROP = "replica.socket.receive.buffer.bytes";
    public static final String REPLICA_FETCH_MAX_BYTES_PROP = "replica.fetch.max.bytes";
    public static final String REPLICA_FETCH_WAIT_MAX_MS_PROP = "replica.fetch.wait.max.ms";
    public static final String REPLICA_FETCH_MIN_BYTES_PROP = "replica.fetch.min.bytes";
    public static final String REPLICA_FETCH_RESPONSE_MAX_BYTES_PROP = "replica.fetch.response.max.bytes";
    public static final String REPLICA_FETCH_BACKOFF_MS_PROP = "replica.fetch.backoff.ms";
    public static final String NUM_REPLICA_FETCHERS_PROP = "num.replica.fetchers";
    public static final String REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_PROP = "replica.high.watermark.checkpoint.interval.ms";
    public static final String FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP = "fetch.purgatory.purge.interval.requests";
    public static final String PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP = "producer.purgatory.purge.interval.requests";
    public static final String DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP = "delete.records.purgatory.purge.interval.requests";
    public static final String AUTO_LEADER_REBALANCE_ENABLE_PROP = "auto.leader.rebalance.enable";
    public static final String LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_PROP = "leader.imbalance.per.broker.percentage";
    public static final String LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_PROP = "leader.imbalance.check.interval.seconds";
    public static final String UNCLEAN_LEADER_ELECTION_ENABLE_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG);
    public static final String INTER_BROKER_SECURITY_PROTOCOL_PROP = "security.inter.broker.protocol";
    public static final String INTER_BROKER_PROTOCOL_VERSION_PROP = "inter.broker.protocol.version";
    public static final String INTER_BROKER_LISTENER_NAME_PROP = "inter.broker.listener.name";
    public static final String REPLICA_SELECTOR_CLASS_PROP = "replica.selector.class";
    /** ********* Controlled shutdown configuration ***********/
    public static final String CONTROLLED_SHUTDOWN_MAX_RETRIES_PROP = "controlled.shutdown.max.retries";
    public static final String CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_PROP = "controlled.shutdown.retry.backoff.ms";
    public static final String CONTROLLED_SHUTDOWN_ENABLE_PROP = "controlled.shutdown.enable";

    /** ********* Group coordinator configuration ***********/
    public static final String GROUP_MIN_SESSION_TIMEOUT_MS_PROP = "group.min.session.timeout.ms";
    public static final String GROUP_MAX_SESSION_TIMEOUT_MS_PROP = "group.max.session.timeout.ms";
    public static final String GROUP_INITIAL_REBALANCE_DELAY_MS_PROP = "group.initial.rebalance.delay.ms";
    public static final String GROUP_MAX_SIZE_PROP = "group.max.size";

    /** ********* New group coordinator configs *********/
    public static final String NEW_GROUP_COORDINATOR_ENABLE_PROP = "group.coordinator.new.enable";
    public static final String GROUP_COORDINATOR_NUM_THREADS_PROP = "group.coordinator.threads";

    /** ********* Consumer group configs *********/
    public static final String CONSUMER_GROUP_SESSION_TIMEOUT_MS_PROP = "group.consumer.session.timeout.ms";
    public static final String CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_PROP = "group.consumer.min.session.timeout.ms";
    public static final String CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_PROP = "group.consumer.max.session.timeout.ms";
    public static final String CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_PROP = "group.consumer.heartbeat.interval.ms";
    public static final String CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_PROP = "group.consumer.min.heartbeat.interval.ms";
    public static final String CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_PROP = "group.consumer.max.heartbeat.interval.ms";
    public static final String CONSUMER_GROUP_MAX_SIZE_PROP = "group.consumer.max.size";
    public static final String CONSUMER_GROUP_ASSIGNORS_PROP = "group.consumer.assignors";

    /** ********* Offset management configuration *********/
    public static final String OFFSET_METADATA_MAX_SIZE_PROP = "offset.metadata.max.bytes";
    public static final String OFFSETS_LOAD_BUFFER_SIZE_PROP = "offsets.load.buffer.size";
    public static final String OFFSETS_TOPIC_REPLICATION_FACTOR_PROP = "offsets.topic.replication.factor";
    public static final String OFFSETS_TOPIC_PARTITIONS_PROP = "offsets.topic.num.partitions";
    public static final String OFFSETS_TOPIC_SEGMENT_BYTES_PROP = "offsets.topic.segment.bytes";
    public static final String OFFSETS_TOPIC_COMPRESSION_CODEC_PROP = "offsets.topic.compression.codec";
    public static final String OFFSETS_RETENTION_MINUTES_PROP = "offsets.retention.minutes";
    public static final String OFFSETS_RETENTION_CHECK_INTERVAL_MS_PROP = "offsets.retention.check.interval.ms";
    public static final String OFFSET_COMMIT_TIMEOUT_MS_PROP = "offsets.commit.timeout.ms";
    public static final String OFFSET_COMMIT_REQUIRED_ACKS_PROP = "offsets.commit.required.acks";

    /** ********* Transaction management configuration *********/
    public static final String TRANSACTIONAL_ID_EXPIRATION_MS_PROP = "transactional.id.expiration.ms";
    public static final String TRANSACTIONS_MAX_TIMEOUT_MS_PROP = "transaction.max.timeout.ms";
    public static final String TRANSACTIONS_TOPIC_MIN_ISR_PROP = "transaction.state.log.min.isr";
    public static final String TRANSACTIONS_LOAD_BUFFER_SIZE_PROP = "transaction.state.log.load.buffer.size";
    public static final String TRANSACTIONS_TOPIC_PARTITIONS_PROP = "transaction.state.log.num.partitions";
    public static final String TRANSACTIONS_TOPIC_SEGMENT_BYTES_PROP = "transaction.state.log.segment.bytes";
    public static final String TRANSACTIONS_TOPIC_REPLICATION_FACTOR_PROP = "transaction.state.log.replication.factor";
    public static final String TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_PROP = "transaction.abort.timed.out.transaction.cleanup.interval.ms";
    public static final String TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_PROP = "transaction.remove.expired.transaction.cleanup.interval.ms";

    public static final String TRANSACTION_PARTITION_VERIFICATION_ENABLE_PROP = "transaction.partition.verification.enable";

    public static final String PRODUCER_ID_EXPIRATION_MS_PROP = ProducerStateManagerConfig.PRODUCER_ID_EXPIRATION_MS;
    public static final String PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_PROP = "producer.id.expiration.check.interval.ms";

    /** ********* Fetch Configuration *********/
    public static final String MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_PROP = "max.incremental.fetch.session.cache.slots";
    public static final String FETCH_MAX_BYTES_PROP = "fetch.max.bytes";

    /** ********* Quota Configuration *********/
    public static final String NUM_QUOTA_SAMPLES_PROP = "quota.window.num";
    public static final String NUM_REPLICATION_QUOTA_SAMPLES_PROP = "replication.quota.window.num";
    public static final String NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_PROP = "alter.log.dirs.replication.quota.window.num";
    public static final String NUM_CONTROLLER_QUOTA_SAMPLES_PROP = "controller.quota.window.num";
    public static final String QUOTA_WINDOW_SIZE_SECONDS_PROP = "quota.window.size.seconds";
    public static final String REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_PROP = "replication.quota.window.size.seconds";
    public static final String ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_PROP = "alter.log.dirs.replication.quota.window.size.seconds";
    public static final String CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_PROP = "controller.quota.window.size.seconds";
    public static final String CLIENT_QUOTA_CALLBACK_CLASS_PROP = "client.quota.callback.class";

    public static final String DELETE_TOPIC_ENABLE_PROP = "delete.topic.enable";
    public static final String COMPRESSION_TYPE_PROP = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.COMPRESSION_TYPE_CONFIG);

    /** ********* Kafka Metrics Configuration ***********/
    public static final String METRIC_SAMPLE_WINDOW_MS_PROP = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;
    public static final String METRIC_NUM_SAMPLES_PROP = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;
    public static final String METRIC_REPORTER_CLASSES_PROP = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;
    public static final String METRIC_RECORDING_LEVEL_PROP = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;
    @Deprecated
    public static final String AUTO_INCLUDE_JMX_REPORTER_PROP = CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_CONFIG;

    /** ********* Kafka Yammer Metrics Reporters Configuration ***********/
    public static final String KAFKA_METRICS_REPORTER_CLASSES_PROP = "kafka.metrics.reporters";
    public static final String KAFKA_METRICS_POLLING_INTERVAL_SECONDS_PROP = "kafka.metrics.polling.interval.secs";

    /** ********* Kafka Client Telemetry Metrics Configuration ***********/
    public static final String CLIENT_TELEMETRY_MAX_BYTES_PROP = "telemetry.max.bytes";

    /** ********* Common Security Configuration ***********/
    public static final String PRINCIPAL_BUILDER_CLASS_PROP = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG;
    public static final String CONNECTIONS_MAX_REAUTH_MS_PROP = BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS;
    public static final String SASL_SERVER_MAX_RECEIVE_SIZE_PROP = BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG;
    public static final String SECURITY_PROVIDER_CLASS_PROP = SecurityConfig.SECURITY_PROVIDERS_CONFIG;

    /** ********* SSL Configuration ********/
    public static final String SSL_PROTOCOL_PROP = SslConfigs.SSL_PROTOCOL_CONFIG;
    public static final String SSL_PROVIDER_PROP = SslConfigs.SSL_PROVIDER_CONFIG;
    public static final String SSL_CIPHER_SUITES_PROP = SslConfigs.SSL_CIPHER_SUITES_CONFIG;
    public static final String SSL_ENABLED_PROTOCOLS_PROP = SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG;
    public static final String SSL_KEYSTORE_TYPE_PROP = SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
    public static final String SSL_KEYSTORE_LOCATION_PROP = SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
    public static final String SSL_KEYSTORE_PASSWORD_PROP = SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
    public static final String SSL_KEY_PASSWORD_PROP = SslConfigs.SSL_KEY_PASSWORD_CONFIG;
    public static final String SSL_KEYSTORE_KEY_PROP = SslConfigs.SSL_KEYSTORE_KEY_CONFIG;
    public static final String SSL_KEYSTORE_CERTIFICATE_CHAIN_PROP = SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG;
    public static final String SSL_TRUSTSTORE_TYPE_PROP = SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;
    public static final String SSL_TRUSTSTORE_LOCATION_PROP = SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
    public static final String SSL_TRUSTSTORE_PASSWORD_PROP = SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
    public static final String SSL_TRUSTSTORE_CERTIFICATES_PROP = SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG;
    public static final String SSL_KEY_MANAGER_ALGORITHM_PROP = SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG;
    public static final String SSL_TRUST_MANAGER_ALGORITHM_PROP = SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG;
    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP = SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
    public static final String SSL_SECURE_RANDOM_IMPLEMENTATION_PROP = SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG;
    public static final String SSL_CLIENT_AUTH_PROP = BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG;
    public static final String SSL_PRINCIPAL_MAPPING_RULES_PROP = BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG;
    public static final String SSL_ENGINE_FACTORY_CLASS_PROP = SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG;

    /** ********* SASL Configuration ********/
    public static final String SASL_MECHANISM_INTER_BROKER_PROTOCOL_PROP = "sasl.mechanism.inter.broker.protocol";
    public static final String SASL_JAAS_CONFIG_PROP = SaslConfigs.SASL_JAAS_CONFIG;
    public static final String SASL_ENABLED_MECHANISMS_PROP = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG;
    public static final String SASL_SERVER_CALLBACK_HANDLER_CLASS_PROP = BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS;
    public static final String SASL_CLIENT_CALLBACK_HANDLER_CLASS_PROP = SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS;
    public static final String SASL_LOGIN_CLASS_PROP = SaslConfigs.SASL_LOGIN_CLASS;
    public static final String SASL_LOGIN_CALLBACK_HANDLER_CLASS_PROP = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS;
    public static final String SASL_KERBEROS_SERVICE_NAME_PROP = SaslConfigs.SASL_KERBEROS_SERVICE_NAME;
    public static final String SASL_KERBEROS_KINIT_CMD_PROP = SaslConfigs.SASL_KERBEROS_KINIT_CMD;
    public static final String SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_PROP = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR;
    public static final String SASL_KERBEROS_TICKET_RENEW_JITTER_PROP = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER;
    public static final String SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_PROP = SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN;
    public static final String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_PROP = BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG;
    public static final String SASL_LOGIN_REFRESH_WINDOW_FACTOR_PROP = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR;
    public static final String SASL_LOGIN_REFRESH_WINDOW_JITTER_PROP = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER;
    public static final String SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_PROP = SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS;
    public static final String SASL_LOGIN_REFRESH_BUFFER_SECONDS_PROP = SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS;

    public static final String SASL_LOGIN_CONNECT_TIMEOUT_MS_PROP = SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
    public static final String SASL_LOGIN_READ_TIMEOUT_MS_PROP = SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS;
    public static final String SASL_LOGIN_RETRY_BACKOFF_MAX_MS_PROP = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
    public static final String SASL_LOGIN_RETRY_BACKOFF_MS_PROP = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
    public static final String SASL_O_AUTH_BEARER_SCOPE_CLAIM_NAME_PROP = SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
    public static final String SASL_O_AUTH_BEARER_SUB_CLAIM_NAME_PROP = SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME;
    public static final String SASL_O_AUTH_BEARER_TOKEN_ENDPOINT_URL_PROP = SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
    public static final String SASL_O_AUTH_BEARER_JWKS_ENDPOINT_URL_PROP = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL;
    public static final String SASL_O_AUTH_BEARER_JWKS_ENDPOINT_REFRESH_MS_PROP = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS;
    public static final String SASL_O_AUTH_BEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_PROP = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS;
    public static final String SASL_O_AUTH_BEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_PROP = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS;
    public static final String SASL_O_AUTH_BEARER_CLOCK_SKEW_SECONDS_PROP = SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS;
    public static final String SASL_O_AUTH_BEARER_EXPECTED_AUDIENCE_PROP = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
    public static final String SASL_O_AUTH_BEARER_EXPECTED_ISSUER_PROP = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER;

    /** ******** Delegation Token Configuration ********/
    public static final String DELEGATION_TOKEN_SECRET_KEY_ALIAS_PROP = "delegation.token.master.key";
    public static final String DELEGATION_TOKEN_SECRET_KEY_PROP = "delegation.token.secret.key";
    public static final String DELEGATION_TOKEN_MAX_LIFE_TIME_PROP = "delegation.token.max.lifetime.ms";
    public static final String DELEGATION_TOKEN_EXPIRY_TIME_MS_PROP = "delegation.token.expiry.time.ms";
    public static final String DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_PROP = "delegation.token.expiry.check.interval.ms";

    /** ******** Password encryption configuration for dynamic configs *******/
    public static final String PASSWORD_ENCODER_SECRET_PROP = "password.encoder.secret";
    public static final String PASSWORD_ENCODER_OLD_SECRET_PROP = "password.encoder.old.secret";
    public static final String PASSWORD_ENCODER_KEY_FACTORY_ALGORITHM_PROP = "password.encoder.keyfactory.algorithm";
    public static final String PASSWORD_ENCODER_CIPHER_ALGORITHM_PROP = "password.encoder.cipher.algorithm";
    public static final String PASSWORD_ENCODER_KEY_LENGTH_PROP = "password.encoder.key.length";
    public static final String PASSWORD_ENCODER_ITERATIONS_PROP = "password.encoder.iterations";

    /** ******** Internal Configurations ********/
    public static final String UNSTABLE_API_VERSIONS_ENABLE_PROP = "unstable.api.versions.enable";
    public static final String UNSTABLE_METADATA_VERSIONS_ENABLE_PROP = "unstable.metadata.versions.enable";

    /* Documentation */
    /** ******** Zookeeper Configuration ********/
    public static final String ZK_CONNECT_DOC = "Specifies the ZooKeeper connection string in the form <code>hostname:port</code> where host and port are the " +
            "host and port of a ZooKeeper server. To allow connecting through other ZooKeeper nodes when that ZooKeeper machine is " +
            "down you can also specify multiple hosts in the form <code>hostname1:port1,hostname2:port2,hostname3:port3</code>.\n" +
            "The server can also have a ZooKeeper chroot path as part of its ZooKeeper connection string which puts its data under some path in the global ZooKeeper namespace. " +
            "For example to give a chroot path of <code>/chroot/path</code> you would give the connection string as <code>hostname1:port1,hostname2:port2,hostname3:port3/chroot/path</code>.";

    public static final String ZK_SESSION_TIMEOUT_MS_DOC = "Zookeeper session timeout";

    public static final String ZK_CONNECTION_TIMEOUT_MS_DOC = "The max time that the client waits to establish a connection to ZooKeeper. If not set, the value in " + ZK_SESSION_TIMEOUT_MS_PROP + " is used";

    public static final String ZK_ENABLE_SECURE_ACLS_DOC = "Set client to use secure ACLs";

    public static final String ZK_MAX_IN_FLIGHT_REQUESTS_DOC = "The maximum number of unacknowledged requests the client will send to ZooKeeper before blocking.";

    public static final String ZK_SSL_CLIENT_ENABLE_DOC = "Set client to use TLS when connecting to ZooKeeper." +
            " An explicit value overrides any value set via the <code>zookeeper.client.secure</code> system property (note the different name)." +
            " Defaults to false if neither is set; when true, <code>" + ZK_CLIENT_CNXN_SOCKET_PROP + "</code> must be set (typically to <code>org.apache.zookeeper.ClientCnxnSocketNetty</code>); other values to set may include " +
            ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.keySet().stream().filter(x -> !x.equals(ZK_SSL_CLIENT_ENABLE_PROP) && !x.equals(ZK_CLIENT_CNXN_SOCKET_PROP)).sorted().collect(Collectors.joining("<code>", "</code>, <code>", "</code>"));

    public static final String ZK_CLIENT_CNXN_SOCKET_DOC = "Typically set to <code>org.apache.zookeeper.ClientCnxnSocketNetty</code> when using TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the same-named <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_CLIENT_CNXN_SOCKET_PROP) + "</code> system property.";

    public static final String ZK_SSL_KEY_STORE_LOCATION_DOC = "Keystore location when using a client-side certificate with TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_KEY_STORE_LOCATION_PROP) + "</code> system property (note the camelCase).";

    public static final String ZK_SSL_KEY_STORE_PASSWORD_DOC = "Keystore password when using a client-side certificate with TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_KEY_STORE_PASSWORD_PROP) + "</code> system property (note the camelCase)." +
            " Note that ZooKeeper does not support a key password different from the keystore password, so be sure to set the key password in the keystore to be identical to the keystore password; otherwise, the connection attempt to Zookeeper will fail.";

    public static final String ZK_SSL_KEY_STORE_TYPE_DOC = "Keystore type when using a client-side certificate with TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_KEY_STORE_TYPE_PROP) + "</code> system property (note the camelCase)." +
            " The default value of <code>null</code> means the type will be auto-detected based on the filename extension of the keystore.";

    public static final String ZK_SSL_TRUST_STORE_LOCATION_DOC = "Truststore location when using TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_TRUST_STORE_LOCATION_PROP) + "</code> system property (note the camelCase).";

    public static final String ZK_SSL_TRUST_STORE_PASSWORD_DOC = "Truststore password when using TLS connectivity to ZooKeeper. Overrides any explicit value set via the <code>"
            + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_TRUST_STORE_PASSWORD_PROP) + "</code> system property (note the camelCase).";

    public static final String ZK_SSL_TRUST_STORE_TYPE_DOC = "Truststore type when using TLS connectivity to ZooKeeper. Overrides any explicit value set via the <code>"
            + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_TRUST_STORE_TYPE_PROP) + "</code> system property (note the camelCase). " +
            "The default value of <code>null</code> means the type will be auto-detected based on the filename extension of the truststore.";

    public static final String ZK_SSL_PROTOCOL_DOC = "Specifies the protocol to be used in ZooKeeper TLS negotiation. An explicit value overrides any value set via the same-named <code>"
            + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_PROTOCOL_PROP) + "</code> system property.";

    public static final String ZK_SSL_ENABLED_PROTOCOLS_DOC = "Specifies the enabled protocol(s) in ZooKeeper TLS negotiation (csv). Overrides any explicit value set via the <code>"
            + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_ENABLED_PROTOCOLS_PROP) + "</code> system property (note the camelCase). The default value of <code>null</code> means the enabled protocol will be the value of the <code>"
            + ZK_SSL_PROTOCOL_PROP + "</code> configuration property.";

    public static final String ZK_SSL_CIPHER_SUITES_DOC = "Specifies the enabled cipher suites to be used in ZooKeeper TLS negotiation (csv). Overrides any explicit value set via the <code>"
            + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_CIPHER_SUITES_PROP) + "</code> system property (note the single word \"ciphersuites\"). " +
            "The default value of <code>null</code> means the list of enabled cipher suites is determined by the Java runtime being used.";

    public static final String ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC = "Specifies whether to enable hostname verification in the ZooKeeper TLS negotiation process, with (case-insensitively) \"https\" " +
            "meaning ZooKeeper hostname verification is enabled and an explicit blank value meaning it is disabled (disabling it is only recommended for testing purposes). An explicit value overrides any \"true\" or \"false\" value set via the <code>"
            + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP) + "</code> system property (note the different name and values; true implies https and false implies blank).";

    public static final String ZK_SSL_CRL_ENABLE_DOC = "Specifies whether to enable Certificate Revocation List in the ZooKeeper TLS protocols. Overrides any explicit value set via the <code>"
            + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_CRL_ENABLE_PROP) + "</code> system property (note the shorter name).";

    public static final String ZK_SSL_OCSP_ENABLE_DOC = "Specifies whether to enable Online Certificate Status Protocol in the ZooKeeper TLS protocols. Overrides any explicit value set via the <code>"
            + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_OCSP_ENABLE_PROP) + "</code> system property (note the shorter name).";

    /** General Configuration ***********/
    public static final String BROKER_ID_GENERATION_ENABLE_DOC = "Enable automatic broker id generation on the server. When enabled the value configured for " + MAX_RESERVED_BROKER_ID_PROP + " should be reviewed.";
    public static final String MAX_RESERVED_BROKER_ID_DOC = "Max number that can be used for a broker.id";
    public static final String BROKER_ID_DOC = "The broker id for this server. If unset, a unique broker id will be generated. To avoid conflicts between ZooKeeper generated broker id's and user configured broker id's, generated broker ids start from "
            + MAX_RESERVED_BROKER_ID_PROP + " + 1.";

    public static final String MESSAGE_MAX_BYTES_DOC = TopicConfig.MAX_MESSAGE_BYTES_DOC +
            "This can be set per topic with the topic level <code>" + TopicConfig.MAX_MESSAGE_BYTES_CONFIG + "</code> config.";

    public static final String NUM_NETWORK_THREADS_DOC = "The number of threads that the server uses for receiving requests from the network and sending responses to the network. Noted: each listener (except for controller listener) creates its own thread pool.";
    public static final String NUM_IO_THREADS_DOC = "The number of threads that the server uses for processing requests, which may include disk I/O";
    public static final String NUM_REPLICA_ALTER_LOG_DIRS_THREADS_DOC = "The number of threads that can move replicas between log directories, which may include disk I/O";
    public static final String BACKGROUND_THREADS_DOC = "The number of threads to use for various background processing tasks";
    public static final String QUEUED_MAX_REQUESTS_DOC = "The number of queued requests allowed for data-plane, before blocking the network threads";
    public static final String QUEUED_MAX_REQUEST_BYTES_DOC = "The number of queued bytes allowed before no more requests are read";
    public static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;
    public static final String CONNECTION_SETUP_TIMEOUT_MS_DOC = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC;
    public static final String CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC;

    /** ******** KRaft mode configs ********/
    public static final String PROCESS_ROLES_DOC = "The roles that this process plays: 'broker', 'controller', or 'broker,controller' if it is both. "
            + "This configuration is only applicable for clusters in KRaft (Kafka Raft) mode (instead of ZooKeeper). Leave this config undefined or empty for ZooKeeper clusters.";

    public static final String INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_DOC = "When initially registering with the controller quorum, the number of milliseconds to wait before declaring failure and exiting the broker process.";

    public static final String BROKER_HEARTBEAT_INTERVAL_MS_DOC = "The length of time in milliseconds between broker heartbeats. Used when running in KRaft mode.";

    public static final String BROKER_SESSION_TIMEOUT_MS_DOC = "The length of time in milliseconds that a broker lease lasts if no heartbeats are made. Used when running in KRaft mode.";

    public static final String NODE_ID_DOC = "The node ID associated with the roles this process is playing when `process.roles` is non-empty. "
            + "This is required configuration when running in KRaft mode.";

    public static final String METADATA_LOG_DIR_DOC = "This configuration determines where we put the metadata log for clusters in KRaft mode. "
            + "If it is not set, the metadata log is placed in the first log directory from log.dirs.";

    public static final String METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_DOC = "This is the maximum number of bytes in the log between the latest "
            + "snapshot and the high-watermark needed before generating a new snapshot. The default value is "
            + Defaults.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES + ". To generate snapshots based on the time elapsed, see "
            + "the <code>" + METADATA_SNAPSHOT_MAX_INTERVAL_MS_PROP + "</code> configuration. The Kafka node will generate a snapshot when "
            + "either the maximum time interval is reached or the maximum bytes limit is reached.";

    public static final String METADATA_SNAPSHOT_MAX_INTERVAL_MS_DOC = "This is the maximum number of milliseconds to wait to generate a snapshot "
            + "if there are committed records in the log that are not included in the latest snapshot. A value of zero disables "
            + "time-based snapshot generation. The default value is " + Defaults.METADATA_SNAPSHOT_MAX_INTERVAL_MS + ". To generate "
            + "snapshots based on the number of metadata bytes, see the <code>" + METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_PROP + "</code> "
            + "configuration. The Kafka node will generate a snapshot when either the maximum time interval is reached or the "
            + "maximum bytes limit is reached.";

    public static final String METADATA_MAX_IDLE_INTERVAL_MS_DOC = "This configuration controls how often the active "
            + "controller should write no-op records to the metadata partition. If the value is 0, no-op records "
            + "are not appended to the metadata partition. The default value is " + Defaults.METADATA_MAX_IDLE_INTERVAL_MS;

    public static final String CONTROLLER_LISTENER_NAMES_DOC = "A comma-separated list of the names of the listeners used by the controller. This is required "
            + "if running in KRaft mode. When communicating with the controller quorum, the broker will always use the first listener in this list.\n "
            + "Note: The ZooKeeper-based controller should not set this configuration.";

    public static final String SASL_MECHANISM_CONTROLLER_PROTOCOL_DOC = "SASL mechanism used for communication with controllers. Default is GSSAPI.";

    public static final String METADATA_LOG_SEGMENT_BYTES_DOC = "The maximum size of a single metadata log file.";

    public static final String METADATA_LOG_SEGMENT_MIN_BYTES_DOC = "Override the minimum size for a single metadata log file. This should be used for testing only.";

    public static final String SERVER_MAX_STARTUP_TIME_MS_DOC = "The maximum number of milliseconds we will wait for the server to come up. "
            + "By default there is no limit. This should be used for testing only.";

    public static final String METADATA_LOG_SEGMENT_MILLIS_DOC = "The maximum time before a new metadata log file is rolled out (in milliseconds).";

    public static final String METADATA_MAX_RETENTION_BYTES_DOC = "The maximum combined size of the metadata log and snapshots before deleting old "
            + "snapshots and log files. Since at least one snapshot must exist before any logs can be deleted, this is a soft limit.";

    public static final String METADATA_MAX_RETENTION_MILLIS_DOC = "The number of milliseconds to keep a metadata log file or snapshot before "
            + "deleting it. Since at least one snapshot must exist before any logs can be deleted, this is a soft limit.";

    /** ********* Authorizer Configuration ***********/
    public static final String AUTHORIZER_CLASS_NAME_DOC = "The fully qualified name of a class that implements <code>"
            + Authorizer.class.getName() + "</code> interface, which is used by the broker for authorization.";

    public static final String EARLY_START_LISTENERS_DOC = "A comma-separated list of listener names which may be started before the authorizer has finished "
            + "initialization. This is useful when the authorizer is dependent on the cluster itself for bootstrapping, as is the case for "
            + "the StandardAuthorizer (which stores ACLs in the metadata log.) By default, all listeners included in controller.listener.names "
            + "will also be early start listeners. A listener should not appear in this list if it accepts external traffic.";

    /** ********* Socket Server Configuration ***********/
    public static final String LISTENERS_DOC = "Listener List - Comma-separated list of URIs we will listen on and the listener names."
            + " If the listener name is not a security protocol, <code>" + LISTENER_SECURITY_PROTOCOL_MAP_PROP + "</code> must also be set.\n"
            + " Listener names and port numbers must be unique unless \n"
            + " one listener is an IPv4 address and the other listener is \n"
            + " an IPv6 address (for the same port).\n"
            + " Specify hostname as 0.0.0.0 to bind to all interfaces.\n"
            + " Leave hostname empty to bind to the default interface.\n"
            + " Examples of legal listener lists:\n"
            + " <code>PLAINTEXT://myhost:9092,SSL://:9091</code>\n"
            + " <code>CLIENT://0.0.0.0:9092,REPLICATION://localhost:9093</code>\n"
            + " <code>PLAINTEXT://127.0.0.1:9092,SSL://[::1]:9092</code>\n";

    public static final String ADVERTISED_LISTENERS_DOC = "Listeners to publish to ZooKeeper for clients to use, if different than the <code>"
            + LISTENERS_PROP + "</code> config property." + " In IaaS environments, this may need to be different from the interface to which the broker binds."
            + " If this is not set, the value for <code>" + LISTENERS_PROP + "</code> will be used." + " Unlike <code>" + LISTENERS_PROP + "</code>, it is not valid to advertise the 0.0.0.0 meta-address.\n"
            + " Also unlike <code>" + LISTENERS_PROP + "</code>, there can be duplicated ports in this property,"
            + " so that one listener can be configured to advertise another listener's address." + " This can be useful in some cases where external load balancers are used.";

    public static final String LISTENER_SECURITY_PROTOCOL_MAP_DOC = "Map between listener names and security protocols. This must be defined for "
            + "the same security protocol to be usable in more than one port or IP. For example, internal and "
            + "external traffic can be separated even if SSL is required for both. Concretely, the user could define listeners "
            + "with names INTERNAL and EXTERNAL and this property as: `INTERNAL:SSL,EXTERNAL:SSL`. As shown, key and value are "
            + "separated by a colon and map entries are separated by commas. Each listener name should only appear once in the map. "
            + "Different security (SSL and SASL) settings can be configured for each listener by adding a normalized "
            + "prefix (the listener name is lowercased) to the config name. For example, to set a different keystore for the "
            + "INTERNAL listener, a config with name <code>listener.name.internal.ssl.keystore.location</code> would be set. "
            + "If the config for the listener name is not set, the config will fallback to the generic config (i.e. <code>ssl.keystore.location</code>). "
            + "Note that in KRaft a default mapping from the listener names defined by <code>controller.listener.names</code> to PLAINTEXT "
            + "is assumed if no explicit mapping is provided and no other security protocol is in use.";

    public static final String CONTROL_PLANE_LISTENER_NAME_DOC = "Name of the listener used for communication between the controller and brokers. "
            + "A broker will use the <code>" + CONTROL_PLANE_LISTENER_NAME_PROP + "</code> to locate the endpoint in " + LISTENERS_PROP + " list, to listen for connections from the controller. "
            + "For example, if a broker's config is:\n"
            + "<code>listeners = INTERNAL://192.1.1.8:9092, EXTERNAL://10.1.1.5:9093, CONTROLLER://192.1.1.8:9094\n"
            + "listener.security.protocol.map = INTERNAL:PLAINTEXT, EXTERNAL:SSL, CONTROLLER:SSL\n"
            + "control.plane.listener.name = CONTROLLER</code>\n"
            + "On startup, the broker will start listening on \"192.1.1.8:9094\" with the security protocol \"SSL\".\n"
            + "On the controller side, when it discovers a broker's published endpoints through ZooKeeper, it will use the <code>" + CONTROL_PLANE_LISTENER_NAME_PROP + "</code> "
            + "to find the endpoint, which it will use to establish a connection to the broker.\n"
            + "For example, if the broker's published endpoints on ZooKeeper are:\n"
            + " <code>\"endpoints\" : [\"INTERNAL://broker1.example.com:9092\",\"EXTERNAL://broker1.example.com:9093\",\"CONTROLLER://broker1.example.com:9094\"]</code>\n"
            + " and the controller's config is:\n"
            + "<code>listener.security.protocol.map = INTERNAL:PLAINTEXT, EXTERNAL:SSL, CONTROLLER:SSL\n"
            + "control.plane.listener.name = CONTROLLER</code>\n"
            + "then the controller will use \"broker1.example.com:9094\" with the security protocol \"SSL\" to connect to the broker.\n"
            + "If not explicitly configured, the default value will be null and there will be no dedicated endpoints for controller connections.\n"
            + "If explicitly configured, the value cannot be the same as the value of <code>" + INTER_BROKER_LISTENER_NAME_PROP + "</code>.";

    public static final String SOCKET_SEND_BUFFER_BYTES_DOC = "The SO_SNDBUF buffer of the socket server sockets. If the value is -1, the OS default will be used.";
    public static final String SOCKET_RECEIVE_BUFFER_BYTES_DOC = "The SO_RCVBUF buffer of the socket server sockets. If the value is -1, the OS default will be used.";
    public static final String SOCKET_REQUEST_MAX_BYTES_DOC = "The maximum number of bytes in a socket request";
    public static final String SOCKET_LISTEN_BACKLOG_SIZE_DOC = "The maximum number of pending connections on the socket. "
            + "In Linux, you may also need to configure `somaxconn` and `tcp_max_syn_backlog` kernel parameters "
            + "accordingly to make the configuration take effect.";

    public static final String MAX_CONNECTIONS_PER_IP_DOC = "The maximum number of connections allowed from each IP address. "
            + "This can be set to 0 if there are overrides configured using " + MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP + " property. "
            + "New connections from the IP address are dropped if the limit is reached.";
    public static final String MAX_CONNECTIONS_PER_IP_OVERRIDES_DOC = "A comma-separated list of per-IP or hostname overrides to the default maximum number of connections. "
            + "An example value is \"hostName:100,127.0.0.1:200\"";
    public static final String MAX_CONNECTIONS_DOC = "The maximum number of connections allowed in the broker at any time. "
            + "This limit is applied in addition to any per-IP limits configured using " + MAX_CONNECTIONS_PER_IP_PROP + ". "
            + "Listener-level limits may also be configured by prefixing the config name with the listener prefix, for example, "
            + "<code>listener.name.internal." + MAX_CONNECTIONS_PROP + "</code>. Broker-wide limit should be configured based on broker capacity "
            + "while listener limits should be configured based on application requirements. New connections are blocked if either the listener "
            + "or broker limit is reached. Connections on the inter-broker listener are permitted even if the broker-wide limit is reached. "
            + "The least recently used connection on another listener will be closed in this case.";
    public static final String MAX_CONNECTION_CREATION_RATE_DOC = "The maximum connection creation rate allowed in the broker at any time. "
            + "Listener-level limits may also be configured by prefixing the config name with the listener prefix, for example, "
            + "<code>listener.name.internal." + MAX_CONNECTION_CREATION_RATE_PROP + "</code>. Broker-wide connection rate limit should be configured based on "
            + "broker capacity while listener limits should be configured based on application requirements. New connections will be throttled "
            + "if either the listener or the broker limit is reached, with the exception of the inter-broker listener. Connections on the inter-broker "
            + "listener will be throttled only when the listener-level rate limit is reached.";
    public static final String CONNECTIONS_MAX_IDLE_MS_DOC = "Idle connections timeout: the server socket processor threads close the connections that idle more than this";
    public static final String FAILED_AUTHENTICATION_DELAY_MS_DOC = "Connection close delay on failed authentication: "
            + "this is the time (in milliseconds) by which connection close will be delayed on authentication failure. "
            + "This must be configured to be less than " + CONNECTIONS_MAX_IDLE_MS_PROP + " to prevent connection timeout.";

    /** ********** Rack Configuration **************/
    public static final String RACK_DOC = "Rack of the broker. This will be used in rack-aware replication assignment for fault tolerance. Examples: `RACK1`, `us-east-1d`";

    /** ********* Log Configuration ***********/
    public static final String NUM_PARTITIONS_DOC = "The default number of log partitions per topic";
    public static final String LOG_DIR_DOC = "The directory in which the log data is kept (supplemental for " + LOG_DIRS_PROP + " property)";
    public static final String LOG_DIRS_DOC = "A comma-separated list of the directories where the log data is stored. If not set, the value in "
            + LOG_DIR_PROP + " is used.";
    public static final String LOG_SEGMENT_BYTES_DOC = "The maximum size of a single log file";
    public static final String LOG_ROLL_TIME_MILLIS_DOC = "The maximum time before a new log segment is rolled out (in milliseconds). "
            + "If not set, the value in " + LOG_ROLL_TIME_HOURS_PROP + " is used";
    public static final String LOG_ROLL_TIME_HOURS_DOC = "The maximum time before a new log segment is rolled out (in hours), secondary to "
            + LOG_ROLL_TIME_MILLIS_PROP + " property";
    public static final String LOG_ROLL_TIME_JITTER_MILLIS_DOC = "The maximum jitter to subtract from logRollTimeMillis (in milliseconds). "
            + "If not set, the value in " + LOG_ROLL_TIME_JITTER_HOURS_PROP + " is used";
    public static final String LOG_ROLL_TIME_JITTER_HOURS_DOC = "The maximum jitter to subtract from logRollTimeMillis (in hours), secondary to "
            + LOG_ROLL_TIME_JITTER_MILLIS_PROP + " property";
    public static final String LOG_RETENTION_TIME_MILLIS_DOC = "The number of milliseconds to keep a log file before deleting it (in milliseconds), "
            + "If not set, the value in " + LOG_RETENTION_TIME_MINUTES_PROP + " is used. If set to -1, no time limit is applied.";
    public static final String LOG_RETENTION_TIME_MINS_DOC = "The number of minutes to keep a log file before deleting it (in minutes), secondary to "
            + LOG_RETENTION_TIME_MILLIS_PROP + " property. If not set, the value in " + LOG_RETENTION_TIME_HOURS_PROP + " is used";
    public static final String LOG_RETENTION_TIME_HOURS_DOC = "The number of hours to keep a log file before deleting it (in hours), tertiary to "
            + LOG_RETENTION_TIME_MILLIS_PROP + " property";
    public static final String LOG_RETENTION_BYTES_DOC = "The maximum size of the log before deleting it";
    public static final String LOG_CLEANUP_INTERVAL_MS_DOC = "The frequency in milliseconds that the log cleaner checks whether any log is eligible for deletion";
    public static final String LOG_CLEANUP_POLICY_DOC = "The default cleanup policy for segments beyond the retention window. A comma-separated list of valid policies. "
            + "Valid policies are: \"delete\" and \"compact\"";
    public static final String LOG_CLEANER_THREADS_DOC = "The number of background threads to use for log cleaning";
    public static final String LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_DOC = "The log cleaner will be throttled so that the sum of its read and write I/O will be less "
            + "than this value on average";
    public static final String LOG_CLEANER_DEDUPE_BUFFER_SIZE_DOC = "The total memory used for log deduplication across all cleaner threads";
    public static final String LOG_CLEANER_IO_BUFFER_SIZE_DOC = "The total memory used for log cleaner I/O buffers across all cleaner threads";
    public static final String LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_DOC = "Log cleaner dedupe buffer load factor. The percentage full the dedupe buffer can become. "
            + "A higher value will allow more log to be cleaned at once but will lead to more hash collisions";
    public static final String LOG_CLEANER_BACKOFF_MS_DOC = "The amount of time to sleep when there are no logs to clean";
    public static final String LOG_CLEANER_MIN_CLEAN_RATIO_DOC = "The minimum ratio of dirty log to total log for a log to be eligible for cleaning. "
            + "If the " + LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP + " or the " + LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP
            + " configurations are also specified, then the log compactor considers the log eligible for compaction "
            + "as soon as either: (i) the dirty ratio threshold has been met and the log has had dirty (uncompacted) "
            + "records for at least the " + LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP + " duration, or (ii) if the log has had "
            + "dirty (uncompacted) records for at most the " + LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP + " period.";
    public static final String LOG_CLEANER_ENABLE_DOC = "Enable the log cleaner process to run on the server. Should be enabled if using any topics with a "
            + "cleanup.policy=compact, including the internal offsets topic. If disabled, those topics will not be compacted and will continually grow in size.";
    public static final String LOG_CLEANER_DELETE_RETENTION_MS_DOC = "The amount of time to retain tombstone message markers for log-compacted topics. "
            + "This setting also gives a bound on the time in which a consumer must complete a read if they begin from offset 0 to ensure that they get a valid "
            + "snapshot of the final stage (otherwise tombstone messages may be collected before a consumer completes their scan).";
    public static final String LOG_CLEANER_MIN_COMPACTION_LAG_MS_DOC = "The minimum time a message will remain uncompacted in the log. Only applicable for logs that are being compacted.";
    public static final String LOG_CLEANER_MAX_COMPACTION_LAG_MS_DOC = "The maximum time a message will remain ineligible for compaction in the log. Only applicable for logs that are being compacted.";
    public static final String LOG_INDEX_SIZE_MAX_BYTES_DOC = "The maximum size in bytes of the offset index";
    public static final String LOG_INDEX_INTERVAL_BYTES_DOC = "The interval with which we add an entry to the offset index.";
    public static final String LOG_FLUSH_INTERVAL_MESSAGES_DOC = "The number of messages accumulated on a log partition before messages are flushed to disk.";
    public static final String LOG_DELETE_DELAY_MS_DOC = "The amount of time to wait before deleting a file from the filesystem";
    public static final String LOG_FLUSH_SCHEDULER_INTERVAL_MS_DOC = "The frequency in ms that the log flusher checks whether any log needs to be flushed to disk";
    public static final String LOG_FLUSH_INTERVAL_MS_DOC = "The maximum time in ms that a message in any topic is kept in memory before being flushed to disk. "
            + "If not set, the value in " + LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP + " is used";
    public static final String LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_DOC = "The frequency with which we update the persistent record of the last flush, which acts as the log recovery point.";
    public static final String LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_DOC = "The frequency with which we update the persistent record of the log start offset";
    public static final String LOG_PRE_ALLOCATE_ENABLE_DOC = "Should pre-allocate file when creating a new segment? If you are using Kafka on Windows, you probably need to set it to true.";
    public static final String LOG_MESSAGE_FORMAT_VERSION_DOC = "Specify the message format version the broker will use to append messages to the logs. "
            + "The value should be a valid MetadataVersion. Some examples are: 0.8.2, 0.9.0.0, 0.10.0. Check MetadataVersion for more details. "
            + "By setting a particular message format version, the user is certifying that all the existing messages on disk are smaller or equal than the specified version. "
            + "Setting this value incorrectly will cause consumers with older versions to break, as they will receive messages with a format that they don't understand.";
    public static final String LOG_MESSAGE_TIMESTAMP_TYPE_DOC = "Define whether the timestamp in the message is message create time or log append time. "
            + "The value should be either `CreateTime` or `LogAppendTime`.";
    public static final String LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC = "[DEPRECATED] The maximum difference allowed between the timestamp when a broker receives "
            + "a message and the timestamp specified in the message. If log.message.timestamp.type=CreateTime, a message will be rejected "
            + "if the difference in timestamp exceeds this threshold. This configuration is ignored if log.message.timestamp.type=LogAppendTime."
            + "The maximum timestamp difference allowed should be no greater than log.retention.ms to avoid unnecessarily frequent log rolling.";
    public static final String LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DOC = "This configuration sets the allowable timestamp difference between the "
            + "broker's timestamp and the message timestamp. The message timestamp can be earlier than or equal to the broker's "
            + "timestamp, with the maximum allowable difference determined by the value set in this configuration. "
            + "If log.message.timestamp.type=CreateTime, the message will be rejected if the difference in timestamps exceeds "
            + "this specified threshold. This configuration is ignored if log.message.timestamp.type=LogAppendTime.";
    public static final String LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_DOC = "This configuration sets the allowable timestamp difference between the "
            + "message timestamp and the broker's timestamp. The message timestamp can be later than or equal to the broker's "
            + "timestamp, with the maximum allowable difference determined by the value set in this configuration. "
            + "If log.message.timestamp.type=CreateTime, the message will be rejected if the difference in timestamps exceeds "
            + "this specified threshold. This configuration is ignored if log.message.timestamp.type=LogAppendTime.";
    public static final String NUM_RECOVERY_THREADS_PER_DATA_DIR_DOC = "The number of threads per data directory to be used for log recovery at startup and flushing at shutdown";
    public static final String AUTO_CREATE_TOPICS_ENABLE_DOC = "Enable auto creation of topic on the server.";
    public static final String MIN_IN_SYNC_REPLICAS_DOC = "When a producer sets acks to \"all\" (or \"-1\"), "
            + "<code>min.insync.replicas</code> specifies the minimum number of replicas that must acknowledge "
            + "a write for the write to be considered successful. If this minimum cannot be met, "
            + "then the producer will raise an exception (either <code>NotEnoughReplicas</code> or "
            + "<code>NotEnoughReplicasAfterAppend</code>).<br>When used together, <code>min.insync.replicas</code> and acks "
            + "allow you to enforce greater durability guarantees. A typical scenario would be to "
            + "create a topic with a replication factor of 3, set <code>min.insync.replicas</code> to 2, and "
            + "produce with acks of \"all\". This will ensure that the producer raises an exception "
            + "if a majority of replicas do not receive a write.";
    public static final String CREATE_TOPIC_POLICY_CLASS_NAME_DOC = "The create topic policy class that should be used for validation. The class should " +
            "implement the <code>org.apache.kafka.server.policy.CreateTopicPolicy</code> interface.";
    public static final String ALTER_CONFIG_POLICY_CLASS_NAME_DOC = "The alter configs policy class that should be used for validation. The class should " +
            "implement the <code>org.apache.kafka.server.policy.AlterConfigPolicy</code> interface.";
    public static final String LOG_MESSAGE_DOWN_CONVERSION_ENABLE_DOC = TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_DOC;
    public static final String CONTROLLER_SOCKET_TIMEOUT_MS_DOC = "The socket timeout for controller-to-broker channels.";
    public static final String REPLICATION_FACTOR_DOC = "The default replication factors for automatically created topics.";
    public static final String REPLICA_LAG_TIME_MAX_MS_DOC = "If a follower hasn't sent any fetch requests or hasn't consumed up to the leaders log end offset for at least this time," +
            " the leader will remove the follower from isr";
    public static final String REPLICA_SOCKET_TIMEOUT_MS_DOC = "The socket timeout for network requests. Its value should be at least replica.fetch.wait.max.ms";
    public static final String REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_DOC = "The socket receive buffer for network requests to the leader for replicating data";
    public static final String REPLICA_FETCH_MAX_BYTES_DOC = "The number of bytes of messages to attempt to fetch for each partition. This is not an absolute maximum, " +
            "if the first record batch in the first non-empty partition of the fetch is larger than this value, the record batch will still be returned " +
            "to ensure that progress can be made. The maximum record batch size accepted by the broker is defined via " +
            "<code>message.max.bytes</code> (broker config) or <code>max.message.bytes</code> (topic config).";
    public static final String REPLICA_FETCH_WAIT_MAX_MS_DOC = "The maximum wait time for each fetcher request issued by follower replicas. This value should always be less than the " +
            "replica.lag.time.max.ms at all times to prevent frequent shrinking of ISR for low throughput topics";
    public static final String REPLICA_FETCH_MIN_BYTES_DOC = "Minimum bytes expected for each fetch response. If not enough bytes, wait up to <code>replica.fetch.wait.max.ms</code> (broker config).";
    public static final String REPLICA_FETCH_RESPONSE_MAX_BYTES_DOC = "Maximum bytes expected for the entire fetch response. Records are fetched in batches, " +
            "and if the first record batch in the first non-empty partition of the fetch is larger than this value, the record batch " +
            "will still be returned to ensure that progress can be made. As such, this is not an absolute maximum. The maximum " +
            "record batch size accepted by the broker is defined via <code>message.max.bytes</code> (broker config) or " +
            "<code>max.message.bytes</code> (topic config).";
    public static final String NUM_REPLICA_FETCHERS_DOC = "Number of fetcher threads used to replicate records from each source broker. The total number of fetchers " +
            "on each broker is bound by <code>num.replica.fetchers</code> multiplied by the number of brokers in the cluster." +
            "Increasing this value can increase the degree of I/O parallelism in the follower and leader broker at the cost " +
            "of higher CPU and memory utilization.";
    public static final String REPLICA_FETCH_BACKOFF_MS_DOC = "The amount of time to sleep when fetch partition error occurs.";
    public static final String REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_DOC = "The frequency with which the high watermark is saved out to disk";
    public static final String FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC = "The purge interval (in the number of requests) of the fetch request purgatory";
    public static final String PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC = "The purge interval (in the number of requests) of the producer request purgatory";
    public static final String DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC = "The purge interval (in the number of requests) of the delete records request purgatory";
    public static final String AUTO_LEADER_REBALANCE_ENABLE_DOC = "Enables auto leader balancing. A background thread checks the distribution of partition leaders at regular intervals, configurable by " + LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_PROP + ". If the leader imbalance exceeds " + LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_PROP + ", leader rebalance to the preferred leader for partitions is triggered.";
    public static final String LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_DOC = "The ratio of leader imbalance allowed per broker. The controller would trigger a leader balance if it goes above this value per broker. The value is specified in percentage.";
    public static final String LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_DOC = "The frequency with which the partition rebalance check is triggered by the controller";
    public static final String UNCLEAN_LEADER_ELECTION_ENABLE_DOC = "Indicates whether to enable replicas not in the ISR set to be elected as leader as a last resort, even though doing so may result in data loss";
    public static final String INTER_BROKER_SECURITY_PROTOCOL_DOC = "Security protocol used to communicate between brokers. Valid values are: " +
            String.join(", ", SecurityProtocol.names()) + ". It is an error to set this and " + INTER_BROKER_LISTENER_NAME_PROP + " " +
            "properties at the same time.";

    public static final String INTER_BROKER_PROTOCOL_VERSION_DOC = "Specify which version of the inter-broker protocol will be used.\n" +
            " This is typically bumped after all brokers were upgraded to a new version.\n" +
            " Example of some valid values are: 0.8.0, 0.8.1, 0.8.1.1, 0.8.2, 0.8.2.0, 0.8.2.1, 0.9.0.0, 0.9.0.1 Check MetadataVersion for the full list.";
    public static final String INTER_BROKER_LISTENER_NAME_DOC = "Name of the listener used for communication between brokers. If this is unset, the listener name is defined by " + INTER_BROKER_SECURITY_PROTOCOL_PROP + ". " +
            "It is an error to set this and " + INTER_BROKER_SECURITY_PROTOCOL_PROP + " properties at the same time.";
    public static final String REPLICA_SELECTOR_CLASS_DOC = "The fully qualified class name that implements ReplicaSelector. This is used by the broker to find the preferred read replica. By default, we use an implementation that returns the leader.";

    /** ********* Controlled shutdown configuration ***********/
    public static final String CONTROLLED_SHUTDOWN_MAX_RETRIES_DOC = "Controlled shutdown can fail for multiple reasons. This determines the number of retries when such failure happens";
    public static final String CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_DOC = "Before each retry, the system needs time to recover from the state that caused the previous failure (Controller fail over, replica lag etc). This config determines the amount of time to wait before retrying.";
    public static final String CONTROLLED_SHUTDOWN_ENABLE_DOC = "Enable controlled shutdown of the server.";

    /** ********* Group coordinator configuration ***********/
    public static final String GROUP_MIN_SESSION_TIMEOUT_MS_DOC = "The minimum allowed session timeout for registered consumers. Shorter timeouts result in quicker failure detection at the cost of more frequent consumer heartbeating, which can overwhelm broker resources.";
    public static final String GROUP_MAX_SESSION_TIMEOUT_MS_DOC = "The maximum allowed session timeout for registered consumers. Longer timeouts give consumers more time to process messages in between heartbeats at the cost of a longer time to detect failures.";
    public static final String GROUP_INITIAL_REBALANCE_DELAY_MS_DOC = "The amount of time the group coordinator will wait for more consumers to join a new group before performing the first rebalance. A longer delay means potentially fewer rebalances, but increases the time until processing begins.";
    public static final String GROUP_MAX_SIZE_DOC = "The maximum number of consumers that a single consumer group can accommodate.";

    /** *********  New group coordinator configs *********/
    public static final String NEW_GROUP_COORDINATOR_ENABLE_DOC = "Enable the new group coordinator.";
    public static final String GROUP_COORDINATOR_NUM_THREADS_DOC = "The number of threads used by the group coordinator.";

    /** ********* Consumer group configs *********/
    public static final String CONSUMER_GROUP_SESSION_TIMEOUT_MS_DOC = "The timeout to detect client failures when using the consumer group protocol.";
    public static final String CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DOC = "The minimum allowed session timeout for registered consumers.";
    public static final String CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DOC = "The maximum allowed session timeout for registered consumers.";
    public static final String CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DOC = "The heartbeat interval given to the members of a consumer group.";
    public static final String CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DOC = "The minimum heartbeat interval for registered consumers.";
    public static final String CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DOC = "The maximum heartbeat interval for registered consumers.";
    public static final String CONSUMER_GROUP_MAX_SIZE_DOC = "The maximum number of consumers that a single consumer group can accommodate.";
    public static final String CONSUMER_GROUP_ASSIGNORS_DOC = "The server-side assignors as a list of full class names. The first one in the list is considered as the default assignor to be used in the case where the consumer does not specify an assignor.";

    /** ********* Offset management configuration ***********/
    public static final String OFFSET_METADATA_MAX_SIZE_DOC = "The maximum size for a metadata entry associated with an offset commit.";
    public static final String OFFSETS_LOAD_BUFFER_SIZE_DOC = "Batch size for reading from the offsets segments when loading offsets into the cache (soft-limit, overridden if records are too large).";
    public static final String OFFSETS_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor for the offsets topic (set higher to ensure availability). " +
            "Internal topic creation will fail until the cluster size meets this replication factor requirement.";
    public static final String OFFSETS_TOPIC_PARTITIONS_DOC = "The number of partitions for the offset commit topic (should not change after deployment).";
    public static final String OFFSETS_TOPIC_SEGMENT_BYTES_DOC = "The offsets topic segment bytes should be kept relatively small to facilitate faster log compaction and cache loads.";
    public static final String OFFSETS_TOPIC_COMPRESSION_CODEC_DOC = "Compression codec for the offsets topic - compression may be used to achieve \"atomic\" commits.";
    public static final String OFFSETS_RETENTION_MINUTES_DOC = "For subscribed consumers, committed offset of a specific partition will be expired and discarded when 1) this retention period has elapsed after the consumer group loses all its consumers (i.e. becomes empty); " +
            "2) this retention period has elapsed since the last time an offset is committed for the partition and the group is no longer subscribed to the corresponding topic. " +
            "For standalone consumers (using manual assignment), offsets will be expired after this retention period has elapsed since the time of last commit. " +
            "Note that when a group is deleted via the delete-group request, its committed offsets will also be deleted without an extra retention period; " +
            "also when a topic is deleted via the delete-topic request, upon propagated metadata update any group's committed offsets for that topic will also be deleted without an extra retention period.";
    public static final String OFFSETS_RETENTION_CHECK_INTERVAL_MS_DOC = "Frequency at which to check for stale offsets";
    public static final String OFFSET_COMMIT_TIMEOUT_MS_DOC = "Offset commit will be delayed until all replicas for the offsets topic receive the commit " +
            "or this timeout is reached. This is similar to the producer request timeout.";
    public static final String OFFSET_COMMIT_REQUIRED_ACKS_DOC = "The required acks before the commit can be accepted. In general, the default (-1) should not be overridden.";

    /** ********* Transaction management configuration ***********/
    public static final String TRANSACTIONAL_ID_EXPIRATION_MS_DOC = "The time in ms that the transaction coordinator will wait without receiving any transaction status updates " +
            "for the current transaction before expiring its transactional id. Transactional IDs will not expire while a transaction is still ongoing.";
    public static final String TRANSACTIONS_MAX_TIMEOUT_MS_DOC = "The maximum allowed timeout for transactions. " +
            "If a client’s requested transaction time exceeds this, then the broker will return an error in InitProducerIdRequest. This prevents a client from too large of a timeout, which can stall consumers reading from topics included in the transaction.";
    public static final String TRANSACTIONS_TOPIC_MIN_ISR_DOC = "Overridden " + MIN_IN_SYNC_REPLICAS_PROP + " config for the transaction topic.";
    public static final String TRANSACTIONS_LOAD_BUFFER_SIZE_DOC = "Batch size for reading from the transaction log segments when loading producer ids and transactions into the cache (soft-limit, overridden if records are too large).";
    public static final String TRANSACTIONS_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor for the transaction topic (set higher to ensure availability). " +
            "Internal topic creation will fail until the cluster size meets this replication factor requirement.";
    public static final String TRANSACTIONS_TOPIC_PARTITIONS_DOC = "The number of partitions for the transaction topic (should not change after deployment).";
    public static final String TRANSACTIONS_TOPIC_SEGMENT_BYTES_DOC = "The transaction topic segment bytes should be kept relatively small to facilitate faster log compaction and cache loads.";
    public static final String TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTIONS_INTERVAL_MS_DOC = "The interval at which to rollback transactions that have timed out.";
    public static final String TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONS_INTERVAL_MS_DOC = "The interval at which to remove transactions that have expired due to <code>transactional.id.expiration.ms</code> passing";

    public static final String TRANSACTION_PARTITION_VERIFICATION_ENABLE_DOC = "Enable verification that checks that the partition has been added to the transaction before writing transactional records to the partition";

    public static final String PRODUCER_ID_EXPIRATION_MS_DOC = "The time in ms that a topic partition leader will wait before expiring producer IDs. Producer IDs will not expire while a transaction associated with them is still ongoing. " +
            "Note that producer IDs may expire sooner if the last write from the producer ID is deleted due to the topic's retention settings. Setting this value the same or higher than " +
            "<code>delivery.timeout.ms</code> can help prevent expiration during retries and protect against message duplication, but the default should be reasonable for most use cases.";
    public static final String PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DOC = "The interval at which to remove producer IDs that have expired due to <code>producer.id.expiration.ms</code> passing.";

    /** ********* Fetch Configuration ******/
    public static final String MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_DOC = "The maximum number of incremental fetch sessions that we will maintain.";
    public static final String FETCH_MAX_BYTES_DOC = "The maximum number of bytes we will return for a fetch request. Must be at least 1024.";

    /** ********* Quota Configuration ***********/
    public static final String NUM_QUOTA_SAMPLES_DOC = "The number of samples to retain in memory for client quotas.";
    public static final String NUM_REPLICATION_QUOTA_SAMPLES_DOC = "The number of samples to retain in memory for replication quotas.";
    public static final String NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_DOC = "The number of samples to retain in memory for alter log dirs replication quotas.";
    public static final String NUM_CONTROLLER_QUOTA_SAMPLES_DOC = "The number of samples to retain in memory for controller mutation quotas.";
    public static final String QUOTA_WINDOW_SIZE_SECONDS_DOC = "The time span of each sample for client quotas.";
    public static final String REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_DOC = "The time span of each sample for replication quotas.";
    public static final String ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_DOC = "The time span of each sample for alter log dirs replication quotas.";
    public static final String CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_DOC = "The time span of each sample for controller mutations quotas.";

    public static final String CLIENT_QUOTA_CALLBACK_CLASS_DOC = "The fully qualified name of a class that implements the ClientQuotaCallback interface, " +
            "which is used to determine quota limits applied to client requests. By default, the &lt;user&gt; and &lt;client-id&gt; " +
            "quotas that are stored in ZooKeeper are applied. For any given request, the most specific quota that matches the user principal " +
            "of the session and the client-id of the request is applied.";

    public static final String DELETE_TOPIC_ENABLE_DOC = "Enables delete topic. Delete topic through the admin tool will have no effect if this config is turned off.";
    public static final String COMPRESSION_TYPE_DOC = "Specify the final compression type for a given topic. This configuration accepts the standard compression codecs " +
            "('gzip', 'snappy', 'lz4', 'zstd'). It additionally accepts 'uncompressed' which is equivalent to no compression; and " +
            "'producer' which means retain the original compression codec set by the producer.";

    /** ********* Kafka Metrics Configuration ***********/
    public static final String METRIC_SAMPLE_WINDOW_MS_DOC = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC;
    public static final String METRIC_NUM_SAMPLES_DOC = CommonClientConfigs.METRICS_NUM_SAMPLES_DOC;
    public static final String METRIC_REPORTER_CLASSES_DOC = CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC;
    public static final String METRIC_RECORDING_LEVEL_DOC = CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC;
    public static final String AUTO_INCLUDE_JMX_REPORTER_DOC = CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_DOC;
    /** ********* Kafka Yammer Metrics Reporter Configuration ***********/
    public static final String KAFKA_METRICS_REPORTER_CLASSES_DOC = "A list of classes to use as Yammer metrics custom reporters." +
            " The reporters should implement <code>kafka.metrics.KafkaMetricsReporter</code> trait. If a client wants" +
            " to expose JMX operations on a custom reporter, the custom reporter needs to additionally implement an MBean" +
            " trait that extends <code>kafka.metrics.KafkaMetricsReporterMBean</code> trait so that the registered MBean is compliant with" +
            " the standard MBean convention.";

    public static final String KAFKA_METRICS_POLLING_INTERVAL_SECONDS_DOC = "The metrics polling interval (in seconds) which can be used in" +
            KAFKA_METRICS_REPORTER_CLASSES_PROP + " implementations";

    /** ********* Kafka Client Telemetry Metrics Configuration ***********/
    public static final String CLIENT_TELEMETRY_MAX_BYTES_DOC = "The maximum size (after compression if compression is used) of" +
            " telemetry metrics pushed from a client to the broker. The default value is 1048576 (1 MB).";

    /** ********* Common Security Configuration *********/
    public static final String PRINCIPAL_BUILDER_CLASS_DOC = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DOC;
    public static final String CONNECTIONS_MAX_REAUTH_MS_DOC = BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_DOC;
    public static final String SASL_SERVER_MAX_RECEIVE_SIZE_DOC = BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_DOC;
    public static final String SECURITY_PROVIDER_CLASS_DOC = SecurityConfig.SECURITY_PROVIDERS_DOC;

    /** ********* SSL Configuration ********/
    public static final String SSL_PROTOCOL_DOC = SslConfigs.SSL_PROTOCOL_DOC;
    public static final String SSL_PROVIDER_DOC = SslConfigs.SSL_PROVIDER_DOC;
    public static final String SSL_CIPHER_SUITES_DOC = SslConfigs.SSL_CIPHER_SUITES_DOC;
    public static final String SSL_ENABLED_PROTOCOLS_DOC = SslConfigs.SSL_ENABLED_PROTOCOLS_DOC;
    public static final String SSL_KEYSTORE_TYPE_DOC = SslConfigs.SSL_KEYSTORE_TYPE_DOC;
    public static final String SSL_KEYSTORE_LOCATION_DOC = SslConfigs.SSL_KEYSTORE_LOCATION_DOC;
    public static final String SSL_KEYSTORE_PASSWORD_DOC = SslConfigs.SSL_KEYSTORE_PASSWORD_DOC;
    public static final String SSL_KEY_PASSWORD_DOC = SslConfigs.SSL_KEY_PASSWORD_DOC;
    public static final String SSL_KEYSTORE_KEY_DOC = SslConfigs.SSL_KEYSTORE_KEY_DOC;
    public static final String SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC = SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC;
    public static final String SSL_TRUSTSTORE_TYPE_DOC = SslConfigs.SSL_TRUSTSTORE_TYPE_DOC;
    public static final String SSL_TRUSTSTORE_PASSWORD_DOC = SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC;
    public static final String SSL_TRUSTSTORE_LOCATION_DOC = SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC;
    public static final String SSL_TRUSTSTORE_CERTIFICATES_DOC = SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_DOC;
    public static final String SSL_KEYMANAGER_ALGORITHM_DOC = SslConfigs.SSL_KEYMANAGER_ALGORITHM_DOC;
    public static final String SSL_TRUSTMANAGER_ALGORITHM_DOC = SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC;
    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC = SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC;
    public static final String SSL_SECURE_RANDOM_IMPLEMENTATION_DOC = SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC;
    public static final String SSL_CLIENT_AUTH_DOC = BrokerSecurityConfigs.SSL_CLIENT_AUTH_DOC;
    public static final String SSL_PRINCIPAL_MAPPING_RULES_DOC = BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_DOC;
    public static final String SSL_ENGINE_FACTORY_CLASS_DOC = SslConfigs.SSL_ENGINE_FACTORY_CLASS_DOC;

    /** ********* Sasl Configuration ********/
    public static final String SASL_MECHANISM_INTER_BROKER_PROTOCOL_DOC = "SASL mechanism used for inter-broker communication. Default is GSSAPI.";
    public static final String SASL_JAAS_CONFIG_DOC = SaslConfigs.SASL_JAAS_CONFIG_DOC;
    public static final String SASL_ENABLED_MECHANISMS_DOC = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_DOC;
    public static final String SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC = BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC;
    public static final String SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC = SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC;
    public static final String SASL_LOGIN_CLASS_DOC = SaslConfigs.SASL_LOGIN_CLASS_DOC;
    public static final String SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC;
    public static final String SASL_KERBEROS_SERVICE_NAME_DOC = SaslConfigs.SASL_KERBEROS_SERVICE_NAME_DOC;
    public static final String SASL_KERBEROS_KINIT_CMD_DOC = SaslConfigs.SASL_KERBEROS_KINIT_CMD_DOC;
    public static final String SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC;
    public static final String SASL_KERBEROS_TICKET_RENEW_JITTER_DOC = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DOC;
    public static final String SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC = SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC;
    public static final String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC = BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC;
    public static final String SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC;
    public static final String SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC;
    public static final String SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC = SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC;
    public static final String SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC = SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC;

    public static final String SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC = SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC;
    public static final String SASL_LOGIN_READ_TIMEOUT_MS_DOC = SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS_DOC;
    public static final String SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC;
    public static final String SASL_LOGIN_RETRY_BACKOFF_MS_DOC = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS_DOC;
    public static final String SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC = SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC;
    public static final String SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC = SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC;
    public static final String SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC = SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC;
    public static final String SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC;
    public static final String SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC;
    public static final String SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC;
    public static final String SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC;
    public static final String SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC = SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC;
    public static final String SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC;
    public static final String SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC;

    /** ********* Delegation Token Configuration ********/
    public static final String DELEGATION_TOKEN_SECRET_KEY_ALIAS_DOC = "DEPRECATED: An alias for " + DELEGATION_TOKEN_SECRET_KEY_PROP + ", which should be used instead of this config.";
    public static final String DELEGATION_TOKEN_SECRET_KEY_DOC = "Secret key to generate and verify delegation tokens. The same key must be configured across all the brokers. " +
            " If using Kafka with KRaft, the key must also be set across all controllers. " +
            " If the key is not set or set to an empty string, brokers will disable the delegation token support.";
    public static final String DELEGATION_TOKEN_MAX_LIFE_TIME_DOC = "The token has a maximum lifetime beyond which it cannot be renewed anymore. Default value 7 days.";
    public static final String DELEGATION_TOKEN_EXPIRY_TIME_MS_DOC = "The token validity time in milliseconds before the token needs to be renewed. Default value 1 day.";
    public static final String DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_DOC = "Scan interval to remove expired delegation tokens.";

    /** ********* Password encryption configuration for dynamic configs *********/
    public static final String PASSWORD_ENCODER_SECRET_DOC = "The secret used for encoding dynamically configured passwords for this broker.";
    public static final String PASSWORD_ENCODER_OLD_SECRET_DOC = "The old secret that was used for encoding dynamically configured passwords. " +
            "This is required only when the secret is updated. If specified, all dynamically encoded passwords are " +
            "decoded using this old secret and re-encoded using " + PASSWORD_ENCODER_SECRET_PROP + " when the broker starts up.";
    public static final String PASSWORD_ENCODER_KEY_FACTORY_ALGORITHM_DOC = "The SecretKeyFactory algorithm used for encoding dynamically configured passwords. " +
            "Default is PBKDF2WithHmacSHA512 if available and PBKDF2WithHmacSHA1 otherwise.";
    public static final String PASSWORD_ENCODER_CIPHER_ALGORITHM_DOC = "The Cipher algorithm used for encoding dynamically configured passwords.";
    public static final String PASSWORD_ENCODER_KEY_LENGTH_DOC = "The key length used for encoding dynamically configured passwords.";
    public static final String PASSWORD_ENCODER_ITERATIONS_DOC = "The iteration count used for encoding dynamically configured passwords.";

    public static void main(String[] args) {
        System.out.println(CONFIG_DEF.toHtml(4, config -> "brokerconfigs_" + config, DynamicBrokerConfigBaseManager.dynamicConfigUpdateModes()));
    }

    public static Optional<String> zooKeeperClientProperty(ZKClientConfig clientConfig, String kafkaPropName) {
        return Optional.ofNullable(clientConfig.getProperty(zkSslConfigToSystemPropertyMap().get(kafkaPropName)));
    }

    public static void setZooKeeperClientProperty(ZKClientConfig clientConfig, String kafkaPropName, Object kafkaPropValue) {
        clientConfig.setProperty(zkSslConfigToSystemPropertyMap().get(kafkaPropName),
                (kafkaPropName.equals(ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP) && kafkaPropValue.toString().equalsIgnoreCase("HTTPS"))
                        ? String.valueOf(kafkaPropValue.toString().equalsIgnoreCase("HTTPS"))
                        : (kafkaPropName.equals(ZK_SSL_ENABLED_PROTOCOLS_PROP) || kafkaPropName.equals(ZK_SSL_CIPHER_SUITES_PROP))
                        ? ((List<?>) kafkaPropValue).stream().map(Object::toString).collect(Collectors.joining(","))
                        : kafkaPropValue.toString());
    }

    public final static Map<String, String> zkSslConfigToSystemPropertyMap() {
        return ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP;
    }
    // For ZooKeeper TLS client authentication to be enabled, the client must (at a minimum)
    // configure itself as using TLS with both a client connection socket and a key store location explicitly set.
    public static boolean zkTlsClientAuthEnabled(ZKClientConfig zkClientConfig) {
        return zooKeeperClientProperty(zkClientConfig, ZK_SSL_CLIENT_ENABLE_PROP).filter(value -> value.equals("true")).isPresent() &&
                zooKeeperClientProperty(zkClientConfig, ZK_CLIENT_CNXN_SOCKET_PROP).isPresent() &&
                zooKeeperClientProperty(zkClientConfig, ZK_SSL_KEY_STORE_LOCATION_PROP).isPresent();
    }

    public static List<String> configNames() {
        return new ArrayList<>(CONFIG_DEF.names());
    }

    public static Map<String, Object> defaultValues() {
        return CONFIG_DEF.defaultValues();
    }

    public static Map<String, ConfigDef.ConfigKey> configKeys() {
        return CONFIG_DEF.configKeys();
    }

    public static Optional<ConfigDef.Type> configType(String configName) {
        Optional<ConfigDef.Type> configType = configTypeExact(configName);
        if (configType.isPresent()) {
            return configType;
        }

        Optional<ConfigDef.Type> typeFromTypeOf = getTypeOf(configName);
        if (typeFromTypeOf.isPresent()) {
            return typeFromTypeOf;
        }

        return DynamicBrokerConfigBaseManager.brokerConfigSynonyms(configName, true)
                .stream()
                .findFirst()
                .flatMap(conf -> getTypeOf(conf));
    }

    private static Optional<ConfigDef.Type> getTypeOf(String name) {
        Map<String, ConfigDef.ConfigKey> configKeys = CONFIG_DEF.configKeys();
        return Optional.ofNullable(configKeys.get(name)).map(configKey -> configKey.type);
    }

    private static Optional<ConfigDef.Type> configTypeExact(String exactName) {
        return getTypeOf(exactName)
                .map(Optional::of)
                .orElseGet(() -> {
                    Map<String, ConfigDef.ConfigKey> configKeys = DynamicConfig.Broker.BROKER_CONFIG_DEF.configKeys();
                    return Optional.ofNullable(configKeys.get(exactName)).map(key -> key.type);
                });
    }

    public static boolean maybeSensitive(Optional<ConfigDef.Type> configType) {
        // If we can't determine the config entry type, treat it as a sensitive config to be safe
        return !configType.isPresent() || configType.get() == ConfigDef.Type.PASSWORD;
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

    @SuppressWarnings("deprecation")
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        /** ********* Zookeeper Configuration ***********/
        .define(ZK_CONNECT_PROP, STRING, null, HIGH, ZK_CONNECT_DOC)
        .define(ZK_SESSION_TIMEOUT_MS_PROP, INT, Defaults.ZK_SESSION_TIMEOUT_MS, HIGH, ZK_SESSION_TIMEOUT_MS_DOC)
        .define(ZK_CONNECTION_TIMEOUT_MS_PROP, INT, null, HIGH, ZK_CONNECTION_TIMEOUT_MS_DOC)
        .define(ZK_ENABLE_SECURE_ACLS_PROP, BOOLEAN, Defaults.ZK_ENABLE_SECURE_ACLS, HIGH, ZK_ENABLE_SECURE_ACLS_DOC)
        .define(ZK_MAX_IN_FLIGHT_REQUESTS_PROP, INT, Defaults.ZK_MAX_IN_FLIGHT_REQUESTS, atLeast(1), HIGH, ZK_MAX_IN_FLIGHT_REQUESTS_DOC)
        .define(ZK_SSL_CLIENT_ENABLE_PROP, BOOLEAN, Defaults.ZK_SSL_CLIENT_ENABLE, MEDIUM, ZK_SSL_CLIENT_ENABLE_DOC)
        .define(ZK_CLIENT_CNXN_SOCKET_PROP, STRING, null, MEDIUM, ZK_CLIENT_CNXN_SOCKET_DOC)
        .define(ZK_SSL_KEY_STORE_LOCATION_PROP, STRING, null, MEDIUM, ZK_SSL_KEY_STORE_LOCATION_DOC)
        .define(ZK_SSL_KEY_STORE_PASSWORD_PROP, PASSWORD, null, MEDIUM, ZK_SSL_KEY_STORE_PASSWORD_DOC)
        .define(ZK_SSL_KEY_STORE_TYPE_PROP, STRING, null, MEDIUM, ZK_SSL_KEY_STORE_TYPE_DOC)
        .define(ZK_SSL_TRUST_STORE_LOCATION_PROP, STRING, null, MEDIUM, ZK_SSL_TRUST_STORE_LOCATION_DOC)
        .define(ZK_SSL_TRUST_STORE_PASSWORD_PROP, PASSWORD, null, MEDIUM, ZK_SSL_TRUST_STORE_PASSWORD_DOC)
        .define(ZK_SSL_TRUST_STORE_TYPE_PROP, STRING, null, MEDIUM, ZK_SSL_TRUST_STORE_TYPE_DOC)
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
        /** ********* KRaft mode configs ********* **/
        .define(METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_PROP, LONG, Defaults.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES, atLeast(1), HIGH, METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_DOC)
        .define(METADATA_SNAPSHOT_MAX_INTERVAL_MS_PROP, LONG, Defaults.METADATA_SNAPSHOT_MAX_INTERVAL_MS, atLeast(0), HIGH, METADATA_SNAPSHOT_MAX_INTERVAL_MS_DOC)
        .define(PROCESS_ROLES_PROP, LIST, Collections.emptyList(), ConfigDef.ValidList.in("broker", "controller"), HIGH, PROCESS_ROLES_DOC)
        .define(NODE_ID_PROP, INT, Defaults.EMPTY_NODE_ID, null, HIGH, NODE_ID_DOC)
        .define(INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_PROP, INT, Defaults.INITIAL_BROKER_REGISTRATION_TIMEOUT_MS, null, MEDIUM, INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_DOC)
        .define(BROKER_HEARTBEAT_INTERVAL_MS_PROP, INT, Defaults.BROKER_HEARTBEAT_INTERVAL_MS, null, MEDIUM, BROKER_HEARTBEAT_INTERVAL_MS_DOC)
        .define(BROKER_SESSION_TIMEOUT_MS_PROP, INT, Defaults.BROKER_SESSION_TIMEOUT_MS, null, MEDIUM, BROKER_SESSION_TIMEOUT_MS_DOC)
        .define(CONTROLLER_LISTENER_NAMES_PROP, STRING, null, null, HIGH, CONTROLLER_LISTENER_NAMES_DOC)
        .define(SASL_MECHANISM_CONTROLLER_PROTOCOL_PROP, STRING, SaslConfigs.SASL_MECHANISM, null, HIGH, SASL_MECHANISM_CONTROLLER_PROTOCOL_DOC)
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

        /************* Authorizer Configuration ***********/
        .define(AUTHORIZER_CLASS_NAME_PROP, STRING, Defaults.AUTHORIZER_CLASS_NAME, new ConfigDef.NonNullValidator(), LOW, AUTHORIZER_CLASS_NAME_DOC)
        .define(EARLY_START_LISTENERS_PROP, STRING, null, HIGH, EARLY_START_LISTENERS_DOC)

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
        .define(LOG_CLEANER_THREADS_PROP, INT, Defaults.LOG_CLEANER_THREADS, atLeast(0), MEDIUM, LOG_CLEANER_THREADS_DOC)
        .define(LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP, DOUBLE, Defaults.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND, MEDIUM, LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_DOC)
        .define(LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, LONG, Defaults.LOG_CLEANER_DEDUPE_BUFFER_SIZE, MEDIUM, LOG_CLEANER_DEDUPE_BUFFER_SIZE_DOC)
        .define(LOG_CLEANER_IO_BUFFER_SIZE_PROP, INT, Defaults.LOG_CLEANER_IO_BUFFER_SIZE, atLeast(0), MEDIUM, LOG_CLEANER_IO_BUFFER_SIZE_DOC)
        .define(LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP, DOUBLE, Defaults.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR, MEDIUM, LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_DOC)
        .define(LOG_CLEANER_BACKOFF_MS_PROP, LONG, Defaults.LOG_CLEANER_BACKOFF_MS, atLeast(0), MEDIUM, LOG_CLEANER_BACKOFF_MS_DOC)
        .define(LOG_CLEANER_MIN_CLEAN_RATIO_PROP, DOUBLE, LogConfig.DEFAULT_MIN_CLEANABLE_DIRTY_RATIO, between(0, 1), MEDIUM, LOG_CLEANER_MIN_CLEAN_RATIO_DOC)
        .define(LOG_CLEANER_ENABLE_PROP, BOOLEAN, Defaults.LOG_CLEANER_ENABLE, MEDIUM, LOG_CLEANER_ENABLE_DOC)
        .define(LOG_CLEANER_DELETE_RETENTION_MS_PROP, LONG, LogConfig.DEFAULT_DELETE_RETENTION_MS, atLeast(0), MEDIUM, LOG_CLEANER_DELETE_RETENTION_MS_DOC)
        .define(LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP, LONG, LogConfig.DEFAULT_MIN_COMPACTION_LAG_MS, atLeast(0), MEDIUM, LOG_CLEANER_MIN_COMPACTION_LAG_MS_DOC)
        .define(LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP, LONG, LogConfig.DEFAULT_MAX_COMPACTION_LAG_MS, atLeast(1), MEDIUM, LOG_CLEANER_MAX_COMPACTION_LAG_MS_DOC)
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
        .define(LOG_MESSAGE_TIMESTAMP_TYPE_PROP, STRING, LogConfig.DEFAULT_MESSAGE_TIMESTAMP_TYPE, in("CreateTime", "LogAppendTime"), MEDIUM, LOG_MESSAGE_TIMESTAMP_TYPE_DOC)
        .define(LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_PROP, LONG, LogConfig.DEFAULT_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS, atLeast(0), MEDIUM, LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC)
        .define(LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_PROP, LONG, LogConfig.DEFAULT_MESSAGE_TIMESTAMP_BEFORE_MAX_MS, atLeast(0), MEDIUM, LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DOC)
        .define(LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_PROP, LONG, LogConfig.DEFAULT_MESSAGE_TIMESTAMP_AFTER_MAX_MS, atLeast(0), MEDIUM, LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_DOC)
        .define(CREATE_TOPIC_POLICY_CLASS_NAME_PROP, CLASS, null, LOW, CREATE_TOPIC_POLICY_CLASS_NAME_DOC)
        .define(ALTER_CONFIG_POLICY_CLASS_NAME_PROP, CLASS, null, LOW, ALTER_CONFIG_POLICY_CLASS_NAME_DOC)
        .define(LOG_MESSAGE_DOWN_CONVERSION_ENABLE_PROP, BOOLEAN, LogConfig.DEFAULT_MESSAGE_DOWNCONVERSION_ENABLE, LOW, LOG_MESSAGE_DOWN_CONVERSION_ENABLE_DOC)

        /** ********* Replication configuration ***********/
        .define(CONTROLLER_SOCKET_TIMEOUT_MS_PROP, INT, Defaults.CONTROLLER_SOCKET_TIMEOUT_MS, MEDIUM, CONTROLLER_SOCKET_TIMEOUT_MS_DOC)
        .define(DEFAULT_REPLICATION_FACTOR_PROP, INT, Defaults.REPLICATION_FACTOR, MEDIUM, REPLICATION_FACTOR_DOC)
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
        .define(INTER_BROKER_SECURITY_PROTOCOL_PROP, STRING, Defaults.INTER_BROKER_SECURITY_PROTOCOL, in(Utils.enumOptions(SecurityProtocol.class)), MEDIUM, INTER_BROKER_SECURITY_PROTOCOL_DOC)
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

        /** ********* New group coordinator configs *********/
        // All properties are kept internal until KIP-848 is released.
        // This property is meant to be here only during the development of KIP-848. It will
        // be replaced by a metadata version before releasing it.
        .defineInternal(NEW_GROUP_COORDINATOR_ENABLE_PROP, BOOLEAN, Defaults.NEW_GROUP_COORDINATOR_ENABLE, null, MEDIUM, NEW_GROUP_COORDINATOR_ENABLE_DOC)
        .defineInternal(GROUP_COORDINATOR_NUM_THREADS_PROP, INT, Defaults.GROUP_COORDINATOR_NUM_THREADS, atLeast(1), MEDIUM, GROUP_COORDINATOR_NUM_THREADS_DOC)

        /** ********* Consumer groups configs *********/
        // All properties are kept internal until KIP-848 is released.
        .defineInternal(CONSUMER_GROUP_SESSION_TIMEOUT_MS_PROP, INT, Defaults.CONSUMER_GROUP_SESSION_TIMEOUT_MS, atLeast(1), MEDIUM, CONSUMER_GROUP_SESSION_TIMEOUT_MS_DOC)
        .defineInternal(CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_PROP, INT, Defaults.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS, atLeast(1), MEDIUM, CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DOC)
        .defineInternal(CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_PROP, INT, Defaults.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS, atLeast(1), MEDIUM, CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DOC)
        .defineInternal(CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_PROP, INT, Defaults.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS, atLeast(1), MEDIUM, CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DOC)
        .defineInternal(CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_PROP, INT, Defaults.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS, atLeast(1), MEDIUM, CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DOC)
        .defineInternal(CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_PROP, INT, Defaults.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS, atLeast(1), MEDIUM, CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DOC)
        .defineInternal(CONSUMER_GROUP_MAX_SIZE_PROP, INT, Defaults.CONSUMER_GROUP_MAX_SIZE, atLeast(1), MEDIUM, CONSUMER_GROUP_MAX_SIZE_DOC)
        .defineInternal(CONSUMER_GROUP_ASSIGNORS_PROP, LIST, Defaults.CONSUMER_GROUP_ASSIGNORS, null, MEDIUM, CONSUMER_GROUP_ASSIGNORS_DOC)

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
        .define(COMPRESSION_TYPE_PROP, STRING, LogConfig.DEFAULT_COMPRESSION_TYPE, in(BrokerCompressionType.names().toArray(new String[0])), HIGH, COMPRESSION_TYPE_DOC)

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

        /** ********* Kafka Metrics Configuration ***********/
        .define(METRIC_NUM_SAMPLES_PROP, INT, Defaults.METRIC_NUM_SAMPLES, atLeast(1), LOW, METRIC_NUM_SAMPLES_DOC)
        .define(METRIC_SAMPLE_WINDOW_MS_PROP, LONG, Defaults.METRIC_SAMPLE_WINDOW_MS, atLeast(1), LOW, METRIC_SAMPLE_WINDOW_MS_DOC)
        .define(METRIC_REPORTER_CLASSES_PROP, LIST, Defaults.METRIC_REPORTER_CLASSES, LOW, METRIC_REPORTER_CLASSES_DOC)
        .define(METRIC_RECORDING_LEVEL_PROP, STRING, Defaults.METRIC_RECORDING_LEVEL, LOW, METRIC_RECORDING_LEVEL_DOC)
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
        .define(SASL_SERVER_MAX_RECEIVE_SIZE_PROP, INT, Defaults.SERVER_MAX_MAX_RECEIVE_SIZE, MEDIUM, SASL_SERVER_MAX_RECEIVE_SIZE_DOC)
        .define(SECURITY_PROVIDER_CLASS_PROP, STRING, null, LOW, SECURITY_PROVIDER_CLASS_DOC)

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
        .define(SSL_CLIENT_AUTH_PROP, STRING, Defaults.SSL_CLIENT_AUTHENTICATION, in(Defaults.SSL_CLIENT_AUTHENTICATION_VALID_VALUES), MEDIUM, SSL_CLIENT_AUTH_DOC)
        .define(SSL_CIPHER_SUITES_PROP, LIST, Collections.emptyList(), MEDIUM, SSL_CIPHER_SUITES_DOC)
        .define(SSL_PRINCIPAL_MAPPING_RULES_PROP, STRING, Defaults.SSL_PRINCIPAL_MAPPING_RULES, LOW, SSL_PRINCIPAL_MAPPING_RULES_DOC)
        .define(SSL_ENGINE_FACTORY_CLASS_PROP, CLASS, null, LOW, SSL_ENGINE_FACTORY_CLASS_DOC)

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

        /** ********* Internal Configurations *********/
        /** This indicates whether unreleased APIs should be advertised by this broker.**/
        .defineInternal(UNSTABLE_API_VERSIONS_ENABLE_PROP, BOOLEAN, false, LOW)
         // This indicates whether unreleased MetadataVersions should be enabled on this node.
        .defineInternal(UNSTABLE_METADATA_VERSIONS_ENABLE_PROP, BOOLEAN, false, HIGH);

    public final static ConfigDef configDef() {
        return CONFIG_DEF;
    }

    static {
        /** ********* Remote Log Management Configuration *********/
        RemoteLogManagerConfig.CONFIG_DEF.configKeys().values().forEach(configKey -> CONFIG_DEF.define(configKey));
    }

    // Cache the current config to avoid acquiring read lock to access from dynamicConfig
    private volatile KafkaConfig currentConfig = this;
    public final Set<KafkaRaftServer.ProcessRole> processRoles() {
        return parseProcessRoles();
    }
    public DynamicBrokerConfigBaseManager dynamicConfig() {
        if (dynamicConfigOverride == null) {
            dynamicConfigOverride = dynamicConfigOverrideProvider.init(this);
        }
        return dynamicConfigOverride;
    }

    public void updateCurrentConfig(KafkaConfig newConfig) {
        this.currentConfig = newConfig;
    }

    // The following captures any system properties impacting ZooKeeper TLS configuration
    // and defines the default values this instance will use if no explicit config is given.
    // We make it part of each instance rather than the object to facilitate testing.
    private ZKClientConfig zkClientConfigViaSystemProperties = new ZKClientConfig();

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

    // During dynamic update, we use the values from this config, these are only used in DynamicBrokerConfig
    public Map<String, Object> originalsFromThisConfig() {
        return super.originals();
    }

    public Map<String, ?> valuesFromThisConfig() {
        return super.values();
    }

    public Map<String, Object> valuesFromThisConfigWithPrefixOverride(String prefix) {
        return super.valuesWithPrefixOverride(prefix);
    }

    /** ********* Zookeeper Configuration ***********/
    public String zkConnect() {
        return getString(ZK_CONNECT_PROP);
    }
    public int zkSessionTimeoutMs() {
        return getInt(ZK_SESSION_TIMEOUT_MS_PROP);
    }
    public int zkConnectionTimeoutMs() {
        return Optional.ofNullable(getInt(ZK_CONNECTION_TIMEOUT_MS_PROP))
                .orElse(getInt(ZK_SESSION_TIMEOUT_MS_PROP));
    }
    public boolean zkEnableSecureAcls() {
        return getBoolean(ZK_ENABLE_SECURE_ACLS_PROP);
    }
    public int zkMaxInFlightRequests() {
        return getInt(ZK_MAX_IN_FLIGHT_REQUESTS_PROP);
    }

    public final RemoteLogManagerConfig remoteLogManagerConfig() {
        return new RemoteLogManagerConfig(this);
    }

    private boolean zkBooleanConfigOrSystemPropertyWithDefaultValue(String propKey) {
        // Use the system property if it exists and the Kafka config value was defaulted rather than actually provided
        // Need to translate any system property value from true/false (String) to true/false (Boolean)
        boolean actuallyProvided = originals().containsKey(propKey);
        if (actuallyProvided) {
            return getBoolean(propKey);
        } else {
            Optional<String> sysPropValue = zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey);
            return sysPropValue.map("true"::equals).orElseGet(() -> getBoolean(propKey)); // not specified so use the default value
        }
    }


    private String zkStringConfigOrSystemPropertyWithDefaultValue(String propKey) {
        // Use the system property if it exists and the Kafka config value was defaulted rather than actually provided
        boolean actuallyProvided = originals().containsKey(propKey);
        if (actuallyProvided) {
            return getString(propKey);
        } else {
            return zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey)
                    .orElseGet(() -> getString(propKey)); // not specified so use the default value
        }
    }

    private Optional<String> zkOptionalStringConfigOrSystemProperty(String propKey) {
        String configValue = getString(propKey);
        return (configValue != null) ? Optional.of(configValue) :
                zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey);
    }

    private Optional<Password> zkPasswordConfigOrSystemProperty(String propKey) {
        Password password = getPassword(propKey);
        return (password != null) ? Optional.of(password) :
                zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey).map(Password::new);
    }

    private Optional<List<String>> zkListConfigOrSystemProperty(String propKey) {
        List<String> listValue = getList(propKey);
        return (listValue != null) ? Optional.of(listValue) :
                zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey)
                        .map(sysProp -> Arrays.asList(sysProp.split("\\s*,\\s*")));
    }

    public final boolean zkSslClientEnable() {
        return zkBooleanConfigOrSystemPropertyWithDefaultValue(ZK_SSL_CLIENT_ENABLE_PROP);
    }
    public final Optional<String> zkClientCnxnSocketClassName() {
        return zkOptionalStringConfigOrSystemProperty(ZK_CLIENT_CNXN_SOCKET_PROP);
    }
    public final Optional<String> zkSslKeyStoreLocation() {
        return zkOptionalStringConfigOrSystemProperty(ZK_SSL_KEY_STORE_LOCATION_PROP);
    }
    public final Optional<Password> zkSslKeyStorePassword() {
        return zkPasswordConfigOrSystemProperty(ZK_SSL_KEY_STORE_PASSWORD_PROP);
    }
    public final Optional<String> zkSslKeyStoreType() {
        return zkOptionalStringConfigOrSystemProperty(ZK_SSL_KEY_STORE_TYPE_PROP);
    }
    public final Optional<String> zkSslTrustStoreLocation() {
        return zkOptionalStringConfigOrSystemProperty(ZK_SSL_TRUST_STORE_LOCATION_PROP);
    }
    public final Optional<Password> zkSslTrustStorePassword() {
        return zkPasswordConfigOrSystemProperty(ZK_SSL_TRUST_STORE_PASSWORD_PROP);
    }
    public final Optional<String> zkSslTrustStoreType() {
        return zkOptionalStringConfigOrSystemProperty(ZK_SSL_TRUST_STORE_TYPE_PROP);
    }
    public final String zkSslProtocol() {
        return zkStringConfigOrSystemPropertyWithDefaultValue(ZK_SSL_PROTOCOL_PROP);
    }
    public final Optional<List<String>> zkSslEnabledProtocols() {
        return zkListConfigOrSystemProperty(ZK_SSL_ENABLED_PROTOCOLS_PROP);
    }
    public final Optional<List<String>> zkSslCipherSuites() {
        return zkListConfigOrSystemProperty(ZK_SSL_CIPHER_SUITES_PROP);
    }

    public final String zkSslEndpointIdentificationAlgorithm() {
        // Use the system property if it exists and the Kafka config value was defaulted rather than actually provided
        // Need to translate any system property value from true/false to HTTPS/<blank>
        String kafkaProp = ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP;
        boolean actuallyProvided = originals().containsKey(kafkaProp);
        if (actuallyProvided) {
            return getString(kafkaProp);
        } else {
            Optional<String> sysPropValue = zooKeeperClientProperty(zkClientConfigViaSystemProperties, kafkaProp);
            return sysPropValue.map(value -> "true".equals(value) ? "HTTPS" : "").orElseGet(() -> getString(kafkaProp)); // not specified so use the default value
        }
    }

    public final boolean zkSslCrlEnable() {
        return zkBooleanConfigOrSystemPropertyWithDefaultValue(ZK_SSL_CRL_ENABLE_PROP);
    }
    public final boolean zkSslOcspEnable() {
        return zkBooleanConfigOrSystemPropertyWithDefaultValue(ZK_SSL_OCSP_ENABLE_PROP);
    }

    /* General Configuration */
    public final boolean brokerIdGenerationEnable() {
        return getBoolean(BROKER_ID_GENERATION_ENABLE_PROP);
    }

    public final int maxReservedBrokerId() {
        return getInt(MAX_RESERVED_BROKER_ID_PROP);
    }

    private Integer brokerId;
    public final int brokerId() {
        return Optional.ofNullable(brokerId).orElse(getInt(BROKER_ID_PROP));
    }

    public final void brokerId(int brokerId) {
        this.brokerId = brokerId;
    }
    public final int nodeId() {
        return getInt(NODE_ID_PROP);
    }
    public final int initialRegistrationTimeoutMs() {
        return getInt(INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_PROP);
    }
    public final int brokerHeartbeatIntervalMs() {
        return getInt(BROKER_HEARTBEAT_INTERVAL_MS_PROP);
    }
    public final int brokerSessionTimeoutMs() {
        return getInt(BROKER_SESSION_TIMEOUT_MS_PROP);
    }

    public final boolean requiresZookeeper() {
        return processRoles().isEmpty();
    }

    public final boolean usesSelfManagedQuorum() {
        return !processRoles().isEmpty();
    }

    public final boolean migrationEnabled() {
        return getBoolean(MIGRATION_ENABLED_PROP);
    }
    public final boolean elrEnabled() {
        return getBoolean(ELR_ENABLED_PROP);
    }

    private Set<KafkaRaftServer.ProcessRole> parseProcessRoles() {
        List<String> rolesList = getList(PROCESS_ROLES_PROP);
        Set<KafkaRaftServer.ProcessRole> roles = rolesList.stream()
                .map(role -> {
                    switch (role) {
                        case "broker":
                            return BrokerRole;
                        case "controller":
                            return ControllerRole;
                        default:
                            throw new ConfigException(String.format("Unknown process role '%s' " +
                                    "(only 'broker' and 'controller' are allowed roles)", role));
                    }
                })
                .collect(Collectors.toSet());

        Set<KafkaRaftServer.ProcessRole> distinctRoles = new HashSet<>(roles);

        if (distinctRoles.size() != roles.size()) {
            throw new ConfigException(String.format("Duplicate role names found in `%s`: %s",
                    PROCESS_ROLES_PROP, roles));
        }

        return distinctRoles;
    }

    public final boolean isKRaftCombinedMode() {
        return processRoles().equals(Utils.mkSet(BrokerRole, ControllerRole));
    }

    public final String metadataLogDir() {
        return Optional.ofNullable(getString(METADATA_LOG_DIR_PROP))
                .orElse(logDirs().get(0));
    }

    public final int metadataLogSegmentBytes() {
        return getInt(METADATA_LOG_SEGMENT_BYTES_PROP);
    }

    public final long metadataLogSegmentMillis() {
        return getLong(METADATA_LOG_SEGMENT_MILLIS_PROP);
    }

    public final long metadataRetentionBytes() {
        return getLong(METADATA_MAX_RETENTION_BYTES_PROP);
    }

    public final long metadataRetentionMillis() {
        return getLong(METADATA_MAX_RETENTION_MILLIS_PROP);
    }

    public final long serverMaxStartupTimeMs() {
        return getLong(SERVER_MAX_STARTUP_TIME_MS_PROP);
    }

    public final int numNetworkThreads() {
        return getInt(NUM_NETWORK_THREADS_PROP);
    }

    public final int backgroundThreads() {
        return getInt(BACKGROUND_THREADS_PROP);
    }

    public final int queuedMaxRequests() {
        return getInt(QUEUED_MAX_REQUESTS_PROP);
    }

    public final long queuedMaxBytes() {
        return getLong(QUEUED_MAX_BYTES_PROP);
    }

    public final int numIoThreads() {
        return getInt(NUM_IO_THREADS_PROP);
    }

    public final int messageMaxBytes() {
        return getInt(MESSAGE_MAX_BYTES_PROP);
    }

    public final int requestTimeoutMs() {
        return getInt(REQUEST_TIMEOUT_MS_PROP);
    }

    public final long connectionSetupTimeoutMs() {
        return getLong(CONNECTION_SETUP_TIMEOUT_MS_PROP);
    }

    public final long connectionSetupTimeoutMaxMs() {
        return getLong(CONNECTION_SETUP_TIMEOUT_MAX_MS_PROP);
    }

    public final int numReplicaAlterLogDirsThreads() {
        return Optional.ofNullable(getInt(NUM_REPLICA_ALTER_LOG_DIRS_THREADS_PROP))
                .orElse(logDirs().size()); // Assuming logDirs is a field or variable in your class.
    }

    public final long metadataSnapshotMaxNewRecordBytes() {
        return getLong(METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_PROP);
    }

    public final Optional<Authorizer> createNewAuthorizer() throws ClassNotFoundException {
        String className = getString(AUTHORIZER_CLASS_NAME_PROP);
        if (className == null || className.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(org.apache.kafka.security.utils.AuthorizerUtils.createAuthorizer(className));
        }
    }

    public Set<ListenerName> earlyStartListeners = earlyStartListeners();
    public final Set<ListenerName> earlyStartListeners() {
        Set<ListenerName> listenersSet = extractListenerNames();
        Set<ListenerName> controllerListenersSet = extractListenerNames(controllerListeners());

        Optional<String> earlyStartListenersProp = Optional.ofNullable(getString(EARLY_START_LISTENERS_PROP));

        return earlyStartListenersProp.map(str -> {
            String[] listenerNames = str.split(",");
            Set<ListenerName> result = new HashSet<>();
            for (String listenerNameStr : listenerNames) {
                String trimmedName = listenerNameStr.trim();
                if (!trimmedName.isEmpty()) {
                    ListenerName listenerName = ListenerName.normalised(trimmedName);
                    if (!listenersSet.contains(listenerName) && !controllerListenersSet.contains(listenerName)) {
                        throw new ConfigException(
                                String.format("%s contains listener %s, but this is not contained in %s or %s",
                                        EARLY_START_LISTENERS_PROP,
                                        listenerName.value(),
                                        LISTENERS_PROP,
                                        CONTROLLER_LISTENER_NAMES_PROP));
                    }
                    result.add(listenerName);
                }
            }
            return result;
        }).orElse(controllerListenersSet);
    }

    public final long metadataSnapshotMaxIntervalMs() {
        return getLong(METADATA_SNAPSHOT_MAX_INTERVAL_MS_PROP);
    }

    public final OptionalLong metadataMaxIdleIntervalNs() {
        long value = TimeUnit.NANOSECONDS.convert(getInt(METADATA_MAX_IDLE_INTERVAL_MS_PROP), TimeUnit.MILLISECONDS);
        return (value > 0) ? OptionalLong.of(value) : OptionalLong.empty();
    }

    /** ********* Socket Server Configuration ***********/
    public final int socketSendBufferBytes() {
        return getInt(SOCKET_SEND_BUFFER_BYTES_PROP);
    }
    public final int socketReceiveBufferBytes() {
        return getInt(SOCKET_RECEIVE_BUFFER_BYTES_PROP);
    }
    public final int socketRequestMaxBytes() {
        return getInt(SOCKET_REQUEST_MAX_BYTES_PROP);
    }
    public final int socketListenBacklogSize() {
        return getInt(SOCKET_LISTEN_BACKLOG_SIZE_PROP);
    }
    public final int maxConnectionsPerIp() {
        return getInt(MAX_CONNECTIONS_PER_IP_PROP);
    }
    public final Map<String, Integer> maxConnectionsPerIpOverrides() {
        return getMap(
                MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP,
                getString(MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP)
        ).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> Integer.parseInt(entry.getValue())));
    }
    public final int maxConnections() {
        return getInt(MAX_CONNECTIONS_PROP);
    }
    public final int maxConnectionCreationRate() {
        return getInt(MAX_CONNECTION_CREATION_RATE_PROP);
    }
    public final long connectionsMaxIdleMs() {
        return getLong(CONNECTIONS_MAX_IDLE_MS_PROP);
    }
    public final int failedAuthenticationDelayMs() {
        return getInt(FAILED_AUTHENTICATION_DELAY_MS_PROP);
    }

    /** ********* rack configuration ***********/
    public final Optional<String> rack() {
        return Optional.ofNullable(getString(RACK_PROP));
    }
    public final Optional<String> replicaSelectorClassName() {
        return Optional.ofNullable(getString(REPLICA_SELECTOR_CLASS_PROP));
    }

    /** ********* Log Configuration ***********/
    public final boolean autoCreateTopicsEnable() {
        return getBoolean(AUTO_CREATE_TOPICS_ENABLE_PROP);
    }
    public final int numPartitions() {
        return getInt(NUM_PARTITIONS_PROP);
    }
    public final List<String> logDirs() {
        return CoreUtils.parseCsvList(
                Optional.ofNullable(getString(LOG_DIRS_PROP))
                        .orElse(getString(LOG_DIR_PROP)));
    }

    // Log Configuration
    public final int logSegmentBytes() {
        return getInt(LOG_SEGMENT_BYTES_PROP);
    }
    public final long logFlushIntervalMessages() {
        return getLong(LOG_FLUSH_INTERVAL_MESSAGES_PROP);
    }
    public final int logCleanerThreads() {
        return getInt(LOG_CLEANER_THREADS_PROP);
    }
    public final int numRecoveryThreadsPerDataDir() {
        return getInt(NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP);
    }
    public final long logFlushSchedulerIntervalMs() {
        return getLong(LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP);
    }
    public final int logFlushOffsetCheckpointIntervalMs() {
        return getInt(LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_PROP);
    }
    public final int logFlushStartOffsetCheckpointIntervalMs() {
        return getInt(LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_PROP);
    }
    public final long logCleanupIntervalMs() {
        return getLong(LOG_CLEANUP_INTERVAL_MS_PROP);
    }
    public final List<String> logCleanupPolicy() {
        return getList(LOG_CLEANUP_POLICY_PROP);
    }
    public final int offsetsRetentionMinutes() {
        return getInt(OFFSETS_RETENTION_MINUTES_PROP);
    }
    public final long offsetsRetentionCheckIntervalMs() {
        return getLong(OFFSETS_RETENTION_CHECK_INTERVAL_MS_PROP);
    }
    public final long logRetentionBytes() {
        return getLong(LOG_RETENTION_BYTES_PROP);
    }
    public final long logCleanerDedupeBufferSize() {
        return getLong(LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP);
    }
    public final Double logCleanerDedupeBufferLoadFactor() {
        return getDouble(LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP);
    }
    public final int logCleanerIoBufferSize() {
        return getInt(LOG_CLEANER_IO_BUFFER_SIZE_PROP);
    }
    public final Double logCleanerIoMaxBytesPerSecond() {
        return getDouble(LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP);
    }
    public final long logCleanerDeleteRetentionMs() {
        return getLong(LOG_CLEANER_DELETE_RETENTION_MS_PROP);
    }
    public final long logCleanerMinCompactionLagMs() {
        return getLong(LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP);
    }
    public final long logCleanerMaxCompactionLagMs() {
        return getLong(LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP);
    }
    public final long logCleanerBackoffMs() {
        return getLong(LOG_CLEANER_BACKOFF_MS_PROP);
    }
    public final double logCleanerMinCleanRatio() {
        return getDouble(LOG_CLEANER_MIN_CLEAN_RATIO_PROP);
    }
    public final boolean logCleanerEnable() {
        return getBoolean(LOG_CLEANER_ENABLE_PROP);
    }
    public final int logIndexSizeMaxBytes() {
        return getInt(LOG_INDEX_SIZE_MAX_BYTES_PROP);
    }
    public final int logIndexIntervalBytes() {
        return getInt(LOG_INDEX_INTERVAL_BYTES_PROP);
    }
    public final long logDeleteDelayMs() {
        return getLong(LOG_DELETE_DELAY_MS_PROP);
    }
    public final long logRollTimeMillis() {
        return Optional.ofNullable(getLong(LOG_ROLL_TIME_MILLIS_PROP))
                .orElse(60 * 60 * 1000L * getInt(LOG_ROLL_TIME_HOURS_PROP));
    }
    public final long logRollTimeJitterMillis() {
        return Optional.ofNullable(getLong(LOG_ROLL_TIME_JITTER_MILLIS_PROP))
                .orElse(60 * 60 * 1000L * getInt(LOG_ROLL_TIME_JITTER_HOURS_PROP));
    }
    public final long logFlushIntervalMs() {
        return Optional.ofNullable(getLong(LOG_FLUSH_INTERVAL_MS_PROP))
                .orElse(getLong(LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP));
    }
    public final int minInSyncReplicas() {
        return getInt(MIN_IN_SYNC_REPLICAS_PROP);
    }
    public final boolean logPreAllocateEnable() {
        return getBoolean(LOG_PRE_ALLOCATE_PROP);
    }

    // We keep the user-provided String as `MetadataVersion.fromVersionString` can choose a slightly different version (eg if `0.10.0`
    // is passed, `0.10.0-IV0` may be picked)
    @SuppressWarnings("deprecation")
    private final String logMessageFormatVersionString() {
        return getString(LOG_MESSAGE_FORMAT_VERSION_PROP);
    }

    /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
    @Deprecated
    public MetadataVersion logMessageFormatVersion() {
        return LogConfig.shouldIgnoreMessageFormatVersion(interBrokerProtocolVersion())
                ? MetadataVersion.fromVersionString(LogConfig.DEFAULT_MESSAGE_FORMAT_VERSION)
                : MetadataVersion.fromVersionString(logMessageFormatVersionString());
    }

    public final TimestampType logMessageTimestampType() {
        return TimestampType.forName(getString(LOG_MESSAGE_TIMESTAMP_TYPE_PROP));
    }

    /* See `TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG` for details */
    @Deprecated
    public final long logMessageTimestampDifferenceMaxMs() {
        return getLong(LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_PROP);
    }

    // In the transition period before logMessageTimestampDifferenceMaxMs is removed, to maintain backward compatibility,
    // we are using its value if logMessageTimestampBeforeMaxMs default value hasn't changed.
    // See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for deprecation details
    @SuppressWarnings("deprecation")
    public final long logMessageTimestampBeforeMaxMs() {
        long messageTimestampBeforeMaxMs = getLong(LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_PROP);
        if (messageTimestampBeforeMaxMs != LogConfig.DEFAULT_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS) {
            return messageTimestampBeforeMaxMs;
        } else {
            return logMessageTimestampDifferenceMaxMs();
        }
    }

    // In the transition period before logMessageTimestampDifferenceMaxMs is removed, to maintain backward compatibility,
    // we are using its value if logMessageTimestampAfterMaxMs default value hasn't changed.
    // See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for deprecation details
    @SuppressWarnings("deprecation")
    public final long logMessageTimestampAfterMaxMs() {
        long messageTimestampAfterMaxMs = getLong(LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_PROP);
        if (messageTimestampAfterMaxMs != Long.MAX_VALUE) {
            return messageTimestampAfterMaxMs;
        } else {
            return logMessageTimestampDifferenceMaxMs();
        }
    }

    public final boolean logMessageDownConversionEnable() {
        return getBoolean(LOG_MESSAGE_DOWN_CONVERSION_ENABLE_PROP);
    }

    /** ********* Replication configuration ***********/
    public final int controllerSocketTimeoutMs() {
        return getInt(CONTROLLER_SOCKET_TIMEOUT_MS_PROP);
    }
    public final int defaultReplicationFactor() {
        return getInt(DEFAULT_REPLICATION_FACTOR_PROP);
    }
    public final long replicaLagTimeMaxMs() {
        return getLong(REPLICA_LAG_TIME_MAX_MS_PROP);
    }
    public final int replicaSocketTimeoutMs() {
        return getInt(REPLICA_SOCKET_TIMEOUT_MS_PROP);
    }
    public final int replicaSocketReceiveBufferBytes() {
        return getInt(REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_PROP);
    }
    public final int replicaFetchMaxBytes() {
        return getInt(REPLICA_FETCH_MAX_BYTES_PROP);
    }
    public final int replicaFetchWaitMaxMs() {
        return getInt(REPLICA_FETCH_WAIT_MAX_MS_PROP);
    }
    public final int replicaFetchMinBytes() {
        return getInt(REPLICA_FETCH_MIN_BYTES_PROP);
    }
    public final int replicaFetchResponseMaxBytes() {
        return getInt(REPLICA_FETCH_RESPONSE_MAX_BYTES_PROP);
    }
    public final int replicaFetchBackoffMs() {
        return getInt(REPLICA_FETCH_BACKOFF_MS_PROP);
    }
    public final int numReplicaFetchers() {
        return getInt(NUM_REPLICA_FETCHERS_PROP);
    }
    public final long replicaHighWatermarkCheckpointIntervalMs() {
        return getLong(REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_PROP);
    }
    public final int fetchPurgatoryPurgeIntervalRequests() {
        return getInt(FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP);
    }
    public final int producerPurgatoryPurgeIntervalRequests() {
        return getInt(PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP);
    }
    public final int deleteRecordsPurgatoryPurgeIntervalRequests() {
        return getInt(DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP);
    }
    public final boolean autoLeaderRebalanceEnable() {
        return getBoolean(AUTO_LEADER_REBALANCE_ENABLE_PROP);
    }
    public final int leaderImbalancePerBrokerPercentage() {
        return getInt(LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_PROP);
    }
    public final long leaderImbalanceCheckIntervalSeconds() {
        return getLong(LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_PROP);
    }
    public final boolean uncleanLeaderElectionEnable() {
        return getBoolean(UNCLEAN_LEADER_ELECTION_ENABLE_PROP);
    }

    // We keep the user-provided String as `MetadataVersion.fromVersionString` can choose a slightly different version (eg if `0.10.0`
    // is passed, `0.10.0-IV0` may be picked)
    public final String interBrokerProtocolVersionString() {
        return getString(INTER_BROKER_PROTOCOL_VERSION_PROP);
    }
    public final MetadataVersion interBrokerProtocolVersion() {
        return getInterBrokerProtocolVersion();
    }
    private MetadataVersion getInterBrokerProtocolVersion() {
        if (processRoles().isEmpty()) {
            return MetadataVersion.fromVersionString(interBrokerProtocolVersionString());
        } else {
            if (originals().containsKey(INTER_BROKER_PROTOCOL_VERSION_PROP)) {
                // A user-supplied IBP was given
                MetadataVersion configuredVersion = MetadataVersion.fromVersionString(interBrokerProtocolVersionString());
                if (!configuredVersion.isKRaftSupported()) {
                    throw new ConfigException("A non-KRaft version " + interBrokerProtocolVersionString() + " given for " +
                            INTER_BROKER_PROTOCOL_VERSION_PROP + ". " +
                            "The minimum version is " + MetadataVersion.MINIMUM_KRAFT_VERSION);
                } else {
                    log.warn(INTER_BROKER_PROTOCOL_VERSION_PROP +
                            " is deprecated in KRaft mode as of 3.3 and will only " +
                            "be read when first upgrading from a KRaft prior to 3.3. See kafka-storage.sh help for details on setting " +
                            "the metadata version for a new KRaft cluster.");
                }
            }
            // In KRaft mode, we pin this value to the minimum KRaft-supported version. This prevents inadvertent usage of
            // the static IBP config in broker components running in KRaft mode
            return MetadataVersion.MINIMUM_KRAFT_VERSION;
        }
    }

    /** ********* Controlled shutdown configuration ***********/
    public final int controlledShutdownMaxRetries() {
        return getInt(CONTROLLED_SHUTDOWN_MAX_RETRIES_PROP);
    }
    public final long controlledShutdownRetryBackoffMs() {
        return getLong(CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_PROP);
    }
    public final boolean controlledShutdownEnable() {
        return getBoolean(CONTROLLED_SHUTDOWN_ENABLE_PROP);
    }

    /** ********* Feature configuration ***********/
    public final boolean isFeatureVersioningSupported() {
        return interBrokerProtocolVersion().isFeatureVersioningSupported();
    }

    /** ********* Group coordinator configuration ***********/
    public final int groupMinSessionTimeoutMs() {
        return getInt(GROUP_MIN_SESSION_TIMEOUT_MS_PROP);
    }
    public final int groupMaxSessionTimeoutMs() {
        return getInt(GROUP_MAX_SESSION_TIMEOUT_MS_PROP);
    }
    public final int groupInitialRebalanceDelay() {
        return getInt(GROUP_INITIAL_REBALANCE_DELAY_MS_PROP);
    }
    public final int groupMaxSize() {
        return getInt(GROUP_MAX_SIZE_PROP);
    }

    /** New group coordinator configs */
    public final boolean isNewGroupCoordinatorEnabled() {
        return getBoolean(NEW_GROUP_COORDINATOR_ENABLE_PROP);
    }
    public final int groupCoordinatorNumThreads() {
        return getInt(GROUP_COORDINATOR_NUM_THREADS_PROP);
    }

    /** Consumer group configs */
    public final int consumerGroupSessionTimeoutMs() {
        return getInt(CONSUMER_GROUP_SESSION_TIMEOUT_MS_PROP);
    }
    public final int consumerGroupMinSessionTimeoutMs() {
        return getInt(CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_PROP);
    }
    public final int consumerGroupMaxSessionTimeoutMs() {
        return getInt(CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_PROP);
    }
    public final int consumerGroupHeartbeatIntervalMs() {
        return getInt(CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_PROP);
    }
    public final int consumerGroupMinHeartbeatIntervalMs() {
        return getInt(CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_PROP);
    }
    public final int consumerGroupMaxHeartbeatIntervalMs() {
        return getInt(CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_PROP);
    }
    public final int consumerGroupMaxSize() {
        return getInt(CONSUMER_GROUP_MAX_SIZE_PROP);
    }
    public final List<PartitionAssignor> consumerGroupAssignors() {
        return getConfiguredInstances(CONSUMER_GROUP_ASSIGNORS_PROP, PartitionAssignor.class);
    }

    /** ********* Offset management configuration ***********/
    public final int offsetMetadataMaxSize() {
        return getInt(OFFSET_METADATA_MAX_SIZE_PROP);
    }
    public final int offsetsLoadBufferSize() {
        return getInt(OFFSETS_LOAD_BUFFER_SIZE_PROP);
    }
    public final Short offsetsTopicReplicationFactor() {
        return getShort(OFFSETS_TOPIC_REPLICATION_FACTOR_PROP);
    }
    public final int offsetsTopicPartitions() {
        return getInt(OFFSETS_TOPIC_PARTITIONS_PROP);
    }
    public final int offsetCommitTimeoutMs() {
        return getInt(OFFSET_COMMIT_TIMEOUT_MS_PROP);
    }
    public final Short offsetCommitRequiredAcks() {
        return getShort(OFFSET_COMMIT_REQUIRED_ACKS_PROP);
    }
    public final int offsetsTopicSegmentBytes() {
        return getInt(OFFSETS_TOPIC_SEGMENT_BYTES_PROP);
    }
    public final CompressionType offsetsTopicCompressionType() {
        return Optional.ofNullable(getInt(OFFSETS_TOPIC_COMPRESSION_CODEC_PROP))
                .map(value -> CompressionType.forId(value)).orElse(null);
    }

    /** ********* Transaction management configuration ***********/
    public final int transactionalIdExpirationMs() {
        return getInt(TRANSACTIONAL_ID_EXPIRATION_MS_PROP);
    }
    public final int transactionMaxTimeoutMs() {
        return getInt(TRANSACTIONS_MAX_TIMEOUT_MS_PROP);
    }
    public final int transactionTopicMinISR() {
        return getInt(TRANSACTIONS_TOPIC_MIN_ISR_PROP);
    }
    public final int transactionsLoadBufferSize() {
        return getInt(TRANSACTIONS_LOAD_BUFFER_SIZE_PROP);
    }
    public final short transactionTopicReplicationFactor() {
        return getShort(TRANSACTIONS_TOPIC_REPLICATION_FACTOR_PROP);
    }
    public final int transactionTopicPartitions() {
        return getInt(TRANSACTIONS_TOPIC_PARTITIONS_PROP);
    }
    public final int transactionTopicSegmentBytes() {
        return getInt(TRANSACTIONS_TOPIC_SEGMENT_BYTES_PROP);
    }
    public final int transactionAbortTimedOutTransactionCleanupIntervalMs() {
        return getInt(TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_PROP);
    }
    public final int transactionRemoveExpiredTransactionalIdCleanupIntervalMs() {
        return getInt(TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_PROP);
    }
    public final boolean transactionPartitionVerificationEnable() {
        return getBoolean(TRANSACTION_PARTITION_VERIFICATION_ENABLE_PROP);
    }
    public final int producerIdExpirationMs() {
        return getInt(PRODUCER_ID_EXPIRATION_MS_PROP);
    }
    public final int producerIdExpirationCheckIntervalMs() {
        return getInt(PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_PROP);
    }

    /** ********* Metric Configuration **************/
    public final int metricNumSamples() {
        return getInt(METRIC_NUM_SAMPLES_PROP);
    }
    public final long metricSampleWindowMs() {
        return getLong(METRIC_SAMPLE_WINDOW_MS_PROP);
    }
    public final String metricRecordingLevel() {
        return getString(METRIC_RECORDING_LEVEL_PROP);
    }

    /** ********* Kafka Client Telemetry Metrics Configuration ***********/
    public final int clientTelemetryMaxBytes() {
        return getInt(KafkaConfig.CLIENT_TELEMETRY_MAX_BYTES_PROP);
    }

    /** ********* SSL/SASL Configuration **************/
    // Security configs may be overridden for listeners, so it is not safe to use the base values
    // Hence the base SSL/SASL configs are not fields of KafkaConfig, listener configs should be
    // retrieved using KafkaConfig#valuesWithPrefixOverride
    @SuppressWarnings("unchecked")
    private Set<String> saslEnabledMechanisms(ListenerName listenerName) {
        return Optional.ofNullable(valuesWithPrefixOverride(listenerName.configPrefix()).get(SASL_ENABLED_MECHANISMS_PROP))
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
        return getControlPlaneListenerNameAndSecurityProtocol().map(entry -> entry.getKey());
    }
    public final Optional<SecurityProtocol> controlPlaneSecurityProtocol() {
        return getControlPlaneListenerNameAndSecurityProtocol().map(entry -> entry.getValue());
    }
    public final String saslMechanismInterBrokerProtocol() {
        return getString(SASL_MECHANISM_INTER_BROKER_PROTOCOL_PROP);
    }
    public final boolean saslInterBrokerHandshakeRequestEnable() {
        return interBrokerProtocolVersion().isSaslInterBrokerHandshakeRequestEnabled();
    }

    /** ********* DelegationToken Configuration **************/
    public final Password delegationTokenSecretKey() {
        return Optional.ofNullable(getPassword(DELEGATION_TOKEN_SECRET_KEY_PROP))
                .orElseGet(() -> getPassword(DELEGATION_TOKEN_SECRET_KEY_ALIAS_PROP));
    }

    public final boolean tokenAuthEnabled() {
        return delegationTokenSecretKey() != null && !delegationTokenSecretKey().value().isEmpty();
    }
    public final long delegationTokenMaxLifeMs() {
        return getLong(DELEGATION_TOKEN_MAX_LIFE_TIME_PROP);
    }
    public final long  delegationTokenExpiryTimeMs() {
        return getLong(DELEGATION_TOKEN_EXPIRY_TIME_MS_PROP);
    }
    public final long  delegationTokenExpiryCheckIntervalMs() {
        return getLong(DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_PROP);
    }

    /** ********* Password encryption configuration for dynamic configs *********/
    public final Optional<Password> passwordEncoderSecret() {
        return Optional.ofNullable(getPassword(PASSWORD_ENCODER_SECRET_PROP));
    }
    public final Optional<Password>  passwordEncoderOldSecret() {
        return Optional.ofNullable(getPassword(PASSWORD_ENCODER_OLD_SECRET_PROP));
    }
    public final String passwordEncoderCipherAlgorithm() {
        return getString(PASSWORD_ENCODER_CIPHER_ALGORITHM_PROP);
    }
    public final String passwordEncoderKeyFactoryAlgorithm() {
        return getString(PASSWORD_ENCODER_KEY_FACTORY_ALGORITHM_PROP);
    }
    public final int passwordEncoderKeyLength() {
        return getInt(PASSWORD_ENCODER_KEY_LENGTH_PROP);
    }
    public final int passwordEncoderIterations() {
        return getInt(PASSWORD_ENCODER_ITERATIONS_PROP);
    }

    /** ********* Quota Configuration **************/
    public final int numQuotaSamples() {
        return getInt(NUM_QUOTA_SAMPLES_PROP);
    }
    public final int quotaWindowSizeSeconds() {
        return getInt(QUOTA_WINDOW_SIZE_SECONDS_PROP);
    }
    public final int numReplicationQuotaSamples() {
        return getInt(NUM_REPLICATION_QUOTA_SAMPLES_PROP);
    }
    public final int replicationQuotaWindowSizeSeconds() {
        return getInt(REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_PROP);
    }
    public final int numAlterLogDirsReplicationQuotaSamples() {
        return getInt(NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_PROP);
    }
    public final int alterLogDirsReplicationQuotaWindowSizeSeconds() {
        return getInt(ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_PROP);
    }
    public final int numControllerQuotaSamples() {
        return getInt(NUM_CONTROLLER_QUOTA_SAMPLES_PROP);
    }
    public final int controllerQuotaWindowSizeSeconds() {
        return getInt(CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_PROP);
    }

    /** ********* Fetch Configuration **************/
    public final int maxIncrementalFetchSessionCacheSlots() {
        return getInt(MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_PROP);
    }
    public final int fetchMaxBytes() {
        return getInt(FETCH_MAX_BYTES_PROP);
    }

    public final boolean deleteTopicEnable() {
        return getBoolean(DELETE_TOPIC_ENABLE_PROP);
    }
    public final String compressionType() {
        return getString(COMPRESSION_TYPE_PROP);
    }


    /** ********* Raft Quorum Configuration *********/
    public final List<String> quorumVoters() {
        return getList(RaftConfig.QUORUM_VOTERS_CONFIG);
    }
    public final int quorumElectionTimeoutMs() {
        return getInt(RaftConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG);
    }
    public final int quorumFetchTimeoutMs() {
        return getInt(RaftConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG);
    }
    public final int quorumElectionBackoffMs() {
        return getInt(RaftConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG);
    }
    public final int quorumLingerMs() {
        return getInt(RaftConfig.QUORUM_LINGER_MS_CONFIG);
    }
    public final int quorumRequestTimeoutMs() {
        return getInt(RaftConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG);
    }
    public final int quorumRetryBackoffMs() {
        return getInt(RaftConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG);
    }

    /** ********* Internal Configurations *********/
    public final boolean unstableApiVersionsEnabled() {
        return getBoolean(UNSTABLE_API_VERSIONS_ENABLE_PROP);
    }

    public final boolean unstableMetadataVersionsEnabled() {
        return getBoolean(KafkaConfig.UNSTABLE_METADATA_VERSIONS_ENABLE_PROP);
    }

    public final void addReconfigurable(Reconfigurable reconfigurable) {
        dynamicConfig().addReconfigurable(reconfigurable);
    }

    public final void removeReconfigurable(Reconfigurable reconfigurable) {
        dynamicConfig().removeReconfigurable(reconfigurable);
    }

    public final long logRetentionTimeMillis() {
        Long millisInMinute = 60L * 1000L;
        Long millisInHour = 60L * millisInMinute;

        Long millis =
                Optional.ofNullable(getLong(LOG_RETENTION_TIME_MILLIS_PROP))
                        .orElse(Optional.ofNullable(getInt(LOG_RETENTION_TIME_MINUTES_PROP))
                                .map(mins -> millisInMinute * mins)
                                .orElse(getInt(LOG_RETENTION_TIME_HOURS_PROP) * millisInHour)
                                );

        if (millis < 0) return -1L;
        return millis;
    }


    private Map<String, String> getMap(String propName, String propValue) {
        try {
            return CoreUtils.parseCsvMap(propValue);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Error parsing configuration property '%s': %s", propName, e.getMessage()));
        }
    }

    public final List<Endpoint> listeners() {
        return CoreUtils.listenerListToEndpoints(getString(LISTENERS_PROP), effectiveListenerSecurityProtocolMap());
    }

    public final List<String> controllerListenerNames() {
        String value = Optional.ofNullable(getString(CONTROLLER_LISTENER_NAMES_PROP)).orElse("");
        if (value.isEmpty()) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(value.split(","));
        }
    }

    public final List<Endpoint> controllerListeners() {
        return listeners().stream()
                .filter(l -> l.listenerName().isPresent() && controllerListenerNames().contains(l.listenerName().get()))
                .collect(Collectors.toList());
    }

    public final String saslMechanismControllerProtocol() {
        return getString(SASL_MECHANISM_CONTROLLER_PROTOCOL_PROP);
    }

    public final Optional<Endpoint> controlPlaneListener() {
        return controlPlaneListenerName().flatMap(listenerName ->
            listeners().stream().filter(endpoint -> endpoint.listenerName().get().equals(listenerName.value())).findFirst()
        );
    }

    public final List<Endpoint> dataPlaneListeners() {
        return listeners().stream().filter(listener -> {
            String name = listener.listenerName().get();
            return !name.equals(getString(CONTROL_PLANE_LISTENER_NAME_PROP)) ||
                    !controllerListenerNames().contains(name);
        }).collect(Collectors.toList());
    }

    public final List<Endpoint> effectiveAdvertisedListeners() {
        String advertisedListenersProp = getString(KafkaConfig.ADVERTISED_LISTENERS_PROP);

        if (advertisedListenersProp != null) {
            return CoreUtils.listenerListToEndpoints(advertisedListenersProp, effectiveListenerSecurityProtocolMap(), false);
        } else {
            return listeners().stream().filter(l -> !controllerListenerNames().contains(l.listenerName().get())).collect(Collectors.toList());
        }
    }

    private Map.Entry<ListenerName, SecurityProtocol> getInterBrokerListenerNameAndSecurityProtocol() throws ConfigException {
        Optional<String> internalBrokerListener = Optional.ofNullable(getString(INTER_BROKER_LISTENER_NAME_PROP));
        return internalBrokerListener.map(name -> {
            if (originals().containsKey(INTER_BROKER_SECURITY_PROTOCOL_PROP)) {
                throw new ConfigException("Only one of " + INTER_BROKER_LISTENER_NAME_PROP + " and " +
                        INTER_BROKER_SECURITY_PROTOCOL_PROP + " should be set.");
            }
            ListenerName listenerName = ListenerName.normalised(name);
            SecurityProtocol securityProtocol = Optional.ofNullable(effectiveListenerSecurityProtocolMap().get(listenerName)).orElseThrow(
                    () -> new ConfigException("Listener with name " + listenerName.value() + " defined in " +
                            INTER_BROKER_LISTENER_NAME_PROP + " not found in " + LISTENER_SECURITY_PROTOCOL_MAP_PROP + ".")
            );
            return new AbstractMap.SimpleEntry<>(listenerName, securityProtocol);
        }).orElseGet(() -> {
            SecurityProtocol securityProtocol = getSecurityProtocol(getString(INTER_BROKER_SECURITY_PROTOCOL_PROP),
                    INTER_BROKER_SECURITY_PROTOCOL_PROP);
            return new AbstractMap.SimpleEntry<>(ListenerName.forSecurityProtocol(securityProtocol), securityProtocol);
        });
    }

    private Optional<Map.Entry<ListenerName, SecurityProtocol>> getControlPlaneListenerNameAndSecurityProtocol() {
        return Optional.ofNullable(getString(CONTROL_PLANE_LISTENER_NAME_PROP)).map(name -> {
            ListenerName listenerName = ListenerName.normalised(name);
            SecurityProtocol securityProtocol = Optional.ofNullable(effectiveListenerSecurityProtocolMap().get(listenerName))
                    .orElseThrow(() -> new ConfigException("Listener with " + listenerName.value() + " defined in " +
                    CONTROL_PLANE_LISTENER_NAME_PROP + " not found in " + LISTENER_SECURITY_PROTOCOL_MAP_PROP  + "."));
            return new AbstractMap.SimpleEntry<>(listenerName, securityProtocol);
        });
    }

    // Nothing was specified explicitly for listener.security.protocol.map, so we are using the default value,
    // and we are using KRaft.
    // Add PLAINTEXT mappings for controller listeners as long as there is no SSL or SASL_{PLAINTEXT,SSL} in use
    private boolean isSslOrSasl(String name) {
        return name.equals(SecurityProtocol.SSL.name) || name.equals(SecurityProtocol.SASL_SSL.name) || name.equals(SecurityProtocol.SASL_PLAINTEXT.name);
    }

    public final Map<ListenerName, SecurityProtocol> effectiveListenerSecurityProtocolMap() {
        Map<ListenerName, SecurityProtocol> mapValue = getMap(LISTENER_SECURITY_PROTOCOL_MAP_PROP, getString(LISTENER_SECURITY_PROTOCOL_MAP_PROP))
                .entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> ListenerName.normalised(entry.getKey()),
                        entry -> getSecurityProtocol(entry.getValue(), LISTENER_SECURITY_PROTOCOL_MAP_PROP)));

        if (usesSelfManagedQuorum() && !originals().containsKey(LISTENER_SECURITY_PROTOCOL_MAP_PROP)) {
            // check controller listener names (they won't appear in listeners when process.roles=broker)
            // as well as listeners for occurrences of SSL or SASL_*
            if (controllerListenerNames().stream().anyMatch(name -> isSslOrSasl(name)) ||
                    CoreUtils.parseCsvList(getString(LISTENERS_PROP)).stream().anyMatch(listenerValue -> isSslOrSasl(CoreUtils.parseListenerName(listenerValue)))) {
                return mapValue; // don't add default mappings since we found something that is SSL or SASL_*
            } else {
                // add the PLAINTEXT mappings for all controller listener names that are not explicitly PLAINTEXT
                mapValue.putAll(
                        controllerListenerNames().stream().filter(name -> !name.equals(SecurityProtocol.PLAINTEXT.name))
                        .collect(Collectors.toMap(name -> ListenerName.normalised(name), name -> SecurityProtocol.PLAINTEXT)));
                return mapValue;
            }
        } else {
            return mapValue;
        }
    }


    // Topic IDs are used with all self-managed quorum clusters and ZK cluster with IBP greater than or equal to 2.8
    public final boolean usesTopicId() {
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

    private void validateNonEmptyQuorumVotersForKRaft(Map<Integer, RaftConfig.AddressSpec> voterAddressSpecsByNodeId) {
        if (voterAddressSpecsByNodeId.isEmpty()) {
            throw new ConfigException(String.format("If using %s, %s must contain a parseable set of voters.", PROCESS_ROLES_PROP, QUORUM_VOTERS_PROP));
        }
    }

    private void validateNonEmptyQuorumVotersForMigration(Map<Integer, RaftConfig.AddressSpec> voterAddressSpecsByNodeId) {
        if (voterAddressSpecsByNodeId.isEmpty()) {
            throw new ConfigException(String.format("If using %s, %s must contain a parseable set of voters.", MIGRATION_ENABLED_PROP, QUORUM_VOTERS_PROP));
        }
    }

    private void require(Boolean requirement, String message) throws IllegalArgumentException {
        if (!requirement) {
            throw new IllegalArgumentException("requirement failed: " + message);
        }
    }

    private void validateControlPlaneListenerEmptyForKRaft() throws IllegalArgumentException {
        require(!controlPlaneListenerName().isPresent(),
                String.format("%s is not supported in KRaft mode.", CONTROL_PLANE_LISTENER_NAME_PROP));
    }
    private void validateAdvertisedListenersDoesNotContainControllerListenersForKRaftBroker(Set<ListenerName> advertisedListenerNames) throws IllegalArgumentException {
        require(!advertisedListenerNames.stream().anyMatch(aln -> controllerListenerNames().contains(aln.value())),
                String.format("The advertised.listeners config must not contain KRaft controller listeners from %s when %s contains the broker role because Kafka clients that send requests via advertised listeners do not send requests to KRaft controllers -- they only send requests to KRaft brokers.", CONTROLLER_LISTENER_NAMES_PROP, PROCESS_ROLES_PROP));
    }
    private void validateControllerQuorumVotersMustContainNodeIdForKRaftController(Map<Integer, RaftConfig.AddressSpec> voterAddressSpecsByNodeId) throws IllegalArgumentException {
        require(voterAddressSpecsByNodeId.containsKey(nodeId()),
                String.format("If %s contains the 'controller' role, the node id $nodeId must be included in the set of voters %s=%s", PROCESS_ROLES_PROP, QUORUM_VOTERS_PROP, voterAddressSpecsByNodeId.keySet()));
    }
    private void validateControllerListenerExistsForKRaftController() throws IllegalArgumentException {
        require(!controllerListeners().isEmpty(),
                String.format("%s must contain at least one value appearing in the '%s' configuration when running the KRaft controller role", CONTROLLER_LISTENER_NAMES_PROP, LISTENERS_PROP));
    }
    private void validateControllerListenerNamesMustAppearInListenersForKRaftController() throws IllegalArgumentException {
        Set<String> listenerNameValues = listeners().stream()
                .filter(l -> l.listenerName().isPresent())
                .map(l -> l.listenerName().get())
                .collect(Collectors.toSet());
        require(controllerListenerNames().stream().allMatch(cln -> listenerNameValues.contains(cln)),
                String.format("%s must only contain values appearing in the '%s' configuration when running the KRaft controller role", CONTROLLER_LISTENER_NAMES_PROP, LISTENERS_PROP));
    }
    private void validateAdvertisedListenersNonEmptyForBroker(Set<ListenerName> advertisedListenerNames) throws IllegalArgumentException {
        require(!advertisedListenerNames.isEmpty(),
                "There must be at least one advertised listener." + (
                            processRoles().contains(BrokerRole) ?
                                    String.format(" Perhaps all listeners appear in %s?", CONTROLLER_LISTENER_NAMES_PROP) :
                                    ""));
    }

    @SuppressWarnings("deprecation")
    public final void validateValues() throws IllegalArgumentException {
        if (nodeId() != brokerId()) {
            throw new ConfigException(String.format("You must set `%s` to the same value as `%s`.", NODE_ID_PROP, BROKER_ID_PROP));
        }
        validateKraftAndZkConfigs();
        validateLogConfigs();
        require(replicaFetchWaitMaxMs() <= replicaSocketTimeoutMs(), "replica.socket.timeout.ms should always be at least replica.fetch.wait.max.ms" +
                " to prevent unnecessary socket timeouts");
        require(replicaFetchWaitMaxMs() <= replicaLagTimeMaxMs(), "replica.fetch.wait.max.ms should always be less than or equal to replica.lag.time.max.ms" +
                " to prevent frequent changes in ISR");
        require(offsetCommitRequiredAcks() >= -1 && offsetCommitRequiredAcks() <= offsetsTopicReplicationFactor(),
                "offsets.commit.required.acks must be greater or equal -1 and less or equal to offsets.topic.replication.factor");
        Set<ListenerName> advertisedListenerNames = extractListenerNames(effectiveAdvertisedListeners());

        // validate KRaft-related configs
        Map<Integer, RaftConfig.AddressSpec> voterAddressSpecsByNodeId = RaftConfig.parseVoterConnections(quorumVoters());
        MetadataVersion interBrokerProtocolVersion = interBrokerProtocolVersion();
        // KRaft broker-only
        validateKraftBrokerRoleConfigs(voterAddressSpecsByNodeId, advertisedListenerNames);
        // KRaft controller-only
        validateKraftControllerRoleConfigs(voterAddressSpecsByNodeId);
        // KRaft combined broker and controller
        validateCombineMode(voterAddressSpecsByNodeId, advertisedListenerNames);
        // ZK-based
        validateZkBasedConfigs(voterAddressSpecsByNodeId, interBrokerProtocolVersion, advertisedListenerNames);

        Set<ListenerName> listenerNames = extractListenerNames();
        validateBrokerListeners(advertisedListenerNames, listenerNames);

        require(!effectiveAdvertisedListeners().stream().anyMatch(endpoint -> "0.0.0.0".equals(endpoint.host())),
                String.format("%s cannot use the nonroutable meta-address 0.0.0.0. Use a routable IP address.",
                        ADVERTISED_LISTENERS_PROP));

        // validate control.plane.listener.name config
        if (controlPlaneListenerName().isPresent()) {
            require(advertisedListenerNames.contains(controlPlaneListenerName().get()),
                    String.format("%s must be a listener name defined in %s. The valid options based on currently configured listeners are %s",
                            CONTROL_PLANE_LISTENER_NAME_PROP, ADVERTISED_LISTENERS_PROP,
                            String.join(",", advertisedListenerNames.stream().map(ListenerName::value).collect(Collectors.toSet()))));
            // controlPlaneListenerName should be different from interBrokerListenerName
            require(!controlPlaneListenerName().get().value().equals(interBrokerListenerName().value()),
                    String.format("%s, when defined, should have a different value from the inter broker listener name. Currently they both have the value %s",
                            CONTROL_PLANE_LISTENER_NAME_PROP, controlPlaneListenerName().get()));
        }

        LogConfig.MessageFormatVersion messageFormatVersion = new LogConfig.MessageFormatVersion(logMessageFormatVersionString(), interBrokerProtocolVersionString());
        if (messageFormatVersion.shouldWarn()) {
            log.warn(createBrokerWarningMessage());
        }

        RecordVersion recordVersion = logMessageFormatVersion().highestSupportedRecordVersion();
        require(interBrokerProtocolVersion.highestSupportedRecordVersion().value >= recordVersion.value,
                String.format("log.message.format.version %s can only be used when inter.broker.protocol.version is set to version %s or higher",
                        logMessageFormatVersionString(), MetadataVersion.minSupportedFor(recordVersion).shortVersion()));

        if (offsetsTopicCompressionType() == CompressionType.ZSTD)
            require(interBrokerProtocolVersion.highestSupportedRecordVersion().value >= IBP_2_1_IV0.highestSupportedRecordVersion().value,
                    String.format("offsets.topic.compression.codec zstd can only be used when inter.broker.protocol.version is set to version %s or higher", IBP_2_1_IV0.shortVersion()));

        boolean interBrokerUsesSasl = interBrokerSecurityProtocol() == SecurityProtocol.SASL_PLAINTEXT || interBrokerSecurityProtocol() == SecurityProtocol.SASL_SSL;
        require(!interBrokerUsesSasl || saslInterBrokerHandshakeRequestEnable() || saslMechanismInterBrokerProtocol() == SaslConfigs.GSSAPI_MECHANISM,
                "Only GSSAPI mechanism is supported for inter-broker communication with SASL when inter.broker.protocol.version is set to " + interBrokerProtocolVersionString());
        require(!interBrokerUsesSasl || saslEnabledMechanisms(interBrokerListenerName()).contains(saslMechanismInterBrokerProtocol()),
                String.format("%s must be included in %s when SASL is used for inter-broker communication",
                        SASL_MECHANISM_INTER_BROKER_PROTOCOL_PROP, SASL_ENABLED_MECHANISMS_PROP));
        require(queuedMaxBytes() <= 0 || queuedMaxBytes() >= socketRequestMaxBytes(),
                String.format("%s must be larger or equal to %s", QUEUED_MAX_BYTES_PROP, SOCKET_REQUEST_MAX_BYTES_PROP));

        if (maxConnectionsPerIp() == 0)
            require(!maxConnectionsPerIpOverrides().isEmpty(), String.format("%s can be set to zero only if %s property is set.", MAX_CONNECTIONS_PER_IP_PROP, MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP));

        Set<String> invalidAddresses = maxConnectionsPerIpOverrides().keySet().stream().filter(address -> !Utils.validHostPattern(address)).collect(Collectors.toSet());
        if (!invalidAddresses.isEmpty()) {
            throw new IllegalArgumentException(String.format("%s contains invalid addresses : %s",
                    MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP,
                    String.join(",", invalidAddresses)
                    ));
        }

        if (connectionsMaxIdleMs() >= 0) {
            require(failedAuthenticationDelayMs() < connectionsMaxIdleMs(), String.format(
                    "%s=%s should always be less than %s=%s to prevent failed authentication responses from timing out",
                    FAILED_AUTHENTICATION_DELAY_MS_PROP,
                    failedAuthenticationDelayMs(),
                    CONNECTIONS_MAX_IDLE_MS_PROP,
                    connectionsMaxIdleMs()));
        }

        Class<?> principalBuilderClass = getClass(PRINCIPAL_BUILDER_CLASS_PROP);
        require(principalBuilderClass != null, String.format("%s must be non-null", PRINCIPAL_BUILDER_CLASS_PROP));
        require(KafkaPrincipalSerde.class.isAssignableFrom(principalBuilderClass),
                String.format("%s must implement KafkaPrincipalSerde", PRINCIPAL_BUILDER_CLASS_PROP));

        // New group coordinator configs validation.
        validateGroupCoordinatorConfigs();
    }

    private void validateLogConfigs() {
        require(logRollTimeMillis() >= 1, "log.roll.ms must be greater than or equal to 1");
        require(logRollTimeJitterMillis() >= 0, "log.roll.jitter.ms must be greater than or equal to 0");
        require(logRetentionTimeMillis() >= 1 || logRetentionTimeMillis() == -1, "log.retention.ms must be unlimited (-1) or, greater than or equal to 1");
        require(!logDirs().isEmpty(), "At least one log directory must be defined via log.dirs or log.dir.");
        if (isRemoteLogStorageSystemEnabled() && logDirs().size() > 1) {
            throw new ConfigException(String.format("Multiple log directories `%s` are not supported when remote log storage is enabled", String.join(",", logDirs())));
        }
        require(logCleanerDedupeBufferSize() / logCleanerThreads() > 1024 * 1024, "log.cleaner.dedupe.buffer.size must be at least 1MB per cleaner thread.");
    }

    private void validateBrokerListeners(Set<ListenerName> advertisedListenerNames, Set<ListenerName> listenerNames) {
        if (processRoles().isEmpty() || processRoles().contains(BrokerRole)) {
            // validations for all broker setups (i.e. ZooKeeper and KRaft broker-only and KRaft co-located)
            validateAdvertisedListenersNonEmptyForBroker(advertisedListenerNames);
            require(advertisedListenerNames.contains(interBrokerListenerName()),
                    String.format("%s must be a listener name defined in %s. The valid options based on currently configured listeners are %s",
                            INTER_BROKER_LISTENER_NAME_PROP, ADVERTISED_LISTENERS_PROP, String.join(", ", advertisedListenerNames.stream().map(ListenerName::value).collect(Collectors.toSet()))));
            require(listenerNames.containsAll(advertisedListenerNames),
                    String.format("%s listener names must be equal to or a subset of the ones defined in %s. Found %s. The valid options based on the current configuration are %s",
                            ADVERTISED_LISTENERS_PROP, LISTENERS_PROP,
                            String.join(",", advertisedListenerNames.stream().map(ListenerName::value).collect(Collectors.toSet())),
                            String.join(", ", listenerNames.stream().map(ListenerName::value).collect(Collectors.toSet()))));
        }
    }

    private void validateGroupCoordinatorConfigs() {
        require(consumerGroupMaxHeartbeatIntervalMs() >= consumerGroupMinHeartbeatIntervalMs(), String.format(
                "%s must be greater than or equals to %s",
                CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_PROP,
                CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_PROP));
        require(consumerGroupHeartbeatIntervalMs() >= consumerGroupMinHeartbeatIntervalMs(),
                String.format("%s must be greater than or equals to %s",
                        CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_PROP,
                        CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_PROP));
        require(consumerGroupHeartbeatIntervalMs() <= consumerGroupMaxHeartbeatIntervalMs(),
                String.format("%s must be less than or equals to %s",
                        CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_PROP,
                        CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_PROP));

        require(consumerGroupMaxSessionTimeoutMs() >= consumerGroupMinSessionTimeoutMs(),
                String.format("%s must be greater than or equals to %s",
                        CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_PROP,
                        CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_PROP));
        require(consumerGroupSessionTimeoutMs() >= consumerGroupMinSessionTimeoutMs(),
                String.format("%s must be greater than or equals to %s",
                        CONSUMER_GROUP_SESSION_TIMEOUT_MS_PROP,
                        CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_PROP));
        require(consumerGroupSessionTimeoutMs() <= consumerGroupMaxSessionTimeoutMs(),
                String.format("%s must be less than or equals to %s",
                        CONSUMER_GROUP_SESSION_TIMEOUT_MS_PROP,
                        CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_PROP));
    }

    private void validateZkBasedConfigs(Map<Integer, RaftConfig.AddressSpec> voterAddressSpecsByNodeId, MetadataVersion interBrokerProtocolVersion, Set<ListenerName> advertisedListenerNames) {
        if (processRoles().isEmpty()) {
            if (migrationEnabled()) {
                validateNonEmptyQuorumVotersForMigration(voterAddressSpecsByNodeId);
                require(!controllerListenerNames().isEmpty(),
                        String.format("%s must not be empty when running in ZooKeeper migration mode: %s",
                                CONTROLLER_LISTENER_NAMES_PROP, controllerListenerNames()));
                require(interBrokerProtocolVersion.isMigrationSupported(), String.format(
                        "Cannot enable ZooKeeper migration without setting '%s' to 3.4 or higher", INTER_BROKER_PROTOCOL_VERSION_PROP));
                require(logDirs().size() == 1, String.format(
                        "Cannot enable ZooKeeper migration with multiple log directories (aka JBOD) without setting '%s' to %s or higher",
                        INTER_BROKER_PROTOCOL_VERSION_PROP, MetadataVersion.IBP_3_7_IV2));
            } else {
                // controller listener names must be empty when not in KRaft mode
                require(controllerListenerNames().isEmpty(),
                        String.format("%s must be empty when not running in KRaft mode: %s", CONTROLLER_LISTENER_NAMES_PROP, controllerListenerNames()));
            }
            validateAdvertisedListenersNonEmptyForBroker(advertisedListenerNames);
        }
    }

    private void validateCombineMode(Map<Integer, RaftConfig.AddressSpec> voterAddressSpecsByNodeId, Set<ListenerName> advertisedListenerNames) {
        if (isKRaftCombinedMode()) {
            validateNonEmptyQuorumVotersForKRaft(voterAddressSpecsByNodeId);
            validateControlPlaneListenerEmptyForKRaft();
            validateAdvertisedListenersDoesNotContainControllerListenersForKRaftBroker(advertisedListenerNames);
            validateControllerQuorumVotersMustContainNodeIdForKRaftController(voterAddressSpecsByNodeId);
            validateControllerListenerExistsForKRaftController();
            validateControllerListenerNamesMustAppearInListenersForKRaftController();
            validateAdvertisedListenersNonEmptyForBroker(advertisedListenerNames);
        }
    }

    private void validateKraftControllerRoleConfigs(Map<Integer, RaftConfig.AddressSpec> voterAddressSpecsByNodeId) {
        if (processRoles().equals(Utils.mkSet(ControllerRole))) {
            validateNonEmptyQuorumVotersForKRaft(voterAddressSpecsByNodeId);
            validateControlPlaneListenerEmptyForKRaft();
            // advertised listeners must be empty when only the controller is configured
            require(
                    getString(ADVERTISED_LISTENERS_PROP) == null,
                    String.format("The %s config must be empty when %s=controller",
                            ADVERTISED_LISTENERS_PROP, PROCESS_ROLES_PROP));
            // listeners should only contain listeners also enumerated in the controller listener
            require(
                    effectiveAdvertisedListeners().isEmpty(),
                    String.format("The %s config must only contain KRaft controller listeners from %s when %s=controller",
                            LISTENERS_PROP, CONTROLLER_LISTENER_NAMES_PROP, PROCESS_ROLES_PROP));
            validateControllerQuorumVotersMustContainNodeIdForKRaftController(voterAddressSpecsByNodeId);
            validateControllerListenerExistsForKRaftController();
            validateControllerListenerNamesMustAppearInListenersForKRaftController();
        }
    }

    private void validateKraftBrokerRoleConfigs(Map<Integer, RaftConfig.AddressSpec> voterAddressSpecsByNodeId, Set<ListenerName> advertisedListenerNames) {
        if (processRoles().equals(Utils.mkSet(BrokerRole))) {
            validateNonEmptyQuorumVotersForKRaft(voterAddressSpecsByNodeId);
            validateControlPlaneListenerEmptyForKRaft();
            validateAdvertisedListenersDoesNotContainControllerListenersForKRaftBroker(advertisedListenerNames);
            // nodeId must not appear in controller.quorum.voters
            require(!voterAddressSpecsByNodeId.containsKey(nodeId()),
                    String.format(
                            "If %s contains just the 'broker' role, the node id %s must not be included in the set of voters %s=%s",
                            PROCESS_ROLES_PROP, nodeId(), QUORUM_VOTERS_PROP, voterAddressSpecsByNodeId.keySet()));
            // controller.listener.names must be non-empty...
            require(!controllerListenerNames().isEmpty(),
                    String.format("%s must contain at least one value when running KRaft with just the broker role", CONTROLLER_LISTENER_NAMES_PROP));
            // controller.listener.names are forbidden in listeners...
            require(controllerListeners().isEmpty(),
                    String.format("%s must not contain a value appearing in the '%s' configuration when running KRaft with just the broker role", CONTROLLER_LISTENER_NAMES_PROP, LISTENERS_PROP));
            // controller.listener.names must all appear in listener.security.protocol.map
            controllerListenerNames().forEach(name -> {
                ListenerName listenerName = ListenerName.normalised(name);
                if (!effectiveListenerSecurityProtocolMap().containsKey(listenerName)) {
                    throw new ConfigException(String.format("Controller listener with name %s defined in ", listenerName.value()) +
                            String.format("%s not found in %s  (an explicit security mapping for each controller listener is required if %s is non-empty, or if there are security protocols other than PLAINTEXT in use)",
                                    CONTROLLER_LISTENER_NAMES_PROP,
                                    LISTENER_SECURITY_PROTOCOL_MAP_PROP,
                                    LISTENER_SECURITY_PROTOCOL_MAP_PROP));
                }
            });
            // warn that only the first controller listener is used if there is more than one
            if (controllerListenerNames().size() > 1) {
                log.warn(String.format("%s has multiple entries; only the first will be used since %s=broker: %s",
                        CONTROLLER_LISTENER_NAMES_PROP,
                        PROCESS_ROLES_PROP,
                        controllerListenerNames()));
            }
            validateAdvertisedListenersNonEmptyForBroker(advertisedListenerNames);
        }
    }

    private void validateKraftAndZkConfigs() {
        if (requiresZookeeper()) {
            if (zkConnect() == null) {
                throw new ConfigException(String.format("Missing required configuration `%s` which has no default value.", ZK_CONNECT_PROP));
            }
            if (brokerIdGenerationEnable()) {
                require(brokerId() >= -1 && brokerId() <= maxReservedBrokerId(), "broker.id must be greater than or equal to -1 and not greater than reserved.broker.max.id");
            } else {
                require(brokerId() >= 0, "broker.id must be greater than or equal to 0");
            }
        } else {
            // KRaft-based metadata quorum
            if (nodeId() < 0) {
                throw new ConfigException(String.format("Missing configuration `%s` which is required ", NODE_ID_PROP) +
                        "when `process.roles` is defined (i.e. when running in KRaft mode).");
            }
            if (migrationEnabled()) {
                if (zkConnect() == null) {
                    throw new ConfigException(String.format("If using `%s` in KRaft mode, `%s` must also be set.", MIGRATION_ENABLED_PROP, ZK_CONNECT_PROP));
                }
            }
        }
    }

    /**
     * Copy the subset of properties that are relevant to Logs. The individual properties
     * are listed here since the names are slightly different in each Config class...
     */
    @SuppressWarnings("deprecation")
    public final Map<String, Object> extractLogConfigMap() {
        Map<String, Object> logProps = new HashMap<String, Object>();
        logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, logSegmentBytes());
        logProps.put(TopicConfig.SEGMENT_MS_CONFIG, logRollTimeMillis());
        logProps.put(TopicConfig.SEGMENT_JITTER_MS_CONFIG, logRollTimeJitterMillis());
        logProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, logIndexSizeMaxBytes());
        logProps.put(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, logFlushIntervalMessages());
        logProps.put(TopicConfig.FLUSH_MS_CONFIG, logFlushIntervalMs());
        logProps.put(TopicConfig.RETENTION_BYTES_CONFIG, logRetentionBytes());
        logProps.put(TopicConfig.RETENTION_MS_CONFIG, logRetentionTimeMillis());
        logProps.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, messageMaxBytes());
        logProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, logIndexIntervalBytes());
        logProps.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, logCleanerDeleteRetentionMs());
        logProps.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, logCleanerMinCompactionLagMs());
        logProps.put(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, logCleanerMaxCompactionLagMs());
        logProps.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, logDeleteDelayMs());
        logProps.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, logCleanerMinCleanRatio());
        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, logCleanupPolicy());
        logProps.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInSyncReplicas());
        logProps.put(TopicConfig.COMPRESSION_TYPE_CONFIG, compressionType());
        logProps.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, uncleanLeaderElectionEnable());
        logProps.put(TopicConfig.PREALLOCATE_CONFIG, logPreAllocateEnable());
        logProps.put(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG, logMessageFormatVersion().version());
        logProps.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, logMessageTimestampType().name);
        logProps.put(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, logMessageTimestampDifferenceMaxMs());
        logProps.put(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, logMessageTimestampBeforeMaxMs());
        logProps.put(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, logMessageTimestampAfterMaxMs());
        logProps.put(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, logMessageDownConversionEnable());
        logProps.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, logLocalRetentionMs());
        logProps.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, logLocalRetentionBytes());
        return logProps;
    }

    private SecurityProtocol getSecurityProtocol(String protocolName, String configName) {
        try {
            return SecurityProtocol.forName(protocolName);
        } catch (IllegalArgumentException e) {
            throw new ConfigException(String.format("Invalid security protocol `%s` defined in %s", protocolName, configName));
        }
    }

    @SuppressWarnings("deprecation")
    private String createBrokerWarningMessage() {
        return String.format("Broker configuration %s with value %s is ignored because the inter-broker protocol version `%s` is greater or equal than 3.0. This configuration is deprecated and it will be removed in Apache Kafka 4.0.",
                LOG_MESSAGE_FORMAT_VERSION_PROP, logMessageFormatVersionString(), interBrokerProtocolVersionString());
    }

    private Set<ListenerName> extractListenerNames(Collection<Endpoint> listeners) {
        return listeners.stream().filter(l -> l.listenerName().isPresent()).map(listener -> ListenerName.normalised(listener.listenerName().get())).collect(Collectors.toSet());
    }

    private Set<ListenerName> extractListenerNames() {
        return extractListenerNames(listeners());
    }

    public interface DynamicConfigProvider {
        DynamicBrokerConfigBaseManager init(KafkaConfig config);
        DynamicConfigProvider NO_PROVIDER = config -> null;
    }
}

