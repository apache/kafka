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
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.ConsumerGroupMigrationPolicy;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.transaction.TransactionLogConfigs;
import org.apache.kafka.coordinator.transaction.TransactionStateManagerConfigs;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.security.PasswordEncoderConfigs;
import org.apache.kafka.security.authorizer.AuthorizerUtils;
import org.apache.kafka.server.ProcessRole;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.metrics.MetricConfigs;
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

/**
 * During moving kafka.server.KafkaConfig out of core AbstractKafkaConfig will be the future KafkaConfig
 * so any new getters, or updates to `CONFIG_DEF` will be defined here.
 * Any code depends on kafka.server.KafkaConfig will keep for using kafka.server.KafkaConfig for the time being until we move it out of core
 * For more details check KAFKA-15853
 */
public abstract class AbstractKafkaConfig extends AbstractConfig {
    public static final ConfigDef CONFIG_DEF = Utils.mergeConfigs(Arrays.asList(
            RemoteLogManagerConfig.configDef(),
            ZkConfigs.CONFIG_DEF,
            ServerConfigs.CONFIG_DEF,
            KRaftConfigs.CONFIG_DEF,
            SocketServerConfigs.CONFIG_DEF,
            ReplicationConfigs.CONFIG_DEF,
            GroupCoordinatorConfig.GROUP_COORDINATOR_CONFIG_DEF,
            GroupCoordinatorConfig.NEW_GROUP_CONFIG_DEF,
            GroupCoordinatorConfig.OFFSET_MANAGEMENT_CONFIG_DEF,
            GroupCoordinatorConfig.CONSUMER_GROUP_CONFIG_DEF,
            CleanerConfig.CONFIG_DEF,
            LogConfig.SERVER_CONFIG_DEF,
            ShareGroupConfigs.CONFIG_DEF,
            TransactionLogConfigs.CONFIG_DEF,
            TransactionStateManagerConfigs.CONFIG_DEF,
            QuorumConfig.CONFIG_DEF,
            MetricConfigs.CONFIG_DEF,
            QuotaConfigs.CONFIG_DEF,
            BrokerSecurityConfigs.CONFIG_DEF,
            DelegationTokenManagerConfigs.CONFIG_DEF,
            PasswordEncoderConfigs.CONFIG_DEF
    ));
    private static final Logger log = LoggerFactory.getLogger(AbstractKafkaConfig.class);
    protected final KafkaConfigValidator configValidator;
    private final RemoteLogManagerConfig remoteLogManagerConfig;

    private Integer brokerId;

    // Cache the current config to avoid acquiring read lock to access from dynamicConfig
    protected volatile AbstractKafkaConfig currentConfig = this;

    public AbstractKafkaConfig(ConfigDef definition, Map<?, ?> original, Map<String, ?> configProviderProps, boolean doLog) {
        super(definition, original, configProviderProps, doLog);
        this.remoteLogManagerConfig = new RemoteLogManagerConfig(original);
        this.configValidator = new KafkaConfigValidator(this.currentConfig, log);
    }

    public KafkaConfigValidator validator() {
        return configValidator;
    }

    public Map<String, Object> valuesFromThisConfigWithPrefixOverride(String prefix) {
        return super.valuesWithPrefixOverride(prefix);
    }

    public Map<String, ?> valuesFromThisConfig() {
        return super.values();
    }

    //  During dynamic update, we use the values from this config, these are only used in DynamicBrokerConfig
    public Map<String, Object> originalsFromThisConfig() {
        return super.originals();
    }

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


    public final RemoteLogManagerConfig remoteLogManagerConfig() {
        return remoteLogManagerConfig;
    }

    /**
     * ******** Socket Server Configuration
     ***********/
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

    public final int maxConnectionsPerIp() {
        return getInt(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG);
    }

    public final Map<String, Integer> maxConnectionsPerIpOverrides() {
        return getMap(
                SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG,
                getString(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG)
        ).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> Integer.parseInt(entry.getValue())));
    }

    public final int maxConnections() {
        return getInt(SocketServerConfigs.MAX_CONNECTIONS_CONFIG);
    }

    public final int maxConnectionCreationRate() {
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

    public final Optional<String> replicaSelectorClassName() {
        return Optional.ofNullable(getString(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG));
    }

    /**
     * ******** Log Configuration
     ***********/
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

    public final int logCleanerThreads() {
        return getInt(CleanerConfig.LOG_CLEANER_THREADS_PROP);
    }

    public final int numRecoveryThreadsPerDataDir() {
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

    public final int offsetsRetentionMinutes() {
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

    public final int logCleanerIoBufferSize() {
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

    public final int logIndexSizeMaxBytes() {
        return getInt(ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_CONFIG);
    }

    public final int logIndexIntervalBytes() {
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

    public final int minInSyncReplicas() {
        return getInt(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG);
    }

    public final boolean logPreAllocateEnable() {
        return getBoolean(ServerLogConfigs.LOG_PRE_ALLOCATE_CONFIG);
    }

    public final Long logInitialTaskDelayMs() {
        return Optional.ofNullable(getLong(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG)).orElse(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_DEFAULT);
    }

    // We keep the user-provided String as `MetadataVersion.fromVersionString` can choose a slightly different version
    // (e.g. if `0.10.0` is passed, `0.10.0-IV0` may be picked)
    @Deprecated
    public final String logMessageFormatVersionString() {
        return getString(ServerLogConfigs.LOG_MESSAGE_FORMAT_VERSION_CONFIG);
    }

    /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details
    deprecated since 3.0
     */
    @Deprecated
    public final MetadataVersion logMessageFormatVersion() {
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
    public final long logMessageTimestampAfterMaxMs() {
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

    /**
     * ******** Replication configuration
     ***********/
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

    public final long leaderImbalanceCheckIntervalSeconds() {
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
        if (processRoles().isEmpty()) {
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

    /**
     * ******** Controlled shutdown configuration
     ***********/
    public final int controlledShutdownMaxRetries() {
        return getInt(ServerConfigs.CONTROLLED_SHUTDOWN_MAX_RETRIES_CONFIG);
    }

    public final long controlledShutdownRetryBackoffMs() {
        return getLong(ServerConfigs.CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_CONFIG);
    }

    public final boolean controlledShutdownEnable() {
        return getBoolean(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG);
    }

    /**
     * ******** Feature configuration
     ***********/
    public final boolean isFeatureVersioningSupported() {
        return interBrokerProtocolVersion().isFeatureVersioningSupported();
    }

    /**
     * ******** Group coordinator configuration
     ***********/
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

    /**
     * New group coordinator configs
     */
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
    public boolean isNewGroupCoordinatorEnabled() {
        return getBoolean(GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG) ||
                groupCoordinatorRebalanceProtocols().contains(Group.GroupType.CONSUMER);
    }

    public final int groupCoordinatorNumThreads() {
        return getInt(GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_CONFIG);
    }

    public final int  groupCoordinatorAppendLingerMs() {
        return getInt(GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG);
    }

    /**
     * Consumer group configs
     */
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

    public final List<ConsumerGroupPartitionAssignor> consumerGroupAssignors() {
        return getConfiguredInstances(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, ConsumerGroupPartitionAssignor.class);
    }

    public final ConsumerGroupMigrationPolicy consumerGroupMigrationPolicy() {
        return ConsumerGroupMigrationPolicy.parse(getString(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG));
    }

    /**
     * Share group configuration
     **/
    public final boolean isShareGroupEnabled() {
        return getBoolean(ShareGroupConfigs.SHARE_GROUP_ENABLE_CONFIG);
    }

    public final int shareGroupPartitionMaxRecordLocks() {
        return getInt(ShareGroupConfigs.SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG);
    }

    public final int shareGroupDeliveryCountLimit() {
        return getInt(ShareGroupConfigs.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG);
    }

    public final short shareGroupMaxGroups() {
        return getShort(ShareGroupConfigs.SHARE_GROUP_MAX_GROUPS_CONFIG);
    }

    public final short shareGroupMaxSize() {
        return getShort(ShareGroupConfigs.SHARE_GROUP_MAX_SIZE_CONFIG);
    }

    public final int shareGroupSessionTimeoutMs() {
        return getInt(ShareGroupConfigs.SHARE_GROUP_SESSION_TIMEOUT_MS_CONFIG);
    }

    public final int shareGroupMinSessionTimeoutMs() {
        return getInt(ShareGroupConfigs.SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG);
    }

    public final int shareGroupMaxSessionTimeoutMs() {
        return getInt(ShareGroupConfigs.SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG);
    }

    public final int shareGroupHeartbeatIntervalMs() {
        return getInt(ShareGroupConfigs.SHARE_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG);
    }

    public final int shareGroupMinHeartbeatIntervalMs() {
        return getInt(ShareGroupConfigs.SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG);
    }

    public final int shareGroupMaxHeartbeatIntervalMs() {
        return getInt(ShareGroupConfigs.SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG);
    }

    public final int shareGroupRecordLockDurationMs() {
        return getInt(ShareGroupConfigs.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG);
    }

    public final int shareGroupMaxRecordLockDurationMs() {
        return getInt(ShareGroupConfigs.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG);
    }

    public final int shareGroupMinRecordLockDurationMs() {
        return getInt(ShareGroupConfigs.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG);
    }


    /**
     * ******** Offset management configuration
     ***********/
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

    public CompressionType offsetsTopicCompressionType() {
        return Optional.ofNullable(getInt(GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_CONFIG))
                .map(CompressionType::forId).orElse(null);
    }

    /**
     * ******** Transaction management configuration
     ***********/
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

    /**
     * ******** Metric Configuration
     **************/
    public final int metricNumSamples() {
        return getInt(MetricConfigs.METRIC_NUM_SAMPLES_CONFIG);
    }

    public final long metricSampleWindowMs() {
        return getLong(MetricConfigs.METRIC_SAMPLE_WINDOW_MS_CONFIG);
    }

    public final String metricRecordingLevel() {
        return getString(MetricConfigs.METRIC_RECORDING_LEVEL_CONFIG);
    }

    /**
     * ******** Kafka Client Telemetry Metrics Configuration
     ***********/
    public final int clientTelemetryMaxBytes() {
        return getInt(MetricConfigs.CLIENT_TELEMETRY_MAX_BYTES_CONFIG);
    }

    /**
     * ******** SSL/SASL Configuration
     **************/
    // Security configs may be overridden for listeners, so it is not safe to use the base values
    // Hence the base SSL/SASL configs are not fields of KafkaConfig, listener configs should be
    // retrieved using KafkaConfig#valuesWithPrefixOverride
    @SuppressWarnings("unchecked")
    protected final Set<String> saslEnabledMechanisms(ListenerName listenerName) {
        return Optional.ofNullable(valuesWithPrefixOverride(listenerName.configPrefix()).get(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG))
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

    public final String saslMechanismInterBrokerProtocol() {
        return getString(BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG);
    }

    public final boolean saslInterBrokerHandshakeRequestEnable() {
        return interBrokerProtocolVersion().isSaslInterBrokerHandshakeRequestEnabled();
    }

    /**
     * ******** DelegationToken Configuration
     **************/
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

    /**
     * ******** Password encryption configuration for dynamic configs
     *********/
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

    /** Quota Configuration **/
    public final int numQuotaSamples() {
        return getInt(QuotaConfigs.NUM_QUOTA_SAMPLES_CONFIG);
    }

    public final List<String> quorumBootstrapServers() {
        return getList(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG);
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

    /**
     * ******** Fetch Configuration
     **************/
    public final int maxIncrementalFetchSessionCacheSlots() {
        return getInt(ServerConfigs.MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_CONFIG);
    }

    public final int fetchMaxBytes() {
        return getInt(ServerConfigs.FETCH_MAX_BYTES_CONFIG);
    }

    /**
     * ******** Request Limit Configuration
     ***********/
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

    /**
     * ******** Raft Quorum Configuration
     *********/
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

    /**
     * Internal Configurations
     **/
    public final boolean unstableApiVersionsEnabled() {
        return getBoolean(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG);
    }

    public final boolean unstableFeatureVersionsEnabled() {
        return getBoolean(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG);
    }

    /**
     * ******** General Configuration
     ***********/
    public Boolean brokerIdGenerationEnable() {
        return getBoolean(ServerConfigs.BROKER_ID_GENERATION_ENABLE_CONFIG);
    }

    public final int maxReservedBrokerId() {
        return getInt(ServerConfigs.RESERVED_BROKER_MAX_ID_CONFIG);
    }

    public final int brokerId() {
        this.brokerId = getInt(ServerConfigs.BROKER_ID_CONFIG);
        return this.brokerId;
    }

    public final void brokerId(int brokerId) {
        this.brokerId = brokerId;
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
        return processRoles().equals(new HashSet<>(Arrays.asList(ProcessRole.BrokerRole, ProcessRole.ControllerRole)));
    }

    public final String metadataLogDir() {
        return Optional.ofNullable(getString(KRaftConfigs.METADATA_LOG_DIR_CONFIG)).orElse(logDirs().get(0));
    }

    public final boolean requiresZookeeper() {
        return processRoles().isEmpty();
    }

    public final boolean usesSelfManagedQuorum() {
        return !processRoles().isEmpty();
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

    public final Set<ProcessRole> processRoles() {
        return parseProcessRoles();
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
        Set<ListenerName> listenersSet = extractListenerNames();
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

    public final long logLocalRetentionBytes() {
        return getLong(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP);
    }

    public final long logLocalRetentionMs() {
        return getLong(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP);
    }

    public final int remoteFetchMaxWaitMs() {
        return getInt(RemoteLogManagerConfig.REMOTE_FETCH_MAX_WAIT_MS_PROP);
    }

    public final long remoteLogIndexFileCacheTotalSizeBytes() {
        return getLong(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP);
    }

    public final long remoteLogManagerCopyMaxBytesPerSecond() {
        return getLong(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP);
    }

    public final long remoteLogManagerFetchMaxBytesPerSecond() {
        return getLong(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP);
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
                            SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG + " not found in " + SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG + "."));
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
        }
        return mapValue; // don't add default mappings since we found something that is SSL or SASL_*
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
                Utils.mkEntry(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, logMessageTimestampType().name),
                Utils.mkEntry(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, logMessageTimestampDifferenceMaxMs()),
                Utils.mkEntry(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, logMessageTimestampBeforeMaxMs()),
                Utils.mkEntry(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, logMessageTimestampAfterMaxMs()),
                Utils.mkEntry(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, logMessageDownConversionEnable()),
                Utils.mkEntry(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, logLocalRetentionMs()),
                Utils.mkEntry(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, logLocalRetentionBytes())
        );
    }
}
