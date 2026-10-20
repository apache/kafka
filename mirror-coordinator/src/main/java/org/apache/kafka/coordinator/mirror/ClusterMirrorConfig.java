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
package org.apache.kafka.coordinator.mirror;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import com.google.re2j.Pattern;

import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.SHORT;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static org.apache.kafka.server.util.MirrorUtils.compilePattern;
import static org.apache.kafka.server.util.MirrorUtils.compilePatternList;

/**
 * This class provides proper validation, defaults, and documentation
 * for all configurations supported by the Cluster Mirroring feature.
 */
public final class ClusterMirrorConfig extends AbstractConfig {
    /***********************************************************************************************************************
       Broker level configs. Set when starting/rebooting the broker. Stored in server.properties or dynamic broker config.
     ***********************************************************************************************************************/

    public static final String MIRROR_COORDINATOR_THREADS_CONFIG = "mirror.coordinator.threads";
    private static final String MIRROR_COORDINATOR_THREADS_DOC = "The number of threads used to process coordinator events for cluster mirroring.";
    private static final int MIRROR_COORDINATOR_THREADS_DEFAULT = 1;

    public static final String MIRROR_COORDINATOR_WRITE_TIMEOUT_MS_CONFIG = "mirror.coordinator.write.timeout.ms";
    private static final String MIRROR_COORDINATOR_WRITE_TIMEOUT_MS_DOC = "The timeout in milliseconds for write operations on the mirror state coordinator.";
    private static final long MIRROR_COORDINATOR_WRITE_TIMEOUT_MS_DEFAULT = 5000L;

    public static final String MIRROR_COORDINATOR_APPEND_LINGER_MS_CONFIG = "mirror.coordinator.append.linger.ms";
    private static final String MIRROR_COORDINATOR_APPEND_LINGER_MS_DOC = "The linger time in milliseconds for batching records before flushing to the mirror state topic.";
    private static final int MIRROR_COORDINATOR_APPEND_LINGER_MS_DEFAULT = 10;

    public static final String MIRROR_COORDINATOR_LOAD_BUFFER_SIZE_CONFIG = "mirror.coordinator.load.buffer.size";
    private static final String MIRROR_COORDINATOR_LOAD_BUFFER_SIZE_DOC = "The buffer size in bytes for loading mirror state records during coordinator startup.";
    private static final int MIRROR_COORDINATOR_LOAD_BUFFER_SIZE_DEFAULT = 5 * 1024 * 1024;

    public static final String MIRROR_STATE_TOPIC_NUM_PARTITIONS_CONFIG = "mirror.state.topic.num.partitions";
    private static final String MIRROR_STATE_TOPIC_NUM_PARTITIONS_DOC = "The number of partitions for the internal topic (should not change after deployment).";
    private static final int MIRROR_STATE_TOPIC_NUM_PARTITIONS_DEFAULT = 50;

    public static final String MIRROR_STATE_TOPIC_REPLICATION_FACTOR_CONFIG = "mirror.state.topic.replication.factor";
    private static final String MIRROR_STATE_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor for the internal topic. " +
            "Topic creation will fail until the cluster size meets this replication factor requirement.";
    private static final short MIRROR_STATE_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;

    public static final String MIRROR_NUM_REPLICA_FETCHERS_CONFIG = "mirror.num.replica.fetchers";
    private static final String MIRROR_NUM_REPLICA_FETCHERS_DOC = "Number of fetcher threads used to replicate records from each source broker in a cluster mirror. " +
            "The total number of mirror fetcher threads on a broker equals this value multiplied by the number of distinct source brokers and the number of cluster mirrors. " +
            "A higher value increases I/O parallelism for cross cluster replication at the cost of higher CPU and memory utilization.";
    private static final int MIRROR_NUM_REPLICA_FETCHERS_DEFAULT = 1;

    public static final String MIRROR_METADATA_REFRESH_INTERVAL_MS_CONFIG = "mirror.metadata.refresh.interval.ms";
    private static final String MIRROR_METADATA_REFRESH_INTERVAL_MS_DOC = "The interval in milliseconds at which the coordinator refreshes metadata from source clusters. " +
            "This controls how frequently the coordinator polls source clusters to detect new topics and metadata changes.";
    private static final long MIRROR_METADATA_REFRESH_INTERVAL_MS_DEFAULT = 60000L;

    public static final String MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_CONFIG = "mirror.failed.retry.initial.backoff.ms";
    private static final String MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_DOC = "The initial backoff time in milliseconds before retrying a mirror partition " +
            "in FAILED state. The actual delay uses full jitter: a uniform random value in [0, backoff].";
    private static final long MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_DEFAULT = 100L;

    public static final String MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_CONFIG = "mirror.failed.retry.max.backoff.ms";
    private static final String MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_DOC = "The maximum backoff time in milliseconds for retrying a mirror partition in FAILED state.";
    private static final long MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_DEFAULT = 300000L;

    public static final String MIRROR_FAILED_RETRY_MAX_ATTEMPTS_CONFIG = "mirror.failed.retry.max.attempts";
    private static final String MIRROR_FAILED_RETRY_MAX_ATTEMPTS_DOC = "The maximum number of automatic retry attempts for a mirror partition in FAILED state. " +
            "After this limit is reached, manual intervention is required via the start-mirror-topics command. " +
            "Set to 0 to disable automatic retries.";
    private static final int MIRROR_FAILED_RETRY_MAX_ATTEMPTS_DEFAULT = 20;

    /***************************************************************************************************
       Mirror level configs. Set when creating/altering a specific mirror. Stored in cluster metadata.
     ***************************************************************************************************/

    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
    private static final String BOOTSTRAP_SERVERS_DOC = "A list of host/port pairs to use for establishing the initial connection to the source cluster. " +
            "This is a required configuration when creating a cluster mirror.";

    public static final String SOURCE_CLUSTER_ID_CONFIG = CommonClientConfigs.SOURCE_CLUSTER_ID_CONFIG;
    private static final String SOURCE_CLUSTER_ID_DOC = CommonClientConfigs.SOURCE_CLUSTER_ID_DOC;

    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    private static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;
    private static final int REQUEST_TIMEOUT_MS_DEFAULT = 30 * 1000;

    public static final String CONNECTION_SETUP_TIMEOUT_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;
    private static final String CONNECTION_SETUP_TIMEOUT_MS_DOC = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC;
    private static final long CONNECTION_SETUP_TIMEOUT_MS_DEFAULT = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS;

    public static final String CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;
    private static final String CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC;
    private static final long CONNECTION_SETUP_TIMEOUT_MAX_MS_DEFAULT = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS;

    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;
    private static final String CONNECTIONS_MAX_IDLE_MS_DOC = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC;
    private static final long CONNECTIONS_MAX_IDLE_MS_DEFAULT = 10 * 60 * 1000L;

    public static final String SOCKET_TIMEOUT_MS_CONFIG = "socket.timeout.ms";
    private static final String SOCKET_TIMEOUT_MS_DOC = "The socket timeout for network requests.";
    private static final int SOCKET_TIMEOUT_MS_DEFAULT = 30 * 1000;

    public static final String SOCKET_RECEIVE_BUFFER_BYTES_CONFIG = "socket.receive.buffer.bytes";
    private static final String SOCKET_RECEIVE_BUFFER_BYTES_DOC = "The socket receive buffer for network requests to the leader for replicating data.";
    private static final int SOCKET_RECEIVE_BUFFER_BYTES_DEFAULT = 64 * 1024;

    public static final String FETCH_WAIT_MAX_MS_CONFIG = "fetch.wait.max.ms";
    private static final String FETCH_WAIT_MAX_MS_DOC = "The maximum wait time for each fetcher request issued by mirror replicas.";
    private static final int FETCH_WAIT_MAX_MS_DEFAULT = 1000;

    public static final String FETCH_BACKOFF_MS_CONFIG = "fetch.backoff.ms";
    private static final String FETCH_BACKOFF_MS_DOC = "The amount of time to wait before retrying mirror fetch requests after a failure. " +
            "This controls the backoff for mirror fetcher threads on connection errors or other exceptions from the source cluster.";
    private static final long FETCH_BACKOFF_MS_DEFAULT = 1000;

    public static final String FETCH_MIN_BYTES_CONFIG = "fetch.min.bytes";
    private static final String FETCH_MIN_BYTES_DOC = "Minimum bytes expected for each fetch response.";
    private static final int FETCH_MIN_BYTES_DEFAULT = 1;

    public static final String FETCH_RESPONSE_MAX_BYTES_CONFIG = "fetch.response.max.bytes";
    private static final String FETCH_RESPONSE_MAX_BYTES_DOC = "Maximum bytes expected for the entire fetch response. Records are fetched in batches, " +
            "and if the first record batch in the first non-empty partition is larger than this value, the record batch will still be returned.";
    private static final int FETCH_RESPONSE_MAX_BYTES_DEFAULT = 10 * 1024 * 1024;

    public static final String FETCH_MAX_BYTES_CONFIG = "fetch.max.bytes";
    private static final String FETCH_MAX_BYTES_DOC = "The number of bytes of messages to attempt to fetch for each partition.";
    private static final int FETCH_MAX_BYTES_DEFAULT = 1024 * 1024;

    public static final String TOPIC_PROPERTIES_EXCLUDE_CONFIG = "topic.properties.exclude";
    private static final String TOPIC_PROPERTIES_EXCLUDE_DOC = "A comma-separated list of topic config property names and/or regexes to exclude from synchronization. "
            + "Properties in this list will not be replicated from the source cluster.";
    private static final String TOPIC_PROPERTIES_EXCLUDE_DEFAULT =
            "follower.replication.throttled.replicas,"
                    + "leader.replication.throttled.replicas,"
                    + "message.timestamp.difference.max.ms,"
                    + "log.message.timestamp.before.max.ms,"
                    + "log.message.timestamp.after.max.ms,"
                    + "message.timestamp.type,"
                    + "unclean.leader.election.enable,"
                    + "min.insync.replicas,"
                    + "mirror.replication.throttled.replicas,"
                    + "mirror.unclean.leader.election.enable";

    public static final String TOPICS_INCLUDE_CONFIG = "topics.include";
    private static final String TOPICS_INCLUDE_DOC = "A comma-separated list of regex patterns for topic names to include in mirroring. "
            + "Topics on the source cluster whose names match at least one of the patterns will be automatically discovered and mirrored.";
    private static final String TOPICS_INCLUDE_DEFAULT = ".*";

    public static final String TOPICS_EXCLUDE_CONFIG = "topics.exclude";
    private static final String TOPICS_EXCLUDE_DOC = "A comma-separated list of regex patterns for topic names to exclude from mirroring. "
            + "Internal topics are excluded by default. Exclusions take precedence over inclusions.";
    private static final String TOPICS_EXCLUDE_DEFAULT = "__.*";

    public static final String GROUPS_INCLUDE_CONFIG = "groups.include";
    private static final String GROUPS_INCLUDE_DOC = "A comma-separated list of regex patterns for consumer group IDs to include in offset synchronization. "
            + "Only consumer groups whose IDs match at least one of the patterns will have their offsets replicated from the source cluster.";
    private static final String GROUPS_INCLUDE_DEFAULT = ".*";

    public static final String GROUPS_EXCLUDE_CONFIG = "groups.exclude";
    private static final String GROUPS_EXCLUDE_DOC = "A comma-separated list of regex patterns for consumer group IDs to exclude from offset synchronization. " +
            "Exclusions take precedence over inclusions.";
    private static final String GROUPS_EXCLUDE_DEFAULT = "";

    public static final String ACLS_INCLUDE_CONFIG = "acls.include";
    private static final String ACLS_INCLUDE_DOC = "A comma-separated list of ACL include rules. " + MirrorAclRule.DOC
            + "Examples: .* (match all ACLs), TOPIC;orders.* (all ACLs for topics matching orders.*), "
            + "TOPIC;.*;READ;ALLOW,TOPIC;.*;WRITE;ALLOW (all topic READ and WRITE ALLOW ACLs), "
            + "ANY;.*;ANY;ANY;User:alice (all ACLs for principal User:alice).";
    private static final String ACLS_INCLUDE_DEFAULT = ".*";

    public static final String ACLS_EXCLUDE_CONFIG = "acls.exclude";
    private static final String ACLS_EXCLUDE_DOC = "A comma-separated list of ACL exclude rules. " + MirrorAclRule.DOC
            + "Exclusions take precedence over inclusions. "
            + "Examples: .* (exclude all ACLs), ANY;.*;WRITE (exclude all WRITE ACLs), "
            + "ANY;.*;ANY;ANY;User:bob (exclude all ACLs for User:bob).";
    private static final String ACLS_EXCLUDE_DEFAULT = "";

    public static final String SECURITY_PROTOCOL_CONFIG = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
    private static final String SECURITY_PROTOCOL_DOC = CommonClientConfigs.SECURITY_PROTOCOL_DOC;
    private static final String SECURITY_PROTOCOL_DEFAULT = SecurityProtocol.PLAINTEXT.name;

    public static final String SECURITY_PROVIDERS_CONFIG = SecurityConfig.SECURITY_PROVIDERS_CONFIG;
    private static final String SECURITY_PROVIDERS_DOC = SecurityConfig.SECURITY_PROVIDERS_DOC;

    private static final ConfigDef CONFIG_DEF = buildConfigDef();

    private static ConfigDef buildConfigDef() {
        ConfigDef configDef = new ConfigDef()
                .define(BOOTSTRAP_SERVERS_CONFIG, LIST, null, HIGH, BOOTSTRAP_SERVERS_DOC)
                .define(SOURCE_CLUSTER_ID_CONFIG, STRING, null, HIGH, SOURCE_CLUSTER_ID_DOC)
                .define(REQUEST_TIMEOUT_MS_CONFIG, INT, REQUEST_TIMEOUT_MS_DEFAULT, atLeast(0), MEDIUM, REQUEST_TIMEOUT_MS_DOC)
                .define(CONNECTION_SETUP_TIMEOUT_MS_CONFIG, LONG, CONNECTION_SETUP_TIMEOUT_MS_DEFAULT, atLeast(0L), MEDIUM, CONNECTION_SETUP_TIMEOUT_MS_DOC)
                .define(CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, LONG, CONNECTION_SETUP_TIMEOUT_MAX_MS_DEFAULT, atLeast(0L), MEDIUM, CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
                .define(CONNECTIONS_MAX_IDLE_MS_CONFIG, LONG, CONNECTIONS_MAX_IDLE_MS_DEFAULT, MEDIUM, CONNECTIONS_MAX_IDLE_MS_DOC)
                .define(FETCH_WAIT_MAX_MS_CONFIG, INT, FETCH_WAIT_MAX_MS_DEFAULT, atLeast(0), MEDIUM, FETCH_WAIT_MAX_MS_DOC)
                .define(FETCH_BACKOFF_MS_CONFIG, LONG, FETCH_BACKOFF_MS_DEFAULT, atLeast(0L), MEDIUM, FETCH_BACKOFF_MS_DOC)
                .define(FETCH_MIN_BYTES_CONFIG, INT, FETCH_MIN_BYTES_DEFAULT, atLeast(0), MEDIUM, FETCH_MIN_BYTES_DOC)
                .define(FETCH_RESPONSE_MAX_BYTES_CONFIG, INT, FETCH_RESPONSE_MAX_BYTES_DEFAULT, atLeast(0), MEDIUM, FETCH_RESPONSE_MAX_BYTES_DOC)
                .define(FETCH_MAX_BYTES_CONFIG, INT, FETCH_MAX_BYTES_DEFAULT, atLeast(0), MEDIUM, FETCH_MAX_BYTES_DOC)
                .define(SOCKET_TIMEOUT_MS_CONFIG, INT, SOCKET_TIMEOUT_MS_DEFAULT, atLeast(0), MEDIUM, SOCKET_TIMEOUT_MS_DOC)
                .define(SOCKET_RECEIVE_BUFFER_BYTES_CONFIG, INT, SOCKET_RECEIVE_BUFFER_BYTES_DEFAULT, atLeast(0), MEDIUM, SOCKET_RECEIVE_BUFFER_BYTES_DOC)
                .define(TOPIC_PROPERTIES_EXCLUDE_CONFIG, LIST, TOPIC_PROPERTIES_EXCLUDE_DEFAULT, LOW, TOPIC_PROPERTIES_EXCLUDE_DOC)
                .define(TOPICS_INCLUDE_CONFIG, LIST, TOPICS_INCLUDE_DEFAULT, LOW, TOPICS_INCLUDE_DOC)
                .define(TOPICS_EXCLUDE_CONFIG, LIST, TOPICS_EXCLUDE_DEFAULT, LOW, TOPICS_EXCLUDE_DOC)
                .define(GROUPS_INCLUDE_CONFIG, LIST, GROUPS_INCLUDE_DEFAULT, LOW, GROUPS_INCLUDE_DOC)
                .define(GROUPS_EXCLUDE_CONFIG, LIST, GROUPS_EXCLUDE_DEFAULT, LOW, GROUPS_EXCLUDE_DOC)
                .define(ACLS_INCLUDE_CONFIG, LIST, ACLS_INCLUDE_DEFAULT, LOW, ACLS_INCLUDE_DOC)
                .define(ACLS_EXCLUDE_CONFIG, LIST, ACLS_EXCLUDE_DEFAULT, LOW, ACLS_EXCLUDE_DOC)
                .define(SECURITY_PROTOCOL_CONFIG, STRING, SECURITY_PROTOCOL_DEFAULT, ConfigDef.ValidString.in(SecurityProtocol.PLAINTEXT.name, SecurityProtocol.SSL.name,
                        SecurityProtocol.SASL_PLAINTEXT.name, SecurityProtocol.SASL_SSL.name), MEDIUM, SECURITY_PROTOCOL_DOC)
                .define(SECURITY_PROVIDERS_CONFIG, STRING, null, LOW, SECURITY_PROVIDERS_DOC);
        SslConfigs.addClientSslSupport(configDef);
        SaslConfigs.addClientSaslSupport(configDef);
        return configDef;
    }

    private static final ConfigDef BROKER_CONFIG_DEF = new ConfigDef()
            .define(MIRROR_COORDINATOR_THREADS_CONFIG, INT, MIRROR_COORDINATOR_THREADS_DEFAULT, atLeast(1), LOW, MIRROR_COORDINATOR_THREADS_DOC)
            .define(MIRROR_COORDINATOR_WRITE_TIMEOUT_MS_CONFIG, LONG, MIRROR_COORDINATOR_WRITE_TIMEOUT_MS_DEFAULT, atLeast(1L), LOW, MIRROR_COORDINATOR_WRITE_TIMEOUT_MS_DOC)
            .define(MIRROR_COORDINATOR_APPEND_LINGER_MS_CONFIG, INT, MIRROR_COORDINATOR_APPEND_LINGER_MS_DEFAULT, atLeast(0), LOW, MIRROR_COORDINATOR_APPEND_LINGER_MS_DOC)
            .define(MIRROR_COORDINATOR_LOAD_BUFFER_SIZE_CONFIG, INT, MIRROR_COORDINATOR_LOAD_BUFFER_SIZE_DEFAULT, atLeast(1), LOW, MIRROR_COORDINATOR_LOAD_BUFFER_SIZE_DOC)
            .define(MIRROR_STATE_TOPIC_NUM_PARTITIONS_CONFIG, INT, MIRROR_STATE_TOPIC_NUM_PARTITIONS_DEFAULT, atLeast(1), HIGH, MIRROR_STATE_TOPIC_NUM_PARTITIONS_DOC)
            .define(MIRROR_STATE_TOPIC_REPLICATION_FACTOR_CONFIG, SHORT, MIRROR_STATE_TOPIC_REPLICATION_FACTOR_DEFAULT, atLeast(1), HIGH, MIRROR_STATE_TOPIC_REPLICATION_FACTOR_DOC)
            .define(MIRROR_NUM_REPLICA_FETCHERS_CONFIG, INT, MIRROR_NUM_REPLICA_FETCHERS_DEFAULT, atLeast(1), HIGH, MIRROR_NUM_REPLICA_FETCHERS_DOC)
            .define(MIRROR_METADATA_REFRESH_INTERVAL_MS_CONFIG, LONG, MIRROR_METADATA_REFRESH_INTERVAL_MS_DEFAULT, atLeast(0L), MEDIUM, MIRROR_METADATA_REFRESH_INTERVAL_MS_DOC)
            .define(MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_CONFIG, LONG, MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_DEFAULT, atLeast(1L), MEDIUM, MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_DOC)
            .define(MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_CONFIG, LONG, MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_DEFAULT, atLeast(1L), MEDIUM, MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_DOC)
            .define(MIRROR_FAILED_RETRY_MAX_ATTEMPTS_CONFIG, INT, MIRROR_FAILED_RETRY_MAX_ATTEMPTS_DEFAULT, between(0, Short.MAX_VALUE), MEDIUM, MIRROR_FAILED_RETRY_MAX_ATTEMPTS_DOC);

    private final Pattern topicPropertiesExcludePattern;
    private final Pattern topicsIncludePattern;
    private final Pattern topicsExcludePattern;
    private final Pattern groupsIncludePattern;
    private final Pattern groupsExcludePattern;
    private final List<MirrorAclRule> aclIncludeRules;
    private final List<MirrorAclRule> aclExcludeRules;

    /**
     * Creates a ClusterMirrorConfig from mirror metadata properties stored in the cluster metadata.
     * Filter configs are loaded from the metadata, with defaults applied for any missing values.
     *
     * @param properties the mirror metadata properties
     * @return a new ClusterMirrorConfig instance
     */
    public static ClusterMirrorConfig fromProperties(Properties properties) {
        return fromProperties(properties, false);
    }

    /**
     * Creates a ClusterMirrorConfig from mirror metadata properties stored in the cluster metadata.
     * Filter configs are loaded from the metadata, with defaults applied for any missing values.
     *
     * @param properties the mirror metadata properties
     * @param doLog whether to log all configuration values
     * @return a new ClusterMirrorConfig instance
     */
    public static ClusterMirrorConfig fromProperties(Properties properties, boolean doLog) {
        return new ClusterMirrorConfig(CONFIG_DEF, properties, doLog);
    }

    private ClusterMirrorConfig(ConfigDef configDef, Properties properties, boolean doLog) {
        super(configDef, properties, doLog);
        this.topicPropertiesExcludePattern = compilePatternList(getList(TOPIC_PROPERTIES_EXCLUDE_CONFIG));
        this.topicsIncludePattern = compilePatternList(getList(TOPICS_INCLUDE_CONFIG));
        this.topicsExcludePattern = compilePatternList(getList(TOPICS_EXCLUDE_CONFIG));
        this.groupsIncludePattern = compilePatternList(getList(GROUPS_INCLUDE_CONFIG));
        this.groupsExcludePattern = compilePatternList(getList(GROUPS_EXCLUDE_CONFIG));
        this.aclIncludeRules = parseAclRules(getList(ACLS_INCLUDE_CONFIG));
        this.aclExcludeRules = parseAclRules(getList(ACLS_EXCLUDE_CONFIG));
    }

    /**
     * Creates a ClusterMirrorConfig from broker-level configuration.
     * Only broker-level mirror configurations are available.
     *
     * @param config the broker configuration
     */
    public ClusterMirrorConfig(AbstractConfig config) {
        super(BROKER_CONFIG_DEF, config.originals(), false);
        this.topicPropertiesExcludePattern = null;
        this.topicsIncludePattern = null;
        this.topicsExcludePattern = null;
        this.groupsIncludePattern = null;
        this.groupsExcludePattern = null;
        this.aclIncludeRules = null;
        this.aclExcludeRules = null;
    }

    /**
     * Supported mirror configurations.
     */
    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG_DEF);
    }

    /**
     * Broker-level mirror configurations.
     * This is merged into AbstractKafkaConfig for validation.
     */
    public static ConfigDef brokerConfigDef() {
        return new ConfigDef(BROKER_CONFIG_DEF);
    }

    /**
     * Checks whether a given configuration name is valid.
     * @param name configuration name
     * @return true if it is a known configuration name, false otherwise
     */
    public static boolean isValidConfigName(String name) {
        return CONFIG_DEF.names().contains(name);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG_DEF.toHtml(4, config -> "clustermirrorconfigs_" + config));
    }

    public int coordinatorNumThreads() {
        return getInt(MIRROR_COORDINATOR_THREADS_CONFIG);
    }

    public long coordinatorWriteTimeoutMs() {
        return getLong(MIRROR_COORDINATOR_WRITE_TIMEOUT_MS_CONFIG);
    }

    public int coordinatorAppendLingerMs() {
        return getInt(MIRROR_COORDINATOR_APPEND_LINGER_MS_CONFIG);
    }

    public int coordinatorLoadBufferSize() {
        return getInt(MIRROR_COORDINATOR_LOAD_BUFFER_SIZE_CONFIG);
    }

    public int stateTopicNumPartitions() {
        return getInt(MIRROR_STATE_TOPIC_NUM_PARTITIONS_CONFIG);
    }

    public short stateTopicReplicationFactor() {
        return getShort(MIRROR_STATE_TOPIC_REPLICATION_FACTOR_CONFIG);
    }

    public int numReplicaFetchers() {
        return getInt(MIRROR_NUM_REPLICA_FETCHERS_CONFIG);
    }

    public long metadataRefreshIntervalMs() {
        return getLong(MIRROR_METADATA_REFRESH_INTERVAL_MS_CONFIG);
    }

    public long failedRetryInitialBackoffMs() {
        return getLong(MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_CONFIG);
    }

    public long failedRetryMaxBackoffMs() {
        return getLong(MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_CONFIG);
    }

    public int failedRetryMaxAttempts() {
        return getInt(MIRROR_FAILED_RETRY_MAX_ATTEMPTS_CONFIG);
    }

    public int requestTimeoutMs() {
        return getInt(REQUEST_TIMEOUT_MS_CONFIG);
    }

    public long connectionSetupTimeoutMs() {
        return getLong(CONNECTION_SETUP_TIMEOUT_MS_CONFIG);
    }

    public long connectionSetupTimeoutMaxMs() {
        return getLong(CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG);
    }

    public long connectionsMaxIdleMs() {
        return getLong(CONNECTIONS_MAX_IDLE_MS_CONFIG);
    }

    public int socketTimeoutMs() {
        return getInt(SOCKET_TIMEOUT_MS_CONFIG);
    }

    public int socketReceiveBufferBytes() {
        return getInt(SOCKET_RECEIVE_BUFFER_BYTES_CONFIG);
    }

    public int fetchWaitMaxMs() {
        return getInt(FETCH_WAIT_MAX_MS_CONFIG);
    }

    public long fetchBackoffMs() {
        return getLong(FETCH_BACKOFF_MS_CONFIG);
    }

    public int fetchMinBytes() {
        return getInt(FETCH_MIN_BYTES_CONFIG);
    }

    public int fetchResponseMaxBytes() {
        return getInt(FETCH_RESPONSE_MAX_BYTES_CONFIG);
    }

    public int fetchMaxBytes() {
        return getInt(FETCH_MAX_BYTES_CONFIG);
    }

    public Pattern topicPropertiesExcludePattern() {
        return topicPropertiesExcludePattern;
    }

    public Pattern topicsIncludePattern() {
        return topicsIncludePattern;
    }

    public Pattern topicsExcludePattern() {
        return topicsExcludePattern;
    }

    public Pattern groupsIncludePattern() {
        return groupsIncludePattern;
    }

    public Pattern groupsExcludePattern() {
        return groupsExcludePattern;
    }

    public List<MirrorAclRule> aclIncludeRules() {
        return aclIncludeRules;
    }

    public List<MirrorAclRule> aclExcludeRules() {
        return aclExcludeRules;
    }

    public String securityProtocol() {
        return getString(SECURITY_PROTOCOL_CONFIG);
    }

    public String saslMechanism() {
        return getString(SaslConfigs.SASL_MECHANISM);
    }

    public List<MirrorAclRule> parseAclRules(List<String> rules) {
        return rules.stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(MirrorAclRule::parse)
                .toList();
    }

    /**
     * Represents an ACL rule parsed from the semicolon-separated format:
     * resourceType;resourceName;operation;permissionType;principal
     *
     * @param resourceType the resource type enum, or null for wildcard
     * @param resourceNamePattern regex pattern for the resource name, or null for wildcard
     * @param operation the ACL operation enum, or null for wildcard
     * @param permissionType the ACL permission type enum, or null for wildcard
     * @param principal the principal string (e.g. User:alice), or null for wildcard
     */
    public record MirrorAclRule(
            ResourceType resourceType,
            Pattern resourceNamePattern,
            AclOperation operation,
            AclPermissionType permissionType,
            String principal
    ) {
        private static final String SEPARATOR = ";";
        private static final String MATCH_ALL = ".*";
        private static final String WILDCARD_PRINCIPAL = "*";
        private static final MirrorAclRule MATCH_ALL_RULE =
                new MirrorAclRule(null, null, null, null, null);

        private static final String DOC = "Each rule uses semicolon-separated fields: "
                + "resourceType;resourceName;operation;permissionType;principal. "
                + "The resourceType, operation, and permissionType fields use enum values (e.g. TOPIC, READ, ALLOW). "
                + "Use ANY to match all values for these fields. "
                + "The resourceName field supports RE2 regex patterns. "
                + "The principal field uses exact match in principalType:name format (e.g. User:alice). "
                + "Use * to match all principals. "
                + "Trailing fields can be omitted and will match anything. "
                + "Use .* as the entire value to match all ACLs. ";

        public static MirrorAclRule parse(String rule) {
            String trimmed = rule.trim();
            if (trimmed.equals(MATCH_ALL)) {
                return MATCH_ALL_RULE;
            }
            String[] parts = trimmed.split(SEPARATOR, -1);
            return new MirrorAclRule(
                    parseResourceType(parts, 0),
                    parsePattern(parts, 1),
                    parseOperation(parts, 2),
                    parsePermissionType(parts, 3),
                    parsePrincipal(parts, 4)
            );
        }

        private static ResourceType parseResourceType(String[] parts, int index) {
            if (index >= parts.length) return null;
            String value = parts[index].trim().toUpperCase(Locale.ROOT);
            if (value.isEmpty() || value.equals(ResourceType.ANY.name())) return null;
            return ResourceType.valueOf(value);
        }

        private static AclOperation parseOperation(String[] parts, int index) {
            if (index >= parts.length) return null;
            String value = parts[index].trim().toUpperCase(Locale.ROOT);
            if (value.isEmpty() || value.equals(AclOperation.ANY.name())) return null;
            return AclOperation.valueOf(value);
        }

        private static AclPermissionType parsePermissionType(String[] parts, int index) {
            if (index >= parts.length) return null;
            String value = parts[index].trim().toUpperCase(Locale.ROOT);
            if (value.isEmpty() || value.equals(AclPermissionType.ANY.name())) return null;
            return AclPermissionType.valueOf(value);
        }

        private static Pattern parsePattern(String[] parts, int index) {
            if (index >= parts.length) return null;
            return compilePattern(parts[index].trim());
        }

        private static String parsePrincipal(String[] parts, int index) {
            if (index >= parts.length) return null;
            String value = parts[index].trim();
            if (value.isEmpty() || value.equals(WILDCARD_PRINCIPAL)) return null;
            return value;
        }

        /**
         * Tests whether the given AclBinding matches this rule.
         * A null field acts as a wildcard and matches any value.
         */
        public boolean matches(AclBinding binding) {
            if (resourceType != null && binding.pattern().resourceType() != resourceType) {
                return false;
            }
            if (resourceNamePattern != null && !resourceNamePattern.matcher(binding.pattern().name()).matches()) {
                return false;
            }
            if (operation != null && binding.entry().operation() != operation) {
                return false;
            }
            if (permissionType != null && binding.entry().permissionType() != permissionType) {
                return false;
            }
            if (principal != null && !principal.equals(binding.entry().principal())) {
                return false;
            }
            return true;
        }
    }
}
