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
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import java.security.InvalidParameterException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_VALIDATOR;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_VALIDATOR;

public class DistributedConfig extends WorkerConfig {

    private static final Logger log = LoggerFactory.getLogger(DistributedConfig.class);

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG STRINGS OR THEIR JAVA VARIABLE NAMES AS
     * THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */

    /**
     * <code>group.id</code>
     */
    public static final String GROUP_ID_CONFIG = CommonClientConfigs.GROUP_ID_CONFIG;
    private static final String GROUP_ID_DOC = "A unique string that identifies the Connect cluster group this worker belongs to.";

    /**
     * <code>session.timeout.ms</code>
     */
    public static final String SESSION_TIMEOUT_MS_CONFIG = CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG;
    private static final String SESSION_TIMEOUT_MS_DOC = "The timeout used to detect worker failures. " +
            "The worker sends periodic heartbeats to indicate its liveness to the broker. If no heartbeats are " +
            "received by the broker before the expiration of this session timeout, then the broker will remove the " +
            "worker from the group and initiate a rebalance. Note that the value must be in the allowable range as " +
            "configured in the broker configuration by <code>group.min.session.timeout.ms</code> " +
            "and <code>group.max.session.timeout.ms</code>.";

    /**
     * <code>heartbeat.interval.ms</code>
     */
    public static final String HEARTBEAT_INTERVAL_MS_CONFIG = CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG;
    private static final String HEARTBEAT_INTERVAL_MS_DOC = "The expected time between heartbeats to the group " +
            "coordinator when using Kafka's group management facilities. Heartbeats are used to ensure that the " +
            "worker's session stays active and to facilitate rebalancing when new members join or leave the group. " +
            "The value must be set lower than <code>session.timeout.ms</code>, but typically should be set no higher " +
            "than 1/3 of that value. It can be adjusted even lower to control the expected time for normal rebalances.";

    /**
     * <code>rebalance.timeout.ms</code>
     */
    public static final String REBALANCE_TIMEOUT_MS_CONFIG = CommonClientConfigs.REBALANCE_TIMEOUT_MS_CONFIG;
    private static final String REBALANCE_TIMEOUT_MS_DOC = CommonClientConfigs.REBALANCE_TIMEOUT_MS_DOC;

    /**
     * <code>worker.sync.timeout.ms</code>
     */
    public static final String WORKER_SYNC_TIMEOUT_MS_CONFIG = "worker.sync.timeout.ms";
    private static final String WORKER_SYNC_TIMEOUT_MS_DOC = "When the worker is out of sync with other workers and needs" +
            " to resynchronize configurations, wait up to this amount of time before giving up, leaving the group, and" +
            " waiting a backoff period before rejoining.";

    /**
     * <code>group.unsync.timeout.ms</code>
     */
    public static final String WORKER_UNSYNC_BACKOFF_MS_CONFIG = "worker.unsync.backoff.ms";
    private static final String WORKER_UNSYNC_BACKOFF_MS_DOC = "When the worker is out of sync with other workers and " +
            " fails to catch up within worker.sync.timeout.ms, leave the Connect cluster for this long before rejoining.";
    public static final int WORKER_UNSYNC_BACKOFF_MS_DEFAULT = 5 * 60 * 1000;

    public static final String CONFIG_STORAGE_PREFIX = "config.storage.";
    public static final String OFFSET_STORAGE_PREFIX = "offset.storage.";
    public static final String STATUS_STORAGE_PREFIX = "status.storage.";
    public static final String TOPIC_SUFFIX = "topic";
    public static final String PARTITIONS_SUFFIX = "partitions";
    public static final String REPLICATION_FACTOR_SUFFIX = "replication.factor";

    /**
     * <code>offset.storage.topic</code>
     */
    public static final String OFFSET_STORAGE_TOPIC_CONFIG = OFFSET_STORAGE_PREFIX + TOPIC_SUFFIX;
    private static final String OFFSET_STORAGE_TOPIC_CONFIG_DOC = "The name of the Kafka topic where connector offsets are stored";

    /**
     * <code>offset.storage.partitions</code>
     */
    public static final String OFFSET_STORAGE_PARTITIONS_CONFIG = OFFSET_STORAGE_PREFIX + PARTITIONS_SUFFIX;
    private static final String OFFSET_STORAGE_PARTITIONS_CONFIG_DOC = "The number of partitions used when creating the offset storage topic";

    /**
     * <code>offset.storage.replication.factor</code>
     */
    public static final String OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG = OFFSET_STORAGE_PREFIX + REPLICATION_FACTOR_SUFFIX;
    private static final String OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG_DOC = "Replication factor used when creating the offset storage topic";

    /**
     * <code>config.storage.topic</code>
     */
    public static final String CONFIG_TOPIC_CONFIG = CONFIG_STORAGE_PREFIX + TOPIC_SUFFIX;
    private static final String CONFIG_TOPIC_CONFIG_DOC = "The name of the Kafka topic where connector configurations are stored";

    /**
     * <code>config.storage.replication.factor</code>
     */
    public static final String CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG = CONFIG_STORAGE_PREFIX + REPLICATION_FACTOR_SUFFIX;
    private static final String CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG_DOC = "Replication factor used when creating the configuration storage topic";

    /**
     * <code>status.storage.topic</code>
     */
    public static final String STATUS_STORAGE_TOPIC_CONFIG = STATUS_STORAGE_PREFIX + TOPIC_SUFFIX;
    public static final String STATUS_STORAGE_TOPIC_CONFIG_DOC = "The name of the Kafka topic where connector and task status are stored";

    /**
     * <code>status.storage.partitions</code>
     */
    public static final String STATUS_STORAGE_PARTITIONS_CONFIG = STATUS_STORAGE_PREFIX + PARTITIONS_SUFFIX;
    private static final String STATUS_STORAGE_PARTITIONS_CONFIG_DOC = "The number of partitions used when creating the status storage topic";

    /**
     * <code>status.storage.replication.factor</code>
     */
    public static final String STATUS_STORAGE_REPLICATION_FACTOR_CONFIG = STATUS_STORAGE_PREFIX + REPLICATION_FACTOR_SUFFIX;
    private static final String STATUS_STORAGE_REPLICATION_FACTOR_CONFIG_DOC = "Replication factor used when creating the status storage topic";

    /**
     * <code>connect.protocol</code>
     */
    public static final String CONNECT_PROTOCOL_CONFIG = "connect.protocol";
    public static final String CONNECT_PROTOCOL_DOC = "Compatibility mode for Kafka Connect Protocol";
    public static final String CONNECT_PROTOCOL_DEFAULT = ConnectProtocolCompatibility.SESSIONED.toString();

    /**
     * <code>scheduled.rebalance.max.delay.ms</code>
     */
    public static final String SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG = "scheduled.rebalance.max.delay.ms";
    public static final String SCHEDULED_REBALANCE_MAX_DELAY_MS_DOC = "The maximum delay that is "
            + "scheduled in order to wait for the return of one or more departed workers before "
            + "rebalancing and reassigning their connectors and tasks to the group. During this "
            + "period the connectors and tasks of the departed workers remain unassigned";
    public static final int SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT = Math.toIntExact(TimeUnit.SECONDS.toMillis(300));

    public static final String INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG = "inter.worker.key.generation.algorithm";
    public static final String INTER_WORKER_KEY_GENERATION_ALGORITHM_DOC = "The algorithm to use for generating internal request keys";
    public static final String INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT = "HmacSHA256";

    public static final String INTER_WORKER_KEY_SIZE_CONFIG = "inter.worker.key.size";
    public static final String INTER_WORKER_KEY_SIZE_DOC = "The size of the key to use for signing internal requests, in bits. "
        + "If null, the default key size for the key generation algorithm will be used.";
    public static final Long INTER_WORKER_KEY_SIZE_DEFAULT = null;

    public static final String INTER_WORKER_KEY_TTL_MS_CONFIG = "inter.worker.key.ttl.ms";
    public static final String INTER_WORKER_KEY_TTL_MS_MS_DOC = "The TTL of generated session keys used for "
        + "internal request validation (in milliseconds)";
    public static final int INTER_WORKER_KEY_TTL_MS_MS_DEFAULT = Math.toIntExact(TimeUnit.HOURS.toMillis(1));

    public static final String INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG = "inter.worker.signature.algorithm";
    public static final String INTER_WORKER_SIGNATURE_ALGORITHM_DOC = "The algorithm used to sign internal requests";
    public static final String INTER_WORKER_SIGNATURE_ALGORITHM_DEFAULT = "HmacSHA256";

    public static final String INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG = "inter.worker.verification.algorithms";
    public static final String INTER_WORKER_VERIFICATION_ALGORITHMS_DOC = "A list of permitted algorithms for verifying internal requests";
    public static final List<String> INTER_WORKER_VERIFICATION_ALGORITHMS_DEFAULT = Collections.singletonList(INTER_WORKER_SIGNATURE_ALGORITHM_DEFAULT);

    @SuppressWarnings("unchecked")
    private static final ConfigDef CONFIG = baseConfigDef()
            .define(GROUP_ID_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    GROUP_ID_DOC)
            .define(SESSION_TIMEOUT_MS_CONFIG,
                    ConfigDef.Type.INT,
                    Math.toIntExact(TimeUnit.SECONDS.toMillis(10)),
                    ConfigDef.Importance.HIGH,
                    SESSION_TIMEOUT_MS_DOC)
            .define(REBALANCE_TIMEOUT_MS_CONFIG,
                    ConfigDef.Type.INT,
                    Math.toIntExact(TimeUnit.MINUTES.toMillis(1)),
                    ConfigDef.Importance.HIGH,
                    REBALANCE_TIMEOUT_MS_DOC)
            .define(HEARTBEAT_INTERVAL_MS_CONFIG,
                    ConfigDef.Type.INT,
                    Math.toIntExact(TimeUnit.SECONDS.toMillis(3)),
                    ConfigDef.Importance.HIGH,
                    HEARTBEAT_INTERVAL_MS_DOC)
            .define(CommonClientConfigs.METADATA_MAX_AGE_CONFIG,
                    ConfigDef.Type.LONG,
                    TimeUnit.MINUTES.toMillis(5),
                    atLeast(0),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.METADATA_MAX_AGE_DOC)
            .define(CommonClientConfigs.CLIENT_ID_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.CLIENT_ID_DOC)
            .define(CommonClientConfigs.SEND_BUFFER_CONFIG,
                    ConfigDef.Type.INT,
                    128 * 1024,
                    atLeast(0),
                    ConfigDef.Importance.MEDIUM,
                    CommonClientConfigs.SEND_BUFFER_DOC)
            .define(CommonClientConfigs.RECEIVE_BUFFER_CONFIG,
                    ConfigDef.Type.INT,
                    32 * 1024,
                    atLeast(0),
                    ConfigDef.Importance.MEDIUM,
                    CommonClientConfigs.RECEIVE_BUFFER_DOC)
            .define(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    50L,
                    atLeast(0L),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
            .define(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    TimeUnit.SECONDS.toMillis(1),
                    atLeast(0L),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
            .define(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
                    atLeast(0L),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
            .define(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
                    atLeast(0L),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
            .define(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    100L,
                    atLeast(0L),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
            .define(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG,
                    ConfigDef.Type.INT,
                    Math.toIntExact(TimeUnit.SECONDS.toMillis(40)),
                    atLeast(0),
                    ConfigDef.Importance.MEDIUM,
                    CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC)
                    /* default is set to be a bit lower than the server default (10 min), to avoid both client and server closing connection at same time */
            .define(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    TimeUnit.MINUTES.toMillis(9),
                    ConfigDef.Importance.MEDIUM,
                    CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
            // security support
            .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                    ConfigDef.Type.STRING,
                    CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                    ConfigDef.Importance.MEDIUM,
                    CommonClientConfigs.SECURITY_PROTOCOL_DOC)
            .withClientSaslSupport()
            .define(WORKER_SYNC_TIMEOUT_MS_CONFIG,
                    ConfigDef.Type.INT,
                    3000,
                    ConfigDef.Importance.MEDIUM,
                    WORKER_SYNC_TIMEOUT_MS_DOC)
            .define(WORKER_UNSYNC_BACKOFF_MS_CONFIG,
                    ConfigDef.Type.INT,
                    WORKER_UNSYNC_BACKOFF_MS_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    WORKER_UNSYNC_BACKOFF_MS_DOC)
            .define(OFFSET_STORAGE_TOPIC_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    OFFSET_STORAGE_TOPIC_CONFIG_DOC)
            .define(OFFSET_STORAGE_PARTITIONS_CONFIG,
                    ConfigDef.Type.INT,
                    25,
                    PARTITIONS_VALIDATOR,
                    ConfigDef.Importance.LOW,
                    OFFSET_STORAGE_PARTITIONS_CONFIG_DOC)
            .define(OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG,
                    ConfigDef.Type.SHORT,
                    (short) 3,
                    REPLICATION_FACTOR_VALIDATOR,
                    ConfigDef.Importance.LOW,
                    OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG_DOC)
            .define(CONFIG_TOPIC_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    CONFIG_TOPIC_CONFIG_DOC)
            .define(CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG,
                    ConfigDef.Type.SHORT,
                    (short) 3,
                    REPLICATION_FACTOR_VALIDATOR,
                    ConfigDef.Importance.LOW,
                    CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG_DOC)
            .define(STATUS_STORAGE_TOPIC_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    STATUS_STORAGE_TOPIC_CONFIG_DOC)
            .define(STATUS_STORAGE_PARTITIONS_CONFIG,
                    ConfigDef.Type.INT,
                    5,
                    PARTITIONS_VALIDATOR,
                    ConfigDef.Importance.LOW,
                    STATUS_STORAGE_PARTITIONS_CONFIG_DOC)
            .define(STATUS_STORAGE_REPLICATION_FACTOR_CONFIG,
                    ConfigDef.Type.SHORT,
                    (short) 3,
                    REPLICATION_FACTOR_VALIDATOR,
                    ConfigDef.Importance.LOW,
                    STATUS_STORAGE_REPLICATION_FACTOR_CONFIG_DOC)
            .define(CONNECT_PROTOCOL_CONFIG,
                    ConfigDef.Type.STRING,
                    CONNECT_PROTOCOL_DEFAULT,
                    ConfigDef.LambdaValidator.with(
                        (name, value) -> {
                            try {
                                ConnectProtocolCompatibility.compatibility((String) value);
                            } catch (Throwable t) {
                                throw new ConfigException(name, value, "Invalid Connect protocol "
                                        + "compatibility");
                            }
                        },
                        () -> "[" + Utils.join(ConnectProtocolCompatibility.values(), ", ") + "]"),
                    ConfigDef.Importance.LOW,
                    CONNECT_PROTOCOL_DOC)
            .define(SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG,
                    ConfigDef.Type.INT,
                    SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT,
                    between(0, Integer.MAX_VALUE),
                    ConfigDef.Importance.LOW,
                    SCHEDULED_REBALANCE_MAX_DELAY_MS_DOC)
            .define(INTER_WORKER_KEY_TTL_MS_CONFIG,
                    ConfigDef.Type.INT,
                    INTER_WORKER_KEY_TTL_MS_MS_DEFAULT,
                    between(0, Integer.MAX_VALUE),
                    ConfigDef.Importance.LOW,
                    INTER_WORKER_KEY_TTL_MS_MS_DOC)
            .define(INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG,
                    ConfigDef.Type.STRING,
                    INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT,
                    ConfigDef.LambdaValidator.with(
                        (name, value) -> validateKeyAlgorithm(name, (String) value),
                        () -> "Any KeyGenerator algorithm supported by the worker JVM"
                    ),
                    ConfigDef.Importance.LOW,
                    INTER_WORKER_KEY_GENERATION_ALGORITHM_DOC)
            .define(INTER_WORKER_KEY_SIZE_CONFIG,
                    ConfigDef.Type.INT,
                    INTER_WORKER_KEY_SIZE_DEFAULT,
                    ConfigDef.Importance.LOW,
                    INTER_WORKER_KEY_SIZE_DOC)
            .define(INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG,
                    ConfigDef.Type.STRING,
                    INTER_WORKER_SIGNATURE_ALGORITHM_DEFAULT,
                    ConfigDef.LambdaValidator.with(
                        (name, value) -> validateSignatureAlgorithm(name, (String) value),
                        () -> "Any MAC algorithm supported by the worker JVM"),
                    ConfigDef.Importance.LOW,
                    INTER_WORKER_SIGNATURE_ALGORITHM_DOC)
            .define(INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG,
                    ConfigDef.Type.LIST,
                    INTER_WORKER_VERIFICATION_ALGORITHMS_DEFAULT,
                    ConfigDef.LambdaValidator.with(
                        (name, value) -> validateSignatureAlgorithms(name, (List<String>) value),
                        () -> "A list of one or more MAC algorithms, each supported by the worker JVM"
                    ),
                    ConfigDef.Importance.LOW,
                    INTER_WORKER_VERIFICATION_ALGORITHMS_DOC);

    @Override
    public Integer getRebalanceTimeout() {
        return getInt(DistributedConfig.REBALANCE_TIMEOUT_MS_CONFIG);
    }

    public DistributedConfig(Map<String, String> props) {
        super(CONFIG, props);
        getInternalRequestKeyGenerator(); // Check here for a valid key size + key algorithm to fail fast if either are invalid
        validateKeyAlgorithmAndVerificationAlgorithms();
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtml(4, config -> "connectconfigs_" + config));
    }

    public KeyGenerator getInternalRequestKeyGenerator() {
        try {
            KeyGenerator result = KeyGenerator.getInstance(getString(INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG));
            Optional.ofNullable(getInt(INTER_WORKER_KEY_SIZE_CONFIG)).ifPresent(result::init);
            return result;
        } catch (NoSuchAlgorithmException | InvalidParameterException e) {
            throw new ConfigException(String.format(
                "Unable to create key generator with algorithm %s and key size %d: %s",
                getString(INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG),
                getInt(INTER_WORKER_KEY_SIZE_CONFIG),
                e.getMessage()
            ));
        }
    }

    private Map<String, Object> topicSettings(String prefix) {
        Map<String, Object> result = originalsWithPrefix(prefix);
        if (CONFIG_STORAGE_PREFIX.equals(prefix) && result.containsKey(PARTITIONS_SUFFIX)) {
            log.warn("Ignoring '{}{}={}' setting, since config topic partitions is always 1", prefix, PARTITIONS_SUFFIX, result.get("partitions"));
        }
        Object removedPolicy = result.remove(TopicConfig.CLEANUP_POLICY_CONFIG);
        if (removedPolicy != null) {
            log.warn("Ignoring '{}cleanup.policy={}' setting, since compaction is always used", prefix, removedPolicy);
        }
        result.remove(TOPIC_SUFFIX);
        result.remove(REPLICATION_FACTOR_SUFFIX);
        result.remove(PARTITIONS_SUFFIX);
        return result;
    }

    public Map<String, Object> configStorageTopicSettings() {
        return topicSettings(CONFIG_STORAGE_PREFIX);
    }

    public Map<String, Object> offsetStorageTopicSettings() {
        return topicSettings(OFFSET_STORAGE_PREFIX);
    }

    public Map<String, Object> statusStorageTopicSettings() {
        return topicSettings(STATUS_STORAGE_PREFIX);
    }

    private void validateKeyAlgorithmAndVerificationAlgorithms() {
        String keyAlgorithm = getString(INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG);
        List<String> verificationAlgorithms = getList(INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG);
        if (!verificationAlgorithms.contains(keyAlgorithm)) {
            throw new ConfigException(
                INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG,
                keyAlgorithm,
                String.format("Key generation algorithm must be present in %s list", INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG)
            );
        }
    }

    private static void validateSignatureAlgorithms(String configName, List<String> algorithms) {
        if (algorithms.isEmpty()) {
            throw new ConfigException(
                configName,
                algorithms,
                "At least one signature verification algorithm must be provided"
            );
        }
        algorithms.forEach(algorithm -> validateSignatureAlgorithm(configName, algorithm));
    }

    private static void validateSignatureAlgorithm(String configName, String algorithm) {
        try {
            Mac.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new ConfigException(configName, algorithm, e.getMessage());
        }
    }

    private static void validateKeyAlgorithm(String configName, String algorithm) {
        try {
            KeyGenerator.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new ConfigException(configName, algorithm, e.getMessage());
        }
    }
}
