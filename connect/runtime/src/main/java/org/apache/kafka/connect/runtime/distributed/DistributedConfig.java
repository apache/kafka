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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.KeyGenerator;
import java.security.InvalidParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.CaseInsensitiveValidString.in;
import static org.apache.kafka.common.utils.Utils.enumOptions;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_VALIDATOR;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_VALIDATOR;

/**
 * Provides configuration for Kafka Connect workers running in distributed mode.
 */
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
    private static final String OFFSET_STORAGE_TOPIC_CONFIG_DOC = "The name of the Kafka topic where source connector offsets are stored";

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
    public static final String INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT = "HmacSHA256";
    public static final String INTER_WORKER_KEY_GENERATION_ALGORITHM_DOC = "The algorithm to use for generating internal request keys. "
            + "The algorithm '" + INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT + "' will be used as a default on JVMs that support it; "
            + "on other JVMs, no default is used and a value for this property must be manually specified in the worker config.";

    public static final String INTER_WORKER_KEY_SIZE_CONFIG = "inter.worker.key.size";
    public static final String INTER_WORKER_KEY_SIZE_DOC = "The size of the key to use for signing internal requests, in bits. "
        + "If null, the default key size for the key generation algorithm will be used.";
    public static final Long INTER_WORKER_KEY_SIZE_DEFAULT = null;

    public static final String INTER_WORKER_KEY_TTL_MS_CONFIG = "inter.worker.key.ttl.ms";
    public static final String INTER_WORKER_KEY_TTL_MS_MS_DOC = "The TTL of generated session keys used for "
        + "internal request validation (in milliseconds)";
    public static final int INTER_WORKER_KEY_TTL_MS_MS_DEFAULT = Math.toIntExact(TimeUnit.HOURS.toMillis(1));

    public static final String INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG = "inter.worker.signature.algorithm";
    public static final String INTER_WORKER_SIGNATURE_ALGORITHM_DEFAULT = "HmacSHA256";
    public static final String INTER_WORKER_SIGNATURE_ALGORITHM_DOC = "The algorithm used to sign internal requests"
            + "The algorithm '" + INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG + "' will be used as a default on JVMs that support it; "
            + "on other JVMs, no default is used and a value for this property must be manually specified in the worker config.";

    public static final String INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG = "inter.worker.verification.algorithms";
    public static final List<String> INTER_WORKER_VERIFICATION_ALGORITHMS_DEFAULT = Collections.singletonList(INTER_WORKER_SIGNATURE_ALGORITHM_DEFAULT);
    public static final String INTER_WORKER_VERIFICATION_ALGORITHMS_DOC = "A list of permitted algorithms for verifying internal requests, "
        + "which must include the algorithm used for the " + INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG + " property. "
        + "The algorithm(s) '" + INTER_WORKER_VERIFICATION_ALGORITHMS_DEFAULT + "' will be used as a default on JVMs that provide them; "
        + "on other JVMs, no default is used and a value for this property must be manually specified in the worker config.";
    private final Crypto crypto;

    public enum ExactlyOnceSourceSupport {
        DISABLED(false),
        PREPARING(true),
        ENABLED(true);

        public final boolean usesTransactionalLeader;

        ExactlyOnceSourceSupport(boolean usesTransactionalLeader) {
            this.usesTransactionalLeader = usesTransactionalLeader;
        }

        public static ExactlyOnceSourceSupport fromProperty(String property) {
            return ExactlyOnceSourceSupport.valueOf(property.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static final String EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG = "exactly.once.source.support";
    public static final String EXACTLY_ONCE_SOURCE_SUPPORT_DOC = "Whether to enable exactly-once support for source connectors in the cluster "
            + "by using transactions to write source records and their source offsets, and by proactively fencing out old task generations before bringing up new ones.\n"
            + "To enable exactly-once source support on a new cluster, set this property to '" + ExactlyOnceSourceSupport.ENABLED + "'. "
            + "To enable support on an existing cluster, first set to '" + ExactlyOnceSourceSupport.PREPARING + "' on every worker in the cluster, "
            + "then set to '" + ExactlyOnceSourceSupport.ENABLED + "'. A rolling upgrade may be used for both changes. "
            + "For more information on this feature, see the "
            + "<a href=\"https://kafka.apache.org/documentation.html#connect_exactlyoncesource\">exactly-once source support documentation</a>.";
    public static final String EXACTLY_ONCE_SOURCE_SUPPORT_DEFAULT = ExactlyOnceSourceSupport.DISABLED.toString();

    private static Object defaultKeyGenerationAlgorithm(Crypto crypto) {
        try {
            validateKeyAlgorithm(crypto, INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG, INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT);
            return INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT;
        } catch (Throwable t) {
            log.info(
                    "The default key generation algorithm '{}' does not appear to be available on this worker."
                            + "A key algorithm will have to be manually specified via the '{}' worker property",
                    INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT,
                    INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG
            );
            return ConfigDef.NO_DEFAULT_VALUE;
        }
    }

    private static Object defaultSignatureAlgorithm(Crypto crypto) {
        try {
            validateSignatureAlgorithm(crypto, INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG, INTER_WORKER_SIGNATURE_ALGORITHM_DEFAULT);
            return INTER_WORKER_SIGNATURE_ALGORITHM_DEFAULT;
        } catch (Throwable t) {
            log.info(
                    "The default signature algorithm '{}' does not appear to be available on this worker."
                            + "A signature algorithm will have to be manually specified via the '{}' worker property",
                    INTER_WORKER_SIGNATURE_ALGORITHM_DEFAULT,
                    INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG
            );
            return ConfigDef.NO_DEFAULT_VALUE;
        }
    }

    private static Object defaultVerificationAlgorithms(Crypto crypto) {
        List<String> result = new ArrayList<>();
        for (String verificationAlgorithm : INTER_WORKER_VERIFICATION_ALGORITHMS_DEFAULT) {
            try {
                validateSignatureAlgorithm(crypto, INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG, verificationAlgorithm);
                result.add(verificationAlgorithm);
            } catch (Throwable t) {
                log.trace("Verification algorithm '{}' not found", verificationAlgorithm);
            }
        }
        if (result.isEmpty()) {
            log.info(
                    "The default verification algorithm '{}' does not appear to be available on this worker."
                            + "One or more verification algorithms will have to be manually specified via the '{}' worker property",
                    INTER_WORKER_VERIFICATION_ALGORITHMS_DEFAULT,
                    INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG
            );
            return ConfigDef.NO_DEFAULT_VALUE;
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static ConfigDef config(Crypto crypto) {
        return baseConfigDef()
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
            .define(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG,
                    ConfigDef.Type.STRING,
                    EXACTLY_ONCE_SOURCE_SUPPORT_DEFAULT,
                    in(enumOptions(ExactlyOnceSourceSupport.class)),
                    ConfigDef.Importance.HIGH,
                    EXACTLY_ONCE_SOURCE_SUPPORT_DOC)
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
                    atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND),
                    ConfigDef.Importance.MEDIUM,
                    CommonClientConfigs.SEND_BUFFER_DOC)
            .define(CommonClientConfigs.RECEIVE_BUFFER_CONFIG,
                    ConfigDef.Type.INT,
                    32 * 1024,
                    atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND),
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
                    CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MS,
                    atLeast(0L),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
            .define(CommonClientConfigs.RETRY_BACKOFF_MAX_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MAX_MS,
                    atLeast(0L),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.RETRY_BACKOFF_MAX_MS_DOC)
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
                    in(Utils.enumOptions(SecurityProtocol.class)),
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
                        () -> Arrays.stream(ConnectProtocolCompatibility.values()).map(ConnectProtocolCompatibility::toString)
                                .collect(Collectors.joining(", ", "[", "]"))),
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
                    defaultKeyGenerationAlgorithm(crypto),
                    ConfigDef.LambdaValidator.with(
                            (name, value) -> validateKeyAlgorithm(crypto, name, (String) value),
                            () -> "Any KeyGenerator algorithm supported by the worker JVM"),
                    ConfigDef.Importance.LOW,
                    INTER_WORKER_KEY_GENERATION_ALGORITHM_DOC)
            .define(INTER_WORKER_KEY_SIZE_CONFIG,
                    ConfigDef.Type.INT,
                    INTER_WORKER_KEY_SIZE_DEFAULT,
                    ConfigDef.Importance.LOW,
                    INTER_WORKER_KEY_SIZE_DOC)
            .define(INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG,
                    ConfigDef.Type.STRING,
                    defaultSignatureAlgorithm(crypto),
                    ConfigDef.LambdaValidator.with(
                            (name, value) -> validateSignatureAlgorithm(crypto, name, (String) value),
                            () -> "Any MAC algorithm supported by the worker JVM"),
                    ConfigDef.Importance.LOW,
                    INTER_WORKER_SIGNATURE_ALGORITHM_DOC)
            .define(INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG,
                    ConfigDef.Type.LIST,
                    defaultVerificationAlgorithms(crypto),
                    ConfigDef.LambdaValidator.with(
                            (name, value) -> validateVerificationAlgorithms(crypto, name, (List<String>) value),
                            () -> "A list of one or more MAC algorithms, each supported by the worker JVM"),
                    ConfigDef.Importance.LOW,
                    INTER_WORKER_VERIFICATION_ALGORITHMS_DOC);
    }

    private final ExactlyOnceSourceSupport exactlyOnceSourceSupport;

    @Override
    public Integer rebalanceTimeout() {
        return getInt(DistributedConfig.REBALANCE_TIMEOUT_MS_CONFIG);
    }

    @Override
    public boolean exactlyOnceSourceEnabled() {
        return exactlyOnceSourceSupport == ExactlyOnceSourceSupport.ENABLED;
    }

    /**
     * @return whether the Connect cluster's leader should use a transactional producer to perform writes to the config
     * topic, which is useful for ensuring that zombie leaders are fenced out and unable to write to the topic after a
     * new leader has been elected.
     */
    public boolean transactionalLeaderEnabled() {
        return exactlyOnceSourceSupport.usesTransactionalLeader;
    }

    /**
     * @return the {@link ProducerConfig#TRANSACTIONAL_ID_CONFIG transactional ID} to use for the worker's producer if
     * using a transactional producer for writes to internal topics such as the config topic.
     */
    public String transactionalProducerId() {
        return transactionalProducerId(groupId());
    }

    public static String transactionalProducerId(String groupId) {
        return "connect-cluster-" + groupId;
    }

    @Override
    public String offsetsTopic() {
        return getString(OFFSET_STORAGE_TOPIC_CONFIG);
    }

    @Override
    public boolean connectorOffsetsTopicsPermitted() {
        return true;
    }

    @Override
    public String groupId() {
        return getString(GROUP_ID_CONFIG);
    }

    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        CommonClientConfigs.warnDisablingExponentialBackoff(this);
        return super.postProcessParsedConfig(parsedValues);
    }

    public DistributedConfig(Map<String, String> props) {
        this(Crypto.SYSTEM, props);
    }

    // Visible for testing
    @SuppressWarnings("this-escape")
    DistributedConfig(Crypto crypto, Map<String, String> props) {
        super(config(crypto), props);
        this.crypto = crypto;
        exactlyOnceSourceSupport = ExactlyOnceSourceSupport.fromProperty(getString(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG));
        validateInterWorkerKeyConfigs();
    }

    public static void main(String[] args) {
        System.out.println(config(Crypto.SYSTEM).toHtml(4, config -> "connectconfigs_" + config));
    }

    public KeyGenerator getInternalRequestKeyGenerator() {
        try {
            KeyGenerator result = crypto.keyGenerator(getString(INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG));
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

    private void validateInterWorkerKeyConfigs() {
        getInternalRequestKeyGenerator();
        ensureVerificationAlgorithmsIncludeSignatureAlgorithm();
    }

    private void ensureVerificationAlgorithmsIncludeSignatureAlgorithm() {
        String signatureAlgorithm = getString(INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG);
        List<String> verificationAlgorithms = getList(INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG);
        if (!verificationAlgorithms.contains(signatureAlgorithm)) {
            throw new ConfigException(
                INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG,
                signatureAlgorithm,
                String.format("Signature algorithm must be present in %s list", INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG)
            );
        }
    }

    private static void validateVerificationAlgorithms(Crypto crypto, String configName, List<String> algorithms) {
        if (algorithms.isEmpty()) {
            throw new ConfigException(
                    configName,
                    algorithms,
                    "At least one signature verification algorithm must be provided"
            );
        }
        for (String algorithm : algorithms) {
            try {
                crypto.mac(algorithm);
            } catch (NoSuchAlgorithmException e) {
                throw unsupportedAlgorithmException(configName, algorithm, "Mac");
            }
        }
    }

    private static void validateSignatureAlgorithm(Crypto crypto, String configName, String algorithm) {
        try {
            crypto.mac(algorithm);
        } catch (NoSuchAlgorithmException e) {
            throw unsupportedAlgorithmException(configName, algorithm, "Mac");
        }
    }

    private static void validateKeyAlgorithm(Crypto crypto, String configName, String algorithm) {
        try {
            crypto.keyGenerator(algorithm);
        } catch (NoSuchAlgorithmException e) {
            throw unsupportedAlgorithmException(configName, algorithm, "KeyGenerator");
        }
    }

    private static ConfigException unsupportedAlgorithmException(String name, Object value, String type) {
        return new ConfigException(
                name,
                value,
                "the algorithm is not supported by this JVM; the supported algorithms are: " + supportedAlgorithms(type)
        );
    }

    // Visible for testing
    static Set<String> supportedAlgorithms(String type) {
        Set<String> result = new HashSet<>();
        for (Provider provider : Security.getProviders()) {
            for (Provider.Service service : provider.getServices()) {
                if (type.equals(service.getType())) {
                    result.add(service.getAlgorithm());
                }
            }
        }
        return result;
    }

}
