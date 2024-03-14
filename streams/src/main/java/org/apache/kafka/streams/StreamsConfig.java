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
package org.apache.kafka.streams;

import java.util.Arrays;
import java.util.HashSet;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.internals.StreamsConfigUtils;
import org.apache.kafka.streams.internals.UpgradeFromValues;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareTaskAssignor;
import org.apache.kafka.streams.state.BuiltInDslStoreSuppliers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.IsolationLevel.READ_COMMITTED;
import static org.apache.kafka.common.config.ConfigDef.ListSize.atMostOfSize;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;
import static org.apache.kafka.common.config.ConfigDef.parseType;

/**
 * Configuration for a {@link KafkaStreams} instance.
 * Can also be used to configure the Kafka Streams internal {@link KafkaConsumer}, {@link KafkaProducer} and {@link Admin}.
 * To avoid consumer/producer/admin property conflicts, you should prefix those properties using
 * {@link #consumerPrefix(String)}, {@link #producerPrefix(String)} and {@link #adminClientPrefix(String)}, respectively.
 * <p>
 * Example:
 * <pre>{@code
 * // potentially wrong: sets "metadata.max.age.ms" to 1 minute for producer AND consumer
 * Properties streamsProperties = new Properties();
 * streamsProperties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 60000);
 * // or
 * streamsProperties.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 60000);
 *
 * // suggested:
 * Properties streamsProperties = new Properties();
 * // sets "metadata.max.age.ms" to 1 minute for consumer only
 * streamsProperties.put(StreamsConfig.consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), 60000);
 * // sets "metadata.max.age.ms" to 1 minute for producer only
 * streamsProperties.put(StreamsConfig.producerPrefix(ProducerConfig.METADATA_MAX_AGE_CONFIG), 60000);
 *
 * StreamsConfig streamsConfig = new StreamsConfig(streamsProperties);
 * }</pre>
 *
 * This instance can also be used to pass in custom configurations to different modules (e.g. passing a special config in your customized serde class).
 * The consumer/producer/admin prefix can also be used to distinguish these custom config values passed to different clients with the same config name.
 * * Example:
 * <pre>{@code
 * Properties streamsProperties = new Properties();
 * // sets "my.custom.config" to "foo" for consumer only
 * streamsProperties.put(StreamsConfig.consumerPrefix("my.custom.config"), "foo");
 * // sets "my.custom.config" to "bar" for producer only
 * streamsProperties.put(StreamsConfig.producerPrefix("my.custom.config"), "bar");
 * // sets "my.custom.config2" to "boom" for all clients universally
 * streamsProperties.put("my.custom.config2", "boom");
 *
 * // as a result, inside producer's serde class configure(..) function,
 * // users can now read both key-value pairs "my.custom.config" -> "foo"
 * // and "my.custom.config2" -> "boom" from the config map
 * StreamsConfig streamsConfig = new StreamsConfig(streamsProperties);
 * }</pre>
 *
 *
 * When increasing {@link ProducerConfig#MAX_BLOCK_MS_CONFIG} to be more resilient to non-available brokers you should also
 * increase {@link ConsumerConfig#MAX_POLL_INTERVAL_MS_CONFIG} using the following guidance:
 * <pre>
 *     max.poll.interval.ms &gt; max.block.ms
 * </pre>
 *
 *
 * Kafka Streams requires at least the following properties to be set:
 * <ul>
 *  <li>{@link #APPLICATION_ID_CONFIG "application.id"}</li>
 *  <li>{@link #BOOTSTRAP_SERVERS_CONFIG "bootstrap.servers"}</li>
 * </ul>
 *
 * By default, Kafka Streams does not allow users to overwrite the following properties (Streams setting shown in parentheses):
 * <ul>
 *   <li>{@link ConsumerConfig#GROUP_ID_CONFIG "group.id"} (&lt;application.id&gt;) - Streams client will always use the application ID a consumer group ID</li>
 *   <li>{@link ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG "enable.auto.commit"} (false) - Streams client will always disable/turn off auto committing</li>
 *   <li>{@link ConsumerConfig#PARTITION_ASSIGNMENT_STRATEGY_CONFIG "partition.assignment.strategy"} (<code>StreamsPartitionAssignor</code>) - Streams client will always use its own partition assignor</li>
 * </ul>
 *
 * If {@link #PROCESSING_GUARANTEE_CONFIG "processing.guarantee"} is set to {@link #EXACTLY_ONCE_V2 "exactly_once_v2"},
 * {@link #EXACTLY_ONCE "exactly_once"} (deprecated), or {@link #EXACTLY_ONCE_BETA "exactly_once_beta"} (deprecated), Kafka Streams does not
 * allow users to overwrite the following properties (Streams setting shown in parentheses):
 * <ul>
 *   <li>{@link ConsumerConfig#ISOLATION_LEVEL_CONFIG "isolation.level"} (read_committed) - Consumers will always read committed data only</li>
 *   <li>{@link ProducerConfig#ENABLE_IDEMPOTENCE_CONFIG "enable.idempotence"} (true) - Producer will always have idempotency enabled</li>
 * </ul>
 *
 * @see KafkaStreams#KafkaStreams(org.apache.kafka.streams.Topology, Properties)
 * @see ConsumerConfig
 * @see ProducerConfig
 */
public class StreamsConfig extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(StreamsConfig.class);

    private static final ConfigDef CONFIG;

    private final boolean eosEnabled;
    private static final long DEFAULT_COMMIT_INTERVAL_MS = 30000L;
    private static final long EOS_DEFAULT_COMMIT_INTERVAL_MS = 100L;
    private static final int DEFAULT_TRANSACTION_TIMEOUT = 10000;

    @SuppressWarnings("unused")
    public static final int DUMMY_THREAD_INDEX = 1;
    public static final long MAX_TASK_IDLE_MS_DISABLED = -1;

    // We impose these limitations because client tags are encoded into the subscription info,
    // which is part of the group metadata message that is persisted into the internal topic.
    public static final int MAX_RACK_AWARE_ASSIGNMENT_TAG_LIST_SIZE = 5;
    public static final int MAX_RACK_AWARE_ASSIGNMENT_TAG_KEY_LENGTH = 20;
    public static final int MAX_RACK_AWARE_ASSIGNMENT_TAG_VALUE_LENGTH = 30;

    /**
     * Prefix used to provide default topic configs to be applied when creating internal topics.
     * These should be valid properties from {@link org.apache.kafka.common.config.TopicConfig TopicConfig}.
     * It is recommended to use {@link #topicPrefix(String)}.
     */
    // TODO: currently we cannot get the full topic configurations and hence cannot allow topic configs without the prefix,
    //       this can be lifted once kafka.log.LogConfig is completely deprecated by org.apache.kafka.common.config.TopicConfig
    @SuppressWarnings("WeakerAccess")
    public static final String TOPIC_PREFIX = "topic.";

    /**
     * Prefix used to isolate {@link KafkaConsumer consumer} configs from other client configs.
     * It is recommended to use {@link #consumerPrefix(String)} to add this prefix to {@link ConsumerConfig consumer
     * properties}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String CONSUMER_PREFIX = "consumer.";

    /**
     * Prefix used to override {@link KafkaConsumer consumer} configs for the main consumer client from
     * the general consumer client configs. The override precedence is the following (from highest to lowest precedence):
     * 1. main.consumer.[config-name]
     * 2. consumer.[config-name]
     * 3. [config-name]
     */
    @SuppressWarnings("WeakerAccess")
    public static final String MAIN_CONSUMER_PREFIX = "main.consumer.";

    /**
     * Prefix used to override {@link KafkaConsumer consumer} configs for the restore consumer client from
     * the general consumer client configs. The override precedence is the following (from highest to lowest precedence):
     * 1. restore.consumer.[config-name]
     * 2. consumer.[config-name]
     * 3. [config-name]
     */
    @SuppressWarnings("WeakerAccess")
    public static final String RESTORE_CONSUMER_PREFIX = "restore.consumer.";

    /**
     * Prefix used to override {@link KafkaConsumer consumer} configs for the global consumer client from
     * the general consumer client configs. The override precedence is the following (from highest to lowest precedence):
     * 1. global.consumer.[config-name]
     * 2. consumer.[config-name]
     * 3. [config-name]
     */
    @SuppressWarnings("WeakerAccess")
    public static final String GLOBAL_CONSUMER_PREFIX = "global.consumer.";

    /**
     * Prefix used to isolate {@link KafkaProducer producer} configs from other client configs.
     * It is recommended to use {@link #producerPrefix(String)} to add this prefix to {@link ProducerConfig producer
     * properties}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String PRODUCER_PREFIX = "producer.";

    /**
     * Prefix used to isolate {@link Admin admin} configs from other client configs.
     * It is recommended to use {@link #adminClientPrefix(String)} to add this prefix to {@link AdminClientConfig admin
     * client properties}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String ADMIN_CLIENT_PREFIX = "admin.";

    /**
     * Prefix used to add arbitrary tags to a Kafka Stream's instance as key-value pairs.
     * Example:
     * client.tag.zone=zone1
     * client.tag.cluster=cluster1
     */
    @SuppressWarnings("WeakerAccess")
    public static final String CLIENT_TAG_PREFIX = "client.tag.";

    /** {@code topology.optimization} */
    public static final String TOPOLOGY_OPTIMIZATION_CONFIG = "topology.optimization";
    private static final String CONFIG_ERROR_MSG = "Acceptable values are:"
            + " \"+NO_OPTIMIZATION+\", \"+OPTIMIZE+\", "
            + "or a comma separated list of specific optimizations: "
            + "(\"+REUSE_KTABLE_SOURCE_TOPICS+\", \"+MERGE_REPARTITION_TOPICS+\" + "
            + "\"SINGLE_STORE_SELF_JOIN+\").";
    private static final String TOPOLOGY_OPTIMIZATION_DOC = "A configuration telling Kafka "
        + "Streams if it should optimize the topology and what optimizations to apply. "
        + CONFIG_ERROR_MSG
        + "\"NO_OPTIMIZATION\" by default.";

    /**
     * Config value for parameter {@link #TOPOLOGY_OPTIMIZATION_CONFIG "topology.optimization"} for disabling topology optimization
     */
    public static final String NO_OPTIMIZATION = "none";

    /**
     * Config value for parameter {@link #TOPOLOGY_OPTIMIZATION_CONFIG "topology.optimization"} for enabling topology optimization
     */
    public static final String OPTIMIZE = "all";

    /**
     * Config value for parameter {@link #TOPOLOGY_OPTIMIZATION_CONFIG "topology.optimization"}
     * for enabling the specific optimization that reuses source topic as changelog topic
     * for KTables.
     */
    public static final String REUSE_KTABLE_SOURCE_TOPICS = "reuse.ktable.source.topics";

    /**
     * Config value for parameter {@link #TOPOLOGY_OPTIMIZATION_CONFIG "topology.optimization"}
     * for enabling the specific optimization that merges duplicated repartition topics.
     */
    public static final String MERGE_REPARTITION_TOPICS = "merge.repartition.topics";

    /**
     * Config value for parameter {@link #TOPOLOGY_OPTIMIZATION_CONFIG "topology.optimization"}
     * for enabling the optimization that optimizes inner stream-stream joins into self-joins when
     * both arguments are the same stream.
     */
    public static final String SINGLE_STORE_SELF_JOIN = "single.store.self.join";

    private static final List<String> TOPOLOGY_OPTIMIZATION_CONFIGS = Arrays.asList(
        OPTIMIZE, NO_OPTIMIZATION, REUSE_KTABLE_SOURCE_TOPICS, MERGE_REPARTITION_TOPICS,
        SINGLE_STORE_SELF_JOIN);

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.10.0.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_0100 = UpgradeFromValues.UPGRADE_FROM_0100.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.10.1.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_0101 = UpgradeFromValues.UPGRADE_FROM_0101.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.10.2.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_0102 = UpgradeFromValues.UPGRADE_FROM_0102.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.11.0.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_0110 = UpgradeFromValues.UPGRADE_FROM_0110.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 1.0.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_10 = UpgradeFromValues.UPGRADE_FROM_10.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 1.1.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_11 = UpgradeFromValues.UPGRADE_FROM_11.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 2.0.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_20 = UpgradeFromValues.UPGRADE_FROM_20.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 2.1.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_21 = UpgradeFromValues.UPGRADE_FROM_21.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 2.2.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_22 = UpgradeFromValues.UPGRADE_FROM_22.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 2.3.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_23 = UpgradeFromValues.UPGRADE_FROM_23.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 2.4.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_24 = UpgradeFromValues.UPGRADE_FROM_24.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 2.5.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_25 = UpgradeFromValues.UPGRADE_FROM_25.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 2.6.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_26 = UpgradeFromValues.UPGRADE_FROM_26.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 2.7.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_27 = UpgradeFromValues.UPGRADE_FROM_27.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 2.8.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_28 = UpgradeFromValues.UPGRADE_FROM_28.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 3.0.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_30 = UpgradeFromValues.UPGRADE_FROM_30.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 3.1.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_31 = UpgradeFromValues.UPGRADE_FROM_31.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 3.2.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_32 = UpgradeFromValues.UPGRADE_FROM_32.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 3.3.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_33 = UpgradeFromValues.UPGRADE_FROM_33.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 3.4.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_34 = UpgradeFromValues.UPGRADE_FROM_34.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 3.5.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_35 = UpgradeFromValues.UPGRADE_FROM_35.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 3.6.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_36 = UpgradeFromValues.UPGRADE_FROM_36.toString();

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 3.7.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_37 = UpgradeFromValues.UPGRADE_FROM_37.toString();

    /**
     * Config value for parameter {@link #PROCESSING_GUARANTEE_CONFIG "processing.guarantee"} for at-least-once processing guarantees.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String AT_LEAST_ONCE = "at_least_once";

    /**
     * Config value for parameter {@link #PROCESSING_GUARANTEE_CONFIG "processing.guarantee"} for exactly-once processing guarantees.
     * <p>
     * Enabling exactly-once processing semantics requires broker version 0.11.0 or higher.
     * If you enable this feature Kafka Streams will use more resources (like broker connections)
     * compared to {@link #AT_LEAST_ONCE "at_least_once"} and {@link #EXACTLY_ONCE_V2 "exactly_once_v2"}.
     *
     * @deprecated Since 3.0.0, will be removed in 4.0. Use {@link #EXACTLY_ONCE_V2 "exactly_once_v2"} instead.
     */
    @SuppressWarnings("WeakerAccess")
    @Deprecated
    public static final String EXACTLY_ONCE = "exactly_once";

    /**
     * Config value for parameter {@link #PROCESSING_GUARANTEE_CONFIG "processing.guarantee"} for exactly-once processing guarantees.
     * <p>
     * Enabling exactly-once (beta) requires broker version 2.5 or higher.
     * If you enable this feature Kafka Streams will use fewer resources (like broker connections)
     * compared to the {@link #EXACTLY_ONCE} (deprecated) case.
     *
     * @deprecated Since 3.0.0, will be removed in 4.0. Use {@link #EXACTLY_ONCE_V2 "exactly_once_v2"} instead.
     */
    @SuppressWarnings("WeakerAccess")
    @Deprecated
    public static final String EXACTLY_ONCE_BETA = "exactly_once_beta";

    /**
     * Config value for parameter {@link #PROCESSING_GUARANTEE_CONFIG "processing.guarantee"} for exactly-once processing guarantees.
     * <p>
     * Enabling exactly-once-v2 requires broker version 2.5 or higher.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String EXACTLY_ONCE_V2 = "exactly_once_v2";

    /**
     * Config value for parameter {@link #BUILT_IN_METRICS_VERSION_CONFIG "built.in.metrics.version"} for the latest built-in metrics version.
     */
    public static final String METRICS_LATEST = "latest";

    /** {@code acceptable.recovery.lag} */
    public static final String ACCEPTABLE_RECOVERY_LAG_CONFIG = "acceptable.recovery.lag";
    private static final String ACCEPTABLE_RECOVERY_LAG_DOC = "The maximum acceptable lag (number of offsets to catch up) for a client to be considered caught-up enough" +
                                                                  " to receive an active task assignment. Upon assignment, it will still restore the rest of the changelog" +
                                                                  " before processing. To avoid a pause in processing during rebalances, this config" +
                                                                  " should correspond to a recovery time of well under a minute for a given workload. Must be at least 0.";

    /** {@code application.id} */
    @SuppressWarnings("WeakerAccess")
    public static final String APPLICATION_ID_CONFIG = "application.id";
    private static final String APPLICATION_ID_DOC = "An identifier for the stream processing application. Must be unique within the Kafka cluster. It is used as 1) the default client-id prefix, 2) the group-id for membership management, 3) the changelog topic prefix.";

    /**{@code application.server} */
    @SuppressWarnings("WeakerAccess")
    public static final String APPLICATION_SERVER_CONFIG = "application.server";
    private static final String APPLICATION_SERVER_DOC = "A host:port pair pointing to a user-defined endpoint that can be used for state store discovery and interactive queries on this KafkaStreams instance.";

    /** {@code bootstrap.servers} */
    @SuppressWarnings("WeakerAccess")
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    /** {@code buffered.records.per.partition} */
    @SuppressWarnings("WeakerAccess")
    public static final String BUFFERED_RECORDS_PER_PARTITION_CONFIG = "buffered.records.per.partition";
    public static final String BUFFERED_RECORDS_PER_PARTITION_DOC = "Maximum number of records to buffer per partition.";

    /** {@code built.in.metrics.version} */
    public static final String BUILT_IN_METRICS_VERSION_CONFIG = "built.in.metrics.version";
    private static final String BUILT_IN_METRICS_VERSION_DOC = "Version of the built-in metrics to use.";

    /** {@code cache.max.bytes.buffering} */
    @SuppressWarnings("WeakerAccess")
    @Deprecated
    public static final String CACHE_MAX_BYTES_BUFFERING_CONFIG = "cache.max.bytes.buffering";
    public static final String CACHE_MAX_BYTES_BUFFERING_DOC = "Maximum number of memory bytes to be used for buffering across all threads";

    /** {@code statestore.cache.max.bytes} */
    @SuppressWarnings("WeakerAccess")
    public static final String STATESTORE_CACHE_MAX_BYTES_CONFIG = "statestore.cache.max.bytes";
    public static final String STATESTORE_CACHE_MAX_BYTES_DOC = "Maximum number of memory bytes to be used for statestore cache across all threads";

    /** {@code client.id} */
    @SuppressWarnings("WeakerAccess")
    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;
    private static final String CLIENT_ID_DOC = "An ID prefix string used for the client IDs of internal (main, restore, and global) consumers , producers, and admin clients" +
        " with pattern <code>&lt;client.id&gt;-[Global]StreamThread[-&lt;threadSequenceNumber&gt;]-&lt;consumer|producer|restore-consumer|global-consumer&gt;</code>.";

    /** {@code enable.metrics.push} */
    @SuppressWarnings("WeakerAccess")
    public static  final String ENABLE_METRICS_PUSH_CONFIG = CommonClientConfigs.ENABLE_METRICS_PUSH_CONFIG;
    public static final String ENABLE_METRICS_PUSH_DOC = "Whether to enable pushing of internal client metrics for (main, restore, and global) consumers, producers, and admin clients." + 
        " The cluster must have a client metrics subscription which corresponds to a client.";

    /** {@code commit.interval.ms} */
    @SuppressWarnings("WeakerAccess")
    public static final String COMMIT_INTERVAL_MS_CONFIG = "commit.interval.ms";
    private static final String COMMIT_INTERVAL_MS_DOC = "The frequency in milliseconds with which to commit processing progress." +
        " For at-least-once processing, committing means to save the position (ie, offsets) of the processor." +
        " For exactly-once processing, it means to commit the transaction which includes to save the position and to make the committed data in the output topic visible to consumers with isolation level read_committed." +
        " (Note, if <code>processing.guarantee</code> is set to <code>" + EXACTLY_ONCE_V2 + "</code>, <code>" + EXACTLY_ONCE + "</code>,the default value is <code>" + EOS_DEFAULT_COMMIT_INTERVAL_MS + "</code>," +
        " otherwise the default value is <code>" + DEFAULT_COMMIT_INTERVAL_MS + "</code>.";

    /** {@code repartition.purge.interval.ms} */
    @SuppressWarnings("WeakerAccess")
    public static final String REPARTITION_PURGE_INTERVAL_MS_CONFIG = "repartition.purge.interval.ms";
    private static final String REPARTITION_PURGE_INTERVAL_MS_DOC = "The frequency in milliseconds with which to delete fully consumed records from repartition topics." +
            " Purging will occur after at least this value since the last purge, but may be delayed until later." +
            " (Note, unlike <code>commit.interval.ms</code>, the default for this value remains unchanged when <code>processing.guarantee</code> is set to <code>" + EXACTLY_ONCE_V2 + "</code>).";

    /** {@code connections.max.idle.ms} */
    @SuppressWarnings("WeakerAccess")
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

    /** {@code default.deserialization.exception.handler} */
    @SuppressWarnings("WeakerAccess")
    public static final String DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG = "default.deserialization.exception.handler";
    public static final String DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_DOC = "Exception handling class that implements the <code>org.apache.kafka.streams.errors.DeserializationExceptionHandler</code> interface.";

    /** {@code default.production.exception.handler} */
    @SuppressWarnings("WeakerAccess")
    public static final String DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG = "default.production.exception.handler";
    private static final String DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_DOC = "Exception handling class that implements the <code>org.apache.kafka.streams.errors.ProductionExceptionHandler</code> interface.";

    /** {@code default.dsl.store} */
    @Deprecated
    @SuppressWarnings("WeakerAccess")
    public static final String DEFAULT_DSL_STORE_CONFIG = "default.dsl.store";
    @Deprecated
    public static final String DEFAULT_DSL_STORE_DOC = "The default state store type used by DSL operators.";

    @Deprecated
    public static final String ROCKS_DB = "rocksDB";
    @Deprecated
    public static final String IN_MEMORY = "in_memory";
    @Deprecated
    public static final String DEFAULT_DSL_STORE = ROCKS_DB;

    /** {@code dsl.store.suppliers.class } */
    public static final String DSL_STORE_SUPPLIERS_CLASS_CONFIG = "dsl.store.suppliers.class";
    static final String DSL_STORE_SUPPLIERS_CLASS_DOC = "Defines which store implementations to plug in to DSL operators. Must implement the <code>org.apache.kafka.streams.state.DslStoreSuppliers</code> interface.";
    static final Class<?> DSL_STORE_SUPPLIERS_CLASS_DEFAULT = BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers.class;

    /** {@code default.windowed.key.serde.inner} */
    @SuppressWarnings("WeakerAccess")
    @Deprecated
    public static final String DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS = "default.windowed.key.serde.inner";
    private static final String DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS_DOC = "Default serializer / deserializer for the inner class of a windowed key. Must implement the " +
        "<code>org.apache.kafka.common.serialization.Serde</code> interface.";

    /** {@code default.windowed.value.serde.inner} */
    @SuppressWarnings("WeakerAccess")
    @Deprecated
    public static final String DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS = "default.windowed.value.serde.inner";
    private static final String DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS_DOC = "Default serializer / deserializer for the inner class of a windowed value. Must implement the " +
        "<code>org.apache.kafka.common.serialization.Serde</code> interface.";

    public static final String WINDOWED_INNER_CLASS_SERDE = "windowed.inner.class.serde";
    private static final String WINDOWED_INNER_CLASS_SERDE_DOC = " Default serializer / deserializer for the inner class of a windowed record. Must implement the " +
        "<code>org.apache.kafka.common.serialization.Serde</code> interface. Note that setting this config in KafkaStreams application would result " +
        "in an error as it is meant to be used only from Plain consumer client.";

    /** {@code default key.serde} */
    @SuppressWarnings("WeakerAccess")
    public static final String DEFAULT_KEY_SERDE_CLASS_CONFIG = "default.key.serde";
    private static final String DEFAULT_KEY_SERDE_CLASS_DOC = "Default serializer / deserializer class for key that implements the <code>org.apache.kafka.common.serialization.Serde</code> interface. "
            + "Note when windowed serde class is used, one needs to set the inner serde class that implements the <code>org.apache.kafka.common.serialization.Serde</code> interface via '"
            + DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS + "' or '" + DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS + "' as well";

    /** {@code default value.serde} */
    @SuppressWarnings("WeakerAccess")
    public static final String DEFAULT_VALUE_SERDE_CLASS_CONFIG = "default.value.serde";
    private static final String DEFAULT_VALUE_SERDE_CLASS_DOC = "Default serializer / deserializer class for value that implements the <code>org.apache.kafka.common.serialization.Serde</code> interface. "
            + "Note when windowed serde class is used, one needs to set the inner serde class that implements the <code>org.apache.kafka.common.serialization.Serde</code> interface via '"
            + DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS + "' or '" + DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS + "' as well";

    /** {@code default.timestamp.extractor} */
    @SuppressWarnings("WeakerAccess")
    public static final String DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG = "default.timestamp.extractor";
    public static final String DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_DOC = "Default timestamp extractor class that implements the <code>org.apache.kafka.streams.processor.TimestampExtractor</code> interface.";

    /** {@code max.task.idle.ms} */
    public static final String MAX_TASK_IDLE_MS_CONFIG = "max.task.idle.ms";
    public static final String MAX_TASK_IDLE_MS_DOC = "This config controls whether joins and merges"
        + " may produce out-of-order results."
        + " The config value is the maximum amount of time in milliseconds a stream task will stay idle"
        + " when it is fully caught up on some (but not all) input partitions"
        + " to wait for producers to send additional records and avoid potential"
        + " out-of-order record processing across multiple input streams."
        + " The default (zero) does not wait for producers to send more records,"
        + " but it does wait to fetch data that is already present on the brokers."
        + " This default means that for records that are already present on the brokers,"
        + " Streams will process them in timestamp order."
        + " Set to -1 to disable idling entirely and process any locally available data,"
        + " even though doing so may produce out-of-order processing.";

    /** {@code max.warmup.replicas} */
    public static final String MAX_WARMUP_REPLICAS_CONFIG = "max.warmup.replicas";
    private static final String MAX_WARMUP_REPLICAS_DOC = "The maximum number of warmup replicas (extra standbys beyond the configured num.standbys) that can be assigned at once for the purpose of keeping " +
                                                              " the task available on one instance while it is warming up on another instance it has been reassigned to. Used to throttle how much extra broker " +
                                                              " traffic and cluster state can be used for high availability. Must be at least 1." +
                                                              "Note that one warmup replica corresponds to one Stream Task. Furthermore, note that each warmup replica can only be promoted to an active task " +
                                                              "during a rebalance (normally during a so-called probing rebalance, which occur at a frequency specified by the `probing.rebalance.interval.ms` config). This means " +
                                                              "that the maximum rate at which active tasks can be migrated from one Kafka Streams Instance to another instance can be determined by " +
                                                              "(`max.warmup.replicas` / `probing.rebalance.interval.ms`).";

    /** {@code metadata.max.age.ms} */
    @SuppressWarnings("WeakerAccess")
    public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;

    /** {@code metrics.num.samples} */
    @SuppressWarnings("WeakerAccess")
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;

    /** {@code metrics.record.level} */
    @SuppressWarnings("WeakerAccess")
    public static final String METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;

    /** {@code metric.reporters} */
    @SuppressWarnings("WeakerAccess")
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

    /** {@code metrics.sample.window.ms} */
    @SuppressWarnings("WeakerAccess")
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;

    /** {@code auto.include.jmx.reporter} */
    @Deprecated
    public static final String AUTO_INCLUDE_JMX_REPORTER_CONFIG = CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_CONFIG;

    /** {@code num.standby.replicas} */
    @SuppressWarnings("WeakerAccess")
    public static final String NUM_STANDBY_REPLICAS_CONFIG = "num.standby.replicas";
    private static final String NUM_STANDBY_REPLICAS_DOC = "The number of standby replicas for each task.";

    /** {@code num.stream.threads} */
    @SuppressWarnings("WeakerAccess")
    public static final String NUM_STREAM_THREADS_CONFIG = "num.stream.threads";
    private static final String NUM_STREAM_THREADS_DOC = "The number of threads to execute stream processing.";

    /** {@code poll.ms} */
    @SuppressWarnings("WeakerAccess")
    public static final String POLL_MS_CONFIG = "poll.ms";
    private static final String POLL_MS_DOC = "The amount of time in milliseconds to block waiting for input.";

    /** {@code probing.rebalance.interval.ms} */
    public static final String PROBING_REBALANCE_INTERVAL_MS_CONFIG = "probing.rebalance.interval.ms";
    private static final String PROBING_REBALANCE_INTERVAL_MS_DOC = "The maximum time in milliseconds to wait before triggering a rebalance to probe for warmup replicas that have finished warming up and are ready to become active." +
        " Probing rebalances will continue to be triggered until the assignment is balanced. Must be at least 1 minute.";

    /** {@code processing.guarantee} */
    @SuppressWarnings("WeakerAccess")
    public static final String PROCESSING_GUARANTEE_CONFIG = "processing.guarantee";
    private static final String PROCESSING_GUARANTEE_DOC = "The processing guarantee that should be used. " +
        "Possible values are <code>" + AT_LEAST_ONCE + "</code> (default) " +
        "and <code>" + EXACTLY_ONCE_V2 + "</code> (requires brokers version 2.5 or higher). " +
        "Deprecated options are <code>" + EXACTLY_ONCE + "</code> (requires brokers version 0.11.0 or higher) " +
        "and <code>" + EXACTLY_ONCE_BETA + "</code> (requires brokers version 2.5 or higher). " +
        "Note that exactly-once processing requires a cluster of at least three brokers by default what is the " +
        "recommended setting for production; for development you can change this, by adjusting broker setting " +
        "<code>transaction.state.log.replication.factor</code> and <code>transaction.state.log.min.isr</code>.";

    /** {@code receive.buffer.bytes} */
    @SuppressWarnings("WeakerAccess")
    public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;

    /** {@code rack.aware.assignment.tags} */
    @SuppressWarnings("WeakerAccess")
    public static final String RACK_AWARE_ASSIGNMENT_TAGS_CONFIG = "rack.aware.assignment.tags";
    private static final String RACK_AWARE_ASSIGNMENT_TAGS_DOC = "List of client tag keys used to distribute standby replicas across Kafka Streams instances." +
                                                                 " When configured, Kafka Streams will make a best-effort to distribute" +
                                                                 " the standby tasks over each client tag dimension.";

    /** {@code reconnect.backoff.ms} */
    @SuppressWarnings("WeakerAccess")
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

    /** {@code reconnect.backoff.max} */
    @SuppressWarnings("WeakerAccess")
    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG;

    /** {@code replication.factor} */
    @SuppressWarnings("WeakerAccess")
    public static final String REPLICATION_FACTOR_CONFIG = "replication.factor";
    private static final String REPLICATION_FACTOR_DOC = "The replication factor for change log topics and repartition topics created by the stream processing application."
        + " The default of <code>-1</code> (meaning: use broker default replication factor) requires broker version 2.4 or newer";

    /** {@code request.timeout.ms} */
    @SuppressWarnings("WeakerAccess")
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;

    /**
     * {@code retries}
     * <p>
     * This config is ignored by Kafka Streams. Note, that the internal clients (producer, admin) are still impacted by this config.
     *
     * @deprecated since 2.7
     */
    @SuppressWarnings("WeakerAccess")
    @Deprecated
    public static final String RETRIES_CONFIG = CommonClientConfigs.RETRIES_CONFIG;

    /** {@code retry.backoff.ms} */
    @SuppressWarnings("WeakerAccess")
    public static final String RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;

    /** {@code rocksdb.config.setter} */
    @SuppressWarnings("WeakerAccess")
    public static final String ROCKSDB_CONFIG_SETTER_CLASS_CONFIG = "rocksdb.config.setter";
    private static final String ROCKSDB_CONFIG_SETTER_CLASS_DOC = "A Rocks DB config setter class or class name that implements the <code>org.apache.kafka.streams.state.RocksDBConfigSetter</code> interface";

    /** {@code security.protocol} */
    @SuppressWarnings("WeakerAccess")
    public static final String SECURITY_PROTOCOL_CONFIG = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;

    /** {@code send.buffer.bytes} */
    @SuppressWarnings("WeakerAccess")
    public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;

    /** {@code state.cleanup.delay} */
    @SuppressWarnings("WeakerAccess")
    public static final String STATE_CLEANUP_DELAY_MS_CONFIG = "state.cleanup.delay.ms";
    private static final String STATE_CLEANUP_DELAY_MS_DOC = "The amount of time in milliseconds to wait before deleting state when a partition has migrated. Only state directories that have not been modified for at least <code>state.cleanup.delay.ms</code> will be removed";

    /** {@code state.dir} */
    @SuppressWarnings("WeakerAccess")
    public static final String STATE_DIR_CONFIG = "state.dir";
    private static final String STATE_DIR_DOC = "Directory location for state store. This path must be unique for each streams instance sharing the same underlying filesystem. Note that if not configured, then the default location will be different in each environment as it is computed using System.getProperty(\"java.io.tmpdir\")";

    /** {@code task.timeout.ms} */
    public static final String TASK_TIMEOUT_MS_CONFIG = "task.timeout.ms";
    public static final String TASK_TIMEOUT_MS_DOC = "The maximum amount of time in milliseconds a task might stall due to internal errors and retries until an error is raised. " +
        "For a timeout of 0ms, a task would raise an error for the first internal error. " +
        "For any timeout larger than 0ms, a task will retry at least once before an error is raised.";


    /** {@code window.size.ms} */
    public static final String WINDOW_SIZE_MS_CONFIG = "window.size.ms";
    private static final String WINDOW_SIZE_MS_DOC = "Sets window size for the deserializer in order to calculate window end times.";

    /** {@code upgrade.from} */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_CONFIG = "upgrade.from";
    private static final String UPGRADE_FROM_DOC = "Allows upgrading in a backward compatible way. " +
        "This is needed when upgrading from [0.10.0, 1.1] to 2.0+, or when upgrading from [2.0, 2.3] to 2.4+. " +
        "When upgrading from 3.3 to a newer version it is not required to specify this config. Default is `null`. " +
        "Accepted values are \"" + UPGRADE_FROM_0100 + "\", \"" + UPGRADE_FROM_0101 + "\", \"" +
        UPGRADE_FROM_0102 + "\", \"" + UPGRADE_FROM_0110 + "\", \"" + UPGRADE_FROM_10 + "\", \"" +
        UPGRADE_FROM_11 + "\", \"" + UPGRADE_FROM_20 + "\", \"" + UPGRADE_FROM_21 + "\", \"" +
        UPGRADE_FROM_22 + "\", \"" + UPGRADE_FROM_23 + "\", \"" + UPGRADE_FROM_24 + "\", \"" +
        UPGRADE_FROM_25 + "\", \"" + UPGRADE_FROM_26 + "\", \"" + UPGRADE_FROM_27 + "\", \"" +
        UPGRADE_FROM_28 + "\", \"" + UPGRADE_FROM_30 + "\", \"" + UPGRADE_FROM_31 + "\", \"" +
        UPGRADE_FROM_32 + "\", \"" + UPGRADE_FROM_33 + "\", \"" + UPGRADE_FROM_34 + "\", \"" +
        UPGRADE_FROM_35 + "\", \"" + UPGRADE_FROM_36 + "\", \"" + UPGRADE_FROM_37 + "(for upgrading from the corresponding old version).";

    /** {@code windowstore.changelog.additional.retention.ms} */
    @SuppressWarnings("WeakerAccess")
    public static final String WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG = "windowstore.changelog.additional.retention.ms";
    private static final String WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_DOC = "Added to a windows maintainMs to ensure data is not deleted from the log prematurely. Allows for clock drift. Default is 1 day";

    /** {@code default.client.supplier} */
    @SuppressWarnings("WeakerAccess")
    public static final String DEFAULT_CLIENT_SUPPLIER_CONFIG = "default.client.supplier";
    public static final String DEFAULT_CLIENT_SUPPLIER_DOC = "Client supplier class that implements the <code>org.apache.kafka.streams.KafkaClientSupplier</code> interface.";

    public static final String RACK_AWARE_ASSIGNMENT_STRATEGY_NONE = "none";
    public static final String RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC = "min_traffic";
    public static final String RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY = "balance_subtopology";

    /** {@code } rack.aware.assignment.strategy */
    @SuppressWarnings("WeakerAccess")
    public static final String RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG = "rack.aware.assignment.strategy";
    public static final String RACK_AWARE_ASSIGNMENT_STRATEGY_DOC = "The strategy we use for rack aware assignment. Rack aware assignment will take <code>client.rack</code> and <code>racks</code> of <code>TopicPartition</code> into account when assigning"
        + " tasks to minimize cross rack traffic. Valid settings are : <code>" + RACK_AWARE_ASSIGNMENT_STRATEGY_NONE + "</code> (default), which will disable rack aware assignment; <code>" + RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC
        + "</code>, which will compute minimum cross rack traffic assignment; <code>" + RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY + "</code>, which will compute minimum cross rack traffic and try to balance the tasks of same subtopolgies across different clients";

    @SuppressWarnings("WeakerAccess")
    public static final String RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG = "rack.aware.assignment.traffic_cost";
    public static final String RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_DOC = "Cost associated with cross rack traffic. This config and <code>rack.aware.assignment.non_overlap_cost</code> controls whether the "
        + "optimization algorithm favors minimizing cross rack traffic or minimize the movement of tasks in existing assignment. If set a larger value <code>" + RackAwareTaskAssignor.class.getName() + "</code> will "
        + "optimize for minimizing cross rack traffic. The default value is null which means it will use default traffic cost values in different assignors.";

    @SuppressWarnings("WeakerAccess")
    public static final String RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG = "rack.aware.assignment.non_overlap_cost";
    public static final String RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_DOC = "Cost associated with moving tasks from existing assignment. This config and <code>" + RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG + "</code> controls whether the "
        + "optimization algorithm favors minimizing cross rack traffic or minimize the movement of tasks in existing assignment. If set a larger value <code>" + RackAwareTaskAssignor.class.getName() + "</code> will "
        + "optimize to maintain the existing assignment. The default value is null which means it will use default non_overlap cost values in different assignors.";


    /**
     * {@code topology.optimization}
     * @deprecated since 2.7; use {@link #TOPOLOGY_OPTIMIZATION_CONFIG} instead
     */
    @Deprecated
    public static final String TOPOLOGY_OPTIMIZATION = TOPOLOGY_OPTIMIZATION_CONFIG;


    private static final String[] NON_CONFIGURABLE_CONSUMER_DEFAULT_CONFIGS =
        new String[] {ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG};
    private static final String[] NON_CONFIGURABLE_CONSUMER_EOS_CONFIGS =
        new String[] {ConsumerConfig.ISOLATION_LEVEL_CONFIG};
    private static final String[] NON_CONFIGURABLE_PRODUCER_EOS_CONFIGS =
        new String[] {
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
            ProducerConfig.TRANSACTIONAL_ID_CONFIG
        };

    static {
        CONFIG = new ConfigDef()

            // HIGH

            .define(APPLICATION_ID_CONFIG, // required with no default value
                    Type.STRING,
                    Importance.HIGH,
                    APPLICATION_ID_DOC)
            .define(BOOTSTRAP_SERVERS_CONFIG, // required with no default value
                    Type.LIST,
                    Importance.HIGH,
                    CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
            .define(NUM_STANDBY_REPLICAS_CONFIG,
                    Type.INT,
                    0,
                    Importance.HIGH,
                    NUM_STANDBY_REPLICAS_DOC)
            .define(STATE_DIR_CONFIG,
                    Type.STRING,
                    System.getProperty("java.io.tmpdir") + File.separator + "kafka-streams",
                    Importance.HIGH,
                    STATE_DIR_DOC,
                    "${java.io.tmpdir}")

            // MEDIUM

            .define(ACCEPTABLE_RECOVERY_LAG_CONFIG,
                    Type.LONG,
                    10_000L,
                    atLeast(0),
                    Importance.MEDIUM,
                    ACCEPTABLE_RECOVERY_LAG_DOC)
            .define(CACHE_MAX_BYTES_BUFFERING_CONFIG,
                    Type.LONG,
                    10 * 1024 * 1024L,
                    atLeast(0),
                    Importance.MEDIUM,
                    CACHE_MAX_BYTES_BUFFERING_DOC)
            .define(STATESTORE_CACHE_MAX_BYTES_CONFIG,
                    Type.LONG,
                    10 * 1024 * 1024L,
                    atLeast(0),
                    Importance.MEDIUM,
                    STATESTORE_CACHE_MAX_BYTES_DOC)
            .define(CLIENT_ID_CONFIG,
                    Type.STRING,
                    "",
                    Importance.MEDIUM,
                    CLIENT_ID_DOC,
                    "<code>&lt;application.id&gt-&lt;random-UUID&gt</code>")
            .define(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                    Type.CLASS,
                    LogAndFailExceptionHandler.class.getName(),
                    Importance.MEDIUM,
                    DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_DOC)
            .define(DEFAULT_KEY_SERDE_CLASS_CONFIG,
                    Type.CLASS,
                    null,
                    Importance.MEDIUM,
                    DEFAULT_KEY_SERDE_CLASS_DOC)
            .define(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS,
                    Type.CLASS,
                    null,
                    Importance.MEDIUM,
                    CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS_DOC)
            .define(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS,
                    Type.CLASS,
                    null,
                    Importance.MEDIUM,
                    CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS_DOC)
            .define(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS,
                    Type.CLASS,
                    null,
                    Importance.MEDIUM,
                    CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS_DOC)
            .define(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS,
                    Type.CLASS,
                    null,
                    Importance.MEDIUM,
                    CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS_DOC)
            .define(DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                    Type.CLASS,
                    DefaultProductionExceptionHandler.class.getName(),
                    Importance.MEDIUM,
                    DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_DOC)
            .define(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                    Type.CLASS,
                    FailOnInvalidTimestamp.class.getName(),
                    Importance.MEDIUM,
                    DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_DOC)
            .define(DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                    Type.CLASS,
                    null,
                    Importance.MEDIUM,
                    DEFAULT_VALUE_SERDE_CLASS_DOC)
            .define(MAX_TASK_IDLE_MS_CONFIG,
                    Type.LONG,
                    0L,
                    Importance.MEDIUM,
                    MAX_TASK_IDLE_MS_DOC)
            .define(MAX_WARMUP_REPLICAS_CONFIG,
                    Type.INT,
                    2,
                    atLeast(1),
                    Importance.MEDIUM,
                    MAX_WARMUP_REPLICAS_DOC)
            .define(NUM_STREAM_THREADS_CONFIG,
                    Type.INT,
                    1,
                    Importance.MEDIUM,
                    NUM_STREAM_THREADS_DOC)
            .define(PROCESSING_GUARANTEE_CONFIG,
                    Type.STRING,
                    AT_LEAST_ONCE,
                    in(AT_LEAST_ONCE, EXACTLY_ONCE, EXACTLY_ONCE_BETA, EXACTLY_ONCE_V2),
                    Importance.MEDIUM,
                    PROCESSING_GUARANTEE_DOC)
            .define(RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG,
                    Type.INT,
                    null,
                    Importance.MEDIUM,
                    RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_DOC)
            .define(RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG,
                    Type.STRING,
                    RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
                    in(RACK_AWARE_ASSIGNMENT_STRATEGY_NONE, RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC, RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY),
                    Importance.MEDIUM,
                    RACK_AWARE_ASSIGNMENT_STRATEGY_DOC)
            .define(RACK_AWARE_ASSIGNMENT_TAGS_CONFIG,
                    Type.LIST,
                    Collections.emptyList(),
                    atMostOfSize(MAX_RACK_AWARE_ASSIGNMENT_TAG_LIST_SIZE),
                    Importance.MEDIUM,
                    RACK_AWARE_ASSIGNMENT_TAGS_DOC)
            .define(RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG,
                    Type.INT,
                    null,
                    Importance.MEDIUM,
                    RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_DOC)
            .define(REPLICATION_FACTOR_CONFIG,
                    Type.INT,
                    -1,
                    Importance.MEDIUM,
                    REPLICATION_FACTOR_DOC)
            .define(SECURITY_PROTOCOL_CONFIG,
                    Type.STRING,
                    CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                    ConfigDef.CaseInsensitiveValidString.in(Utils.enumOptions(SecurityProtocol.class)),
                    Importance.MEDIUM,
                    CommonClientConfigs.SECURITY_PROTOCOL_DOC)
            .define(TASK_TIMEOUT_MS_CONFIG,
                    Type.LONG,
                    Duration.ofMinutes(5L).toMillis(),
                    atLeast(0L),
                    Importance.MEDIUM,
                    TASK_TIMEOUT_MS_DOC)
            .define(TOPOLOGY_OPTIMIZATION_CONFIG,
                    Type.STRING,
                    NO_OPTIMIZATION,
                    (name, value) -> verifyTopologyOptimizationConfigs((String) value),
                    Importance.MEDIUM,
                    TOPOLOGY_OPTIMIZATION_DOC)

            // LOW

            .define(APPLICATION_SERVER_CONFIG,
                    Type.STRING,
                    "",
                    Importance.LOW,
                    APPLICATION_SERVER_DOC)
            .define(BUFFERED_RECORDS_PER_PARTITION_CONFIG,
                    Type.INT,
                    1000,
                    Importance.LOW,
                    BUFFERED_RECORDS_PER_PARTITION_DOC)
            .define(BUILT_IN_METRICS_VERSION_CONFIG,
                    Type.STRING,
                    METRICS_LATEST,
                    in(
                        METRICS_LATEST
                    ),
                    Importance.LOW,
                    BUILT_IN_METRICS_VERSION_DOC)
            .define(COMMIT_INTERVAL_MS_CONFIG,
                    Type.LONG,
                    DEFAULT_COMMIT_INTERVAL_MS,
                    atLeast(0),
                    Importance.LOW,
                    COMMIT_INTERVAL_MS_DOC)
            .define(ENABLE_METRICS_PUSH_CONFIG,
                    Type.BOOLEAN,
                    true,
                    Importance.LOW,
                    ENABLE_METRICS_PUSH_DOC)
            .define(REPARTITION_PURGE_INTERVAL_MS_CONFIG,
                    Type.LONG,
                    DEFAULT_COMMIT_INTERVAL_MS,
                    atLeast(0),
                    Importance.LOW,
                    REPARTITION_PURGE_INTERVAL_MS_DOC)
            .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                    Type.LONG,
                    9 * 60 * 1000L,
                    Importance.LOW,
                    CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
            .define(DEFAULT_DSL_STORE_CONFIG,
                    Type.STRING,
                    DEFAULT_DSL_STORE,
                    in(ROCKS_DB, IN_MEMORY),
                    Importance.LOW,
                    DEFAULT_DSL_STORE_DOC)
            .define(DSL_STORE_SUPPLIERS_CLASS_CONFIG,
                    Type.CLASS,
                    DSL_STORE_SUPPLIERS_CLASS_DEFAULT,
                    Importance.LOW,
                    DSL_STORE_SUPPLIERS_CLASS_DOC)
            .define(DEFAULT_CLIENT_SUPPLIER_CONFIG,
                    Type.CLASS,
                    DefaultKafkaClientSupplier.class.getName(),
                    Importance.LOW,
                    DEFAULT_CLIENT_SUPPLIER_DOC)
            .define(METADATA_MAX_AGE_CONFIG,
                    Type.LONG,
                    5 * 60 * 1000L,
                    atLeast(0),
                    Importance.LOW,
                    CommonClientConfigs.METADATA_MAX_AGE_DOC)
            .define(METRICS_NUM_SAMPLES_CONFIG,
                    Type.INT,
                    2,
                    atLeast(1),
                    Importance.LOW,
                    CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
            .define(METRIC_REPORTER_CLASSES_CONFIG,
                    Type.LIST,
                    "",
                    Importance.LOW,
                    CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
            .define(METRICS_RECORDING_LEVEL_CONFIG,
                    Type.STRING,
                    Sensor.RecordingLevel.INFO.toString(),
                    in(Sensor.RecordingLevel.INFO.toString(), Sensor.RecordingLevel.DEBUG.toString(), RecordingLevel.TRACE.toString()),
                    Importance.LOW,
                    CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC)
            .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
                    Type.LONG,
                    30000L,
                    atLeast(0),
                    Importance.LOW,
                    CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
            .define(AUTO_INCLUDE_JMX_REPORTER_CONFIG,
                    Type.BOOLEAN,
                    true,
                    Importance.LOW,
                    CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_DOC)
            .define(POLL_MS_CONFIG,
                    Type.LONG,
                    100L,
                    Importance.LOW,
                    POLL_MS_DOC)
            .define(PROBING_REBALANCE_INTERVAL_MS_CONFIG,
                    Type.LONG,
                    10 * 60 * 1000L,
                    atLeast(60 * 1000L),
                    Importance.LOW,
                    PROBING_REBALANCE_INTERVAL_MS_DOC)
            .define(RECEIVE_BUFFER_CONFIG,
                    Type.INT,
                    32 * 1024,
                    atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND),
                    Importance.LOW,
                    CommonClientConfigs.RECEIVE_BUFFER_DOC)
            .define(RECONNECT_BACKOFF_MS_CONFIG,
                    Type.LONG,
                    50L,
                    atLeast(0L),
                    Importance.LOW,
                    CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
            .define(RECONNECT_BACKOFF_MAX_MS_CONFIG,
                    Type.LONG,
                    1000L,
                    atLeast(0L),
                    Importance.LOW,
                    CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
            .define(RETRIES_CONFIG,
                    Type.INT,
                    0,
                    between(0, Integer.MAX_VALUE),
                    Importance.LOW,
                    CommonClientConfigs.RETRIES_DOC)
            .define(RETRY_BACKOFF_MS_CONFIG,
                    Type.LONG,
                    100L,
                    atLeast(0L),
                    Importance.LOW,
                    CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
            .define(REQUEST_TIMEOUT_MS_CONFIG,
                    Type.INT,
                    40 * 1000,
                    atLeast(0),
                    Importance.LOW,
                    CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC)
            .define(ROCKSDB_CONFIG_SETTER_CLASS_CONFIG,
                    Type.CLASS,
                    null,
                    Importance.LOW,
                    ROCKSDB_CONFIG_SETTER_CLASS_DOC)
            .define(SEND_BUFFER_CONFIG,
                    Type.INT,
                    128 * 1024,
                    atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND),
                    Importance.LOW,
                    CommonClientConfigs.SEND_BUFFER_DOC)
            .define(STATE_CLEANUP_DELAY_MS_CONFIG,
                    Type.LONG,
                    10 * 60 * 1000L,
                    Importance.LOW,
                    STATE_CLEANUP_DELAY_MS_DOC)
            .define(UPGRADE_FROM_CONFIG,
                    Type.STRING,
                    null,
                    in(Stream.concat(
                            Stream.of((String) null),
                            Arrays.stream(UpgradeFromValues.values()).map(UpgradeFromValues::toString)
                        ).toArray(String[]::new)
                    ),
                    Importance.LOW,
                    UPGRADE_FROM_DOC)
            .define(WINDOWED_INNER_CLASS_SERDE,
                Type.STRING,
                null,
                Importance.LOW,
                WINDOWED_INNER_CLASS_SERDE_DOC)
            .define(WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG,
                    Type.LONG,
                    24 * 60 * 60 * 1000L,
                    Importance.LOW,
                    WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_DOC)
            .define(WINDOW_SIZE_MS_CONFIG,
                    Type.LONG,
                    null,
                    Importance.LOW,
                    WINDOW_SIZE_MS_DOC);
    }

    // this is the list of configs for underlying clients
    // that streams prefer different default values
    private static final Map<String, Object> PRODUCER_DEFAULT_OVERRIDES;
    static {
        final Map<String, Object> tempProducerDefaultOverrides = new HashMap<>();
        tempProducerDefaultOverrides.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        PRODUCER_DEFAULT_OVERRIDES = Collections.unmodifiableMap(tempProducerDefaultOverrides);
    }

    private static final Map<String, Object> PRODUCER_EOS_OVERRIDES;
    static {
        final Map<String, Object> tempProducerDefaultOverrides = new HashMap<>(PRODUCER_DEFAULT_OVERRIDES);
        tempProducerDefaultOverrides.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE);
        tempProducerDefaultOverrides.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // Reduce the transaction timeout for quicker pending offset expiration on broker side.
        tempProducerDefaultOverrides.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, DEFAULT_TRANSACTION_TIMEOUT);

        PRODUCER_EOS_OVERRIDES = Collections.unmodifiableMap(tempProducerDefaultOverrides);
    }

    private static final Map<String, Object> CONSUMER_DEFAULT_OVERRIDES;
    static {
        final Map<String, Object> tempConsumerDefaultOverrides = new HashMap<>();
        tempConsumerDefaultOverrides.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        tempConsumerDefaultOverrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        tempConsumerDefaultOverrides.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        tempConsumerDefaultOverrides.put("internal.leave.group.on.close", false);
        CONSUMER_DEFAULT_OVERRIDES = Collections.unmodifiableMap(tempConsumerDefaultOverrides);
    }

    private static final Map<String, Object> CONSUMER_EOS_OVERRIDES;
    static {
        final Map<String, Object> tempConsumerDefaultOverrides = new HashMap<>(CONSUMER_DEFAULT_OVERRIDES);
        tempConsumerDefaultOverrides.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, READ_COMMITTED.toString());
        CONSUMER_EOS_OVERRIDES = Collections.unmodifiableMap(tempConsumerDefaultOverrides);
    }

    public static class InternalConfig {
        // This is settable in the main Streams config, but it's a private API for now
        public static final String INTERNAL_TASK_ASSIGNOR_CLASS = "internal.task.assignor.class";

        // These are not settable in the main Streams config; they are set by the StreamThread to pass internal
        // state into the assignor.
        public static final String REFERENCE_CONTAINER_PARTITION_ASSIGNOR = "__reference.container.instance__";

        // This is settable in the main Streams config, but it's a private API for testing
        public static final String ASSIGNMENT_LISTENER = "__assignment.listener__";

        // Private API used to control the emit latency for left/outer join results (https://issues.apache.org/jira/browse/KAFKA-10847)
        public static final String EMIT_INTERVAL_MS_KSTREAMS_OUTER_JOIN_SPURIOUS_RESULTS_FIX = "__emit.interval.ms.kstreams.outer.join.spurious.results.fix__";

        // Private API used to control the emit latency for windowed aggregation results for ON_WINDOW_CLOSE emit strategy
        public static final String EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION = "__emit.interval.ms.kstreams.windowed.aggregation__";

        // Private API used to control the usage of consistency offset vectors
        public static final String IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED = "__iq.consistency.offset"
            + ".vector.enabled__";

        // Private API used to control the prefix of the auto created topics
        public static final String TOPIC_PREFIX_ALTERNATIVE = "__internal.override.topic.prefix__";

        // Private API to enable the state updater (i.e. state updating on a dedicated thread)
        public static final String STATE_UPDATER_ENABLED = "__state.updater.enabled__";

        public static boolean getStateUpdaterEnabled(final Map<String, Object> configs) {
            return InternalConfig.getBoolean(configs, InternalConfig.STATE_UPDATER_ENABLED, false);
        }
        
        // Private API to enable processing threads (i.e. polling is decoupled from processing)
        public static final String PROCESSING_THREADS_ENABLED = "__processing.threads.enabled__";

        public static boolean getProcessingThreadsEnabled(final Map<String, Object> configs) {
            return InternalConfig.getBoolean(configs, InternalConfig.PROCESSING_THREADS_ENABLED, false);
        }

        public static boolean getBoolean(final Map<String, Object> configs, final String key, final boolean defaultValue) {
            final Object value = configs.getOrDefault(key, defaultValue);
            if (value instanceof Boolean) {
                return (boolean) value;
            } else if (value instanceof String) {
                return Boolean.parseBoolean((String) value);
            } else {
                log.warn("Invalid value (" + value + ") on internal configuration '" + key + "'. Please specify a true/false value.");
                return defaultValue;
            }
        }

        public static long getLong(final Map<String, Object> configs, final String key, final long defaultValue) {
            final Object value = configs.getOrDefault(key, defaultValue);
            if (value instanceof Number) {
                return ((Number) value).longValue();
            } else if (value instanceof String) {
                return Long.parseLong((String) value);
            } else {
                log.warn("Invalid value (" + value + ") on internal configuration '" + key + "'. Please specify a numeric value.");
                return defaultValue;
            }
        }

        public static String getString(final Map<String, Object> configs, final String key, final String defaultValue) {
            final Object value = configs.getOrDefault(key, defaultValue);
            if (value instanceof String) {
                return (String) value;
            } else {
                log.warn("Invalid value (" + value + ") on internal configuration '" + key + "'. Please specify a String value.");
                return defaultValue;
            }
        }
    }

    /**
     * Prefix a property with {@link #CONSUMER_PREFIX}. This is used to isolate {@link ConsumerConfig consumer configs}
     * from other client configs.
     *
     * @param consumerProp the consumer property to be masked
     * @return {@link #CONSUMER_PREFIX} + {@code consumerProp}
     */
    @SuppressWarnings("WeakerAccess")
    public static String consumerPrefix(final String consumerProp) {
        return CONSUMER_PREFIX + consumerProp;
    }

    /**
     * Prefix a property with {@link #MAIN_CONSUMER_PREFIX}. This is used to isolate {@link ConsumerConfig main consumer configs}
     * from other client configs.
     *
     * @param consumerProp the consumer property to be masked
     * @return {@link #MAIN_CONSUMER_PREFIX} + {@code consumerProp}
     */
    @SuppressWarnings("WeakerAccess")
    public static String mainConsumerPrefix(final String consumerProp) {
        return MAIN_CONSUMER_PREFIX + consumerProp;
    }

    /**
     * Prefix a property with {@link #RESTORE_CONSUMER_PREFIX}. This is used to isolate {@link ConsumerConfig restore consumer configs}
     * from other client configs.
     *
     * @param consumerProp the consumer property to be masked
     * @return {@link #RESTORE_CONSUMER_PREFIX} + {@code consumerProp}
     */
    @SuppressWarnings("WeakerAccess")
    public static String restoreConsumerPrefix(final String consumerProp) {
        return RESTORE_CONSUMER_PREFIX + consumerProp;
    }

    /**
     * Prefix a client tag key with {@link #CLIENT_TAG_PREFIX}.
     *
     * @param clientTagKey client tag key
     * @return {@link #CLIENT_TAG_PREFIX} + {@code clientTagKey}
     */
    public static String clientTagPrefix(final String clientTagKey) {
        return CLIENT_TAG_PREFIX + clientTagKey;
    }

    /**
     * Prefix a property with {@link #GLOBAL_CONSUMER_PREFIX}. This is used to isolate {@link ConsumerConfig global consumer configs}
     * from other client configs.
     *
     * @param consumerProp the consumer property to be masked
     * @return {@link #GLOBAL_CONSUMER_PREFIX} + {@code consumerProp}
     */
    @SuppressWarnings("WeakerAccess")
    public static String globalConsumerPrefix(final String consumerProp) {
        return GLOBAL_CONSUMER_PREFIX + consumerProp;
    }

    /**
     * Prefix a property with {@link #PRODUCER_PREFIX}. This is used to isolate {@link ProducerConfig producer configs}
     * from other client configs.
     *
     * @param producerProp the producer property to be masked
     * @return PRODUCER_PREFIX + {@code producerProp}
     */
    @SuppressWarnings("WeakerAccess")
    public static String producerPrefix(final String producerProp) {
        return PRODUCER_PREFIX + producerProp;
    }

    /**
     * Prefix a property with {@link #ADMIN_CLIENT_PREFIX}. This is used to isolate {@link AdminClientConfig admin configs}
     * from other client configs.
     *
     * @param adminClientProp the admin client property to be masked
     * @return ADMIN_CLIENT_PREFIX + {@code adminClientProp}
     */
    @SuppressWarnings("WeakerAccess")
    public static String adminClientPrefix(final String adminClientProp) {
        return ADMIN_CLIENT_PREFIX + adminClientProp;
    }

    /**
     * Prefix a property with {@link #TOPIC_PREFIX}
     * used to provide default topic configs to be applied when creating internal topics.
     *
     * @param topicProp the topic property to be masked
     * @return TOPIC_PREFIX + {@code topicProp}
     */
    @SuppressWarnings("WeakerAccess")
    public static String topicPrefix(final String topicProp) {
        return TOPIC_PREFIX + topicProp;
    }

    /**
     * Return a copy of the config definition.
     *
     * @return a copy of the config definition
     */
    @SuppressWarnings("unused")
    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG);
    }

    /**
     * Create a new {@code StreamsConfig} using the given properties.
     *
     * @param props properties that specify Kafka Streams and internal consumer/producer configuration
     */
    public StreamsConfig(final Map<?, ?> props) {
        this(props, true);
    }

    @SuppressWarnings("this-escape")
    protected StreamsConfig(final Map<?, ?> props,
                            final boolean doLog) {
        super(CONFIG, props, doLog);
        eosEnabled = StreamsConfigUtils.eosEnabled(this);

        final String processingModeConfig = getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG);
        if (processingModeConfig.equals(EXACTLY_ONCE)) {
            log.warn("Configuration parameter `{}` is deprecated and will be removed in the 4.0.0 release. " +
                         "Please use `{}` instead. Note that this requires broker version 2.5+ so you should prepare "
                         + "to upgrade your brokers if necessary.", EXACTLY_ONCE, EXACTLY_ONCE_V2);
        }
        if (processingModeConfig.equals(EXACTLY_ONCE_BETA)) {
            log.warn("Configuration parameter `{}` is deprecated and will be removed in the 4.0.0 release. " +
                         "Please use `{}` instead.", EXACTLY_ONCE_BETA, EXACTLY_ONCE_V2);
        }

        if (props.containsKey(RETRIES_CONFIG)) {
            log.warn("Configuration parameter `{}` is deprecated and will be removed in the 4.0.0 release.", RETRIES_CONFIG);
        }

        if (eosEnabled) {
            verifyEOSTransactionTimeoutCompatibility();
        }
        verifyTopologyOptimizationConfigs(getString(TOPOLOGY_OPTIMIZATION_CONFIG));
    }

    private void verifyEOSTransactionTimeoutCompatibility() {
        final long commitInterval = getLong(COMMIT_INTERVAL_MS_CONFIG);
        final String transactionTimeoutConfigKey = producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
        final int transactionTimeout =
                originals().containsKey(transactionTimeoutConfigKey) ?
                    (int) Objects.requireNonNull(
                        parseType(transactionTimeoutConfigKey, originals().get(transactionTimeoutConfigKey), Type.INT),
                        "Could not parse config `" + COMMIT_INTERVAL_MS_CONFIG + "` because it's set to `null`") :
                    DEFAULT_TRANSACTION_TIMEOUT;

        if (transactionTimeout < commitInterval) {
            throw new IllegalArgumentException(String.format("Transaction timeout %d was set lower than " +
                "streams commit interval %d. This will cause ongoing transaction always timeout due to inactivity " +
                "caused by long commit interval. Consider reconfiguring commit interval to match " +
                "transaction timeout by tuning 'commit.interval.ms' config, or increase the transaction timeout to match " +
                "commit interval by tuning `producer.transaction.timeout.ms` config.",
                transactionTimeout, commitInterval));
        }
    }

    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        final Map<String, Object> configUpdates =
            CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);

        if (StreamsConfigUtils.eosEnabled(this) && !originals().containsKey(COMMIT_INTERVAL_MS_CONFIG)) {
            log.debug("Using {} default value of {} as exactly once is enabled.",
                    COMMIT_INTERVAL_MS_CONFIG, EOS_DEFAULT_COMMIT_INTERVAL_MS);
            configUpdates.put(COMMIT_INTERVAL_MS_CONFIG, EOS_DEFAULT_COMMIT_INTERVAL_MS);
        }

        validateRackAwarenessConfiguration();

        return configUpdates;
    }

    private void validateRackAwarenessConfiguration() {
        final List<String> rackAwareAssignmentTags = getList(RACK_AWARE_ASSIGNMENT_TAGS_CONFIG);
        final Map<String, String> clientTags = getClientTags();

        if (clientTags.size() > MAX_RACK_AWARE_ASSIGNMENT_TAG_LIST_SIZE) {
            throw new ConfigException("At most " + MAX_RACK_AWARE_ASSIGNMENT_TAG_LIST_SIZE + " client tags " +
                                      "can be specified using " + CLIENT_TAG_PREFIX + " prefix.");
        }

        for (final String rackAwareAssignmentTag : rackAwareAssignmentTags) {
            if (!clientTags.containsKey(rackAwareAssignmentTag)) {
                throw new ConfigException(RACK_AWARE_ASSIGNMENT_TAGS_CONFIG,
                                          rackAwareAssignmentTags,
                                          "Contains invalid value [" + rackAwareAssignmentTag + "] " +
                                          "which doesn't have corresponding tag set via [" + CLIENT_TAG_PREFIX + "] prefix.");
            }
        }

        clientTags.forEach((tagKey, tagValue) -> {
            if (tagKey.length() > MAX_RACK_AWARE_ASSIGNMENT_TAG_KEY_LENGTH) {
                throw new ConfigException(CLIENT_TAG_PREFIX,
                                          tagKey,
                                          "Tag key exceeds maximum length of " + MAX_RACK_AWARE_ASSIGNMENT_TAG_KEY_LENGTH + ".");
            }
            if (tagValue.length() > MAX_RACK_AWARE_ASSIGNMENT_TAG_VALUE_LENGTH) {
                throw new ConfigException(CLIENT_TAG_PREFIX,
                                          tagValue,
                                          "Tag value exceeds maximum length of " + MAX_RACK_AWARE_ASSIGNMENT_TAG_VALUE_LENGTH + ".");
            }
        });
    }

    private Map<String, Object> getCommonConsumerConfigs() {
        final Map<String, Object> clientProvidedProps = getClientPropsWithPrefix(CONSUMER_PREFIX, ConsumerConfig.configNames());

        checkIfUnexpectedUserSpecifiedConsumerConfig(clientProvidedProps, NON_CONFIGURABLE_CONSUMER_DEFAULT_CONFIGS);
        checkIfUnexpectedUserSpecifiedConsumerConfig(clientProvidedProps, NON_CONFIGURABLE_CONSUMER_EOS_CONFIGS);

        final Map<String, Object> consumerProps = new HashMap<>(eosEnabled ? CONSUMER_EOS_OVERRIDES : CONSUMER_DEFAULT_OVERRIDES);
        if (StreamsConfigUtils.processingMode(this) == StreamsConfigUtils.ProcessingMode.EXACTLY_ONCE_V2) {
            consumerProps.put("internal.throw.on.fetch.stable.offset.unsupported", true);
        }
        consumerProps.putAll(getClientCustomProps());
        consumerProps.putAll(clientProvidedProps);

        // bootstrap.servers should be from StreamsConfig
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, originals().get(BOOTSTRAP_SERVERS_CONFIG));

        return consumerProps;
    }

    private void checkIfUnexpectedUserSpecifiedConsumerConfig(final Map<String, Object> clientProvidedProps,
                                                              final String[] nonConfigurableConfigs) {
        // Streams does not allow users to configure certain consumer/producer configurations, for example,
        // enable.auto.commit. In cases where user tries to override such non-configurable
        // consumer/producer configurations, log a warning and remove the user defined value from the Map.
        // Thus the default values for these consumer/producer configurations that are suitable for
        // Streams will be used instead.

        final String nonConfigurableConfigMessage = "Unexpected user-specified %s config: %s found. %sUser setting (%s) will be ignored and the Streams default setting (%s) will be used ";
        final String eosMessage = PROCESSING_GUARANTEE_CONFIG + " is set to " + getString(PROCESSING_GUARANTEE_CONFIG) + ". Hence, ";

        for (final String config: nonConfigurableConfigs) {
            if (clientProvidedProps.containsKey(config)) {

                if (CONSUMER_DEFAULT_OVERRIDES.containsKey(config)) {
                    if (!clientProvidedProps.get(config).equals(CONSUMER_DEFAULT_OVERRIDES.get(config))) {
                        log.warn(String.format(nonConfigurableConfigMessage, "consumer", config, "", clientProvidedProps.get(config),  CONSUMER_DEFAULT_OVERRIDES.get(config)));
                        clientProvidedProps.remove(config);
                    }
                } else if (eosEnabled) {
                    if (CONSUMER_EOS_OVERRIDES.containsKey(config)) {
                        if (!clientProvidedProps.get(config).equals(CONSUMER_EOS_OVERRIDES.get(config))) {
                            log.warn(String.format(nonConfigurableConfigMessage,
                                    "consumer", config, eosMessage, clientProvidedProps.get(config), CONSUMER_EOS_OVERRIDES.get(config)));
                            clientProvidedProps.remove(config);
                        }
                    } else if (PRODUCER_EOS_OVERRIDES.containsKey(config)) {
                        if (!clientProvidedProps.get(config).equals(PRODUCER_EOS_OVERRIDES.get(config))) {
                            log.warn(String.format(nonConfigurableConfigMessage,
                                    "producer", config, eosMessage, clientProvidedProps.get(config), PRODUCER_EOS_OVERRIDES.get(config)));
                            clientProvidedProps.remove(config);
                        }
                    } else if (ProducerConfig.TRANSACTIONAL_ID_CONFIG.equals(config)) {
                        log.warn(String.format(nonConfigurableConfigMessage,
                            "producer", config, eosMessage, clientProvidedProps.get(config), "<appId>-<generatedSuffix>"));
                        clientProvidedProps.remove(config);
                    }
                }
            }
        }

        if (eosEnabled) {
            verifyMaxInFlightRequestPerConnection(clientProvidedProps.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
        }
    }

    private void verifyMaxInFlightRequestPerConnection(final Object maxInFlightRequests) {
        if (maxInFlightRequests != null) {
            final int maxInFlightRequestsAsInteger;
            if (maxInFlightRequests instanceof Integer) {
                maxInFlightRequestsAsInteger = (Integer) maxInFlightRequests;
            } else if (maxInFlightRequests instanceof String) {
                try {
                    maxInFlightRequestsAsInteger = Integer.parseInt(((String) maxInFlightRequests).trim());
                } catch (final NumberFormatException e) {
                    throw new ConfigException(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequests, "String value could not be parsed as 32-bit integer");
                }
            } else {
                throw new ConfigException(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequests, "Expected value to be a 32-bit integer, but it was a " + maxInFlightRequests.getClass().getName());
            }

            if (maxInFlightRequestsAsInteger > 5) {
                throw new ConfigException(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequestsAsInteger, "Can't exceed 5 when exactly-once processing is enabled");
            }
        }
    }

    /**
     * Get the configs to the {@link KafkaConsumer main consumer}.
     * Properties using the prefix {@link #MAIN_CONSUMER_PREFIX} will be used in favor over
     * the properties prefixed with {@link #CONSUMER_PREFIX} and the non-prefixed versions
     * (read the override precedence ordering in {@link #MAIN_CONSUMER_PREFIX}
     * except in the case of {@link ConsumerConfig#BOOTSTRAP_SERVERS_CONFIG} where we always use the non-prefixed
     * version as we only support reading/writing from/to the same Kafka Cluster.
     * If not specified by {@link #MAIN_CONSUMER_PREFIX}, main consumer will share the general consumer configs
     * prefixed by {@link #CONSUMER_PREFIX}.
     *
     * @param groupId      consumer groupId
     * @param clientId     clientId
     * @param threadIdx    stream thread index
     * @return Map of the consumer configuration.
     */
    @SuppressWarnings("WeakerAccess")
    public Map<String, Object> getMainConsumerConfigs(final String groupId, final String clientId, final int threadIdx) {
        final Map<String, Object> consumerProps = getCommonConsumerConfigs();

        // Get main consumer override configs
        final Map<String, Object> mainConsumerProps = originalsWithPrefix(MAIN_CONSUMER_PREFIX);
        consumerProps.putAll(mainConsumerProps);

        // this is a hack to work around StreamsConfig constructor inside StreamsPartitionAssignor to avoid casting
        consumerProps.put(APPLICATION_ID_CONFIG, groupId);

        // add group id, client id with stream client id prefix, and group instance id
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
        final String groupInstanceId = (String) consumerProps.get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
        // Suffix each thread consumer with thread.id to enforce uniqueness of group.instance.id.
        if (groupInstanceId != null) {
            consumerProps.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId + "-" + threadIdx);
        }

        // add configs required for stream partition assignor
        consumerProps.put(UPGRADE_FROM_CONFIG, getString(UPGRADE_FROM_CONFIG));
        consumerProps.put(REPLICATION_FACTOR_CONFIG, getInt(REPLICATION_FACTOR_CONFIG));
        consumerProps.put(APPLICATION_SERVER_CONFIG, getString(APPLICATION_SERVER_CONFIG));
        consumerProps.put(NUM_STANDBY_REPLICAS_CONFIG, getInt(NUM_STANDBY_REPLICAS_CONFIG));
        consumerProps.put(ACCEPTABLE_RECOVERY_LAG_CONFIG, getLong(ACCEPTABLE_RECOVERY_LAG_CONFIG));
        consumerProps.put(MAX_WARMUP_REPLICAS_CONFIG, getInt(MAX_WARMUP_REPLICAS_CONFIG));
        consumerProps.put(PROBING_REBALANCE_INTERVAL_MS_CONFIG, getLong(PROBING_REBALANCE_INTERVAL_MS_CONFIG));
        consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StreamsPartitionAssignor.class.getName());
        consumerProps.put(WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, getLong(WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG));
        consumerProps.put(RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG, getInt(RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG));
        consumerProps.put(RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG, getString(RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG));
        consumerProps.put(RACK_AWARE_ASSIGNMENT_TAGS_CONFIG, getList(RACK_AWARE_ASSIGNMENT_TAGS_CONFIG));
        consumerProps.put(RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG, getInt(RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG));

        // disable auto topic creation
        consumerProps.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");

        // verify that producer batch config is no larger than segment size, then add topic configs required for creating topics
        final Map<String, Object> topicProps = originalsWithPrefix(TOPIC_PREFIX, false);
        final Map<String, Object> producerProps = getClientPropsWithPrefix(PRODUCER_PREFIX, ProducerConfig.configNames());

        if (topicProps.containsKey(topicPrefix(TopicConfig.SEGMENT_BYTES_CONFIG)) &&
            producerProps.containsKey(ProducerConfig.BATCH_SIZE_CONFIG)) {
            final int segmentSize = Integer.parseInt(topicProps.get(topicPrefix(TopicConfig.SEGMENT_BYTES_CONFIG)).toString());
            final int batchSize = Integer.parseInt(producerProps.get(ProducerConfig.BATCH_SIZE_CONFIG).toString());

            if (segmentSize < batchSize) {
                throw new IllegalArgumentException(String.format("Specified topic segment size %d is is smaller than the configured producer batch size %d, this will cause produced batch not able to be appended to the topic",
                        segmentSize,
                        batchSize));
            }
        }

        consumerProps.putAll(topicProps);

        return consumerProps;
    }

    /**
     * Get the configs for the {@link KafkaConsumer restore-consumer}.
     * Properties using the prefix {@link #RESTORE_CONSUMER_PREFIX} will be used in favor over
     * the properties prefixed with {@link #CONSUMER_PREFIX} and the non-prefixed versions
     * (read the override precedence ordering in {@link #RESTORE_CONSUMER_PREFIX}
     * except in the case of {@link ConsumerConfig#BOOTSTRAP_SERVERS_CONFIG} where we always use the non-prefixed
     * version as we only support reading/writing from/to the same Kafka Cluster.
     * If not specified by {@link #RESTORE_CONSUMER_PREFIX}, restore consumer will share the general consumer configs
     * prefixed by {@link #CONSUMER_PREFIX}.
     *
     * @param clientId clientId
     * @return Map of the restore consumer configuration.
     */
    @SuppressWarnings("WeakerAccess")
    public Map<String, Object> getRestoreConsumerConfigs(final String clientId) {
        final Map<String, Object> baseConsumerProps = getCommonConsumerConfigs();

        // Get restore consumer override configs
        final Map<String, Object> restoreConsumerProps = originalsWithPrefix(RESTORE_CONSUMER_PREFIX);
        baseConsumerProps.putAll(restoreConsumerProps);

        // no need to set group id for a restore consumer
        baseConsumerProps.remove(ConsumerConfig.GROUP_ID_CONFIG);
        // no need to set instance id for a restore consumer
        baseConsumerProps.remove(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);

        // add client id with stream client id prefix
        baseConsumerProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
        baseConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        return baseConsumerProps;
    }

    /**
     * Get the configs for the {@link KafkaConsumer global consumer}.
     * Properties using the prefix {@link #GLOBAL_CONSUMER_PREFIX} will be used in favor over
     * the properties prefixed with {@link #CONSUMER_PREFIX} and the non-prefixed versions
     * (read the override precedence ordering in {@link #GLOBAL_CONSUMER_PREFIX}
     * except in the case of {@link ConsumerConfig#BOOTSTRAP_SERVERS_CONFIG} where we always use the non-prefixed
     * version as we only support reading/writing from/to the same Kafka Cluster.
     * If not specified by {@link #GLOBAL_CONSUMER_PREFIX}, global consumer will share the general consumer configs
     * prefixed by {@link #CONSUMER_PREFIX}.
     *
     * @param clientId clientId
     * @return Map of the global consumer configuration.
     */
    @SuppressWarnings("WeakerAccess")
    public Map<String, Object> getGlobalConsumerConfigs(final String clientId) {
        final Map<String, Object> baseConsumerProps = getCommonConsumerConfigs();

        // Get global consumer override configs
        final Map<String, Object> globalConsumerProps = originalsWithPrefix(GLOBAL_CONSUMER_PREFIX);
        baseConsumerProps.putAll(globalConsumerProps);

        // no need to set group id for a global consumer
        baseConsumerProps.remove(ConsumerConfig.GROUP_ID_CONFIG);
        // no need to set instance id for a restore consumer
        baseConsumerProps.remove(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);

        // add client id with stream client id prefix
        baseConsumerProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-global-consumer");
        baseConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        return baseConsumerProps;
    }

    /**
     * Get the configs for the {@link KafkaProducer producer}.
     * Properties using the prefix {@link #PRODUCER_PREFIX} will be used in favor over their non-prefixed versions
     * except in the case of {@link ProducerConfig#BOOTSTRAP_SERVERS_CONFIG} where we always use the non-prefixed
     * version as we only support reading/writing from/to the same Kafka Cluster.
     *
     * @param clientId clientId
     * @return Map of the producer configuration.
     */
    @SuppressWarnings("WeakerAccess")
    public Map<String, Object> getProducerConfigs(final String clientId) {
        final Map<String, Object> clientProvidedProps = getClientPropsWithPrefix(PRODUCER_PREFIX, ProducerConfig.configNames());

        checkIfUnexpectedUserSpecifiedConsumerConfig(clientProvidedProps, NON_CONFIGURABLE_PRODUCER_EOS_CONFIGS);

        // generate producer configs from original properties and overridden maps
        final Map<String, Object> props = new HashMap<>(eosEnabled ? PRODUCER_EOS_OVERRIDES : PRODUCER_DEFAULT_OVERRIDES);
        props.putAll(getClientCustomProps());
        props.putAll(clientProvidedProps);

        // When using EOS alpha, stream should auto-downgrade the transactional commit protocol to be compatible with older brokers.
        if (StreamsConfigUtils.processingMode(this) == StreamsConfigUtils.ProcessingMode.EXACTLY_ONCE_ALPHA) {
            props.put("internal.auto.downgrade.txn.commit", true);
        }

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, originals().get(BOOTSTRAP_SERVERS_CONFIG));
        // add client id with stream client id prefix
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);

        return props;
    }

    /**
     * Get the configs for the {@link Admin admin client}.
     * @param clientId clientId
     * @return Map of the admin client configuration.
     */
    @SuppressWarnings("WeakerAccess")
    public Map<String, Object> getAdminConfigs(final String clientId) {
        final Map<String, Object> clientProvidedProps = getClientPropsWithPrefix(ADMIN_CLIENT_PREFIX, AdminClientConfig.configNames());

        final Map<String, Object> props = new HashMap<>();
        props.putAll(getClientCustomProps());
        props.putAll(clientProvidedProps);

        // add client id with stream client id prefix
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);

        return props;
    }

    /**
     * Get the configured client tags set with {@link #CLIENT_TAG_PREFIX} prefix.
     *
     * @return Map of the client tags.
     */
    @SuppressWarnings("WeakerAccess")
    public Map<String, String> getClientTags() {
        return originalsWithPrefix(CLIENT_TAG_PREFIX).entrySet().stream().collect(
            Collectors.toMap(
                Map.Entry::getKey,
                tagEntry -> Objects.toString(tagEntry.getValue())
            )
        );
    }

    private Map<String, Object> getClientPropsWithPrefix(final String prefix,
                                                         final Set<String> configNames) {
        final Map<String, Object> props = clientProps(configNames, originals());
        props.putAll(originalsWithPrefix(prefix));
        return props;
    }

    /**
     * Get a map of custom configs by removing from the originals all the Streams, Consumer, Producer, and AdminClient configs.
     * Prefixed properties are also removed because they are already added by {@link #getClientPropsWithPrefix(String, Set)}.
     * This allows to set a custom property for a specific client alone if specified using a prefix, or for all
     * when no prefix is used.
     *
     * @return a map with the custom properties
     */
    private Map<String, Object> getClientCustomProps() {
        final Map<String, Object> props = originals();
        props.keySet().removeAll(CONFIG.names());
        props.keySet().removeAll(ConsumerConfig.configNames());
        props.keySet().removeAll(ProducerConfig.configNames());
        props.keySet().removeAll(AdminClientConfig.configNames());
        props.keySet().removeAll(originalsWithPrefix(CONSUMER_PREFIX, false).keySet());
        props.keySet().removeAll(originalsWithPrefix(PRODUCER_PREFIX, false).keySet());
        props.keySet().removeAll(originalsWithPrefix(ADMIN_CLIENT_PREFIX, false).keySet());
        return props;
    }

    public static Set<String> verifyTopologyOptimizationConfigs(final String config) {
        final List<String> configs = Arrays.asList(config.split("\\s*,\\s*"));
        final Set<String> verifiedConfigs = new HashSet<>();
        // Verify it doesn't contain none or all plus a list of optimizations
        if (configs.contains(NO_OPTIMIZATION) || configs.contains(OPTIMIZE)) {
            if (configs.size() > 1) {
                throw new ConfigException("\"" + config + "\" is not a valid optimization config. " + CONFIG_ERROR_MSG);
            }
        }
        for (final String conf: configs) {
            if (!TOPOLOGY_OPTIMIZATION_CONFIGS.contains(conf)) {
                throw new ConfigException("Unrecognized config. " + CONFIG_ERROR_MSG);
            }
        }
        if (configs.contains(OPTIMIZE)) {
            verifiedConfigs.add(REUSE_KTABLE_SOURCE_TOPICS);
            verifiedConfigs.add(MERGE_REPARTITION_TOPICS);
            verifiedConfigs.add(SINGLE_STORE_SELF_JOIN);
        } else if (!configs.contains(NO_OPTIMIZATION)) {
            verifiedConfigs.addAll(configs);
        }
        return verifiedConfigs;
    }

    /**
     * Return configured KafkaClientSupplier
     * @return Configured KafkaClientSupplier
     */
    public KafkaClientSupplier getKafkaClientSupplier() {
        return getConfiguredInstance(StreamsConfig.DEFAULT_CLIENT_SUPPLIER_CONFIG,
            KafkaClientSupplier.class);
    }

    /**
     * Return an {@link Serde#configure(Map, boolean) configured} instance of {@link #DEFAULT_KEY_SERDE_CLASS_CONFIG key Serde
     * class}.
     *
     * @return an configured instance of key Serde class
     */
    @SuppressWarnings("WeakerAccess")
    public Serde<?> defaultKeySerde() {
        final Object keySerdeConfigSetting = get(DEFAULT_KEY_SERDE_CLASS_CONFIG);
        if (keySerdeConfigSetting ==  null) {
            throw new ConfigException("Please specify a key serde or set one through StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG");
        }
        try {
            final Serde<?> serde = getConfiguredInstance(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serde.class);
            serde.configure(originals(), true);
            return serde;
        } catch (final Exception e) {
            throw new StreamsException(
                String.format("Failed to configure key serde %s", keySerdeConfigSetting), e);
        }
    }

    /**
     * Return an {@link Serde#configure(Map, boolean) configured} instance of {@link #DEFAULT_VALUE_SERDE_CLASS_CONFIG value
     * Serde class}.
     *
     * @return an configured instance of value Serde class
     */
    @SuppressWarnings("WeakerAccess")
    public Serde<?> defaultValueSerde() {
        final Object valueSerdeConfigSetting = get(DEFAULT_VALUE_SERDE_CLASS_CONFIG);
        if (valueSerdeConfigSetting == null) {
            throw new ConfigException("Please specify a value serde or set one through StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG");
        }
        try {
            final Serde<?> serde = getConfiguredInstance(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serde.class);
            serde.configure(originals(), false);
            return serde;
        } catch (final Exception e) {
            throw new StreamsException(
                String.format("Failed to configure value serde %s", valueSerdeConfigSetting), e);
        }
    }

    @SuppressWarnings("WeakerAccess")
    public TimestampExtractor defaultTimestampExtractor() {
        return getConfiguredInstance(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimestampExtractor.class);
    }

    @SuppressWarnings("WeakerAccess")
    public DeserializationExceptionHandler defaultDeserializationExceptionHandler() {
        return getConfiguredInstance(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, DeserializationExceptionHandler.class);
    }

    @SuppressWarnings("WeakerAccess")
    public ProductionExceptionHandler defaultProductionExceptionHandler() {
        return getConfiguredInstance(DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, ProductionExceptionHandler.class);
    }

    /**
     * Override any client properties in the original configs with overrides
     *
     * @param configNames The given set of configuration names.
     * @param originals   The original configs to be filtered.
     * @return client config with any overrides
     */
    private Map<String, Object> clientProps(final Set<String> configNames,
                                            final Map<String, Object> originals) {
        // iterate all client config names, filter out non-client configs from the original
        // property map and use the overridden values when they are not specified by users
        final Map<String, Object> parsed = new HashMap<>();
        for (final String configName: configNames) {
            if (originals.containsKey(configName)) {
                parsed.put(configName, originals.get(configName));
            }
        }

        return parsed;
    }

    public static void main(final String[] args) {
        System.out.println(CONFIG.toHtml(4, config -> "streamsconfigs_" + config));
    }
}
