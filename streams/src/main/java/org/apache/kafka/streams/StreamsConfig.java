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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.DefaultPartitionGrouper;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;
import static org.apache.kafka.common.requests.IsolationLevel.READ_COMMITTED;

/**
 * Configuration for a {@link KafkaStreams} instance.
 * Can also be used to configure the Kafka Streams internal {@link KafkaConsumer}, {@link KafkaProducer} and {@link AdminClient}.
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
 * When increasing both {@link ProducerConfig#RETRIES_CONFIG} and {@link ProducerConfig#MAX_BLOCK_MS_CONFIG} to be more resilient to non-available brokers you should also
 * consider increasing {@link ConsumerConfig#MAX_POLL_INTERVAL_MS_CONFIG} using the following guidance:
 * <pre>
 *     max.poll.interval.ms > min ( max.block.ms, (retries +1) * request.timeout.ms )
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
 *   <li>{@link ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG "enable.auto.commit"} (false) - Streams client will always disable/turn off auto committing</li>
 * </ul>
 *
 * If {@link #PROCESSING_GUARANTEE_CONFIG "processing.guarantee"} is set to {@link #EXACTLY_ONCE "exactly_once"}, Kafka Streams does not allow users to overwrite the following properties (Streams setting shown in parentheses):
 * <ul>
 *   <li>{@link ConsumerConfig#ISOLATION_LEVEL_CONFIG "isolation.level"} (read_committed) - Consumers will always read committed data only</li>
 *   <li>{@link ProducerConfig#ENABLE_IDEMPOTENCE_CONFIG "enable.idempotence"} (true) - Producer will always have idempotency enabled</li>
 *   <li>{@link ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION "max.in.flight.requests.per.connection"} (5) - Producer will always have one in-flight request per connection</li>
 * </ul>
 *
 *
 * @see KafkaStreams#KafkaStreams(org.apache.kafka.streams.Topology, Properties)
 * @see ConsumerConfig
 * @see ProducerConfig
 */
public class StreamsConfig extends AbstractConfig {

    private final static Logger log = LoggerFactory.getLogger(StreamsConfig.class);

    private static final ConfigDef CONFIG;

    private final boolean eosEnabled;
    private final static long DEFAULT_COMMIT_INTERVAL_MS = 30000L;
    private final static long EOS_DEFAULT_COMMIT_INTERVAL_MS = 100L;

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
     * Prefix used to isolate {@link org.apache.kafka.clients.admin.AdminClient admin} configs from other client configs.
     * It is recommended to use {@link #adminClientPrefix(String)} to add this prefix to {@link ProducerConfig producer
     * properties}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String ADMIN_CLIENT_PREFIX = "admin.";

    /**
     * Config value for parameter (@link #TOPOLOGY_OPTIMIZATION "topology.optimization" for disabling topology optimization
     */
    public static final String NO_OPTIMIZATION = "none";

    /**
     * Config value for parameter (@link #TOPOLOGY_OPTIMIZATION "topology.optimization" for enabling topology optimization
     */
    public static final String OPTIMIZE = "all";

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.10.0.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_0100 = "0.10.0";

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.10.1.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_0101 = "0.10.1";

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.10.2.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_0102 = "0.10.2";

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.11.0.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_0110 = "0.11.0";

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 1.0.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_10 = "1.0";

    /**
     * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 1.1.x}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_11 = "1.1";

    /**
     * Config value for parameter {@link #PROCESSING_GUARANTEE_CONFIG "processing.guarantee"} for at-least-once processing guarantees.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String AT_LEAST_ONCE = "at_least_once";

    /**
     * Config value for parameter {@link #PROCESSING_GUARANTEE_CONFIG "processing.guarantee"} for exactly-once processing guarantees.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String EXACTLY_ONCE = "exactly_once";

    /** {@code application.id} */
    @SuppressWarnings("WeakerAccess")
    public static final String APPLICATION_ID_CONFIG = "application.id";
    private static final String APPLICATION_ID_DOC = "An identifier for the stream processing application. Must be unique within the Kafka cluster. It is used as 1) the default client-id prefix, 2) the group-id for membership management, 3) the changelog topic prefix.";

    /**{@code user.endpoint} */
    @SuppressWarnings("WeakerAccess")
    public static final String APPLICATION_SERVER_CONFIG = "application.server";
    private static final String APPLICATION_SERVER_DOC = "A host:port pair pointing to an embedded user defined endpoint that can be used for discovering the locations of state stores within a single KafkaStreams application";

    /** {@code bootstrap.servers} */
    @SuppressWarnings("WeakerAccess")
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    /** {@code buffered.records.per.partition} */
    @SuppressWarnings("WeakerAccess")
    public static final String BUFFERED_RECORDS_PER_PARTITION_CONFIG = "buffered.records.per.partition";
    private static final String BUFFERED_RECORDS_PER_PARTITION_DOC = "The maximum number of records to buffer per partition.";

    /** {@code cache.max.bytes.buffering} */
    @SuppressWarnings("WeakerAccess")
    public static final String CACHE_MAX_BYTES_BUFFERING_CONFIG = "cache.max.bytes.buffering";
    private static final String CACHE_MAX_BYTES_BUFFERING_DOC = "Maximum number of memory bytes to be used for buffering across all threads";

    /** {@code client.id} */
    @SuppressWarnings("WeakerAccess")
    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;
    private static final String CLIENT_ID_DOC = "An ID prefix string used for the client IDs of internal consumer, producer and restore-consumer," +
        " with pattern '<client.id>-StreamThread-<threadSequenceNumber>-<consumer|producer|restore-consumer>'.";

    /** {@code commit.interval.ms} */
    @SuppressWarnings("WeakerAccess")
    public static final String COMMIT_INTERVAL_MS_CONFIG = "commit.interval.ms";
    private static final String COMMIT_INTERVAL_MS_DOC = "The frequency with which to save the position of the processor." +
        " (Note, if 'processing.guarantee' is set to '" + EXACTLY_ONCE + "', the default value is " + EOS_DEFAULT_COMMIT_INTERVAL_MS + "," +
        " otherwise the default value is " + DEFAULT_COMMIT_INTERVAL_MS + ".";

    /** {@code connections.max.idle.ms} */
    @SuppressWarnings("WeakerAccess")
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

    /**
     * {@code default.deserialization.exception.handler}
     */
    @SuppressWarnings("WeakerAccess")
    public static final String DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG = "default.deserialization.exception.handler";
    private static final String DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_DOC = "Exception handling class that implements the <code>org.apache.kafka.streams.errors.DeserializationExceptionHandler</code> interface.";

    /**
     * {@code default.production.exception.handler}
     */
    @SuppressWarnings("WeakerAccess")
    public static final String DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG = "default.production.exception.handler";
    private static final String DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_DOC = "Exception handling class that implements the <code>org.apache.kafka.streams.errors.ProductionExceptionHandler</code> interface.";

    /**
     * {@code default.windowed.key.serde.inner}
     */
    @SuppressWarnings("WeakerAccess")
    public static final String DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS = "default.windowed.key.serde.inner";

    /**
     * {@code default.windowed.value.serde.inner}
     */
    @SuppressWarnings("WeakerAccess")
    public static final String DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS = "default.windowed.value.serde.inner";

    /** {@code default key.serde} */
    @SuppressWarnings("WeakerAccess")
    public static final String DEFAULT_KEY_SERDE_CLASS_CONFIG = "default.key.serde";
    private static final String DEFAULT_KEY_SERDE_CLASS_DOC = " Default serializer / deserializer class for key that implements the <code>org.apache.kafka.common.serialization.Serde</code> interface. "
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
    private static final String DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_DOC = "Default timestamp extractor class that implements the <code>org.apache.kafka.streams.processor.TimestampExtractor</code> interface.";

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

    /** {@code num.standby.replicas} */
    @SuppressWarnings("WeakerAccess")
    public static final String NUM_STANDBY_REPLICAS_CONFIG = "num.standby.replicas";
    private static final String NUM_STANDBY_REPLICAS_DOC = "The number of standby replicas for each task.";

    /** {@code num.stream.threads} */
    @SuppressWarnings("WeakerAccess")
    public static final String NUM_STREAM_THREADS_CONFIG = "num.stream.threads";
    private static final String NUM_STREAM_THREADS_DOC = "The number of threads to execute stream processing.";

    /** {@code partition.grouper} */
    @SuppressWarnings("WeakerAccess")
    public static final String PARTITION_GROUPER_CLASS_CONFIG = "partition.grouper";
    private static final String PARTITION_GROUPER_CLASS_DOC = "Partition grouper class that implements the <code>org.apache.kafka.streams.processor.PartitionGrouper</code> interface.";

    /** {@code poll.ms} */
    @SuppressWarnings("WeakerAccess")
    public static final String POLL_MS_CONFIG = "poll.ms";
    private static final String POLL_MS_DOC = "The amount of time in milliseconds to block waiting for input.";

    /** {@code processing.guarantee} */
    @SuppressWarnings("WeakerAccess")
    public static final String PROCESSING_GUARANTEE_CONFIG = "processing.guarantee";
    private static final String PROCESSING_GUARANTEE_DOC = "The processing guarantee that should be used. Possible values are <code>" + AT_LEAST_ONCE + "</code> (default) and <code>" + EXACTLY_ONCE + "</code>. " +
        "Note that exactly-once processing requires a cluster of at least three brokers by default what is the recommended setting for production; for development you can change this, by adjusting broker setting `transaction.state.log.replication.factor`.";

    /** {@code receive.buffer.bytes} */
    @SuppressWarnings("WeakerAccess")
    public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;

    /** {@code reconnect.backoff.ms} */
    @SuppressWarnings("WeakerAccess")
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

    /** {@code reconnect.backoff.max} */
    @SuppressWarnings("WeakerAccess")
    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG;

    /** {@code replication.factor} */
    @SuppressWarnings("WeakerAccess")
    public static final String REPLICATION_FACTOR_CONFIG = "replication.factor";
    private static final String REPLICATION_FACTOR_DOC = "The replication factor for change log topics and repartition topics created by the stream processing application.";

    /** {@code request.timeout.ms} */
    @SuppressWarnings("WeakerAccess")
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;

    /** {@code retries} */
    @SuppressWarnings("WeakerAccess")
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
    private static final String STATE_CLEANUP_DELAY_MS_DOC = "The amount of time in milliseconds to wait before deleting state when a partition has migrated. Only state directories that have not been modified for at least state.cleanup.delay.ms will be removed";

    /** {@code state.dir} */
    @SuppressWarnings("WeakerAccess")
    public static final String STATE_DIR_CONFIG = "state.dir";
    private static final String STATE_DIR_DOC = "Directory location for state store.";

    /** {@code topology.optimization} */
    public static final String TOPOLOGY_OPTIMIZATION = "topology.optimization";
    private static final String TOPOLOGY_OPTIMIZATION_DOC = "A configuration telling Kafka Streams if it should optimize the topology, disabled by default";

    /** {@code upgrade.from} */
    @SuppressWarnings("WeakerAccess")
    public static final String UPGRADE_FROM_CONFIG = "upgrade.from";
    private static final String UPGRADE_FROM_DOC = "Allows upgrading from versions 0.10.0/0.10.1/0.10.2/0.11.0/1.0/1.1 to version 1.2 (or newer) in a backward compatible way. " +
        "When upgrading from 1.2 to a newer version it is not required to specify this config." +
        "Default is null. Accepted values are \"" + UPGRADE_FROM_0100 + "\", \"" + UPGRADE_FROM_0101 + "\", \"" + UPGRADE_FROM_0102 + "\", \"" + UPGRADE_FROM_0110 + "\", \"" + UPGRADE_FROM_10 + "\", \"" + UPGRADE_FROM_11 + "\" (for upgrading from the corresponding old version).";

    /** {@code windowstore.changelog.additional.retention.ms} */
    @SuppressWarnings("WeakerAccess")
    public static final String WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG = "windowstore.changelog.additional.retention.ms";
    private static final String WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_DOC = "Added to a windows maintainMs to ensure data is not deleted from the log prematurely. Allows for clock drift. Default is 1 day";

    private static final String[] NON_CONFIGURABLE_CONSUMER_DEFAULT_CONFIGS = new String[] {ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG};
    private static final String[] NON_CONFIGURABLE_CONSUMER_EOS_CONFIGS = new String[] {ConsumerConfig.ISOLATION_LEVEL_CONFIG};
    private static final String[] NON_CONFIGURABLE_PRODUCER_EOS_CONFIGS = new String[] {ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                                                                                        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION};

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
            .define(REPLICATION_FACTOR_CONFIG,
                    Type.INT,
                    1,
                    Importance.HIGH,
                    REPLICATION_FACTOR_DOC)
            .define(STATE_DIR_CONFIG,
                    Type.STRING,
                    "/tmp/kafka-streams",
                    Importance.HIGH,
                    STATE_DIR_DOC)

            // MEDIUM

            .define(CACHE_MAX_BYTES_BUFFERING_CONFIG,
                    Type.LONG,
                    10 * 1024 * 1024L,
                    atLeast(0),
                    Importance.MEDIUM,
                    CACHE_MAX_BYTES_BUFFERING_DOC)
            .define(CLIENT_ID_CONFIG,
                    Type.STRING,
                    "",
                    Importance.MEDIUM,
                    CLIENT_ID_DOC)
            .define(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                    Type.CLASS,
                    LogAndFailExceptionHandler.class.getName(),
                    Importance.MEDIUM,
                    DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_DOC)
            .define(DEFAULT_KEY_SERDE_CLASS_CONFIG,
                    Type.CLASS,
                    Serdes.ByteArraySerde.class.getName(),
                    Importance.MEDIUM,
                    DEFAULT_KEY_SERDE_CLASS_DOC)
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
                    Serdes.ByteArraySerde.class.getName(),
                    Importance.MEDIUM,
                    DEFAULT_VALUE_SERDE_CLASS_DOC)
            .define(NUM_STANDBY_REPLICAS_CONFIG,
                    Type.INT,
                    0,
                    Importance.MEDIUM,
                    NUM_STANDBY_REPLICAS_DOC)
            .define(NUM_STREAM_THREADS_CONFIG,
                    Type.INT,
                    1,
                    Importance.MEDIUM,
                    NUM_STREAM_THREADS_DOC)
            .define(PROCESSING_GUARANTEE_CONFIG,
                    Type.STRING,
                    AT_LEAST_ONCE,
                    in(AT_LEAST_ONCE, EXACTLY_ONCE),
                    Importance.MEDIUM,
                    PROCESSING_GUARANTEE_DOC)
            .define(SECURITY_PROTOCOL_CONFIG,
                    Type.STRING,
                    CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                    Importance.MEDIUM,
                    CommonClientConfigs.SECURITY_PROTOCOL_DOC)
            .define(TOPOLOGY_OPTIMIZATION,
                    Type.STRING,
                    NO_OPTIMIZATION,
                    in(NO_OPTIMIZATION, OPTIMIZE),
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
            .define(COMMIT_INTERVAL_MS_CONFIG,
                    Type.LONG,
                    DEFAULT_COMMIT_INTERVAL_MS,
                    Importance.LOW,
                    COMMIT_INTERVAL_MS_DOC)
            .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    9 * 60 * 1000L,
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
            .define(METADATA_MAX_AGE_CONFIG,
                    ConfigDef.Type.LONG,
                    5 * 60 * 1000L,
                    atLeast(0),
                    ConfigDef.Importance.LOW,
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
                    in(Sensor.RecordingLevel.INFO.toString(), Sensor.RecordingLevel.DEBUG.toString()),
                    Importance.LOW,
                    CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC)
            .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
                    Type.LONG,
                    30000L,
                    atLeast(0),
                    Importance.LOW,
                    CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
            .define(PARTITION_GROUPER_CLASS_CONFIG,
                    Type.CLASS,
                    DefaultPartitionGrouper.class.getName(),
                    Importance.LOW,
                    PARTITION_GROUPER_CLASS_DOC)
            .define(POLL_MS_CONFIG,
                    Type.LONG,
                    100L,
                    Importance.LOW,
                    POLL_MS_DOC)
            .define(RECEIVE_BUFFER_CONFIG,
                    Type.INT,
                    32 * 1024,
                    atLeast(0),
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
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
            .define(RETRIES_CONFIG,
                    Type.INT,
                    0,
                    between(0, Integer.MAX_VALUE),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.RETRIES_DOC)
            .define(RETRY_BACKOFF_MS_CONFIG,
                    Type.LONG,
                    100L,
                    atLeast(0L),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
            .define(REQUEST_TIMEOUT_MS_CONFIG,
                    Type.INT,
                    40 * 1000,
                    atLeast(0),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC)
            .define(ROCKSDB_CONFIG_SETTER_CLASS_CONFIG,
                    Type.CLASS,
                    null,
                    Importance.LOW,
                    ROCKSDB_CONFIG_SETTER_CLASS_DOC)
            .define(SEND_BUFFER_CONFIG,
                    Type.INT,
                    128 * 1024,
                    atLeast(0),
                    Importance.LOW,
                    CommonClientConfigs.SEND_BUFFER_DOC)
            .define(STATE_CLEANUP_DELAY_MS_CONFIG,
                    Type.LONG,
                    10 * 60 * 1000L,
                    Importance.LOW,
                    STATE_CLEANUP_DELAY_MS_DOC)
            .define(UPGRADE_FROM_CONFIG,
                    ConfigDef.Type.STRING,
                    null,
                    in(null, UPGRADE_FROM_0100, UPGRADE_FROM_0101, UPGRADE_FROM_0102, UPGRADE_FROM_0110, UPGRADE_FROM_10, UPGRADE_FROM_11),
                    Importance.LOW,
                    UPGRADE_FROM_DOC)
            .define(WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG,
                    Type.LONG,
                    24 * 60 * 60 * 1000L,
                    Importance.LOW,
                    WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_DOC);
    }

    // this is the list of configs for underlying clients
    // that streams prefer different default values
    private static final Map<String, Object> PRODUCER_DEFAULT_OVERRIDES;
    static {
        final Map<String, Object> tempProducerDefaultOverrides = new HashMap<>();
        tempProducerDefaultOverrides.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        tempProducerDefaultOverrides.put(ProducerConfig.RETRIES_CONFIG, 10);

        PRODUCER_DEFAULT_OVERRIDES = Collections.unmodifiableMap(tempProducerDefaultOverrides);
    }

    private static final Map<String, Object> PRODUCER_EOS_OVERRIDES;
    static {
        final Map<String, Object> tempProducerDefaultOverrides = new HashMap<>(PRODUCER_DEFAULT_OVERRIDES);
        tempProducerDefaultOverrides.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        tempProducerDefaultOverrides.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        PRODUCER_EOS_OVERRIDES = Collections.unmodifiableMap(tempProducerDefaultOverrides);
    }

    private static final Map<String, Object> CONSUMER_DEFAULT_OVERRIDES;
    static {
        final Map<String, Object> tempConsumerDefaultOverrides = new HashMap<>();
        tempConsumerDefaultOverrides.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        tempConsumerDefaultOverrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        tempConsumerDefaultOverrides.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        tempConsumerDefaultOverrides.put("internal.leave.group.on.close", false);
        // MAX_POLL_INTERVAL_MS_CONFIG needs to be large for streams to handle cases when
        // streams is recovering data from state stores. We may set it to Integer.MAX_VALUE since
        // the streams code itself catches most exceptions and acts accordingly without needing
        // this timeout. Note however that deadlocks are not detected (by definition) so we
        // are losing the ability to detect them by setting this value to large. Hopefully
        // deadlocks happen very rarely or never.
        tempConsumerDefaultOverrides.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        CONSUMER_DEFAULT_OVERRIDES = Collections.unmodifiableMap(tempConsumerDefaultOverrides);
    }

    private static final Map<String, Object> CONSUMER_EOS_OVERRIDES;
    static {
        final Map<String, Object> tempConsumerDefaultOverrides = new HashMap<>(CONSUMER_DEFAULT_OVERRIDES);
        tempConsumerDefaultOverrides.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, READ_COMMITTED.name().toLowerCase(Locale.ROOT));
        CONSUMER_EOS_OVERRIDES = Collections.unmodifiableMap(tempConsumerDefaultOverrides);
    }

    public static class InternalConfig {
        public static final String TASK_MANAGER_FOR_PARTITION_ASSIGNOR = "__task.manager.instance__";
        public static final String VERSION_PROBING_FLAG = "__version.probing.flag__";
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
        super(CONFIG, props);
        eosEnabled = EXACTLY_ONCE.equals(getString(PROCESSING_GUARANTEE_CONFIG));
    }

    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        final Map<String, Object> configUpdates =
            CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);

        final boolean eosEnabled = EXACTLY_ONCE.equals(parsedValues.get(PROCESSING_GUARANTEE_CONFIG));
        if (eosEnabled && !originals().containsKey(COMMIT_INTERVAL_MS_CONFIG)) {
            log.debug("Using {} default value of {} as exactly once is enabled.",
                    COMMIT_INTERVAL_MS_CONFIG, EOS_DEFAULT_COMMIT_INTERVAL_MS);
            configUpdates.put(COMMIT_INTERVAL_MS_CONFIG, EOS_DEFAULT_COMMIT_INTERVAL_MS);
        }

        return configUpdates;
    }

    private Map<String, Object> getCommonConsumerConfigs() {
        final Map<String, Object> clientProvidedProps = getClientPropsWithPrefix(CONSUMER_PREFIX, ConsumerConfig.configNames());

        checkIfUnexpectedUserSpecifiedConsumerConfig(clientProvidedProps, NON_CONFIGURABLE_CONSUMER_DEFAULT_CONFIGS);
        checkIfUnexpectedUserSpecifiedConsumerConfig(clientProvidedProps, NON_CONFIGURABLE_CONSUMER_EOS_CONFIGS);

        final Map<String, Object> consumerProps = new HashMap<>(eosEnabled ? CONSUMER_EOS_OVERRIDES : CONSUMER_DEFAULT_OVERRIDES);
        consumerProps.putAll(getClientCustomProps());
        consumerProps.putAll(clientProvidedProps);

        // bootstrap.servers should be from StreamsConfig
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, originals().get(BOOTSTRAP_SERVERS_CONFIG));

        return consumerProps;
    }

    private void checkIfUnexpectedUserSpecifiedConsumerConfig(final Map<String, Object> clientProvidedProps, final String[] nonConfigurableConfigs) {
        // Streams does not allow users to configure certain consumer/producer configurations, for example,
        // enable.auto.commit. In cases where user tries to override such non-configurable
        // consumer/producer configurations, log a warning and remove the user defined value from the Map.
        // Thus the default values for these consumer/producer configurations that are suitable for
        // Streams will be used instead.
        final Object maxInFlightRequests = clientProvidedProps.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        if (eosEnabled && maxInFlightRequests != null && 5 < (int) maxInFlightRequests) {
            throw new ConfigException(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + " can't exceed 5 when using the idempotent producer");
        }
        for (final String config: nonConfigurableConfigs) {
            if (clientProvidedProps.containsKey(config)) {
                final String eosMessage =  PROCESSING_GUARANTEE_CONFIG + " is set to " + EXACTLY_ONCE + ". Hence, ";
                final String nonConfigurableConfigMessage = "Unexpected user-specified %s config: %s found. %sUser setting (%s) will be ignored and the Streams default setting (%s) will be used ";

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
                    }
                }
            }

        }
    }

    /**
     * Get the configs to the {@link KafkaConsumer consumer}.
     * Properties using the prefix {@link #CONSUMER_PREFIX} will be used in favor over their non-prefixed versions
     * except in the case of {@link ConsumerConfig#BOOTSTRAP_SERVERS_CONFIG} where we always use the non-prefixed
     * version as we only support reading/writing from/to the same Kafka Cluster.
     *
     * @param groupId      consumer groupId
     * @param clientId     clientId
     * @return Map of the consumer configuration.
     * @deprecated use {@link StreamsConfig#getMainConsumerConfigs(String, String)}
     */
    @SuppressWarnings("WeakerAccess")
    @Deprecated
    public Map<String, Object> getConsumerConfigs(final String groupId,
                                                  final String clientId) {
        return getMainConsumerConfigs(groupId, clientId);
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
     * @return Map of the consumer configuration.
     */
    @SuppressWarnings("WeakerAccess")
    public Map<String, Object> getMainConsumerConfigs(final String groupId,
                                                      final String clientId) {
        final Map<String, Object> consumerProps = getCommonConsumerConfigs();

        // Get main consumer override configs
        final Map<String, Object> mainConsumerProps = originalsWithPrefix(MAIN_CONSUMER_PREFIX);
        for (final Map.Entry<String, Object> entry: mainConsumerProps.entrySet()) {
            consumerProps.put(entry.getKey(), entry.getValue());
        }

        // add client id with stream client id prefix, and group id
        consumerProps.put(APPLICATION_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-consumer");

        // add configs required for stream partition assignor
        consumerProps.put(UPGRADE_FROM_CONFIG, getString(UPGRADE_FROM_CONFIG));
        consumerProps.put(REPLICATION_FACTOR_CONFIG, getInt(REPLICATION_FACTOR_CONFIG));
        consumerProps.put(APPLICATION_SERVER_CONFIG, getString(APPLICATION_SERVER_CONFIG));
        consumerProps.put(NUM_STANDBY_REPLICAS_CONFIG, getInt(NUM_STANDBY_REPLICAS_CONFIG));
        consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StreamsPartitionAssignor.class.getName());
        consumerProps.put(WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, getLong(WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG));

        // add admin retries configs for creating topics
        final AdminClientConfig adminClientDefaultConfig = new AdminClientConfig(getClientPropsWithPrefix(ADMIN_CLIENT_PREFIX, AdminClientConfig.configNames()));
        consumerProps.put(adminClientPrefix(AdminClientConfig.RETRIES_CONFIG), adminClientDefaultConfig.getInt(AdminClientConfig.RETRIES_CONFIG));

        // verify that producer batch config is no larger than segment size, then add topic configs required for creating topics
        final Map<String, Object> topicProps = originalsWithPrefix(TOPIC_PREFIX, false);

        if (topicProps.containsKey(topicPrefix(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG))) {
            final int segmentSize = Integer.parseInt(topicProps.get(topicPrefix(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG)).toString());
            final Map<String, Object> producerProps = getClientPropsWithPrefix(PRODUCER_PREFIX, ProducerConfig.configNames());
            final int batchSize;
            if (producerProps.containsKey(ProducerConfig.BATCH_SIZE_CONFIG)) {
                batchSize = Integer.parseInt(producerProps.get(ProducerConfig.BATCH_SIZE_CONFIG).toString());
            } else {
                final ProducerConfig producerDefaultConfig = new ProducerConfig(new Properties());
                batchSize = producerDefaultConfig.getInt(ProducerConfig.BATCH_SIZE_CONFIG);
            }

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
        for (final Map.Entry<String, Object> entry: restoreConsumerProps.entrySet()) {
            baseConsumerProps.put(entry.getKey(), entry.getValue());
        }

        // no need to set group id for a restore consumer
        baseConsumerProps.remove(ConsumerConfig.GROUP_ID_CONFIG);
        // add client id with stream client id prefix
        baseConsumerProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-restore-consumer");
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
        for (final Map.Entry<String, Object> entry: globalConsumerProps.entrySet()) {
            baseConsumerProps.put(entry.getKey(), entry.getValue());
        }

        // no need to set group id for a global consumer
        baseConsumerProps.remove(ConsumerConfig.GROUP_ID_CONFIG);
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

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, originals().get(BOOTSTRAP_SERVERS_CONFIG));
        // add client id with stream client id prefix
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-producer");

        return props;
    }

    /**
     * Get the configs for the {@link org.apache.kafka.clients.admin.AdminClient admin client}.
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
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-admin");

        return props;
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

    /**
     * Return an {@link Serde#configure(Map, boolean) configured} instance of {@link #DEFAULT_KEY_SERDE_CLASS_CONFIG key Serde
     * class}.
     *
     * @return an configured instance of key Serde class
     */
    @SuppressWarnings("WeakerAccess")
    public Serde defaultKeySerde() {
        final Object keySerdeConfigSetting = get(DEFAULT_KEY_SERDE_CLASS_CONFIG);
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
    public Serde defaultValueSerde() {
        final Object valueSerdeConfigSetting = get(DEFAULT_VALUE_SERDE_CLASS_CONFIG);
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
        System.out.println(CONFIG.toHtmlTable());
    }
}
