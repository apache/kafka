/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.ConsumerRecordTimestampExtractor;
import org.apache.kafka.streams.processor.DefaultPartitionGrouper;
import org.apache.kafka.streams.processor.internals.StreamPartitionAssignor;
import org.apache.kafka.streams.processor.internals.StreamThread;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

/**
 * Configuration for Kafka Streams. Documentation for these configurations can be found in the <a
 * href="http://kafka.apache.org/documentation.html#streamsconfigs">Kafka documentation</a>
 */
public class StreamsConfig extends AbstractConfig {

    private static final ConfigDef CONFIG;

    // Prefix used to isolate consumer configs from producer configs.
    public static final String CONSUMER_PREFIX = "consumer.";

    // Prefix used to isolate producer configs from consumer configs.
    public static final String PRODUCER_PREFIX = "producer.";

    /** <code>state.dir</code> */
    public static final String STATE_DIR_CONFIG = "state.dir";
    private static final String STATE_DIR_DOC = "Directory location for state store.";

    /** <code>zookeeper.connect<code/> */
    public static final String ZOOKEEPER_CONNECT_CONFIG = "zookeeper.connect";
    private static final String ZOOKEEPER_CONNECT_DOC = "Zookeeper connect string for Kafka topics management.";

    /** <code>commit.interval.ms</code> */
    public static final String COMMIT_INTERVAL_MS_CONFIG = "commit.interval.ms";
    private static final String COMMIT_INTERVAL_MS_DOC = "The frequency with which to save the position of the processor.";

    /** <code>poll.ms</code> */
    public static final String POLL_MS_CONFIG = "poll.ms";
    private static final String POLL_MS_DOC = "The amount of time in milliseconds to block waiting for input.";

    /** <code>num.stream.threads</code> */
    public static final String NUM_STREAM_THREADS_CONFIG = "num.stream.threads";
    private static final String NUM_STREAM_THREADS_DOC = "The number of threads to execute stream processing.";

    /** <code>num.standby.replicas</code> */
    public static final String NUM_STANDBY_REPLICAS_CONFIG = "num.standby.replicas";
    private static final String NUM_STANDBY_REPLICAS_DOC = "The number of standby replicas for each task.";

    /** <code>buffered.records.per.partition</code> */
    public static final String BUFFERED_RECORDS_PER_PARTITION_CONFIG = "buffered.records.per.partition";
    private static final String BUFFERED_RECORDS_PER_PARTITION_DOC = "The maximum number of records to buffer per partition.";

    /** <code>state.cleanup.delay</code> */
    public static final String STATE_CLEANUP_DELAY_MS_CONFIG = "state.cleanup.delay.ms";
    private static final String STATE_CLEANUP_DELAY_MS_DOC = "The amount of time in milliseconds to wait before deleting state when a partition has migrated.";

    /** <code>timestamp.extractor</code> */
    public static final String TIMESTAMP_EXTRACTOR_CLASS_CONFIG = "timestamp.extractor";
    private static final String TIMESTAMP_EXTRACTOR_CLASS_DOC = "Timestamp extractor class that implements the <code>TimestampExtractor</code> interface.";

    /** <code>partition.grouper</code> */
    public static final String PARTITION_GROUPER_CLASS_CONFIG = "partition.grouper";
    private static final String PARTITION_GROUPER_CLASS_DOC = "Partition grouper class that implements the <code>PartitionGrouper</code> interface.";

    /** <code>application.id</code> */
    public static final String APPLICATION_ID_CONFIG = "application.id";
    public static final String APPLICATION_ID_DOC = "An identifier for the stream processing application. Must be unique within the Kafka cluster. It is used as 1) the default client-id prefix, 2) the group-id for membership management, 3) the changelog topic prefix.";

    /** <code>replication.factor</code> */
    public static final String REPLICATION_FACTOR_CONFIG = "replication.factor";
    public static final String REPLICATION_FACTOR_DOC = "The replication factor for change log topics and repartition topics created by the stream processing application.";

    /** <code>key.serde</code> */
    public static final String KEY_SERDE_CLASS_CONFIG = "key.serde";
    public static final String KEY_SERDE_CLASS_DOC = "Serializer / deserializer class for key that implements the <code>Serde</code> interface.";

    /** <code>value.serde</code> */
    public static final String VALUE_SERDE_CLASS_CONFIG = "value.serde";
    public static final String VALUE_SERDE_CLASS_DOC = "Serializer / deserializer class for value that implements the <code>Serde</code> interface.";

    /**<code>user.endpoint</code> */
    public static final String APPLICATION_SERVER_CONFIG = "application.server";
    public static final String APPLICATION_SERVER_DOC = "A host:port pair pointing to an embedded user defined endpoint that can be used for discovering the locations of state stores within a single KafkaStreams application";

    /** <code>metrics.sample.window.ms</code> */
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;

    /** <code>metrics.num.samples</code> */
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;

    /** <code>metric.reporters</code> */
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

    /** <code>bootstrap.servers</code> */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    /** <code>client.id</code> */
    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;

    /** <code>rocksdb.config.setter</code> */
    public static final String ROCKSDB_CONFIG_SETTER_CLASS_CONFIG = "rocksdb.config.setter";
    public static final String ROCKSDB_CONFIG_SETTER_CLASS_DOC = "A Rocks DB config setter class that implements the <code>RocksDBConfigSetter</code> interface";

    /** <code>windowstore.changelog.additional.retention.ms</code> */
    public static final String WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG = "windowstore.changelog.additional.retention.ms";
    public static final String WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_DOC = "Added to a windows maintainMs to ensure data is not deleted from the log prematurely. Allows for clock drift. Default is 1 day";

    /** <code>cache.max.bytes.buffering</code> */
    public static final String CACHE_MAX_BYTES_BUFFERING_CONFIG = "cache.max.bytes.buffering";
    public static final String CACHE_MAX_BYTES_BUFFERING_DOC = "Maximum number of memory bytes to be used for buffering across all threads";

    static {
        CONFIG = new ConfigDef().define(APPLICATION_ID_CONFIG,      // required with no default value
                                        Type.STRING,
                                        Importance.HIGH,
                                        StreamsConfig.APPLICATION_ID_DOC)
                                .define(BOOTSTRAP_SERVERS_CONFIG,       // required with no default value
                                        Type.LIST,
                                        Importance.HIGH,
                                        CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                                .define(CLIENT_ID_CONFIG,
                                        Type.STRING,
                                        "",
                                        Importance.HIGH,
                                        CommonClientConfigs.CLIENT_ID_DOC)
                                .define(ZOOKEEPER_CONNECT_CONFIG,
                                        Type.STRING,
                                        "",
                                        Importance.HIGH,
                                        StreamsConfig.ZOOKEEPER_CONNECT_DOC)
                                .define(STATE_DIR_CONFIG,
                                        Type.STRING,
                                        "/tmp/kafka-streams",
                                        Importance.MEDIUM,
                                        STATE_DIR_DOC)
                                .define(REPLICATION_FACTOR_CONFIG,
                                        Type.INT,
                                        1,
                                        Importance.MEDIUM,
                                        REPLICATION_FACTOR_DOC)
                                .define(TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                                        Type.CLASS,
                                        ConsumerRecordTimestampExtractor.class.getName(),
                                        Importance.MEDIUM,
                                        TIMESTAMP_EXTRACTOR_CLASS_DOC)
                                .define(PARTITION_GROUPER_CLASS_CONFIG,
                                        Type.CLASS,
                                        DefaultPartitionGrouper.class.getName(),
                                        Importance.MEDIUM,
                                        PARTITION_GROUPER_CLASS_DOC)
                                .define(KEY_SERDE_CLASS_CONFIG,
                                        Type.CLASS,
                                        Serdes.ByteArraySerde.class.getName(),
                                        Importance.MEDIUM,
                                        KEY_SERDE_CLASS_DOC)
                                .define(VALUE_SERDE_CLASS_CONFIG,
                                        Type.CLASS,
                                        Serdes.ByteArraySerde.class.getName(),
                                        Importance.MEDIUM,
                                        VALUE_SERDE_CLASS_DOC)
                                .define(COMMIT_INTERVAL_MS_CONFIG,
                                        Type.LONG,
                                        30000,
                                        Importance.LOW,
                                        COMMIT_INTERVAL_MS_DOC)
                                .define(POLL_MS_CONFIG,
                                        Type.LONG,
                                        100,
                                        Importance.LOW,
                                        POLL_MS_DOC)
                                .define(NUM_STREAM_THREADS_CONFIG,
                                        Type.INT,
                                        1,
                                        Importance.LOW,
                                        NUM_STREAM_THREADS_DOC)
                                .define(NUM_STANDBY_REPLICAS_CONFIG,
                                        Type.INT,
                                        0,
                                        Importance.LOW,
                                        NUM_STANDBY_REPLICAS_DOC)
                                .define(BUFFERED_RECORDS_PER_PARTITION_CONFIG,
                                        Type.INT,
                                        1000,
                                        Importance.LOW,
                                        BUFFERED_RECORDS_PER_PARTITION_DOC)
                                .define(STATE_CLEANUP_DELAY_MS_CONFIG,
                                        Type.LONG,
                                        60000,
                                        Importance.LOW,
                                        STATE_CLEANUP_DELAY_MS_DOC)
                                .define(METRIC_REPORTER_CLASSES_CONFIG,
                                        Type.LIST,
                                        "",
                                        Importance.LOW,
                                        CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
                                .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
                                        Type.LONG,
                                        30000,
                                        atLeast(0),
                                        Importance.LOW,
                                        CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
                                .define(METRICS_NUM_SAMPLES_CONFIG,
                                        Type.INT,
                                        2,
                                        atLeast(1),
                                        Importance.LOW,
                                        CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
                                .define(APPLICATION_SERVER_CONFIG,
                                        Type.STRING,
                                        "",
                                        Importance.LOW,
                                        APPLICATION_SERVER_DOC)
                                .define(ROCKSDB_CONFIG_SETTER_CLASS_CONFIG,
                                        Type.CLASS,
                                        null,
                                        Importance.LOW,
                                        ROCKSDB_CONFIG_SETTER_CLASS_DOC)
                                .define(WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG,
                                        Type.LONG,
                                        24 * 60 * 60 * 1000,
                                        Importance.MEDIUM,
                                        WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_DOC)
                                .define(CACHE_MAX_BYTES_BUFFERING_CONFIG,
                                        Type.LONG,
                                        10 * 1024 * 1024L,
                                        atLeast(0),
                                        Importance.LOW,
                                        CACHE_MAX_BYTES_BUFFERING_DOC);
    }

    // this is the list of configs for underlying clients
    // that streams prefer different default values
    private static final Map<String, Object> PRODUCER_DEFAULT_OVERRIDES;
    static
    {
        Map<String, Object> tempProducerDefaultOverrides = new HashMap<>();
        tempProducerDefaultOverrides.put(ProducerConfig.LINGER_MS_CONFIG, "100");

        PRODUCER_DEFAULT_OVERRIDES = Collections.unmodifiableMap(tempProducerDefaultOverrides);
    }

    private static final Map<String, Object> CONSUMER_DEFAULT_OVERRIDES;
    static
    {
        Map<String, Object> tempConsumerDefaultOverrides = new HashMap<>();
        tempConsumerDefaultOverrides.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        tempConsumerDefaultOverrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        tempConsumerDefaultOverrides.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        CONSUMER_DEFAULT_OVERRIDES = Collections.unmodifiableMap(tempConsumerDefaultOverrides);
    }

    public static class InternalConfig {
        public static final String STREAM_THREAD_INSTANCE = "__stream.thread.instance__";
    }

    /**
     * Prefix a property with {@link StreamsConfig#CONSUMER_PREFIX}. This is used to isolate consumer configs
     * from producer configs
     * @param consumerProp
     * @return CONSUMER_PREFIX + consumerProp
     */
    public static String consumerPrefix(final String consumerProp) {
        return CONSUMER_PREFIX + consumerProp;
    }

    /**
     * Prefix a property with {@link StreamsConfig#PRODUCER_PREFIX}. This is used to isolate producer configs
     * from consumer configs
     * @param producerProp
     * @return PRODUCER_PREFIX + consumerProp
     */
    public static String producerPrefix(final String producerProp) {
        return PRODUCER_PREFIX + producerProp;
    }

    public StreamsConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

    /**
     * Get the configs specific to the Consumer. Properties using the prefix {@link StreamsConfig#CONSUMER_PREFIX}
     * will be used in favor over their non-prefixed versions except in the case of {@link ConsumerConfig#BOOTSTRAP_SERVERS_CONFIG}
     * where we always use the non-prefixed version as we only support reading/writing from/to the same Kafka Cluster
     * @param streamThread   the {@link StreamThread} creating a consumer
     * @param groupId        consumer groupId
     * @param clientId       clientId
     * @return  Map of the Consumer configuration.
     * @throws ConfigException
     */
    public Map<String, Object> getConsumerConfigs(StreamThread streamThread, String groupId, String clientId) throws ConfigException {
        final Map<String, Object> consumerProps = getClientPropsWithPrefix(CONSUMER_PREFIX, ConsumerConfig.configNames());

        // disable auto commit and throw exception if there is user overridden values,
        // this is necessary for streams commit semantics
        if (consumerProps.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            throw new ConfigException("Unexpected user-specified consumer config " + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG
                    + ", as the streams client will always turn off auto committing.");
        }

        consumerProps.putAll(CONSUMER_DEFAULT_OVERRIDES);

        // bootstrap.servers should be from StreamsConfig
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.originals().get(BOOTSTRAP_SERVERS_CONFIG));
        // add client id with stream client id prefix, and group id
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-consumer");

        // add configs required for stream partition assignor
        consumerProps.put(StreamsConfig.InternalConfig.STREAM_THREAD_INSTANCE, streamThread);
        consumerProps.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, getInt(REPLICATION_FACTOR_CONFIG));
        consumerProps.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, getInt(NUM_STANDBY_REPLICAS_CONFIG));
        consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StreamPartitionAssignor.class.getName());
        consumerProps.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, getLong(WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG));
        if (!getString(ZOOKEEPER_CONNECT_CONFIG).equals("")) {
            consumerProps.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, getString(ZOOKEEPER_CONNECT_CONFIG));
        }

        consumerProps.put(APPLICATION_SERVER_CONFIG, getString(APPLICATION_SERVER_CONFIG));
        return consumerProps;
    }


    /**
     * Get the consumer config for the restore-consumer. Properties using the prefix {@link StreamsConfig#CONSUMER_PREFIX}
     * will be used in favor over their non-prefixed versions except in the case of {@link ConsumerConfig#BOOTSTRAP_SERVERS_CONFIG}
     * where we always use the non-prefixed version as we only support reading/writing from/to the same Kafka Cluster
     * @param clientId  clientId
     * @return  Map of the Consumer configuration
     * @throws ConfigException
     */
    public Map<String, Object> getRestoreConsumerConfigs(String clientId) throws ConfigException {
        Map<String, Object> consumerProps = getClientPropsWithPrefix(CONSUMER_PREFIX, ConsumerConfig.configNames());

        // disable auto commit and throw exception if there is user overridden values,
        // this is necessary for streams commit semantics
        if (consumerProps.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            throw new ConfigException("Unexpected user-specified consumer config " + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG
                    + ", as the streams client will always turn off auto committing.");
        }

        consumerProps.putAll(CONSUMER_DEFAULT_OVERRIDES);

        // bootstrap.servers should be from StreamsConfig
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.originals().get(BOOTSTRAP_SERVERS_CONFIG));

        // no need to set group id for a restore consumer
        consumerProps.remove(ConsumerConfig.GROUP_ID_CONFIG);

        // add client id with stream client id prefix
        consumerProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-restore-consumer");

        return consumerProps;
    }


    /**
     * Get the configs for the Producer. Properties using the prefix {@link StreamsConfig#PRODUCER_PREFIX}
     * will be used in favor over their non-prefixed versions except in the case of {@link ProducerConfig#BOOTSTRAP_SERVERS_CONFIG}
     * where we always use the non-prefixed version as we only support reading/writing from/to the same Kafka Cluster
     * @param clientId  clientId
     * @return  Map of the Consumer configuration
     * @throws ConfigException
     */
    public Map<String, Object> getProducerConfigs(String clientId) {
        // generate producer configs from original properties and overridden maps
        final Map<String, Object> props = getClientPropsWithPrefix(PRODUCER_PREFIX, ProducerConfig.configNames());
        props.putAll(PRODUCER_DEFAULT_OVERRIDES);

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.originals().get(BOOTSTRAP_SERVERS_CONFIG));
        // add client id with stream client id prefix
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-producer");

        return props;
    }

    private Map<String, Object> getClientPropsWithPrefix(final String prefix, final Set<String> configNames) {
        final Map<String, Object> props = clientProps(configNames, originals());
        props.putAll(this.originalsWithPrefix(prefix));
        return props;
    }

    public Serde keySerde() {
        try {
            Serde<?> serde = getConfiguredInstance(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serde.class);
            serde.configure(originals(), true);
            return serde;
        } catch (Exception e) {
            throw new StreamsException(String.format("Failed to configure key serde %s", get(StreamsConfig.KEY_SERDE_CLASS_CONFIG)), e);
        }
    }

    public Serde valueSerde() {
        try {
            Serde<?> serde = getConfiguredInstance(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serde.class);
            serde.configure(originals(), false);
            return serde;
        } catch (Exception e) {
            throw new StreamsException(String.format("Failed to configure value serde %s", get(StreamsConfig.VALUE_SERDE_CLASS_CONFIG)), e);
        }
    }

    /**
     * Override any client properties in the original configs with overrides
     *
     * @param configNames The given set of configuration names.
     * @param originals The original configs to be filtered.
     * @return client config with any overrides
     */
    private Map<String, Object> clientProps(Set<String> configNames, Map<String, Object> originals) {
        // iterate all client config names, filter out non-client configs from the original
        // property map and use the overridden values when they are not specified by users
        Map<String, Object> parsed = new HashMap<>();
        for (String configName: configNames) {
            if (originals.containsKey(configName)) {
                parsed.put(configName, originals.get(configName));
            }
        }

        return parsed;
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtmlTable());
    }
}
