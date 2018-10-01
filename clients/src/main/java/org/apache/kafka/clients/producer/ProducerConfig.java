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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * Configuration for the Kafka Producer. Documentation for these configurations can be found in the <a
 * href="http://kafka.apache.org/documentation.html#producerconfigs">Kafka documentation</a>
 */
public class ProducerConfig extends AbstractConfig {

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG STRINGS OR THEIR JAVA VARIABLE NAMES AS THESE ARE PART OF THE PUBLIC API AND
     * CHANGE WILL BREAK USER CODE.
     */

    private static final ConfigDef CONFIG;

    /** <code>bootstrap.servers</code> */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    /** <code>metadata.max.age.ms</code> */
    public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;
    private static final String METADATA_MAX_AGE_DOC = CommonClientConfigs.METADATA_MAX_AGE_DOC;

    /** <code>batch.size</code> */
    public static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_DOC = "The producer will attempt to batch records together into fewer requests whenever multiple records are being sent"
                                                 + " to the same partition. This helps performance on both the client and the server. This configuration controls the "
                                                 + "default batch size in bytes. "
                                                 + "<p>"
                                                 + "No attempt will be made to batch records larger than this size. "
                                                 + "<p>"
                                                 + "Requests sent to brokers will contain multiple batches, one for each partition with data available to be sent. "
                                                 + "<p>"
                                                 + "A small batch size will make batching less common and may reduce throughput (a batch size of zero will disable "
                                                 + "batching entirely). A very large batch size may use memory a bit more wastefully as we will always allocate a "
                                                 + "buffer of the specified batch size in anticipation of additional records.";

    /** <code>acks</code> */
    public static final String ACKS_CONFIG = "acks";
    private static final String ACKS_DOC = "The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the "
                                           + " durability of records that are sent. The following settings are allowed: "
                                           + " <ul>"
                                           + " <li><code>acks=0</code> If set to zero then the producer will not wait for any acknowledgment from the"
                                           + " server at all. The record will be immediately added to the socket buffer and considered sent. No guarantee can be"
                                           + " made that the server has received the record in this case, and the <code>retries</code> configuration will not"
                                           + " take effect (as the client won't generally know of any failures). The offset given back for each record will"
                                           + " always be set to -1."
                                           + " <li><code>acks=1</code> This will mean the leader will write the record to its local log but will respond"
                                           + " without awaiting full acknowledgement from all followers. In this case should the leader fail immediately after"
                                           + " acknowledging the record but before the followers have replicated it then the record will be lost."
                                           + " <li><code>acks=all</code> This means the leader will wait for the full set of in-sync replicas to"
                                           + " acknowledge the record. This guarantees that the record will not be lost as long as at least one in-sync replica"
                                           + " remains alive. This is the strongest available guarantee. This is equivalent to the acks=-1 setting.";

    /** <code>linger.ms</code> */
    public static final String LINGER_MS_CONFIG = "linger.ms";
    private static final String LINGER_MS_DOC = "The producer groups together any records that arrive in between request transmissions into a single batched request. "
                                                + "Normally this occurs only under load when records arrive faster than they can be sent out. However in some circumstances the client may want to "
                                                + "reduce the number of requests even under moderate load. This setting accomplishes this by adding a small amount "
                                                + "of artificial delay&mdash;that is, rather than immediately sending out a record the producer will wait for up to "
                                                + "the given delay to allow other records to be sent so that the sends can be batched together. This can be thought "
                                                + "of as analogous to Nagle's algorithm in TCP. This setting gives the upper bound on the delay for batching: once "
                                                + "we get <code>" + BATCH_SIZE_CONFIG + "</code> worth of records for a partition it will be sent immediately regardless of this "
                                                + "setting, however if we have fewer than this many bytes accumulated for this partition we will 'linger' for the "
                                                + "specified time waiting for more records to show up. This setting defaults to 0 (i.e. no delay). Setting <code>" + LINGER_MS_CONFIG + "=5</code>, "
                                                + "for example, would have the effect of reducing the number of requests sent but would add up to 5ms of latency to records sent in the absence of load.";

    /** <code>request.timeout.ms</code> */
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    private static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC
        + " This should be larger than <code>replica.lag.time.max.ms</code> (a broker configuration)"
        + " to reduce the possibility of message duplication due to unnecessary producer retries.";

    /** <code>delivery.timeout.ms</code> */
    public static final String DELIVERY_TIMEOUT_MS_CONFIG = "delivery.timeout.ms";
    private static final String DELIVERY_TIMEOUT_MS_DOC = "An upper bound on the time to report success or failure "
            + "after a call to <code>send()</code> returns. This limits the total time that a record will be delayed "
            + "prior to sending, the time to await acknowledgement from the broker (if expected), and the time allowed "
            + "for retriable send failures. The producer may report failure to send a record earlier than this config if "
            + "either an unrecoverable error is encountered, the retries have been exhausted, "
            + "or the record is added to a batch which reached an earlier delivery expiration deadline. "
            + "The value of this config should be greater than or equal to the sum of <code>" + REQUEST_TIMEOUT_MS_CONFIG + "</code> "
            + "and <code>" + LINGER_MS_CONFIG + "</code>.";

    /** <code>client.id</code> */
    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;

    /** <code>send.buffer.bytes</code> */
    public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;

    /** <code>receive.buffer.bytes</code> */
    public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;

    /** <code>max.request.size</code> */
    public static final String MAX_REQUEST_SIZE_CONFIG = "max.request.size";
    private static final String MAX_REQUEST_SIZE_DOC = "The maximum size of a request in bytes. This setting will limit the number of record "
                                                       + "batches the producer will send in a single request to avoid sending huge requests. "
                                                       + "This is also effectively a cap on the maximum record batch size. Note that the server "
                                                       + "has its own cap on record batch size which may be different from this.";

    /** <code>reconnect.backoff.ms</code> */
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

    /** <code>reconnect.backoff.max.ms</code> */
    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG;

    /** <code>max.block.ms</code> */
    public static final String MAX_BLOCK_MS_CONFIG = "max.block.ms";
    private static final String MAX_BLOCK_MS_DOC = "The configuration controls how long <code>KafkaProducer.send()</code> and <code>KafkaProducer.partitionsFor()</code> will block."
                                                    + "These methods can be blocked either because the buffer is full or metadata unavailable."
                                                    + "Blocking in the user-supplied serializers or partitioner will not be counted against this timeout.";

    /** <code>buffer.memory</code> */
    public static final String BUFFER_MEMORY_CONFIG = "buffer.memory";
    private static final String BUFFER_MEMORY_DOC = "The total bytes of memory the producer can use to buffer records waiting to be sent to the server. If records are "
                                                    + "sent faster than they can be delivered to the server the producer will block for <code>" + MAX_BLOCK_MS_CONFIG + "</code> after which it will throw an exception."
                                                    + "<p>"
                                                    + "This setting should correspond roughly to the total memory the producer will use, but is not a hard bound since "
                                                    + "not all memory the producer uses is used for buffering. Some additional memory will be used for compression (if "
                                                    + "compression is enabled) as well as for maintaining in-flight requests.";

    /** <code>retry.backoff.ms</code> */
    public static final String RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;

    /** <code>compression.type</code> */
    public static final String COMPRESSION_TYPE_CONFIG = "compression.type";
    private static final String COMPRESSION_TYPE_DOC = "The compression type for all data generated by the producer. The default is none (i.e. no compression). Valid "
                                                       + " values are <code>none</code>, <code>gzip</code>, <code>snappy</code>, or <code>lz4</code>. "
                                                       + "Compression is of full batches of data, so the efficacy of batching will also impact the compression ratio (more batching means better compression).";

    /** <code>metrics.sample.window.ms</code> */
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;

    /** <code>metrics.num.samples</code> */
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;

    /**
     * <code>metrics.recording.level</code>
     */
    public static final String METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;

    /** <code>metric.reporters</code> */
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

    /** <code>max.in.flight.requests.per.connection</code> */
    public static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "max.in.flight.requests.per.connection";
    private static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DOC = "The maximum number of unacknowledged requests the client will send on a single connection before blocking."
                                                                            + " Note that if this setting is set to be greater than 1 and there are failed sends, there is a risk of"
                                                                            + " message re-ordering due to retries (i.e., if retries are enabled).";

    /** <code>retries</code> */
    public static final String RETRIES_CONFIG = CommonClientConfigs.RETRIES_CONFIG;
    private static final String RETRIES_DOC = "Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient error."
            + " Note that this retry is no different than if the client resent the record upon receiving the error."
            + " Allowing retries without setting <code>" + MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + "</code> to 1 will potentially change the"
            + " ordering of records because if two batches are sent to a single partition, and the first fails and is retried but the second"
            + " succeeds, then the records in the second batch may appear first. Note additionall that produce requests will be"
            + " failed before the number of retries has been exhausted if the timeout configured by"
            + " <code>" + DELIVERY_TIMEOUT_MS_CONFIG + "</code> expires first before successful acknowledgement. Users should generally"
            + " prefer to leave this config unset and instead use <code>" + DELIVERY_TIMEOUT_MS_CONFIG + "</code> to control"
            + " retry behavior.";

    /** <code>key.serializer</code> */
    public static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
    public static final String KEY_SERIALIZER_CLASS_DOC = "Serializer class for key that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.";

    /** <code>value.serializer</code> */
    public static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";
    public static final String VALUE_SERIALIZER_CLASS_DOC = "Serializer class for value that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.";

    /** <code>connections.max.idle.ms</code> */
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

    /** <code>partitioner.class</code> */
    public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
    private static final String PARTITIONER_CLASS_DOC = "Partitioner class that implements the <code>org.apache.kafka.clients.producer.Partitioner</code> interface.";

    /** <code>interceptor.classes</code> */
    public static final String INTERCEPTOR_CLASSES_CONFIG = "interceptor.classes";
    public static final String INTERCEPTOR_CLASSES_DOC = "A list of classes to use as interceptors. "
                                                        + "Implementing the <code>org.apache.kafka.clients.producer.ProducerInterceptor</code> interface allows you to intercept (and possibly mutate) the records "
                                                        + "received by the producer before they are published to the Kafka cluster. By default, there are no interceptors.";

    /** <code>enable.idempotence</code> */
    public static final String ENABLE_IDEMPOTENCE_CONFIG = "enable.idempotence";
    public static final String ENABLE_IDEMPOTENCE_DOC = "When set to 'true', the producer will ensure that exactly one copy of each message is written in the stream. If 'false', producer "
                                                        + "retries due to broker failures, etc., may write duplicates of the retried message in the stream. "
                                                        + "Note that enabling idempotence requires <code>" + MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + "</code> to be less than or equal to 5, "
                                                        + "<code>" + RETRIES_CONFIG + "</code> to be greater than 0 and <code>" + ACKS_CONFIG + "</code> must be 'all'. If these values "
                                                        + "are not explicitly set by the user, suitable values will be chosen. If incompatible values are set, "
                                                        + "a <code>ConfigException</code> will be thrown.";

    /** <code> transaction.timeout.ms </code> */
    public static final String TRANSACTION_TIMEOUT_CONFIG = "transaction.timeout.ms";
    public static final String TRANSACTION_TIMEOUT_DOC = "The maximum amount of time in ms that the transaction coordinator will wait for a transaction status update from the producer before proactively aborting the ongoing transaction." +
            "If this value is larger than the transaction.max.timeout.ms setting in the broker, the request will fail with a <code>InvalidTransactionTimeout</code> error.";

    /** <code> transactional.id </code> */
    public static final String TRANSACTIONAL_ID_CONFIG = "transactional.id";
    public static final String TRANSACTIONAL_ID_DOC = "The TransactionalId to use for transactional delivery. This enables reliability semantics which span multiple producer sessions since it allows the client to guarantee that transactions using the same TransactionalId have been completed prior to starting any new transactions. If no TransactionalId is provided, then the producer is limited to idempotent delivery. " +
            "Note that <code>enable.idempotence</code> must be enabled if a TransactionalId is configured. " +
            "The default is <code>null</code>, which means transactions cannot be used. " +
            "Note that, by default, transactions require a cluster of at least three brokers which is the recommended setting for production; for development you can change this, by adjusting broker setting <code>transaction.state.log.replication.factor</code>.";

    static {
        CONFIG = new ConfigDef().define(BOOTSTRAP_SERVERS_CONFIG, Type.LIST, Collections.emptyList(), new ConfigDef.NonNullValidator(), Importance.HIGH, CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                                .define(BUFFER_MEMORY_CONFIG, Type.LONG, 32 * 1024 * 1024L, atLeast(0L), Importance.HIGH, BUFFER_MEMORY_DOC)
                                .define(RETRIES_CONFIG, Type.INT, Integer.MAX_VALUE, between(0, Integer.MAX_VALUE), Importance.HIGH, RETRIES_DOC)
                                .define(ACKS_CONFIG,
                                        Type.STRING,
                                        "1",
                                        in("all", "-1", "0", "1"),
                                        Importance.HIGH,
                                        ACKS_DOC)
                                .define(COMPRESSION_TYPE_CONFIG, Type.STRING, "none", Importance.HIGH, COMPRESSION_TYPE_DOC)
                                .define(BATCH_SIZE_CONFIG, Type.INT, 16384, atLeast(0), Importance.MEDIUM, BATCH_SIZE_DOC)
                                .define(LINGER_MS_CONFIG, Type.INT, 0, atLeast(0), Importance.MEDIUM, LINGER_MS_DOC)
                                .define(DELIVERY_TIMEOUT_MS_CONFIG, Type.INT, 120 * 1000, atLeast(0), Importance.MEDIUM, DELIVERY_TIMEOUT_MS_DOC)
                                .define(CLIENT_ID_CONFIG, Type.STRING, "", Importance.MEDIUM, CommonClientConfigs.CLIENT_ID_DOC)
                                .define(SEND_BUFFER_CONFIG, Type.INT, 128 * 1024, atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND), Importance.MEDIUM, CommonClientConfigs.SEND_BUFFER_DOC)
                                .define(RECEIVE_BUFFER_CONFIG, Type.INT, 32 * 1024, atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND), Importance.MEDIUM, CommonClientConfigs.RECEIVE_BUFFER_DOC)
                                .define(MAX_REQUEST_SIZE_CONFIG,
                                        Type.INT,
                                        1 * 1024 * 1024,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        MAX_REQUEST_SIZE_DOC)
                                .define(RECONNECT_BACKOFF_MS_CONFIG, Type.LONG, 50L, atLeast(0L), Importance.LOW, CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
                                .define(RECONNECT_BACKOFF_MAX_MS_CONFIG, Type.LONG, 1000L, atLeast(0L), Importance.LOW, CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
                                .define(RETRY_BACKOFF_MS_CONFIG, Type.LONG, 100L, atLeast(0L), Importance.LOW, CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
                                .define(MAX_BLOCK_MS_CONFIG,
                                        Type.LONG,
                                        60 * 1000,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        MAX_BLOCK_MS_DOC)
                                .define(REQUEST_TIMEOUT_MS_CONFIG,
                                        Type.INT,
                                        30 * 1000,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        REQUEST_TIMEOUT_MS_DOC)
                                .define(METADATA_MAX_AGE_CONFIG, Type.LONG, 5 * 60 * 1000, atLeast(0), Importance.LOW, METADATA_MAX_AGE_DOC)
                                .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
                                        Type.LONG,
                                        30000,
                                        atLeast(0),
                                        Importance.LOW,
                                        CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
                                .define(METRICS_NUM_SAMPLES_CONFIG, Type.INT, 2, atLeast(1), Importance.LOW, CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
                                .define(METRICS_RECORDING_LEVEL_CONFIG,
                                        Type.STRING,
                                        Sensor.RecordingLevel.INFO.toString(),
                                        in(Sensor.RecordingLevel.INFO.toString(), Sensor.RecordingLevel.DEBUG.toString()),
                                        Importance.LOW,
                                        CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC)
                                .define(METRIC_REPORTER_CLASSES_CONFIG,
                                        Type.LIST,
                                        Collections.emptyList(),
                                        new ConfigDef.NonNullValidator(),
                                        Importance.LOW,
                                        CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
                                .define(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                                        Type.INT,
                                        5,
                                        atLeast(1),
                                        Importance.LOW,
                                        MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DOC)
                                .define(KEY_SERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        KEY_SERIALIZER_CLASS_DOC)
                                .define(VALUE_SERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        VALUE_SERIALIZER_CLASS_DOC)
                                /* default is set to be a bit lower than the server default (10 min), to avoid both client and server closing connection at same time */
                                .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                                        Type.LONG,
                                        9 * 60 * 1000,
                                        Importance.MEDIUM,
                                        CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
                                .define(PARTITIONER_CLASS_CONFIG,
                                        Type.CLASS,
                                        DefaultPartitioner.class,
                                        Importance.MEDIUM, PARTITIONER_CLASS_DOC)
                                .define(INTERCEPTOR_CLASSES_CONFIG,
                                        Type.LIST,
                                        Collections.emptyList(),
                                        new ConfigDef.NonNullValidator(),
                                        Importance.LOW,
                                        INTERCEPTOR_CLASSES_DOC)
                                .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                        Type.STRING,
                                        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                                        Importance.MEDIUM,
                                        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                                .withClientSslSupport()
                                .withClientSaslSupport()
                                .define(ENABLE_IDEMPOTENCE_CONFIG,
                                        Type.BOOLEAN,
                                        false,
                                        Importance.LOW,
                                        ENABLE_IDEMPOTENCE_DOC)
                                .define(TRANSACTION_TIMEOUT_CONFIG,
                                        Type.INT,
                                        60000,
                                        Importance.LOW,
                                        TRANSACTION_TIMEOUT_DOC)
                                .define(TRANSACTIONAL_ID_CONFIG,
                                        Type.STRING,
                                        null,
                                        new ConfigDef.NonEmptyString(),
                                        Importance.LOW,
                                        TRANSACTIONAL_ID_DOC);
    }

    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        return CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);
    }

    public static Map<String, Object> addSerializerToConfig(Map<String, Object> configs,
                                                            Serializer<?> keySerializer, Serializer<?> valueSerializer) {
        Map<String, Object> newConfigs = new HashMap<>(configs);
        if (keySerializer != null)
            newConfigs.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass());
        if (valueSerializer != null)
            newConfigs.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass());
        return newConfigs;
    }

    public static Properties addSerializerToConfig(Properties properties,
                                                   Serializer<?> keySerializer,
                                                   Serializer<?> valueSerializer) {
        Properties newProperties = new Properties();
        newProperties.putAll(properties);
        if (keySerializer != null)
            newProperties.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass().getName());
        if (valueSerializer != null)
            newProperties.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass().getName());
        return newProperties;
    }

    public ProducerConfig(Properties props) {
        super(CONFIG, props);
    }

    public ProducerConfig(Map<String, Object> props) {
        super(CONFIG, props);
    }

    ProducerConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtmlTable());
    }

    public static ProducerConfigBuilder builder() {
        return new ProducerConfigBuilder();
    }

    /**
     * Builder for {@link ProducerConfig}
     */
    public static class ProducerConfigBuilder {

        private final Map<String, Object> props = new HashMap<String, Object>();

        private ProducerConfigBuilder() { }

        /**
         * @see ProducerConfig#BOOTSTRAP_SERVERS_CONFIG
         *
         * @param bootstrapServer boot strap server (if provided multiple times, will be overwritten each time)
         * @return the builder
         */
        ProducerConfigBuilder bootstrapServer(final String bootstrapServer) {
            props.put(BOOTSTRAP_SERVERS_CONFIG, Collections.singletonList(bootstrapServer));
            return this;
        }

        /**
         * @see ProducerConfig#BOOTSTRAP_SERVERS_CONFIG
         *
         * @param bootstrapServers boot strap servers configuration
         * @return the builder
         */
        public ProducerConfigBuilder bootstrapServers(final List<String> bootstrapServers) {
            props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            return this;
        }

        /**
         * @see ProducerConfig#METADATA_MAX_AGE_CONFIG
         *
         * @param metadataMaxAge metadata max age (ms)
         * @return the builder
         */
        public ProducerConfigBuilder metadataMaxAge(final long metadataMaxAge) {
            props.put(METADATA_MAX_AGE_CONFIG, metadataMaxAge);
            return this;
        }

        /**
         * @see ProducerConfig#BATCH_SIZE_CONFIG
         *
         * @param batchSize batch size
         * @return the builder
         */
        public ProducerConfigBuilder batchSize(final int batchSize) {
            props.put(BATCH_SIZE_CONFIG, batchSize);
            return this;
        }

        /**
         * @see ProducerConfig#ACKS_CONFIG
         *
         * @param acks the number of acks
         * @return the builder
         */
        public ProducerConfigBuilder acks(final String acks) {
            props.put(ACKS_CONFIG, acks);
            return this;
        }

        /**
         * @see ProducerConfig#LINGER_MS_CONFIG
         *
         * @param linger how long to linger (ms)
         * @return the builder
         */
        public ProducerConfigBuilder linger(final int linger) {
            props.put(LINGER_MS_CONFIG, linger);
            return this;
        }

        /**
         * @see ProducerConfig#REQUEST_TIMEOUT_MS_CONFIG
         *
         * @param requestTimeout request timeout (ms)
         * @return the builder
         */
        public ProducerConfigBuilder requestTimeout(final int requestTimeout) {
            props.put(REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
            return this;
        }

        /**
         * @see ProducerConfig#DELIVERY_TIMEOUT_MS_CONFIG
         *
         * @param deliveryTimeout delivery timeout (ms)
         * @return the builder
         */
        public ProducerConfigBuilder deliveryTimeout(final int deliveryTimeout) {
            props.put(DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeout);
            return this;
        }

        /**
         * @see ProducerConfig#CLIENT_ID_CONFIG
         *
         * @param clientId client ID
         * @return the builder
         */
        public ProducerConfigBuilder clientId(final String clientId) {
            props.put(CLIENT_ID_CONFIG, clientId);
            return this;
        }

        /**
         * @see ProducerConfig#SEND_BUFFER_CONFIG
         *
         * @param sendBuffer number of bytes
         * @return the builder
         */
        public ProducerConfigBuilder sendBuffer(final int sendBuffer) {
            props.put(SEND_BUFFER_CONFIG, sendBuffer);
            return this;
        }

        /**
         * @see ProducerConfig#RECEIVE_BUFFER_CONFIG
         *
         * @param receiveBuffer number of bytes
         * @return the builder
         */
        public ProducerConfigBuilder receiveBuffer(final int receiveBuffer) {
            props.put(RECEIVE_BUFFER_CONFIG, receiveBuffer);
            return this;
        }

        /**
         * @see ProducerConfig#MAX_REQUEST_SIZE_CONFIG
         *
         * @param maxRequestSize maximum request size in bytes
         * @return the builder
         */
        public ProducerConfigBuilder maxRequestSize(final int maxRequestSize) {
            props.put(MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
            return this;
        }

        /**
         * @see ProducerConfig#RECONNECT_BACKOFF_MS_CONFIG
         *
         * @param reconnectBackoff reconnect backoff (ms)
         * @return the builder
         */
        public ProducerConfigBuilder reconnectBackoff(final long reconnectBackoff) {
            props.put(RECONNECT_BACKOFF_MS_CONFIG, reconnectBackoff);
            return this;
        }

        /**
         * @see ProducerConfig#RECONNECT_BACKOFF_MAX_MS_CONFIG
         *
         * @param reconnectBackoffMax reconnect backoff maximum (ms)
         * @return the builder
         */
        public ProducerConfigBuilder reconnectBackoffMax(final long reconnectBackoffMax) {
            props.put(RECONNECT_BACKOFF_MAX_MS_CONFIG, reconnectBackoffMax);
            return this;
        }

        /**
         * @see ProducerConfig#MAX_BLOCK_MS_CONFIG
         *
         * @param maxBlock maximum blocking time (ms)
         * @return the builder
         */
        public ProducerConfigBuilder maxBlock(final long maxBlock) {
            props.put(MAX_BLOCK_MS_CONFIG, maxBlock);
            return this;
        }

        /**
         * @see ProducerConfig#BUFFER_MEMORY_CONFIG
         *
         * @param bufferMemory buffer memory in bytes
         * @return the builder
         */
        public ProducerConfigBuilder bufferMemory(final long bufferMemory) {
            props.put(BUFFER_MEMORY_CONFIG, bufferMemory);
            return this;
        }

        /**
         * @see ProducerConfig#RETRY_BACKOFF_MS_CONFIG
         *
         * @param retryBackoff retry backoff (ms)
         * @return the builder
         */
        public ProducerConfigBuilder retryBackoff(final long retryBackoff) {
            props.put(RETRY_BACKOFF_MS_CONFIG, retryBackoff);
            return this;
        }

        /**
         * @see ProducerConfig#COMPRESSION_TYPE_CONFIG
         *
         * @param compressionType the compression type
         * @return the builder
         */
        public ProducerConfigBuilder compressionType(final String compressionType) {
            props.put(COMPRESSION_TYPE_CONFIG, compressionType);
            return this;
        }

        /**
         * @see ProducerConfig#METRICS_SAMPLE_WINDOW_MS_CONFIG
         *
         * @param metricsSampleWindow metrics sample window (ms)
         * @return the builder
         */
        public ProducerConfigBuilder metricsSampleWindow(final long metricsSampleWindow) {
            props.put(METRICS_SAMPLE_WINDOW_MS_CONFIG, metricsSampleWindow);
            return this;
        }

        /**
         * @see ProducerConfig#METRICS_NUM_SAMPLES_CONFIG
         *
         * @param metricsNumSamples metrics numbner of sample
         * @return the builder
         */
        public ProducerConfigBuilder metricsNumSamples(final int metricsNumSamples) {
            props.put(METRICS_NUM_SAMPLES_CONFIG, metricsNumSamples);
            return this;
        }

        /**
         * @see ProducerConfig#METRICS_RECORDING_LEVEL_CONFIG
         *
         * @param metricsRecordingLevel the metrics recording level
         * @return the builder
         */
        public ProducerConfigBuilder metricsRecordingLevel(final String metricsRecordingLevel) {
            props.put(METRICS_RECORDING_LEVEL_CONFIG, metricsRecordingLevel);
            return this;
        }

        /**
         * @see ProducerConfig#METRIC_REPORTER_CLASSES_CONFIG
         *
         * @param metricsReporterClasses the metrics reporter classes (nulls ignored)
         * @return the builder
         */
        public ProducerConfigBuilder metricsReporterClasses(final List<Class<?>> metricsReporterClasses) {
            final List<String> metricReporterClassList = metricsReporterClasses.stream()
                    .filter(Objects::nonNull)
                    .map(Class::getName)
                    .collect(Collectors.toList());
            props.put(METRIC_REPORTER_CLASSES_CONFIG, metricReporterClassList);
            return this;
        }

        /**
         * @see ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
         *
         * @param maxInFlightRequestsPerConnection maximum number of in-flight requests per connection
         * @return the builder
         */
        public ProducerConfigBuilder maxInFlightRequestsPerConnection(final int maxInFlightRequestsPerConnection) {
            props.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequestsPerConnection);
            return this;
        }

        /**
         * @see ProducerConfig#RETRIES_CONFIG
         *
         * @param retries number of retries
         * @return the builder
         */
        public ProducerConfigBuilder retries(final int retries) {
            props.put(RETRIES_CONFIG, retries);
            return this;
        }

        /**
         * @see ProducerConfig#KEY_SERIALIZER_CLASS_CONFIG
         *
         * @param keySerializerClass the key serializer class
         * @return the builder
         */
        public ProducerConfigBuilder keySerializerClass(final Class<? extends Serializer> keySerializerClass) {
            props.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getName());
            return this;
        }

        /**
         * @see ProducerConfig#VALUE_SERIALIZER_CLASS_CONFIG
         *
         * @param valueSerializerClass the value serializer class
         * @return the builder
         */
        public ProducerConfigBuilder valueSerializerClass(final Class<? extends Serializer> valueSerializerClass) {
            props.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getName());
            return this;
        }

        /**
         * @see ProducerConfig#CONNECTIONS_MAX_IDLE_MS_CONFIG
         *
         * @param connectionsMaxIdle connections max idle (ms)
         * @return the builder
         */
        public ProducerConfigBuilder connectionsMaxIdle(final long connectionsMaxIdle) {
            props.put(CONNECTIONS_MAX_IDLE_MS_CONFIG, connectionsMaxIdle);
            return this;
        }

        /**
         * @see ProducerConfig#PARTITIONER_CLASS_CONFIG
         *
         * @param partitionerClass the partitioner class
         * @return the builder
         */
        public ProducerConfigBuilder partitionerClass(final Class<?> partitionerClass) {
            props.put(PARTITIONER_CLASS_CONFIG, partitionerClass.getName());
            return this;
        }

        /**
         * @see ProducerConfig#INTERCEPTOR_CLASSES_CONFIG
         *
         * @param interceptorClasses the interceptor classes (nulls ignored)
         * @return the builder
         */
        public ProducerConfigBuilder interceptorClasses(final List<Class<?>> interceptorClasses) {
            final List<String> interceptorClassList = interceptorClasses.stream()
                    .filter(Objects::nonNull)
                    .map(Class::getName)
                    .collect(Collectors.toList());
            props.put(INTERCEPTOR_CLASSES_CONFIG, interceptorClassList);
            return this;
        }

        /**
         * @see ProducerConfig#ENABLE_IDEMPOTENCE_CONFIG
         *
         * @param enableIdempotence whether or not to enable idempotence
         * @return the builder
         */
        public ProducerConfigBuilder enableIdempotence(final boolean enableIdempotence) {
            props.put(ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
            return this;
        }

        /**
         * @see ProducerConfig#TRANSACTION_TIMEOUT_CONFIG
         *
         * @param transactionTimeout transaction timeout (ms)
         * @return the builder
         */
        public ProducerConfigBuilder transactionalTimeout(final long transactionTimeout) {
            props.put(TRANSACTION_TIMEOUT_CONFIG, transactionTimeout);
            return this;
        }

        /**
         * @see ProducerConfig#TRANSACTIONAL_ID_CONFIG
         *
         * @param transactionalId transactional ID
         * @return the builder
         */
        public ProducerConfigBuilder transactionalId(final String transactionalId) {
            props.put(TRANSACTIONAL_ID_CONFIG, transactionalId);
            return this;
        }

        public ProducerConfigBuilder property(final String key, final String value) {
            props.put(key, value);
            return this;
        }

        /**
         * Build the producer configuration properties
         *
         * @return an instance of {@link Map}
         */
        public Map<String, Object> build() {
            return props;
        }

    }

}
