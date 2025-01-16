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

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * Configuration for the Kafka Producer. Documentation for these configurations can be found in the <a
 * href="http://kafka.apache.org/documentation.html#producerconfigs">Kafka documentation</a>
 */
public class ProducerConfig extends AbstractConfig {
    private static final Logger log = LoggerFactory.getLogger(ProducerConfig.class);

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG STRINGS OR THEIR JAVA VARIABLE NAMES AS THESE ARE PART OF THE PUBLIC API AND
     * CHANGE WILL BREAK USER CODE.
     */

    private static final ConfigDef CONFIG;

    /** <code>bootstrap.servers</code> */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    /** <code>client.dns.lookup</code> */
    public static final String CLIENT_DNS_LOOKUP_CONFIG = CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG;

    /** <code>metadata.max.age.ms</code> */
    public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;
    private static final String METADATA_MAX_AGE_DOC = CommonClientConfigs.METADATA_MAX_AGE_DOC;

    /** <code>metadata.max.idle.ms</code> */
    public static final String METADATA_MAX_IDLE_CONFIG = "metadata.max.idle.ms";
    private static final String METADATA_MAX_IDLE_DOC =
            "Controls how long the producer will cache metadata for a topic that's idle. If the elapsed " +
            "time since a topic was last produced to exceeds the metadata idle duration, then the topic's " +
            "metadata is forgotten and the next access to it will force a metadata fetch request.";

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
                                                 + "buffer of the specified batch size in anticipation of additional records."
                                                 + "<p>"
                                                 + "Note: This setting gives the upper bound of the batch size to be sent. If we have fewer than this many bytes accumulated "
                                                 + "for this partition, we will 'linger' for the <code>linger.ms</code> time waiting for more records to show up. "
                                                 + "This <code>linger.ms</code> setting defaults to 5, which means the producer will wait for 5ms or until the record batch is "
                                                 + "of <code>batch.size</code>(whichever happens first) before sending the record batch. Note that broker backpressure can "
                                                 + " result in a higher effective linger time than this setting."
                                                 + "The default changed from 0 to 5 in Apache Kafka 4.0 as the efficiency gains from larger batches typically result in "
                                                 + "similar or lower producer latency despite the increased linger.";

    /** <code>partitioner.adaptive.partitioning.enable</code> */
    public static final String PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG = "partitioner.adaptive.partitioning.enable";
    private static final String PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_DOC =
            "When set to 'true', the producer will try to adapt to broker performance and produce more messages to partitions hosted on faster brokers. "
            + "If 'false', producer will try to distribute messages uniformly. Note: this setting has no effect if a custom partitioner is used";

    /** <code>partitioner.availability.timeout.ms</code> */
    public static final String PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG = "partitioner.availability.timeout.ms";
    private static final String PARTITIONER_AVAILABILITY_TIMEOUT_MS_DOC =
            "If a broker cannot process produce requests from a partition for <code>" + PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG + "</code> time, "
            + "the partitioner treats that partition as not available.  If the value is 0, this logic is disabled. "
            + "Note: this setting has no effect if a custom partitioner is used or <code>" + PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG
            + "</code> is set to 'false'";

    /** <code>partitioner.ignore.keys</code> */
    public static final String PARTITIONER_IGNORE_KEYS_CONFIG = "partitioner.ignore.keys";
    private static final String PARTITIONER_IGNORE_KEYS_DOC = "When set to 'true' the producer won't use record keys to choose a partition. "
            + "If 'false', producer would choose a partition based on a hash of the key when a key is present. "
            + "Note: this setting has no effect if a custom partitioner is used.";

    /** <code>acks</code> */
    public static final String ACKS_CONFIG = "acks";
    private static final String ACKS_DOC = "The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the "
                                           + " durability of records that are sent. The following settings are allowed: "
                                           + " <ul>"
                                           + " <li><code>acks=0</code> If set to zero then the producer will not wait for any acknowledgment from the"
                                           + " server at all. The record will be immediately added to the socket buffer and considered sent. No guarantee can be"
                                           + " made that the server has received the record in this case, and the <code>retries</code> configuration will not"
                                           + " take effect (as the client won't generally know of any failures). The offset given back for each record will"
                                           + " always be set to <code>-1</code>."
                                           + " <li><code>acks=1</code> This will mean the leader will write the record to its local log but will respond"
                                           + " without awaiting full acknowledgement from all followers. In this case should the leader fail immediately after"
                                           + " acknowledging the record but before the followers have replicated it then the record will be lost."
                                           + " <li><code>acks=all</code> This means the leader will wait for the full set of in-sync replicas to"
                                           + " acknowledge the record. This guarantees that the record will not be lost as long as at least one in-sync replica"
                                           + " remains alive. This is the strongest available guarantee. This is equivalent to the acks=-1 setting."
                                           + "</ul>"
                                           + "<p>"
                                           + "Note that enabling idempotence requires this config value to be 'all'."
                                           + " If conflicting configurations are set and idempotence is not explicitly enabled, idempotence is disabled.";

    /** <code>linger.ms</code> */
    public static final String LINGER_MS_CONFIG = "linger.ms";
    private static final String LINGER_MS_DOC = "The producer groups together any records that arrive in between request transmissions into a single batched request. "
                                                + "Normally this occurs only under load when records arrive faster than they can be sent out. However in some circumstances the client may want to "
                                                + "reduce the number of requests even under moderate load. This setting accomplishes this by adding a small amount "
                                                + "of artificial delay&mdash;that is, rather than immediately sending out a record, the producer will wait for up to "
                                                + "the given delay to allow other records to be sent so that the sends can be batched together. This can be thought "
                                                + "of as analogous to Nagle's algorithm in TCP. This setting gives the upper bound on the delay for batching: once "
                                                + "we get <code>" + BATCH_SIZE_CONFIG + "</code> worth of records for a partition it will be sent immediately regardless of this "
                                                + "setting, however if we have fewer than this many bytes accumulated for this partition we will 'linger' for the "
                                                + "specified time waiting for more records to show up. This setting defaults to 5 (i.e. 5ms delay). Increasing <code>" + LINGER_MS_CONFIG + "=50</code>, "
                                                + "for example, would have the effect of reducing the number of requests sent but would add up to 50ms of latency to records sent in the absence of load."
                                                + "The default changed from 0 to 5 in Apache Kafka 4.0 as the efficiency gains from larger batches typically result in "
                                                + "similar or lower producer latency despite the increased linger.";

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
    private static final String MAX_REQUEST_SIZE_DOC =
        "The maximum size of a request in bytes. This setting will limit the number of record " +
        "batches the producer will send in a single request to avoid sending huge requests. " +
        "This is also effectively a cap on the maximum uncompressed record batch size. Note that the server " +
        "has its own cap on the record batch size (after compression if compression is enabled) which may be different from this.";

    /** <code>reconnect.backoff.ms</code> */
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

    /** <code>reconnect.backoff.max.ms</code> */
    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG;

    /** <code>max.block.ms</code> */
    public static final String MAX_BLOCK_MS_CONFIG = "max.block.ms";
    private static final String MAX_BLOCK_MS_DOC = "The configuration controls how long the <code>KafkaProducer</code>'s <code>send()</code>, <code>partitionsFor()</code>, "
                                                    + "<code>initTransactions()</code>, <code>sendOffsetsToTransaction()</code>, <code>commitTransaction()</code> "
                                                    + "and <code>abortTransaction()</code> methods will block. "
                                                    + "For <code>send()</code> this timeout bounds the total time waiting for both metadata fetch and buffer allocation "
                                                    + "(blocking in the user-supplied serializers or partitioner is not counted against this timeout). "
                                                    + "For <code>partitionsFor()</code> this timeout bounds the time spent waiting for metadata if it is unavailable. "
                                                    + "The transaction-related methods always block, but may timeout if "
                                                    + "the transaction coordinator could not be discovered or did not respond within the timeout.";

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

    /** <code>retry.backoff.max.ms</code> */
    public static final String RETRY_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MAX_MS_CONFIG;

    /**
     * <code>enable.metrics.push</code>
     */
    public static final String ENABLE_METRICS_PUSH_CONFIG = CommonClientConfigs.ENABLE_METRICS_PUSH_CONFIG;
    public static final String ENABLE_METRICS_PUSH_DOC = CommonClientConfigs.ENABLE_METRICS_PUSH_DOC;

    /** <code>compression.type</code> */
    public static final String COMPRESSION_TYPE_CONFIG = "compression.type";
    private static final String COMPRESSION_TYPE_DOC = "The compression type for all data generated by the producer. The default is none (i.e. no compression). Valid "
                                                       + " values are <code>none</code>, <code>gzip</code>, <code>snappy</code>, <code>lz4</code>, or <code>zstd</code>. "
                                                       + "Compression is of full batches of data, so the efficacy of batching will also impact the compression ratio (more batching means better compression).";

    /** <code>compression.gzip.level</code> */
    public static final String COMPRESSION_GZIP_LEVEL_CONFIG = "compression.gzip.level";
    private static final String COMPRESSION_GZIP_LEVEL_DOC = "The compression level to use if " + COMPRESSION_TYPE_CONFIG + " is set to <code>gzip</code>.";

    /** <code>compression.lz4.level</code> */
    public static final String COMPRESSION_LZ4_LEVEL_CONFIG = "compression.lz4.level";
    private static final String COMPRESSION_LZ4_LEVEL_DOC = "The compression level to use if " + COMPRESSION_TYPE_CONFIG + " is set to <code>lz4</code>.";

    /** <code>compression.zstd.level</code> */
    public static final String COMPRESSION_ZSTD_LEVEL_CONFIG = "compression.zstd.level";
    private static final String COMPRESSION_ZSTD_LEVEL_DOC = "The compression level to use if " + COMPRESSION_TYPE_CONFIG + " is set to <code>zstd</code>.";

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

    // max.in.flight.requests.per.connection should be less than or equal to 5 when idempotence producer enabled to ensure message ordering
    // The value 5 is aligned with ProducerStateEntry#NUM_BATCHES_TO_RETAIN.
    private static final int MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE = 5;

    /** <code>max.in.flight.requests.per.connection</code> */
    public static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "max.in.flight.requests.per.connection";
    private static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DOC = "The maximum number of unacknowledged requests the client will send on a single connection before blocking."
                                                                            + " Note that if this configuration is set to be greater than 1 and <code>enable.idempotence</code> is set to false, there is a risk of"
                                                                            + " message reordering after a failed send due to retries (i.e., if retries are enabled); "
                                                                            + " if retries are disabled or if <code>enable.idempotence</code> is set to true, ordering will be preserved."
                                                                            + " Additionally, enabling idempotence requires the value of this configuration to be less than or equal to " + MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE + ","
                                                                            + " because broker only retains at most 5 batches for each producer. If the value is more than 5, previous batches may be removed on broker side.";

    /** <code>retries</code> */
    public static final String RETRIES_CONFIG = CommonClientConfigs.RETRIES_CONFIG;
    private static final String RETRIES_DOC = "Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient error."
            + " Note that this retry is no different than if the client resent the record upon receiving the error."
            + " Produce requests will be failed before the number of retries has been exhausted if the timeout configured by"
            + " <code>" + DELIVERY_TIMEOUT_MS_CONFIG + "</code> expires first before successful acknowledgement. Users should generally"
            + " prefer to leave this config unset and instead use <code>" + DELIVERY_TIMEOUT_MS_CONFIG + "</code> to control"
            + " retry behavior."
            + "<p>"
            + "Enabling idempotence requires this config value to be greater than 0."
            + " If conflicting configurations are set and idempotence is not explicitly enabled, idempotence is disabled."
            + "<p>"
            + "Allowing retries while setting <code>enable.idempotence</code> to <code>false</code> and <code>" + MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + "</code> to greater than 1 will potentially change the"
            + " ordering of records because if two batches are sent to a single partition, and the first fails and is retried but the second"
            + " succeeds, then the records in the second batch may appear first.";

    /** <code>key.serializer</code> */
    public static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
    public static final String KEY_SERIALIZER_CLASS_DOC = "Serializer class for key that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.";

    /** <code>value.serializer</code> */
    public static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";
    public static final String VALUE_SERIALIZER_CLASS_DOC = "Serializer class for value that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.";

    /** <code>socket.connection.setup.timeout.ms</code> */
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;

    /** <code>socket.connection.setup.timeout.max.ms</code> */
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;

    /** <code>connections.max.idle.ms</code> */
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

    /** <code>partitioner.class</code> */
    public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
    private static final String PARTITIONER_CLASS_DOC = "Determines which partition to send a record to when records are produced. Available options are:" +
            "<ul>" +
            "<li>If not set, the default partitioning logic is used. " + 
            "This strategy send records to a partition until at least " + BATCH_SIZE_CONFIG + " bytes is produced to the partition. It works with the strategy:" + 
            "<ol>" +
            "<li>If no partition is specified but a key is present, choose a partition based on a hash of the key.</li>" +
            "<li>If no partition or key is present, choose the sticky partition that changes when at least " + BATCH_SIZE_CONFIG + " bytes are produced to the partition.</li>" +
            "</ol>" +
            "</li>" +
            "<li><code>org.apache.kafka.clients.producer.RoundRobinPartitioner</code>: A partitioning strategy where " +
            "each record in a series of consecutive records is sent to a different partition, regardless of whether the 'key' is provided or not, " +
            "until partitions run out and the process starts over again. Note: There's a known issue that will cause uneven distribution when a new batch is created. " +
            "See KAFKA-9965 for more detail." +
            "</li>" +
            "</ul>" +
            "<p>Implementing the <code>org.apache.kafka.clients.producer.Partitioner</code> interface allows you to plug in a custom partitioner.";

    /** <code>interceptor.classes</code> */
    public static final String INTERCEPTOR_CLASSES_CONFIG = "interceptor.classes";
    public static final String INTERCEPTOR_CLASSES_DOC = "A list of classes to use as interceptors. "
                                                        + "Implementing the <code>org.apache.kafka.clients.producer.ProducerInterceptor</code> interface allows you to intercept (and possibly mutate) the records "
                                                        + "received by the producer before they are published to the Kafka cluster. By default, there are no interceptors.";

    /** <code>enable.idempotence</code> */
    public static final String ENABLE_IDEMPOTENCE_CONFIG = "enable.idempotence";
    public static final String ENABLE_IDEMPOTENCE_DOC = "When set to 'true', the producer will ensure that exactly one copy of each message is written in the stream. If 'false', producer "
                                                        + "retries due to broker failures, etc., may write duplicates of the retried message in the stream. "
                                                        + "Note that enabling idempotence requires <code>" + MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + "</code> to be less than or equal to " + MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE
                                                        + " (with message ordering preserved for any allowable value), <code>" + RETRIES_CONFIG + "</code> to be greater than 0, and <code>"
                                                        + ACKS_CONFIG + "</code> must be 'all'. "
                                                        + "<p>"
                                                        + "Idempotence is enabled by default if no conflicting configurations are set. "
                                                        + "If conflicting configurations are set and idempotence is not explicitly enabled, idempotence is disabled. "
                                                        + "If idempotence is explicitly enabled and conflicting configurations are set, a <code>ConfigException</code> is thrown.";

    /** <code> transaction.timeout.ms </code> */
    public static final String TRANSACTION_TIMEOUT_CONFIG = "transaction.timeout.ms";
    public static final String TRANSACTION_TIMEOUT_DOC = "The maximum amount of time in milliseconds that a transaction will remain open before the coordinator proactively aborts it. " +
            "The start of the transaction is set at the time that the first partition is added to it. " +
            "If this value is larger than the <code>transaction.max.timeout.ms</code> setting in the broker, the request will fail with a <code>InvalidTxnTimeoutException</code> error.";

    /** <code> transactional.id </code> */
    public static final String TRANSACTIONAL_ID_CONFIG = "transactional.id";
    public static final String TRANSACTIONAL_ID_DOC = "The TransactionalId to use for transactional delivery. This enables reliability semantics which span multiple producer sessions since it allows the client to guarantee that transactions using the same TransactionalId have been completed prior to starting any new transactions. If no TransactionalId is provided, then the producer is limited to idempotent delivery. " +
            "If a TransactionalId is configured, <code>enable.idempotence</code> is implied. " +
            "By default the TransactionId is not configured, which means transactions cannot be used. " +
            "Note that, by default, transactions require a cluster of at least three brokers which is the recommended setting for production; for development you can change this, by adjusting broker setting <code>transaction.state.log.replication.factor</code>.";

    /**
     * <code>security.providers</code>
     */
    public static final String SECURITY_PROVIDERS_CONFIG = SecurityConfig.SECURITY_PROVIDERS_CONFIG;
    private static final String SECURITY_PROVIDERS_DOC = SecurityConfig.SECURITY_PROVIDERS_DOC;

    private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    static {
        CONFIG = new ConfigDef().define(BOOTSTRAP_SERVERS_CONFIG, Type.LIST, Collections.emptyList(), new ConfigDef.NonNullValidator(), Importance.HIGH, CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                                .define(CLIENT_DNS_LOOKUP_CONFIG,
                                        Type.STRING,
                                        ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                        in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                           ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
                                .define(BUFFER_MEMORY_CONFIG, Type.LONG, 32 * 1024 * 1024L, atLeast(0L), Importance.HIGH, BUFFER_MEMORY_DOC)
                                .define(RETRIES_CONFIG, Type.INT, Integer.MAX_VALUE, between(0, Integer.MAX_VALUE), Importance.HIGH, RETRIES_DOC)
                                .define(ACKS_CONFIG,
                                        Type.STRING,
                                        "all",
                                        in("all", "-1", "0", "1"),
                                        Importance.LOW,
                                        ACKS_DOC)
                                .define(COMPRESSION_TYPE_CONFIG, Type.STRING, CompressionType.NONE.name, in(Utils.enumOptions(CompressionType.class)), Importance.HIGH, COMPRESSION_TYPE_DOC)
                                .define(COMPRESSION_GZIP_LEVEL_CONFIG, Type.INT, CompressionType.GZIP.defaultLevel(), CompressionType.GZIP.levelValidator(), Importance.MEDIUM, COMPRESSION_GZIP_LEVEL_DOC)
                                .define(COMPRESSION_LZ4_LEVEL_CONFIG, Type.INT, CompressionType.LZ4.defaultLevel(), CompressionType.LZ4.levelValidator(), Importance.MEDIUM, COMPRESSION_LZ4_LEVEL_DOC)
                                .define(COMPRESSION_ZSTD_LEVEL_CONFIG, Type.INT, CompressionType.ZSTD.defaultLevel(), CompressionType.ZSTD.levelValidator(), Importance.MEDIUM, COMPRESSION_ZSTD_LEVEL_DOC)
                                .define(BATCH_SIZE_CONFIG, Type.INT, 16384, atLeast(0), Importance.MEDIUM, BATCH_SIZE_DOC)
                                .define(PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG, Type.BOOLEAN, true, Importance.LOW, PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_DOC)
                                .define(PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG, Type.LONG, 0, atLeast(0), Importance.LOW, PARTITIONER_AVAILABILITY_TIMEOUT_MS_DOC)
                                .define(PARTITIONER_IGNORE_KEYS_CONFIG, Type.BOOLEAN, false, Importance.MEDIUM, PARTITIONER_IGNORE_KEYS_DOC)
                                .define(LINGER_MS_CONFIG, Type.LONG, 5, atLeast(0), Importance.MEDIUM, LINGER_MS_DOC)
                                .define(DELIVERY_TIMEOUT_MS_CONFIG, Type.INT, 120 * 1000, atLeast(0), Importance.MEDIUM, DELIVERY_TIMEOUT_MS_DOC)
                                .define(CLIENT_ID_CONFIG, Type.STRING, "", Importance.MEDIUM, CommonClientConfigs.CLIENT_ID_DOC)
                                .define(SEND_BUFFER_CONFIG, Type.INT, 128 * 1024, atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND), Importance.MEDIUM, CommonClientConfigs.SEND_BUFFER_DOC)
                                .define(RECEIVE_BUFFER_CONFIG, Type.INT, 32 * 1024, atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND), Importance.MEDIUM, CommonClientConfigs.RECEIVE_BUFFER_DOC)
                                .define(MAX_REQUEST_SIZE_CONFIG,
                                        Type.INT,
                                        1024 * 1024,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        MAX_REQUEST_SIZE_DOC)
                                .define(RECONNECT_BACKOFF_MS_CONFIG, Type.LONG, 50L, atLeast(0L), Importance.LOW, CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
                                .define(RECONNECT_BACKOFF_MAX_MS_CONFIG, Type.LONG, 1000L, atLeast(0L), Importance.LOW, CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
                                .define(RETRY_BACKOFF_MS_CONFIG,
                                        Type.LONG,
                                        CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MS,
                                        atLeast(0L),
                                        Importance.LOW,
                                        CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
                                .define(RETRY_BACKOFF_MAX_MS_CONFIG,
                                        Type.LONG,
                                        CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MAX_MS,
                                        atLeast(0L),
                                        Importance.LOW,
                                        CommonClientConfigs.RETRY_BACKOFF_MAX_MS_DOC)
                                .define(ENABLE_METRICS_PUSH_CONFIG,
                                        Type.BOOLEAN,
                                        true,
                                        Importance.LOW,
                                        ENABLE_METRICS_PUSH_DOC)
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
                                .define(METADATA_MAX_IDLE_CONFIG,
                                        Type.LONG,
                                        5 * 60 * 1000,
                                        atLeast(5000),
                                        Importance.LOW,
                                        METADATA_MAX_IDLE_DOC)
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
                                        in(Sensor.RecordingLevel.INFO.toString(), Sensor.RecordingLevel.DEBUG.toString(), Sensor.RecordingLevel.TRACE.toString()),
                                        Importance.LOW,
                                        CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC)
                                .define(METRIC_REPORTER_CLASSES_CONFIG,
                                        Type.LIST,
                                        JmxReporter.class.getName(),
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
                                .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                                        Type.LONG,
                                        CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
                                        Importance.MEDIUM,
                                        CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
                                .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                                        Type.LONG,
                                        CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
                                        Importance.MEDIUM,
                                        CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
                                /* default is set to be a bit lower than the server default (10 min), to avoid both client and server closing connection at same time */
                                .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                                        Type.LONG,
                                        9 * 60 * 1000,
                                        Importance.MEDIUM,
                                        CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
                                .define(PARTITIONER_CLASS_CONFIG,
                                        Type.CLASS,
                                        null,
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
                                        ConfigDef.CaseInsensitiveValidString
                                                .in(Utils.enumOptions(SecurityProtocol.class)),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                                .define(SECURITY_PROVIDERS_CONFIG,
                                        Type.STRING,
                                        null,
                                        Importance.LOW,
                                        SECURITY_PROVIDERS_DOC)
                                .withClientSslSupport()
                                .withClientSaslSupport()
                                .define(ENABLE_IDEMPOTENCE_CONFIG,
                                        Type.BOOLEAN,
                                        true,
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
                                        TRANSACTIONAL_ID_DOC)
                                .define(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG,
                                        Type.STRING,
                                        CommonClientConfigs.DEFAULT_METADATA_RECOVERY_STRATEGY,
                                        ConfigDef.CaseInsensitiveValidString
                                                .in(Utils.enumOptions(MetadataRecoveryStrategy.class)),
                                        Importance.LOW,
                                        CommonClientConfigs.METADATA_RECOVERY_STRATEGY_DOC)
                                .define(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG,
                                        Type.LONG,
                                        CommonClientConfigs.DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS,
                                        atLeast(0),
                                        Importance.LOW,
                                        CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC);
    }

    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        CommonClientConfigs.postValidateSaslMechanismConfig(this);
        CommonClientConfigs.warnDisablingExponentialBackoff(this);
        Map<String, Object> refinedConfigs = CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);
        postProcessAndValidateIdempotenceConfigs(refinedConfigs);
        maybeOverrideClientId(refinedConfigs);
        return refinedConfigs;
    }

    private void maybeOverrideClientId(final Map<String, Object> configs) {
        String refinedClientId;
        boolean userConfiguredClientId = this.originals().containsKey(CLIENT_ID_CONFIG);
        if (userConfiguredClientId) {
            refinedClientId = this.getString(CLIENT_ID_CONFIG);
        } else {
            String transactionalId = this.getString(TRANSACTIONAL_ID_CONFIG);
            refinedClientId = "producer-" + (transactionalId != null ? transactionalId : PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement());
        }
        configs.put(CLIENT_ID_CONFIG, refinedClientId);
    }

    private void postProcessAndValidateIdempotenceConfigs(final Map<String, Object> configs) {
        final Map<String, Object> originalConfigs = this.originals();
        final String acksStr = parseAcks(this.getString(ACKS_CONFIG));
        configs.put(ACKS_CONFIG, acksStr);
        final boolean userConfiguredIdempotence = this.originals().containsKey(ENABLE_IDEMPOTENCE_CONFIG);
        boolean idempotenceEnabled = this.getBoolean(ENABLE_IDEMPOTENCE_CONFIG);
        boolean shouldDisableIdempotence = false;

        // For idempotence producers, values for `retries` and `acks` and `max.in.flight.requests.per.connection` need validation
        if (idempotenceEnabled) {
            final int retries = this.getInt(RETRIES_CONFIG);
            if (retries == 0) {
                if (userConfiguredIdempotence) {
                    throw new ConfigException("Must set " + RETRIES_CONFIG + " to non-zero when using the idempotent producer.");
                }
                log.info("Idempotence will be disabled because {} is set to 0.", RETRIES_CONFIG);
                shouldDisableIdempotence = true;
            }

            final short acks = Short.parseShort(acksStr);
            if (acks != (short) -1) {
                if (userConfiguredIdempotence) {
                    throw new ConfigException("Must set " + ACKS_CONFIG + " to all in order to use the idempotent " +
                        "producer. Otherwise we cannot guarantee idempotence.");
                }
                log.info("Idempotence will be disabled because {} is set to {}, not set to 'all'.", ACKS_CONFIG, acks);
                shouldDisableIdempotence = true;
            }

            final int inFlightConnection = this.getInt(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
            if (MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE < inFlightConnection) {
                throw new ConfigException("To use the idempotent producer, " + MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION +
                                          " must be set to at most 5. Current value is " + inFlightConnection + ".");
            }
        }

        if (shouldDisableIdempotence) {
            configs.put(ENABLE_IDEMPOTENCE_CONFIG, false);
            idempotenceEnabled = false;
        }

        // validate `transaction.id` after validating idempotence dependant configs because `enable.idempotence` config might be overridden
        boolean userConfiguredTransactions = originalConfigs.containsKey(TRANSACTIONAL_ID_CONFIG);
        if (!idempotenceEnabled && userConfiguredTransactions) {
            throw new ConfigException("Cannot set a " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " without also enabling idempotence.");
        }
    }

    private static String parseAcks(String acksString) {
        try {
            return acksString.trim().equalsIgnoreCase("all") ? "-1" : Short.parseShort(acksString.trim()) + "";
        } catch (NumberFormatException e) {
            throw new ConfigException("Invalid configuration value for 'acks': " + acksString);
        }
    }

    static Map<String, Object> appendSerializerToConfig(Map<String, Object> configs,
            Serializer<?> keySerializer,
            Serializer<?> valueSerializer) {
        // validate serializer configuration, if the passed serializer instance is null, the user must explicitly set a valid serializer configuration value
        Map<String, Object> newConfigs = new HashMap<>(configs);
        if (keySerializer != null)
            newConfigs.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass());
        else if (newConfigs.get(KEY_SERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(KEY_SERIALIZER_CLASS_CONFIG, null, "must be non-null.");
        if (valueSerializer != null)
            newConfigs.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass());
        else if (newConfigs.get(VALUE_SERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(VALUE_SERIALIZER_CLASS_CONFIG, null, "must be non-null.");
        return newConfigs;
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

    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtml(4, config -> "producerconfigs_" + config));
    }

}
