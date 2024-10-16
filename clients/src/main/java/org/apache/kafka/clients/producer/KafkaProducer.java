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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.clients.producer.internals.KafkaProducerMetrics;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.clients.producer.internals.ProducerMetrics;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryUtils;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


/**
 * A Kafka client that publishes records to the Kafka cluster.
 * <P>
 * The producer is <i>thread safe</i> and sharing a single producer instance across threads will generally be faster than
 * having multiple instances.
 * <p>
 * Here is a simple example of using the producer to send records with strings containing sequential numbers as the key/value
 * pairs.
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("linger.ms", 1);
 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *
 * Producer<String, String> producer = new KafkaProducer<>(props);
 * for (int i = 0; i < 100; i++)
 *     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * }</pre>
 * <p>
 * The producer consists of a pool of buffer space that holds records that haven't yet been transmitted to the server
 * as well as a background I/O thread that is responsible for turning these records into requests and transmitting them
 * to the cluster. Failure to close the producer after use will leak these resources.
 * <p>
 * The {@link #send(ProducerRecord) send()} method is asynchronous. When called, it adds the record to a buffer of pending record sends
 * and immediately returns. This allows the producer to batch together individual records for efficiency.
 * <p>
 * The <code>acks</code> config controls the criteria under which requests are considered complete. The default setting "all"
 * will result in blocking on the full commit of the record, the slowest but most durable setting.
 * <p>
 * If the request fails, the producer can automatically retry. The <code>retries</code> setting defaults to <code>Integer.MAX_VALUE</code>, and
 * it's recommended to use <code>delivery.timeout.ms</code> to control retry behavior, instead of <code>retries</code>.
 * <p>
 * The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by
 * the <code>batch.size</code> config. Making this larger can result in more batching, but requires more memory (since we will
 * generally have one of these buffers for each active partition).
 * <p>
 * By default a buffer is available to send immediately even if there is additional unused space in the buffer. However if you
 * want to reduce the number of requests you can set <code>linger.ms</code> to something greater than 0. This will
 * instruct the producer to wait up to that number of milliseconds before sending a request in hope that more records will
 * arrive to fill up the same batch. This is analogous to Nagle's algorithm in TCP. For example, in the code snippet above,
 * likely all 100 records would be sent in a single request since we set our linger time to 1 millisecond. However this setting
 * would add 1 millisecond of latency to our request waiting for more records to arrive if we didn't fill up the buffer. Note that
 * records that arrive close together in time will generally batch together even with <code>linger.ms=0</code>. So, under heavy load,
 * batching will occur regardless of the linger configuration; however setting this to something larger than 0 can lead to fewer, more
 * efficient requests when not under maximal load at the cost of a small amount of latency.
 * <p>
 * The <code>buffer.memory</code> controls the total amount of memory available to the producer for buffering. If records
 * are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is
 * exhausted additional send calls will block. The threshold for time to block is determined by <code>max.block.ms</code> after which it throws
 * a TimeoutException.
 * <p>
 * The <code>key.serializer</code> and <code>value.serializer</code> instruct how to turn the key and value objects the user provides with
 * their <code>ProducerRecord</code> into bytes. You can use the included {@link org.apache.kafka.common.serialization.ByteArraySerializer} or
 * {@link org.apache.kafka.common.serialization.StringSerializer} for simple byte or string types.
 * <p>
 * From Kafka 0.11, the KafkaProducer supports two additional modes: the idempotent producer and the transactional producer.
 * The idempotent producer strengthens Kafka's delivery semantics from at least once to exactly once delivery. In particular
 * producer retries will no longer introduce duplicates. The transactional producer allows an application to send messages
 * to multiple partitions (and topics!) atomically.
 * </p>
 * <p>
 * From Kafka 3.0, the <code>enable.idempotence</code> configuration defaults to true. When enabling idempotence,
 * <code>retries</code> config will default to <code>Integer.MAX_VALUE</code> and the <code>acks</code> config will
 * default to <code>all</code>. There are no API changes for the idempotent producer, so existing applications will
 * not need to be modified to take advantage of this feature.
 * </p>
 * <p>
 * To take advantage of the idempotent producer, it is imperative to avoid application level re-sends since these cannot
 * be de-duplicated. As such, if an application enables idempotence, it is recommended to leave the <code>retries</code>
 * config unset, as it will be defaulted to <code>Integer.MAX_VALUE</code>. Additionally, if a {@link #send(ProducerRecord)}
 * returns an error even with infinite retries (for instance if the message expires in the buffer before being sent),
 * then it is recommended to shut down the producer and check the contents of the last produced message to ensure that
 * it is not duplicated. Finally, the producer can only guarantee idempotence for messages sent within a single session.
 * </p>
 * <p>To use the transactional producer and the attendant APIs, you must set the <code>transactional.id</code>
 * configuration property. If the <code>transactional.id</code> is set, idempotence is automatically enabled along with
 * the producer configs which idempotence depends on. Further, topics which are included in transactions should be configured
 * for durability. In particular, the <code>replication.factor</code> should be at least <code>3</code>, and the
 * <code>min.insync.replicas</code> for these topics should be set to 2. Finally, in order for transactional guarantees
 * to be realized from end-to-end, the consumers must be configured to read only committed messages as well.
 * </p>
 * <p>
 * The purpose of the <code>transactional.id</code> is to enable transaction recovery across multiple sessions of a
 * single producer instance. It would typically be derived from the shard identifier in a partitioned, stateful, application.
 * As such, it should be unique to each producer instance running within a partitioned application.
 * </p>
 * <p>All the new transactional APIs are blocking and will throw exceptions on failure. The example
 * below illustrates how the new APIs are meant to be used. It is similar to the example above, except that all
 * 100 messages are part of a single transaction.
 * </p>
 * <p>
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("transactional.id", "my-transactional-id");
 * Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
 *
 * producer.initTransactions();
 *
 * try {
 *     producer.beginTransaction();
 *     for (int i = 0; i < 100; i++)
 *         producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
 *     producer.commitTransaction();
 * } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
 *     // We can't recover from these exceptions, so our only option is to close the producer and exit.
 *     producer.close();
 * } catch (KafkaException e) {
 *     // For all other exceptions, just abort the transaction and try again.
 *     producer.abortTransaction();
 * }
 * producer.close();
 * } </pre>
 * </p>
 * <p>
 * As is hinted at in the example, there can be only one open transaction per producer. All messages sent between the
 * {@link #beginTransaction()} and {@link #commitTransaction()} calls will be part of a single transaction. When the
 * <code>transactional.id</code> is specified, all messages sent by the producer must be part of a transaction.
 * </p>
 * <p>
 * The transactional producer uses exceptions to communicate error states. In particular, it is not required
 * to specify callbacks for <code>producer.send()</code> or to call <code>.get()</code> on the returned Future: a
 * <code>KafkaException</code> would be thrown if any of the
 * <code>producer.send()</code> or transactional calls hit an irrecoverable error during a transaction. See the {@link #send(ProducerRecord)}
 * documentation for more details about detecting errors from a transactional send.
 * </p>
 * </p>By calling
 * <code>producer.abortTransaction()</code> upon receiving a <code>KafkaException</code> we can ensure that any
 * successful writes are marked as aborted, hence keeping the transactional guarantees.
 * </p>
 * <p>
 * This client can communicate with brokers that are version 0.10.0 or newer. Older or newer brokers may not support
 * certain client features.  For instance, the transactional APIs need broker versions 0.11.0 or later. You will receive an
 * <code>UnsupportedVersionException</code> when invoking an API that is not available in the running broker version.
 * </p>
 */
public class KafkaProducer<K, V> implements Producer<K, V> {

    private final Logger log;
    private static final String JMX_PREFIX = "kafka.producer";
    public static final String NETWORK_THREAD_PREFIX = "kafka-producer-network-thread";
    public static final String PRODUCER_METRIC_GROUP_NAME = "producer-metrics";

    private final String clientId;
    // Visible for testing
    final Metrics metrics;
    private final KafkaProducerMetrics producerMetrics;
    private final Partitioner partitioner;
    private final int maxRequestSize;
    private final long totalMemorySize;
    private final ProducerMetadata metadata;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Thread ioThread;
    private final Compression compression;
    private final Sensor errors;
    private final Time time;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final ProducerConfig producerConfig;
    private final long maxBlockTimeMs;
    private final boolean includeWaitTimeOnMetadataInMaxBlockTime;
    private final long maxWaitTimeMsOnMetadata;
    private final boolean partitionerIgnoreKeys;
    private final ProducerInterceptors<K, V> interceptors;
    private final ApiVersions apiVersions;
    private final TransactionManager transactionManager;
    // Init value is needed to avoid NPE in case of exception raised in the constructor
    private Optional<ClientTelemetryReporter> clientTelemetryReporter = Optional.empty();

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param configs   The producer configs
     *
     */
    public KafkaProducer(final Map<String, Object> configs) {
        this(configs, null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * Values can be either strings or Objects of the appropriate type (for example a numeric configuration would accept
     * either the string "42" or the integer 42).
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param configs   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.appendSerializerToConfig(configs, keySerializer, valueSerializer)),
                keySerializer, valueSerializer, null, null, null, Time.SYSTEM);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     */
    public KafkaProducer(Properties properties) {
        this(properties, null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(Utils.propsToMap(properties), keySerializer, valueSerializer);
    }

    /**
     * Check if partitioner is deprecated and log a warning if it is.
     */
    @SuppressWarnings("deprecation")
    private void warnIfPartitionerDeprecated() {
        // Using DefaultPartitioner and UniformStickyPartitioner is deprecated, see KIP-794.
        if (partitioner instanceof org.apache.kafka.clients.producer.internals.DefaultPartitioner) {
            log.warn("DefaultPartitioner is deprecated.  Please clear " + ProducerConfig.PARTITIONER_CLASS_CONFIG
                    + " configuration setting to get the default partitioning behavior");
        }
        if (partitioner instanceof org.apache.kafka.clients.producer.UniformStickyPartitioner) {
            log.warn("UniformStickyPartitioner is deprecated.  Please clear " + ProducerConfig.PARTITIONER_CLASS_CONFIG
                    + " configuration setting and set " + ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG
                    + " to 'true' to get the uniform sticky partitioning behavior");
        }
    }

    // visible for testing
    @SuppressWarnings({"unchecked", "this-escape"})
    KafkaProducer(ProducerConfig config,
                  Serializer<K> keySerializer,
                  Serializer<V> valueSerializer,
                  ProducerMetadata metadata,
                  KafkaClient kafkaClient,
                  ProducerInterceptors<K, V> interceptors,
                  Time time) {
        try {
            this.producerConfig = config;
            this.time = time;

            String transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG);

            this.clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);

            LogContext logContext;
            if (transactionalId == null)
                logContext = new LogContext(String.format("[Producer clientId=%s] ", clientId));
            else
                logContext = new LogContext(String.format("[Producer clientId=%s, transactionalId=%s] ", clientId, transactionalId));
            log = logContext.logger(KafkaProducer.class);
            log.trace("Starting the Kafka producer");

            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    .recordLevel(Sensor.RecordingLevel.forName(config.getString(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                    .tags(metricTags);
            List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
            this.clientTelemetryReporter = CommonClientConfigs.telemetryReporter(clientId, config);
            this.clientTelemetryReporter.ifPresent(reporters::add);
            MetricsContext metricsContext = new KafkaMetricsContext(JMX_PREFIX,
                    config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
            this.metrics = new Metrics(metricConfig, reporters, time, metricsContext);
            this.producerMetrics = new KafkaProducerMetrics(metrics);
            this.partitioner = config.getConfiguredInstance(
                    ProducerConfig.PARTITIONER_CLASS_CONFIG,
                    Partitioner.class,
                    Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId));
            warnIfPartitionerDeprecated();
            this.partitionerIgnoreKeys = config.getBoolean(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG);
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            long retryBackoffMaxMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
            if (keySerializer == null) {
                this.keySerializer = config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                                                                         Serializer.class);
                this.keySerializer.configure(config.originals(Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId)), true);
            } else {
                config.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
                this.keySerializer = keySerializer;
            }
            if (valueSerializer == null) {
                this.valueSerializer = config.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                                                                           Serializer.class);
                this.valueSerializer.configure(config.originals(Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId)), false);
            } else {
                config.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
                this.valueSerializer = valueSerializer;
            }

            List<ProducerInterceptor<K, V>> interceptorList = ClientUtils.configuredInterceptors(config,
                    ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    ProducerInterceptor.class);
            if (interceptors != null)
                this.interceptors = interceptors;
            else
                this.interceptors = new ProducerInterceptors<>(interceptorList);
            ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(
                    interceptorList,
                    reporters,
                    Arrays.asList(this.keySerializer, this.valueSerializer));
            this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
            this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
            this.compression = configureCompression(config);

            this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
            this.includeWaitTimeOnMetadataInMaxBlockTime = config.getBoolean(ProducerConfig.INCLUDE_WAIT_TIME_ON_METADATA_IN_MAX_BLOCK_TIME_CONFIG);
            this.maxWaitTimeMsOnMetadata = config.getLong(ProducerConfig.MAX_WAIT_TIME_MS_ON_METADATA_CONFIG);
            int deliveryTimeoutMs = configureDeliveryTimeout(config, log);

            this.apiVersions = new ApiVersions();
            this.transactionManager = configureTransactionState(config, logContext);
            // There is no need to do work required for adaptive partitioning, if we use a custom partitioner.
            boolean enableAdaptivePartitioning = partitioner == null &&
                config.getBoolean(ProducerConfig.PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG);
            RecordAccumulator.PartitionerConfig partitionerConfig = new RecordAccumulator.PartitionerConfig(
                enableAdaptivePartitioning,
                config.getLong(ProducerConfig.PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG)
            );
            // As per Kafka producer configuration documentation batch.size may be set to 0 to explicitly disable
            // batching which in practice actually means using a batch size of 1.
            int batchSize = Math.max(1, config.getInt(ProducerConfig.BATCH_SIZE_CONFIG));
            this.accumulator = new RecordAccumulator(logContext,
                    batchSize,
                    compression,
                    lingerMs(config),
                    retryBackoffMs,
                    retryBackoffMaxMs,
                    deliveryTimeoutMs,
                    partitionerConfig,
                    metrics,
                    PRODUCER_METRIC_GROUP_NAME,
                    time,
                    apiVersions,
                    transactionManager,
                    new BufferPool(this.totalMemorySize, batchSize, metrics, time, PRODUCER_METRIC_GROUP_NAME));

            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
            if (metadata != null) {
                this.metadata = metadata;
            } else {
                this.metadata = new ProducerMetadata(retryBackoffMs,
                        retryBackoffMaxMs,
                        config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG),
                        config.getLong(ProducerConfig.METADATA_MAX_IDLE_CONFIG),
                        logContext,
                        clusterResourceListeners,
                        Time.SYSTEM);
                this.metadata.bootstrap(addresses);
            }
            this.errors = this.metrics.sensor("errors");
            this.sender = newSender(logContext, kafkaClient, this.metadata);
            String ioThreadName = NETWORK_THREAD_PREFIX + " | " + clientId;
            this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
            this.ioThread.start();
            config.logUnused();
            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());
            log.debug("Kafka producer started");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed this is to prevent resource leak. see KAFKA-2121
            close(Duration.ofMillis(0), true);
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka producer", t);
        }
    }

    // visible for testing
    KafkaProducer(ProducerConfig config,
                  LogContext logContext,
                  Metrics metrics,
                  Serializer<K> keySerializer,
                  Serializer<V> valueSerializer,
                  ProducerMetadata metadata,
                  RecordAccumulator accumulator,
                  TransactionManager transactionManager,
                  Sender sender,
                  ProducerInterceptors<K, V> interceptors,
                  Partitioner partitioner,
                  Time time,
                  KafkaThread ioThread,
                  Optional<ClientTelemetryReporter> clientTelemetryReporter) {
        this.producerConfig = config;
        this.time = time;
        this.clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
        this.log = logContext.logger(KafkaProducer.class);
        this.metrics = metrics;
        this.producerMetrics = new KafkaProducerMetrics(metrics);
        this.partitioner = partitioner;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.interceptors = interceptors;
        this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
        this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
        this.compression = configureCompression(config);
        this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
        this.includeWaitTimeOnMetadataInMaxBlockTime = config.getBoolean(ProducerConfig.INCLUDE_WAIT_TIME_ON_METADATA_IN_MAX_BLOCK_TIME_CONFIG);
        this.maxWaitTimeMsOnMetadata = config.getLong(ProducerConfig.MAX_WAIT_TIME_MS_ON_METADATA_CONFIG);
        this.partitionerIgnoreKeys = config.getBoolean(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG);
        this.apiVersions = new ApiVersions();
        this.transactionManager = transactionManager;
        this.accumulator = accumulator;
        this.errors = this.metrics.sensor("errors");
        this.metadata = metadata;
        this.sender = sender;
        this.ioThread = ioThread;
        this.clientTelemetryReporter = clientTelemetryReporter;
    }

    // visible for testing
    Sender newSender(LogContext logContext, KafkaClient kafkaClient, ProducerMetadata metadata) {
        int maxInflightRequests = producerConfig.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        int requestTimeoutMs = producerConfig.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        ProducerMetrics metricsRegistry = new ProducerMetrics(this.metrics);
        Sensor throttleTimeSensor = Sender.throttleTimeSensor(metricsRegistry.senderMetrics);
        KafkaClient client = kafkaClient != null ? kafkaClient : ClientUtils.createNetworkClient(producerConfig,
                this.metrics,
                "producer",
                logContext,
                apiVersions,
                time,
                maxInflightRequests,
                metadata,
                throttleTimeSensor,
                clientTelemetryReporter.map(ClientTelemetryReporter::telemetrySender).orElse(null));

        short acks = Short.parseShort(producerConfig.getString(ProducerConfig.ACKS_CONFIG));
        return new Sender(logContext,
                client,
                metadata,
                this.accumulator,
                maxInflightRequests == 1,
                producerConfig.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
                acks,
                producerConfig.getInt(ProducerConfig.RETRIES_CONFIG),
                metricsRegistry.senderMetrics,
                time,
                requestTimeoutMs,
                producerConfig.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG),
                this.transactionManager,
                apiVersions);
    }

    private static Compression configureCompression(ProducerConfig config) {
        CompressionType type = CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        switch (type) {
            case GZIP: {
                return Compression.gzip()
                        .level(config.getInt(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG))
                        .build();
            }
            case LZ4: {
                return Compression.lz4()
                        .level(config.getInt(ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG))
                        .build();
            }
            case ZSTD: {
                return Compression.zstd()
                        .level(config.getInt(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG))
                        .build();
            }
            default:
                return Compression.of(type).build();
        }
    }

    private static int lingerMs(ProducerConfig config) {
        return (int) Math.min(config.getLong(ProducerConfig.LINGER_MS_CONFIG), Integer.MAX_VALUE);
    }

    private static int configureDeliveryTimeout(ProducerConfig config, Logger log) {
        int deliveryTimeoutMs = config.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        int lingerMs = lingerMs(config);
        int requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        int lingerAndRequestTimeoutMs = (int) Math.min((long) lingerMs + requestTimeoutMs, Integer.MAX_VALUE);

        if (deliveryTimeoutMs < lingerAndRequestTimeoutMs) {
            if (config.originals().containsKey(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)) {
                // throw an exception if the user explicitly set an inconsistent value
                throw new ConfigException(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG
                    + " should be equal to or larger than " + ProducerConfig.LINGER_MS_CONFIG
                    + " + " + ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            } else {
                // override deliveryTimeoutMs default value to lingerMs + requestTimeoutMs for backward compatibility
                deliveryTimeoutMs = lingerAndRequestTimeoutMs;
                log.warn("{} should be equal to or larger than {} + {}. Setting it to {}.",
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, ProducerConfig.LINGER_MS_CONFIG,
                    ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
            }
        }
        return deliveryTimeoutMs;
    }

    private TransactionManager configureTransactionState(ProducerConfig config,
                                                         LogContext logContext) {
        TransactionManager transactionManager = null;

        if (config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)) {
            final String transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
            final int transactionTimeoutMs = config.getInt(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
            final long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            transactionManager = new TransactionManager(
                logContext,
                transactionalId,
                transactionTimeoutMs,
                retryBackoffMs,
                apiVersions
            );

            if (transactionManager.isTransactional())
                log.info("Instantiated a transactional producer.");
            else
                log.info("Instantiated an idempotent producer.");
        } else {
            // ignore unretrieved configurations related to producer transaction
            config.ignore(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
        }
        return transactionManager;
    }

    /**
     * Needs to be called before any other methods when the {@code transactional.id} is set in the configuration.
     * This method does the following:
     * <ol>
     * <li>Ensures any transactions initiated by previous instances of the producer with the same
     *      {@code transactional.id} are completed. If the previous instance had failed with a transaction in
     *      progress, it will be aborted. If the last transaction had begun completion,
     *      but not yet finished, this method awaits its completion.</li>
     * <li>Gets the internal producer id and epoch, used in all future transactional
     *      messages issued by the producer.</li>
     * </ol>
     * Note that this method will raise {@link TimeoutException} if the transactional state cannot
     * be initialized before expiration of {@code max.block.ms}. Additionally, it will raise {@link InterruptException}
     * if interrupted. It is safe to retry in either case, but once the transactional state has been successfully
     * initialized, this method should no longer be used.
     *
     * @throws IllegalStateException if no {@code transactional.id} has been configured
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException error indicating that the configured
     *         transactional.id is not authorized, or the idempotent producer id is unavailable. See the exception for
     *         more details.  User may retry this function call after fixing the permission.
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     * @throws TimeoutException if the time taken for initialize the transaction has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    public void initTransactions() {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        long now = time.nanoseconds();
        TransactionalRequestResult result = transactionManager.initializeTransactions();
        sender.wakeup();
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
        producerMetrics.recordInit(time.nanoseconds() - now);
    }

    /**
     * Should be called before the start of each new transaction. Note that prior to the first invocation
     * of this method, you must invoke {@link #initTransactions()} exactly one time.
     *
     * @throws IllegalStateException if no {@code transactional.id} has been configured or if {@link #initTransactions()}
     *         has not yet been invoked
     * @throws ProducerFencedException if another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         {@code transactional.id} is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     */
    public void beginTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        long now = time.nanoseconds();
        transactionManager.beginTransaction();
        producerMetrics.recordBeginTxn(time.nanoseconds() - now);
    }

    /**
     * Sends a list of specified offsets to the consumer group coordinator, and also marks
     * those offsets as part of the current transaction. These offsets will be considered
     * committed only if the transaction is committed successfully. The committed offset should
     * be the next message your application will consume, i.e. lastProcessedMessageOffset + 1.
     * <p>
     * This method should be used when you need to batch consumed and produced messages
     * together, typically in a consume-transform-produce pattern. Thus, the specified
     * {@code consumerGroupId} should be the same as config parameter {@code group.id} of the used
     * {@link KafkaConsumer consumer}. Note, that the consumer should have {@code enable.auto.commit=false}
     * and should also not commit offsets manually (via {@link KafkaConsumer#commitSync(Map) sync} or
     * {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback) async} commits).
     *
     * <p>
     * This method is a blocking call that waits until the request has been received and acknowledged by the consumer group
     * coordinator; but the offsets are not considered as committed until the transaction itself is successfully committed later (via
     * the {@link #commitTransaction()} call).
     *
     * @throws IllegalStateException if no transactional.id has been configured, no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException fatal error indicating the message
     *         format used for the offsets topic on the broker does not support transactions
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized, or the consumer group id is not authorized.
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * @throws TimeoutException if the time taken for sending the offsets has surpassed <code>max.block.ms</code>.
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     *
     * @deprecated Since 3.0.0, please use {@link #sendOffsetsToTransaction(Map, ConsumerGroupMetadata)} instead.
     */
    @Deprecated
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         String consumerGroupId) throws ProducerFencedException {
        sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata(consumerGroupId));
    }

    /**
     * Sends a list of specified offsets to the consumer group coordinator, and also marks
     * those offsets as part of the current transaction. These offsets will be considered
     * committed only if the transaction is committed successfully. The committed offset should
     * be the next message your application will consume, i.e. lastProcessedMessageOffset + 1.
     * <p>
     * This method should be used when you need to batch consumed and produced messages
     * together, typically in a consume-transform-produce pattern. Thus, the specified
     * {@code groupMetadata} should be extracted from the used {@link KafkaConsumer consumer} via
     * {@link KafkaConsumer#groupMetadata()} to leverage consumer group metadata. This will provide
     * stronger fencing than just supplying the {@code consumerGroupId} and passing in {@code new ConsumerGroupMetadata(consumerGroupId)},
     * however note that the full set of consumer group metadata returned by {@link KafkaConsumer#groupMetadata()}
     * requires the brokers to be on version 2.5 or newer to understand.
     *
     * <p>
     * This method is a blocking call that waits until the request has been received and acknowledged by the consumer group
     * coordinator; but the offsets are not considered as committed until the transaction itself is successfully committed later (via
     * the {@link #commitTransaction()} call).
     *
     * <p>
     * Note, that the consumer should have {@code enable.auto.commit=false} and should
     * also not commit offsets manually (via {@link KafkaConsumer#commitSync(Map) sync} or
     * {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback) async} commits).
     * This method will raise {@link TimeoutException} if the producer cannot send offsets before expiration of {@code max.block.ms}.
     * Additionally, it will raise {@link InterruptException} if interrupted.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started.
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0) or
     *         the broker doesn't support the latest version of transactional API with all consumer group metadata
     *         (i.e. if its version is lower than 2.5.0).
     * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException fatal error indicating the message
     *         format used for the offsets topic on the broker does not support transactions
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized, or the consumer group id is not authorized.
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried
     *         (e.g. if the consumer has been kicked out of the group). Users should handle this by aborting the transaction.
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this producer instance gets fenced by broker due to a
     *                                                                  mis-configured consumer instance id within group metadata.
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     * @throws TimeoutException if the time taken for sending the offsets has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
        throwIfInvalidGroupMetadata(groupMetadata);
        throwIfNoTransactionManager();
        throwIfProducerClosed();

        if (!offsets.isEmpty()) {
            long start = time.nanoseconds();
            TransactionalRequestResult result = transactionManager.sendOffsetsToTransaction(offsets, groupMetadata);
            sender.wakeup();
            result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
            producerMetrics.recordSendOffsets(time.nanoseconds() - start);
        }
    }

    /**
     * Commits the ongoing transaction. This method will flush any unsent records before actually committing the transaction.
     * <p>
     * Further, if any of the {@link #send(ProducerRecord)} calls which were part of the transaction hit irrecoverable
     * errors, this method will throw the last received exception immediately and the transaction will not be committed.
     * So all {@link #send(ProducerRecord)} calls in a transaction must succeed in order for this method to succeed.
     * <p>
     * If the transaction is committed successfully and this method returns without throwing an exception, it is guaranteed
     * that all {@link Callback callbacks} for records in the transaction will have been invoked and completed.
     * Note that exceptions thrown by callbacks are ignored; the producer proceeds to commit the transaction in any case.
     * <p>
     * Note that this method will raise {@link TimeoutException} if the transaction cannot be committed before expiration
     * of {@code max.block.ms}, but this does not mean the request did not actually reach the broker. In fact, it only indicates
     * that we cannot get the acknowledgement response in time, so it's up to the application's logic
     * to decide how to handle timeouts.
     * Additionally, it will raise {@link InterruptException} if interrupted.
     * It is safe to retry in either case, but it is not possible to attempt a different operation (such as abortTransaction)
     * since the commit may already be in the progress of completing. If not retrying, the only option is to close the producer.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     * @throws TimeoutException if the time taken for committing the transaction has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    public void commitTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        long commitStart = time.nanoseconds();
        TransactionalRequestResult result = transactionManager.beginCommit();
        sender.wakeup();
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
        producerMetrics.recordCommitTxn(time.nanoseconds() - commitStart);
    }

    /**
     * Aborts the ongoing transaction. Any unflushed produce messages will be aborted when this call is made.
     * This call will throw an exception immediately if any prior {@link #send(ProducerRecord)} calls failed with a
     * {@link ProducerFencedException} or an instance of {@link org.apache.kafka.common.errors.AuthorizationException}.
     * <p>
     * Note that this method will raise {@link TimeoutException} if the transaction cannot be aborted before expiration
     * of {@code max.block.ms}, but this does not mean the request did not actually reach the broker. In fact, it only indicates
     * that we cannot get the acknowledgement response in time, so it's up to the application's logic
     * to decide how to handle timeouts. Additionally, it will raise {@link InterruptException} if interrupted.
     * It is safe to retry in either case, but it is not possible to attempt a different operation (such as {@link #commitTransaction})
     * since the abort may already be in the progress of completing. If not retrying, the only option is to close the producer.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     * @throws TimeoutException if the time taken for aborting the transaction has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    public void abortTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        log.info("Aborting incomplete transaction");
        long abortStart = time.nanoseconds();
        TransactionalRequestResult result = transactionManager.beginAbort();
        sender.wakeup();
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
        producerMetrics.recordAbortTxn(time.nanoseconds() - abortStart);
    }

    /**
     * Asynchronously send a record to a topic. Equivalent to <code>send(record, null)</code>.
     * See {@link #send(ProducerRecord, Callback)} for details.
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    /**
     * Asynchronously send a record to a topic and invoke the provided callback when the send has been acknowledged.
     * <p>
     * The send is asynchronous and this method will return immediately (except for rare cases described below)
     * once the record has been stored in the buffer of records waiting to be sent.
     * This allows sending many records in parallel without blocking to wait for the response after each one.
     * Can block for the following cases: 1) For the first record being sent to 
     * the cluster by this client for the given topic. In this case it will block for up to {@code max.block.ms} milliseconds if 
     * Kafka cluster is unreachable; 2) Allocating a buffer if buffer pool doesn't have any free buffers.
     * <p>
     * The result of the send is a {@link RecordMetadata} specifying the partition the record was sent to, the offset
     * it was assigned and the timestamp of the record. If the producer is configured with acks = 0, the {@link RecordMetadata}
     * will have offset = -1 because the producer does not wait for the acknowledgement from the broker.
     * If {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime} is used by the topic, the timestamp
     * will be the user provided timestamp or the record send time if the user did not specify a timestamp for the
     * record. If {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime} is used for the
     * topic, the timestamp will be the Kafka broker local time when the message is appended.
     * <p>
     * Since the send call is asynchronous it returns a {@link java.util.concurrent.Future Future} for the
     * {@link RecordMetadata} that will be assigned to this record. Invoking {@link java.util.concurrent.Future#get()
     * get()} on this future will block until the associated request completes and then return the metadata for the record
     * or throw any exception that occurred while sending the record.
     * <p>
     * If you want to simulate a simple blocking call you can call the <code>get()</code> method immediately:
     *
     * <pre>
     * {@code
     * byte[] key = "key".getBytes();
     * byte[] value = "value".getBytes();
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("my-topic", key, value)
     * producer.send(record).get();
     * }</pre>
     * <p>
     * Fully non-blocking usage can make use of the {@link Callback} parameter to provide a callback that
     * will be invoked when the request is complete.
     *
     * <pre>
     * {@code
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("the-topic", key, value);
     * producer.send(myRecord,
     *               new Callback() {
     *                   public void onCompletion(RecordMetadata metadata, Exception e) {
     *                       if(e != null) {
     *                          e.printStackTrace();
     *                       } else {
     *                          System.out.println("The offset of the record we just sent is: " + metadata.offset());
     *                       }
     *                   }
     *               });
     * }
     * </pre>
     *
     * Callbacks for records being sent to the same partition are guaranteed to execute in order. That is, in the
     * following example <code>callback1</code> is guaranteed to execute before <code>callback2</code>:
     *
     * <pre>
     * {@code
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key1, value1), callback1);
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key2, value2), callback2);
     * }
     * </pre>
     * <p>
     * When used as part of a transaction, it is not necessary to define a callback or check the result of the future
     * in order to detect errors from <code>send</code>. If any of the send calls failed with an irrecoverable error,
     * the final {@link #commitTransaction()} call will fail and throw the exception from the last failed send. When
     * this happens, your application should call {@link #abortTransaction()} to reset the state and continue to send
     * data.
     * </p>
     * <p>
     * Some transactional send errors cannot be resolved with a call to {@link #abortTransaction()}.  In particular,
     * if a transactional send finishes with a {@link ProducerFencedException}, a {@link org.apache.kafka.common.errors.OutOfOrderSequenceException},
     * a {@link org.apache.kafka.common.errors.UnsupportedVersionException}, or an
     * {@link org.apache.kafka.common.errors.AuthorizationException}, then the only option left is to call {@link #close()}.
     * Fatal errors cause the producer to enter a defunct state in which future API calls will continue to raise
     * the same underlying error wrapped in a new {@link KafkaException}.
     * </p>
     * <p>
     * It is a similar picture when idempotence is enabled, but no <code>transactional.id</code> has been configured.
     * In this case, {@link org.apache.kafka.common.errors.UnsupportedVersionException} and
     * {@link org.apache.kafka.common.errors.AuthorizationException} are considered fatal errors. However,
     * {@link ProducerFencedException} does not need to be handled. Additionally, it is possible to continue
     * sending after receiving an {@link org.apache.kafka.common.errors.OutOfOrderSequenceException}, but doing so
     * can result in out of order delivery of pending messages. To ensure proper ordering, you should close the
     * producer and create a new instance.
     * </p>
     * <p>
     * If the message format of the destination topic is not upgraded to 0.11.0.0, idempotent and transactional
     * produce requests will fail with an {@link org.apache.kafka.common.errors.UnsupportedForMessageFormatException}
     * error. If this is encountered during a transaction, it is possible to abort and continue. But note that future
     * sends to the same topic will continue receiving the same exception until the topic is upgraded.
     * </p>
     * <p>
     * Note that callbacks will generally execute in the I/O thread of the producer and so should be reasonably fast or
     * they will delay the sending of messages from other threads. If you want to execute blocking or computationally
     * expensive callbacks it is recommended to use your own {@link java.util.concurrent.Executor} in the callback body
     * to parallelize processing.
     *
     * @param record The record to send
     * @param callback A user-supplied callback to execute when the record has been acknowledged by the server (null
     *        indicates no callback)
     *
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException fatal error indicating that the producer is not allowed to write
     * @throws IllegalStateException if a transactional.id has been configured and no transaction has been started, or
     *                               when send is invoked after producer has been closed.
     * @throws InterruptException If the thread is interrupted while blocked
     * @throws SerializationException If the key or value are not valid objects given the configured serializers
     * @throws TimeoutException If the record could not be appended to the send buffer due to memory unavailable
     *                          or missing metadata within {@code max.block.ms}.
     * @throws KafkaException If a Kafka related error occurs that does not belong to the public API exceptions.
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        // intercept the record, which can be potentially modified; this method does not throw exceptions
        ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
        return doSend(interceptedRecord, callback);
    }

    // Verify that this producer instance has not been closed. This method throws IllegalStateException if the producer
    // has already been closed.
    private void throwIfProducerClosed() {
        if (sender == null || !sender.isRunning())
            throw new IllegalStateException("Cannot perform operation after producer has been closed");
    }

    /**
     * Call deprecated {@link Partitioner#onNewBatch}
     */
    @SuppressWarnings("deprecation")
    private void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        assert partitioner != null;
        partitioner.onNewBatch(topic, cluster, prevPartition);
    }

    /**
     * Implementation of asynchronously send a record to a topic.
     */
    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        // Append callback takes care of the following:
        //  - call interceptors and user callback on completion
        //  - remember partition that is calculated in RecordAccumulator.append
        AppendCallbacks appendCallbacks = new AppendCallbacks(callback, this.interceptors, record);

        try {
            throwIfProducerClosed();
            // first make sure the metadata for the topic is available
            long nowMs = time.milliseconds();
            ClusterAndWaitTime clusterAndWaitTime;
            try {
                clusterAndWaitTime = waitOnMetadata(record.topic(), record.partition(), nowMs, this.includeWaitTimeOnMetadataInMaxBlockTime ? maxBlockTimeMs : this.maxWaitTimeMsOnMetadata);
            } catch (KafkaException e) {
                if (metadata.isClosed())
                    throw new KafkaException("Producer closed while send in progress", e);
                throw e;
            }
            nowMs += clusterAndWaitTime.waitedOnMetadataMs;
            long remainingWaitMs = Math.max(0, this.includeWaitTimeOnMetadataInMaxBlockTime ? maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs : maxBlockTimeMs);
            Cluster cluster = clusterAndWaitTime.cluster;
            byte[] serializedKey;
            try {
                serializedKey = keySerializer.serialize(record.topic(), record.headers(), record.key());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in key.serializer", cce);
            }
            byte[] serializedValue;
            try {
                serializedValue = valueSerializer.serialize(record.topic(), record.headers(), record.value());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in value.serializer", cce);
            }

            // Try to calculate partition, but note that after this call it can be RecordMetadata.UNKNOWN_PARTITION,
            // which means that the RecordAccumulator would pick a partition using built-in logic (which may
            // take into account broker load, the amount of data produced to each partition, etc.).
            int partition = partition(record, serializedKey, serializedValue, cluster);

            setReadOnly(record.headers());
            Header[] headers = record.headers().toArray();

            int serializedSize = AbstractRecords.estimateSizeInBytesUpperBound(apiVersions.maxUsableProduceMagic(),
                    compression.type(), serializedKey, serializedValue, headers);
            ensureValidRecordSize(serializedSize);
            long timestamp = record.timestamp() == null ? nowMs : record.timestamp();

            // A custom partitioner may take advantage on the onNewBatch callback.
            boolean abortOnNewBatch = partitioner != null;

            // Append the record to the accumulator.  Note, that the actual partition may be
            // calculated there and can be accessed via appendCallbacks.topicPartition.
            RecordAccumulator.RecordAppendResult result = accumulator.append(record.topic(), partition, timestamp, serializedKey,
                    serializedValue, headers, appendCallbacks, remainingWaitMs, abortOnNewBatch, nowMs, cluster);
            assert appendCallbacks.getPartition() != RecordMetadata.UNKNOWN_PARTITION;

            if (result.abortForNewBatch) {
                int prevPartition = partition;
                onNewBatch(record.topic(), cluster, prevPartition);
                partition = partition(record, serializedKey, serializedValue, cluster);
                if (log.isTraceEnabled()) {
                    log.trace("Retrying append due to new batch creation for topic {} partition {}. The old partition was {}", record.topic(), partition, prevPartition);
                }
                result = accumulator.append(record.topic(), partition, timestamp, serializedKey,
                    serializedValue, headers, appendCallbacks, remainingWaitMs, false, nowMs, cluster);
            }

            // Add the partition to the transaction (if in progress) after it has been successfully
            // appended to the accumulator. We cannot do it before because the partition may be
            // unknown or the initially selected partition may be changed when the batch is closed
            // (as indicated by `abortForNewBatch`). Note that the `Sender` will refuse to dequeue
            // batches from the accumulator until they have been added to the transaction.
            if (transactionManager != null) {
                transactionManager.maybeAddPartition(appendCallbacks.topicPartition());
            }

            if (result.batchIsFull || result.newBatchCreated) {
                log.trace("Waking up the sender since topic {} partition {} is either full or getting a new batch", record.topic(), appendCallbacks.getPartition());
                this.sender.wakeup();
            }
            return result.future;
            // handling exceptions and record the errors;
            // for API exceptions return them in the future,
            // for other exceptions throw directly
        } catch (ApiException e) {
            log.debug("Exception occurred during message send:", e);
            if (callback != null) {
                TopicPartition tp = appendCallbacks.topicPartition();
                RecordMetadata nullMetadata = new RecordMetadata(tp, -1, -1, RecordBatch.NO_TIMESTAMP, -1, -1);
                callback.onCompletion(nullMetadata, e);
            }
            this.errors.record();
            this.interceptors.onSendError(record, appendCallbacks.topicPartition(), e);
            if (transactionManager != null) {
                transactionManager.maybeTransitionToErrorState(e);
            }
            return new FutureFailure(e);
        } catch (InterruptedException e) {
            this.errors.record();
            this.interceptors.onSendError(record, appendCallbacks.topicPartition(), e);
            throw new InterruptException(e);
        } catch (KafkaException e) {
            this.errors.record();
            this.interceptors.onSendError(record, appendCallbacks.topicPartition(), e);
            throw e;
        } catch (Exception e) {
            // we notify interceptor about all exceptions, since onSend is called before anything else in this method
            this.interceptors.onSendError(record, appendCallbacks.topicPartition(), e);
            throw e;
        }
    }

    private void setReadOnly(Headers headers) {
        if (headers instanceof RecordHeaders) {
            ((RecordHeaders) headers).setReadOnly();
        }
    }

    /**
     * Wait for cluster metadata including partitions for the given topic to be available.
     * @param topic The topic we want metadata for
     * @param partition A specific partition expected to exist in metadata, or null if there's no preference
     * @param nowMs The current time in ms
     * @param maxWaitMs The maximum time in ms for waiting on the metadata
     * @return The cluster containing topic metadata and the amount of time we waited in ms
     * @throws TimeoutException if metadata could not be refreshed within {@code max.block.ms}
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method is called after producer close
     */
    private ClusterAndWaitTime waitOnMetadata(String topic, Integer partition, long nowMs, long maxWaitMs) throws InterruptedException {
        Cluster cluster = metadata.fetch();

        if (cluster.invalidTopics().contains(topic))
            throw new InvalidTopicException(topic);

        // add topic to metadata topic list if it is not there already and reset expiry
        metadata.add(topic, nowMs);

        Integer partitionsCount = cluster.partitionCountForTopic(topic);
        // Return cached metadata if we have it, and if the record's partition is either undefined
        // or within the known partition range
        if (partitionsCount != null && (partition == null || partition < partitionsCount))
            return new ClusterAndWaitTime(cluster, 0);

        long remainingWaitMs = maxWaitMs;
        long elapsed = 0;
        // Issue metadata requests until we have metadata for the topic and the requested partition,
        // or until maxWaitTimeMs is exceeded. This is necessary in case the metadata
        // is stale and the number of partitions for this topic has increased in the meantime.
        long nowNanos = time.nanoseconds();
        do {
            if (partition != null) {
                log.trace("Requesting metadata update for partition {} of topic {}.", partition, topic);
            } else {
                log.trace("Requesting metadata update for topic {}.", topic);
            }
            metadata.add(topic, nowMs + elapsed);
            int version = metadata.requestUpdateForTopic(topic);
            sender.wakeup();
            try {
                metadata.awaitUpdate(version, remainingWaitMs);
            } catch (TimeoutException ex) {
                // Rethrow with original maxWaitMs to prevent logging exception with remainingWaitMs
                final String errorMessage = String.format("Topic %s not present in metadata after %d ms.",
                        topic, maxWaitMs);
                if (metadata.getError(topic) != null) {
                    throw new TimeoutException(errorMessage, metadata.getError(topic).exception());
                }
                throw new TimeoutException(errorMessage);
            }
            cluster = metadata.fetch();
            elapsed = time.milliseconds() - nowMs;
            if (elapsed >= maxWaitMs) {
                final String errorMessage = partitionsCount == null ?
                        String.format("Topic %s not present in metadata after %d ms.",
                                topic, maxWaitMs) :
                        String.format("Partition %d of topic %s with partition count %d is not present in metadata after %d ms.",
                                partition, topic, partitionsCount, maxWaitMs);
                if (metadata.getError(topic) != null && metadata.getError(topic).exception() instanceof RetriableException) {
                    throw new TimeoutException(errorMessage, metadata.getError(topic).exception());
                }
                throw new TimeoutException(errorMessage);
            }
            metadata.maybeThrowExceptionForTopic(topic);
            remainingWaitMs = maxWaitMs - elapsed;
            partitionsCount = cluster.partitionCountForTopic(topic);
        } while (partitionsCount == null || (partition != null && partition >= partitionsCount));

        producerMetrics.recordMetadataWait(time.nanoseconds() - nowNanos);

        return new ClusterAndWaitTime(cluster, elapsed);
    }

    /**
     * Validate that the record size isn't too large
     */
    private void ensureValidRecordSize(int size) {
        if (size > maxRequestSize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than " + maxRequestSize + ", which is the value of the " +
                    ProducerConfig.MAX_REQUEST_SIZE_CONFIG + " configuration.");
        if (size > totalMemorySize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the total memory buffer you have configured with the " +
                    ProducerConfig.BUFFER_MEMORY_CONFIG +
                    " configuration.");
    }

    /**
     * Invoking this method makes all buffered records immediately available to send (even if <code>linger.ms</code> is
     * greater than 0) and blocks on the completion of the requests associated with these records. The post-condition
     * of <code>flush()</code> is that any previously sent record will have completed (e.g. <code>Future.isDone() == true</code>).
     * A request is considered completed when it is successfully acknowledged
     * according to the <code>acks</code> configuration you have specified or else it results in an error.
     * <p>
     * Other threads can continue sending records while one thread is blocked waiting for a flush call to complete,
     * however no guarantee is made about the completion of records sent after the flush call begins.
     * <p>
     * This method can be useful when consuming from some input system and producing into Kafka. The <code>flush()</code> call
     * gives a convenient way to ensure all previously sent messages have actually completed.
     * <p>
     * This example shows how to consume from one Kafka topic and produce to another Kafka topic:
     * <pre>
     * {@code
     * for(ConsumerRecord<String, String> record: consumer.poll(100))
     *     producer.send(new ProducerRecord("my-topic", record.key(), record.value());
     * producer.flush();
     * consumer.commitSync();
     * }
     * </pre>
     *
     * Note that the above example may drop records if the produce request fails. If we want to ensure that this does not occur
     * we need to set <code>retries=&lt;large_number&gt;</code> in our config.
     * </p>
     * <p>
     * Applications don't need to call this method for transactional producers, since the {@link #commitTransaction()} will
     * flush all buffered records before performing the commit. This ensures that all the {@link #send(ProducerRecord)}
     * calls made since the previous {@link #beginTransaction()} are completed before the commit.
     * </p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void flush() {
        log.trace("Flushing accumulated records in producer.");

        long start = time.nanoseconds();
        this.accumulator.beginFlush();
        this.sender.wakeup();
        try {
            this.accumulator.awaitFlushCompletion();
        } catch (InterruptedException e) {
            throw new InterruptException("Flush interrupted.", e);
        } finally {
            producerMetrics.recordFlush(time.nanoseconds() - start);
        }
    }

    /**
     * Get the partition metadata for the given topic. This can be used for custom partitioning.
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException if not authorized to the specified topic. See the exception for more details
     * @throws InterruptException if the thread is interrupted while blocked
     * @throws TimeoutException if metadata could not be refreshed within {@code max.block.ms}
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method is called after producer close
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        Objects.requireNonNull(topic, "topic cannot be null");
        try {
            return waitOnMetadata(topic, null, time.milliseconds(), maxBlockTimeMs).cluster.partitionsForTopic(topic);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    /**
     * Get the full set of internal metrics maintained by the producer.
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * Determines the client's unique client instance ID used for telemetry. This ID is unique to
     * this specific client instance and will not change after it is initially generated.
     * The ID is useful for correlating client operations with telemetry sent to the broker and
     * to its eventual monitoring destinations.
     * <p>
     * If telemetry is enabled, this will first require a connection to the cluster to generate
     * the unique client instance ID. This method waits up to {@code timeout} for the producer
     * client to complete the request.
     * <p>
     * Client telemetry is controlled by the {@link ProducerConfig#ENABLE_METRICS_PUSH_CONFIG}
     * configuration option.
     *
     * @param timeout The maximum time to wait for producer client to determine its client instance ID.
     *                The value must be non-negative. Specifying a timeout of zero means do not
     *                wait for the initial request to complete if it hasn't already.
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to determine the client
     *                        instance ID, though this error does not necessarily imply the
     *                        producer client is otherwise unusable.
     * @throws IllegalArgumentException If the {@code timeout} is negative.
     * @throws IllegalStateException If telemetry is not enabled ie, config `{@code enable.metrics.push}`
     *                               is set to `{@code false}`.
     * @return The client's assigned instance id used for metrics collection.
     */
    @Override
    public Uuid clientInstanceId(Duration timeout) {
        if (!clientTelemetryReporter.isPresent()) {
            throw new IllegalStateException("Telemetry is not enabled. Set config `" + ProducerConfig.ENABLE_METRICS_PUSH_CONFIG + "` to `true`.");
        }

        return ClientTelemetryUtils.fetchClientInstanceId(clientTelemetryReporter.get(), timeout);
    }

    /**
     * Close this producer. This method blocks until all previously sent requests complete.
     * This method is equivalent to <code>close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)</code>.
     * <p>
     * <strong>If close() is called from {@link Callback}, a warning message will be logged and close(0, TimeUnit.MILLISECONDS)
     * will be called instead. We do this because the sender thread would otherwise try to join itself and
     * block forever.</strong>
     * <p>
     *
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to close the client, this error should be treated
     *                        as fatal and indicate the client is no longer usable.
     */
    @Override
    public void close() {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
     * <p>
     * If the producer is unable to complete all requests before the timeout expires, this method will fail
     * any unsent and unacknowledged records immediately. It will also abort the ongoing transaction if it's not
     * already completing.
     * <p>
     * If invoked from within a {@link Callback} this method will not block and will be equivalent to
     * <code>close(Duration.ofMillis(0))</code>. This is done since no further sending will happen while
     * blocking the I/O thread of the producer.
     *
     * @param timeout The maximum time to wait for producer to complete any pending requests. The value should be
     *                non-negative. Specifying a timeout of zero means do not wait for pending send requests to complete.
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to close the client, this error should be treated
     *                        as fatal and indicate the client is no longer usable.
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     *
     */
    @Override
    public void close(Duration timeout) {
        close(timeout, false);
    }

    private void close(Duration timeout, boolean swallowException) {
        long timeoutMs = timeout.toMillis();
        if (timeoutMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        log.info("Closing the Kafka producer with timeoutMillis = {} ms.", timeoutMs);

        // this will keep track of the first encountered exception
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        boolean invokedFromCallback = Thread.currentThread() == this.ioThread;
        if (timeoutMs > 0) {
            if (invokedFromCallback) {
                log.warn("Overriding close timeout {} ms to 0 ms in order to prevent useless blocking due to self-join. " +
                        "This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.",
                        timeoutMs);
            } else {
                // Try to close gracefully.
                final Timer closeTimer = time.timer(timeout);
                clientTelemetryReporter.ifPresent(ClientTelemetryReporter::initiateClose);
                closeTimer.update();

                if (this.sender != null) {
                    this.sender.initiateClose();
                    closeTimer.update();
                }
                if (this.ioThread != null) {
                    try {
                        this.ioThread.join(closeTimer.remainingMs());
                    } catch (InterruptedException t) {
                        firstException.compareAndSet(null, new InterruptException(t));
                        log.error("Interrupted while joining ioThread", t);
                    } finally {
                        closeTimer.update();
                    }
                }
            }
        }

        if (this.sender != null && this.ioThread != null && this.ioThread.isAlive()) {
            log.info("Proceeding to force close the producer since pending requests could not be completed " +
                    "within timeout {} ms.", timeoutMs);
            this.sender.forceClose();
            // Only join the sender thread when not calling from callback.
            if (!invokedFromCallback) {
                try {
                    this.ioThread.join();
                } catch (InterruptedException e) {
                    firstException.compareAndSet(null, new InterruptException(e));
                }
            }
        }

        Utils.closeQuietly(interceptors, "producer interceptors", firstException);
        Utils.closeQuietly(producerMetrics, "producer metrics wrapper", firstException);
        Utils.closeQuietly(metrics, "producer metrics", firstException);
        Utils.closeQuietly(keySerializer, "producer keySerializer", firstException);
        Utils.closeQuietly(valueSerializer, "producer valueSerializer", firstException);
        Utils.closeQuietly(partitioner, "producer partitioner", firstException);
        clientTelemetryReporter.ifPresent(reporter -> Utils.closeQuietly(reporter, "producer telemetry reporter", firstException));
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka producer", exception);
        }
        log.debug("Kafka producer has been closed");
    }

    /**
     * computes partition for given record.
     * if the record has partition returns the value otherwise
     * if custom partitioner is specified, call it to compute partition
     * otherwise try to calculate partition based on key.
     * If there is no key or key should be ignored return
     * RecordMetadata.UNKNOWN_PARTITION to indicate any partition
     * can be used (the partition is then calculated by built-in
     * partitioning logic).
     */
    private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
        if (record.partition() != null)
            return record.partition();

        if (partitioner != null) {
            int customPartition = partitioner.partition(
                record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
            if (customPartition < 0) {
                throw new IllegalArgumentException(String.format(
                    "The partitioner generated an invalid partition number: %d. Partition number should always be non-negative.", customPartition));
            }
            return customPartition;
        }

        if (serializedKey != null && !partitionerIgnoreKeys) {
            // hash the keyBytes to choose a partition
            return BuiltInPartitioner.partitionForKey(serializedKey, cluster.partitionsForTopic(record.topic()).size());
        } else {
            return RecordMetadata.UNKNOWN_PARTITION;
        }
    }

    private void throwIfInvalidGroupMetadata(ConsumerGroupMetadata groupMetadata) {
        if (groupMetadata == null) {
            throw new IllegalArgumentException("Consumer group metadata could not be null");
        } else if (groupMetadata.generationId() > 0
            && JoinGroupRequest.UNKNOWN_MEMBER_ID.equals(groupMetadata.memberId())) {
            throw new IllegalArgumentException("Passed in group metadata " + groupMetadata + " has generationId > 0 but member.id ");
        }
    }

    private void throwIfNoTransactionManager() {
        if (transactionManager == null)
            throw new IllegalStateException("Cannot use transactional methods without enabling transactions " +
                    "by setting the " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " configuration property");
    }

    // Visible for testing
    String getClientId() {
        return clientId;
    }

    private static class ClusterAndWaitTime {
        final Cluster cluster;
        final long waitedOnMetadataMs;
        ClusterAndWaitTime(Cluster cluster, long waitedOnMetadataMs) {
            this.cluster = cluster;
            this.waitedOnMetadataMs = waitedOnMetadataMs;
        }
    }

    private static class FutureFailure implements Future<RecordMetadata> {

        private final ExecutionException exception;

        public FutureFailure(Exception exception) {
            this.exception = new ExecutionException(exception);
        }

        @Override
        public boolean cancel(boolean interrupt) {
            return false;
        }

        @Override
        public RecordMetadata get() throws ExecutionException {
            throw this.exception;
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
            throw this.exception;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

    }

    /**
     * Callbacks that are called by the RecordAccumulator append functions:
     *  - user callback
     *  - interceptor callbacks
     *  - partition callback
     */
    private class AppendCallbacks implements RecordAccumulator.AppendCallbacks {
        private final Callback userCallback;
        private final ProducerInterceptors<K, V> interceptors;
        private final String topic;
        private final Integer recordPartition;
        private final String recordLogString;
        private volatile int partition = RecordMetadata.UNKNOWN_PARTITION;
        private volatile TopicPartition topicPartition;

        private AppendCallbacks(Callback userCallback, ProducerInterceptors<K, V> interceptors, ProducerRecord<K, V> record) {
            this.userCallback = userCallback;
            this.interceptors = interceptors;
            // Extract record info as we don't want to keep a reference to the record during
            // whole lifetime of the batch.
            // We don't want to have an NPE here, because the interceptors would not be notified (see .doSend).
            topic = record != null ? record.topic() : null;
            recordPartition = record != null ? record.partition() : null;
            recordLogString = log.isTraceEnabled() && record != null ? record.toString() : "";
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (metadata == null) {
                metadata = new RecordMetadata(topicPartition(), -1, -1, RecordBatch.NO_TIMESTAMP, -1, -1);
            }
            this.interceptors.onAcknowledgement(metadata, exception);
            if (this.userCallback != null)
                this.userCallback.onCompletion(metadata, exception);
        }

        @Override
        public void setPartition(int partition) {
            assert partition != RecordMetadata.UNKNOWN_PARTITION;
            this.partition = partition;

            if (log.isTraceEnabled()) {
                // Log the message here, because we don't know the partition before that.
                log.trace("Attempting to append record {} with callback {} to topic {} partition {}", recordLogString, userCallback, topic, partition);
            }
        }

        public int getPartition() {
            return partition;
        }

        public TopicPartition topicPartition() {
            if (topicPartition == null && topic != null) {
                if (partition != RecordMetadata.UNKNOWN_PARTITION)
                    topicPartition = new TopicPartition(topic, partition);
                else if (recordPartition != null)
                    topicPartition = new TopicPartition(topic, recordPartition);
                else
                    topicPartition = new TopicPartition(topic, RecordMetadata.UNKNOWN_PARTITION);
            }
            return topicPartition;
        }
    }
}
