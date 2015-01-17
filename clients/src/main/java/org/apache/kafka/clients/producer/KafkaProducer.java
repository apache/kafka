/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.producer.internals.Metadata;
import org.apache.kafka.clients.producer.internals.Partitioner;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.ClientUtils;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Kafka client that publishes records to the Kafka cluster.
 * <P>
 * The producer is <i>thread safe</i> and should generally be shared among all threads for best performance.
 * <p>
 * The producer manages a single background thread that does I/O as well as a TCP connection to each of the brokers it
 * needs to communicate with. Failure to close the producer after use will leak these resources.
 */
public class KafkaProducer<K,V> implements Producer<K,V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    private final Partitioner partitioner;
    private final int maxRequestSize;
    private final long metadataFetchTimeoutMs;
    private final long totalMemorySize;
    private final Metadata metadata;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Metrics metrics;
    private final Thread ioThread;
    private final CompressionType compressionType;
    private final Sensor errors;
    private final Time time;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final ProducerConfig producerConfig;
    private static final AtomicInteger producerAutoId = new AtomicInteger(1);

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * @param configs   The producer configs
     *
     */
    public KafkaProducer(Map<String, Object> configs) {
        this(new ProducerConfig(configs), null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * Values can be either strings or Objects of the appropriate type (for example a numeric configuration would accept
     * either the string "42" or the integer 42).
     * @param configs   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called when the serializer is passed in directly.
     */
    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(addSerializerToConfig(configs, keySerializer, valueSerializer)),
             keySerializer, valueSerializer);
    }

    private static Map<String, Object> addSerializerToConfig(Map<String, Object> configs,
                                                      Serializer<?> keySerializer, Serializer<?> valueSerializer) {
        Map<String, Object> newConfigs = new HashMap<String, Object>();
        newConfigs.putAll(configs);
        if (keySerializer != null)
            newConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass());
        if (valueSerializer != null)
            newConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass());
        return newConfigs;
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * @param properties   The producer configs
     */
    public KafkaProducer(Properties properties) {
        this(new ProducerConfig(properties), null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * @param properties   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called when the serializer is passed in directly.
     */
    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(addSerializerToConfig(properties, keySerializer, valueSerializer)),
             keySerializer, valueSerializer);
    }

    private static Properties addSerializerToConfig(Properties properties,
                                                    Serializer<?> keySerializer, Serializer<?> valueSerializer) {
        Properties newProperties = new Properties();
        newProperties.putAll(properties);
        if (keySerializer != null)
            newProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass().getName());
        if (valueSerializer != null)
            newProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass().getName());
        return newProperties;
    }

    private KafkaProducer(ProducerConfig config, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        log.trace("Starting the Kafka producer");
        this.producerConfig = config;
        this.time = new SystemTime();
        MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                                                      .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                                                                  TimeUnit.MILLISECONDS);
        String clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
        if(clientId.length() <= 0)
          clientId = "producer-" + producerAutoId.getAndIncrement();
        String jmxPrefix = "kafka.producer";
        List<MetricsReporter> reporters = config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                                                                        MetricsReporter.class);
        reporters.add(new JmxReporter(jmxPrefix));
        this.metrics = new Metrics(metricConfig, reporters, time);
        this.partitioner = new Partitioner();
        long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
        this.metadataFetchTimeoutMs = config.getLong(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG);
        this.metadata = new Metadata(retryBackoffMs, config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG));
        this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
        this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
        this.compressionType = CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        Map<String, String> metricTags = new LinkedHashMap<String, String>();
        metricTags.put("client-id", clientId);
        this.accumulator = new RecordAccumulator(config.getInt(ProducerConfig.BATCH_SIZE_CONFIG),
                                                 this.totalMemorySize,
                                                 config.getLong(ProducerConfig.LINGER_MS_CONFIG),
                                                 retryBackoffMs,
                                                 config.getBoolean(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG),
                                                 metrics,
                                                 time,
                                                 metricTags);
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        this.metadata.update(Cluster.bootstrap(addresses), time.milliseconds());

        NetworkClient client = new NetworkClient(new Selector(this.metrics, time , "producer", metricTags),
                                                 this.metadata,
                                                 clientId,
                                                 config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION),
                                                 config.getLong(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                                                 config.getInt(ProducerConfig.SEND_BUFFER_CONFIG),
                                                 config.getInt(ProducerConfig.RECEIVE_BUFFER_CONFIG));
        this.sender = new Sender(client,
                                 this.metadata,
                                 this.accumulator,
                                 config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
                                 (short) parseAcks(config.getString(ProducerConfig.ACKS_CONFIG)),
                                 config.getInt(ProducerConfig.RETRIES_CONFIG),
                                 config.getInt(ProducerConfig.TIMEOUT_CONFIG),
                                 this.metrics,
                                 new SystemTime(),
                                 clientId);
        String ioThreadName = "kafka-producer-network-thread" + (clientId.length() > 0 ? " | " + clientId : "");
        this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
        this.ioThread.start();

        this.errors = this.metrics.sensor("errors");

        if (keySerializer == null) {
            this.keySerializer = config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                                              Serializer.class);
            this.keySerializer.configure(config.originals(), true);
        }
        else
            this.keySerializer = keySerializer;
        if (valueSerializer == null) {
            this.valueSerializer = config.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                                                Serializer.class);
            this.valueSerializer.configure(config.originals(), false);
        }
        else
            this.valueSerializer = valueSerializer;

        config.logUnused();
        log.debug("Kafka producer started");
    }

    private static int parseAcks(String acksString) {
        try {
            return acksString.trim().toLowerCase().equals("all") ? -1 : Integer.parseInt(acksString.trim());
        } catch (NumberFormatException e) {
            throw new ConfigException("Invalid configuration value for 'acks': " + acksString);
        }
    }

    /**
     * Asynchronously send a record to a topic. Equivalent to {@link #send(ProducerRecord, Callback) send(record, null)}
     * @param record  The record to be sent
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K,V> record) {
        return send(record, null);
    }

    /**
     * Asynchronously send a record to a topic and invoke the provided callback when the send has been acknowledged.
     * <p>
     * The send is asynchronous and this method will return immediately once the record has been stored in the buffer of
     * records waiting to be sent. This allows sending many records in parallel without blocking to wait for the
     * response after each one.
     * <p>
     * The result of the send is a {@link RecordMetadata} specifying the partition the record was sent to and the offset
     * it was assigned.
     * <p>
     * Since the send call is asynchronous it returns a {@link java.util.concurrent.Future Future} for the
     * {@link RecordMetadata} that will be assigned to this record. Invoking {@link java.util.concurrent.Future#get()
     * get()} on this future will result in the metadata for the record or throw any exception that occurred while
     * sending the record.
     * <p>
     * If you want to simulate a simple blocking call you can do the following:
     *
     * <pre>{@code
     * producer.send(new ProducerRecord<byte[],byte[]>("the-topic", "key".getBytes(), "value".getBytes())).get();
     * }</pre>
     * <p>
     * Those desiring fully non-blocking usage can make use of the {@link Callback} parameter to provide a callback that
     * will be invoked when the request is complete.
     *
     * <pre>{@code
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("the-topic", "key".getBytes(), "value".getBytes());
     *   producer.send(myRecord,
     *                new Callback() {
     *                     public void onCompletion(RecordMetadata metadata, Exception e) {
     *                         if(e != null)
     *                             e.printStackTrace();
     *                         System.out.println("The offset of the record we just sent is: " + metadata.offset());
     *                     }
     *                });
     * }</pre>
     *
     * Callbacks for records being sent to the same partition are guaranteed to execute in order. That is, in the
     * following example <code>callback1</code> is guaranteed to execute before <code>callback2</code>:
     *
     * <pre>{@code
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key1, value1), callback1);
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key2, value2), callback2);
     * }</pre>
     * <p>
     * Note that callbacks will generally execute in the I/O thread of the producer and so should be reasonably fast or
     * they will delay the sending of messages from other threads. If you want to execute blocking or computationally
     * expensive callbacks it is recommended to use your own {@link java.util.concurrent.Executor} in the callback body
     * to parallelize processing.
     * <p>
     * The producer manages a buffer of records waiting to be sent. This buffer has a hard limit on it's size, which is
     * controlled by the configuration <code>total.memory.bytes</code>. If <code>send()</code> is called faster than the
     * I/O thread can transfer data to the brokers the buffer will eventually run out of space. The default behavior in
     * this case is to block the send call until the I/O thread catches up and more buffer space is available. However
     * in cases where non-blocking usage is desired the setting <code>block.on.buffer.full=false</code> will cause the
     * producer to instead throw an exception when buffer memory is exhausted.
     * 
     * @param record The record to send
     * @param callback A user-supplied callback to execute when the record has been acknowledged by the server (null
     *        indicates no callback)
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K,V> record, Callback callback) {
        try {
            // first make sure the metadata for the topic is available
            waitOnMetadata(record.topic(), this.metadataFetchTimeoutMs);
            byte[] serializedKey;
            try {
                serializedKey = keySerializer.serialize(record.topic(), record.key());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in key.serializer");
            }
            byte[] serializedValue;
            try {
                serializedValue = valueSerializer.serialize(record.topic(), record.value());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in value.serializer");
            }
            ProducerRecord<byte[], byte[]> serializedRecord = new ProducerRecord<byte[], byte[]>(record.topic(), record.partition(), serializedKey, serializedValue);
            int partition = partitioner.partition(serializedRecord, metadata.fetch());
            int serializedSize = Records.LOG_OVERHEAD + Record.recordSize(serializedKey, serializedValue);
            ensureValidRecordSize(serializedSize);
            TopicPartition tp = new TopicPartition(record.topic(), partition);
            log.trace("Sending record {} with callback {} to topic {} partition {}", record, callback, record.topic(), partition);
            RecordAccumulator.RecordAppendResult result = accumulator.append(tp, serializedKey, serializedValue, compressionType, callback);
            if (result.batchIsFull || result.newBatchCreated) {
                log.trace("Waking up the sender since topic {} partition {} is either full or getting a new batch", record.topic(), partition);
                this.sender.wakeup();
            }
            return result.future;
            // Handling exceptions and record the errors;
            // For API exceptions return them in the future,
            // for other exceptions throw directly
        } catch (ApiException e) {
            log.debug("Exception occurred during message send:", e);
            if (callback != null)
                callback.onCompletion(null, e);
            this.errors.record();
            return new FutureFailure(e);
        } catch (InterruptedException e) {
            this.errors.record();
            throw new KafkaException(e);
        } catch (KafkaException e) {
            this.errors.record();
            throw e;
        }
    }

    /**
     * Wait for cluster metadata including partitions for the given topic to be available.
     * @param topic The topic we want metadata for
     * @param maxWaitMs The maximum time in ms for waiting on the metadata
     */
    private void waitOnMetadata(String topic, long maxWaitMs) {
        if (metadata.fetch().partitionsForTopic(topic) != null) {
            return;
        } else {
            long begin = time.milliseconds();
            long remainingWaitMs = maxWaitMs;
            while (metadata.fetch().partitionsForTopic(topic) == null) {
                log.trace("Requesting metadata update for topic {}.", topic);
                int version = metadata.requestUpdate();
                metadata.add(topic);
                sender.wakeup();
                metadata.awaitUpdate(version, remainingWaitMs);
                long elapsed = time.milliseconds() - begin;
                if (elapsed >= maxWaitMs)
                    throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
                remainingWaitMs = maxWaitMs - elapsed;
            }
        }
    }

    /**
     * Validate that the record size isn't too large
     */
    private void ensureValidRecordSize(int size) {
        if (size > this.maxRequestSize)
            throw new RecordTooLargeException("The message is " + size +
                                              " bytes when serialized which is larger than the maximum request size you have configured with the " +
                                              ProducerConfig.MAX_REQUEST_SIZE_CONFIG +
                                              " configuration.");
        if (size > this.totalMemorySize)
            throw new RecordTooLargeException("The message is " + size +
                                              " bytes when serialized which is larger than the total memory buffer you have configured with the " +
                                              ProducerConfig.BUFFER_MEMORY_CONFIG +
                                              " configuration.");
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        waitOnMetadata(topic, this.metadataFetchTimeoutMs);
        return this.metadata.fetch().partitionsForTopic(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * Close this producer. This method blocks until all in-flight requests complete.
     */
    @Override
    public void close() {
        log.trace("Closing the Kafka producer.");
        this.sender.initiateClose();
        try {
            this.ioThread.join();
        } catch (InterruptedException e) {
            throw new KafkaException(e);
        }
        this.metrics.close();
        this.keySerializer.close();
        this.valueSerializer.close();
        log.debug("The Kafka producer has closed.");
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

}
