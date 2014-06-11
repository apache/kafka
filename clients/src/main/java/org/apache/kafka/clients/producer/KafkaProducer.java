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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.producer.internals.FutureRecordMetadata;
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
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
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
public class KafkaProducer implements Producer {

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

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     */
    public KafkaProducer(Map<String, Object> configs) {
        this(new ProducerConfig(configs));
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     */
    public KafkaProducer(Properties properties) {
        this(new ProducerConfig(properties));
    }

    private KafkaProducer(ProducerConfig config) {
        log.trace("Starting the Kafka producer");
        Time time = new SystemTime();
        MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                                                      .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                                                                  TimeUnit.MILLISECONDS);
        String clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
        String jmxPrefix = "kafka.producer." + (clientId.length() > 0 ? clientId + "." : "");
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
        this.accumulator = new RecordAccumulator(config.getInt(ProducerConfig.BATCH_SIZE_CONFIG),
                                                 this.totalMemorySize,
                                                 config.getLong(ProducerConfig.LINGER_MS_CONFIG),
                                                 retryBackoffMs,
                                                 config.getBoolean(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG),
                                                 metrics,
                                                 time);
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        this.metadata.update(Cluster.bootstrap(addresses), 0);

        NetworkClient client = new NetworkClient(new Selector(this.metrics, time),
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
                                 new SystemTime());
        this.ioThread = new KafkaThread("kafka-producer-network-thread", this.sender, true);
        this.ioThread.start();

        this.errors = this.metrics.sensor("errors");

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
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord record) {
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
     * <pre>
     *   producer.send(new ProducerRecord("the-topic", "key, "value")).get();
     * </pre>
     * <p>
     * Those desiring fully non-blocking usage can make use of the {@link Callback} parameter to provide a callback that
     * will be invoked when the request is complete.
     * 
     * <pre>
     *   ProducerRecord record = new ProducerRecord("the-topic", "key, "value");
     *   producer.send(myRecord,
     *                 new Callback() {
     *                     public void onCompletion(RecordMetadata metadata, Exception e) {
     *                         if(e != null)
     *                             e.printStackTrace();
     *                         System.out.println("The offset of the record we just sent is: " + metadata.offset());
     *                     }
     *                 });
     * </pre>
     * 
     * Callbacks for records being sent to the same partition are guaranteed to execute in order. That is, in the
     * following example <code>callback1</code> is guaranteed to execute before <code>callback2</code>:
     * 
     * <pre>
     * producer.send(new ProducerRecord(topic, partition, key, value), callback1);
     * producer.send(new ProducerRecord(topic, partition, key2, value2), callback2);
     * </pre>
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
    public Future<RecordMetadata> send(ProducerRecord record, Callback callback) {
        try {
            Cluster cluster = metadata.fetch(record.topic(), this.metadataFetchTimeoutMs);
            int partition = partitioner.partition(record, cluster);
            int serializedSize = Records.LOG_OVERHEAD + Record.recordSize(record.key(), record.value());
            ensureValidRecordSize(serializedSize);
            TopicPartition tp = new TopicPartition(record.topic(), partition);
            log.trace("Sending record {} with callback {} to topic {} partition {}", record, callback, record.topic(), partition);
            FutureRecordMetadata future = accumulator.append(tp, record.key(), record.value(), compressionType, callback);
            this.sender.wakeup();
            return future;
            // For API exceptions return them in the future;
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

    public List<PartitionInfo> partitionsFor(String topic) {
        return this.metadata.fetch(topic, this.metadataFetchTimeoutMs).partitionsForTopic(topic);
    }

    @Override
    public Map<String, ? extends Metric> metrics() {
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
