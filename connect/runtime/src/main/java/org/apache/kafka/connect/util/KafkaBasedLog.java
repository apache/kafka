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
package org.apache.kafka.connect.util;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;


/**
 * <p>
 *     KafkaBasedLog provides a generic implementation of a shared, compacted log of records stored in Kafka that all
 *     clients need to consume and, at times, agree on their offset / that they have read to the end of the log.
 * </p>
 * <p>
 *     This functionality is useful for storing different types of data that all clients may need to agree on --
 *     offsets or config for example. This class runs a consumer in a background thread to continuously tail the target
 *     topic, accepts write requests which it writes to the topic using an internal producer, and provides some helpful
 *     utilities like checking the current log end offset and waiting until the current end of the log is reached.
 * </p>
 * <p>
 *     To support different use cases, this class works with either single- or multi-partition topics.
 * </p>
 * <p>
 *     Since this class is generic, it delegates the details of data storage via a callback that is invoked for each
 *     record that is consumed from the topic. The invocation of callbacks is guaranteed to be serialized -- if the
 *     calling class keeps track of state based on the log and only writes to it when consume callbacks are invoked
 *     and only reads it in {@link #readToEnd(Callback)} callbacks then no additional synchronization will be required.
 * </p>
 * <p>
 *     This is a useful utility that has been used outside of Connect. This isn't in Connect's public API,
 *     but we've tried to maintain the method signatures and backward compatibility since early Kafka versions.
 * </p>
 */
public class KafkaBasedLog<K, V> {
    private static final Logger log = LoggerFactory.getLogger(KafkaBasedLog.class);
    private static final long CREATE_TOPIC_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(30);
    private static final long MAX_SLEEP_MS = TimeUnit.SECONDS.toMillis(1);
    // 15min of admin retry duration to ensure successful metadata propagation.  10 seconds of backoff
    // in between retries
    private static final Duration ADMIN_CLIENT_RETRY_DURATION = Duration.ofMinutes(15);
    private static final long ADMIN_CLIENT_RETRY_BACKOFF_MS = TimeUnit.SECONDS.toMillis(10);

    private final Time time;
    private final String topic;
    private int partitionCount;
    private final Map<String, Object> producerConfigs;
    private final Map<String, Object> consumerConfigs;
    private final Callback<ConsumerRecord<K, V>> consumedCallback;
    private final Supplier<TopicAdmin> topicAdminSupplier;
    private final boolean requireAdminForOffsets;
    private Consumer<K, V> consumer;
    private Optional<Producer<K, V>> producer;
    private TopicAdmin admin;
    // Visible for testing
    Thread thread;
    private boolean stopRequested;
    private final Queue<Callback<Void>> readLogEndOffsetCallbacks;
    private final java.util.function.Consumer<TopicAdmin> initializer;
    // initialized as false for backward compatibility
    private volatile boolean reportErrorsToCallback = false;

    /**
     * Create a new KafkaBasedLog object. This does not start reading the log and writing is not permitted until
     * {@link #start()} is invoked.
     *
     * @param topic the topic to treat as a log
     * @param producerConfigs configuration options to use when creating the internal producer. At a minimum this must
     *                        contain compatible serializer settings for the generic types used on this class. Some
     *                        setting, such as the number of acks, will be overridden to ensure correct behavior of this
     *                        class.
     * @param consumerConfigs configuration options to use when creating the internal consumer. At a minimum this must
     *                        contain compatible serializer settings for the generic types used on this class. Some
     *                        setting, such as the auto offset reset policy, will be overridden to ensure correct
     *                        behavior of this class.
     * @param consumedCallback callback to invoke for each {@link ConsumerRecord} consumed when tailing the log
     * @param time Time interface
     * @param initializer the component that should be run when this log is {@link #start() started}; may be null
     * @deprecated Replaced by {@link #KafkaBasedLog(String, Map, Map, Supplier, Callback, Time, java.util.function.Consumer)}
     */
    @Deprecated
    public KafkaBasedLog(String topic,
                         Map<String, Object> producerConfigs,
                         Map<String, Object> consumerConfigs,
                         Callback<ConsumerRecord<K, V>> consumedCallback,
                         Time time,
                         Runnable initializer) {
        this(topic, producerConfigs, consumerConfigs, () -> null, consumedCallback, time, initializer != null ? admin -> initializer.run() : null);
    }

    /**
     * Create a new KafkaBasedLog object. This does not start reading the log and writing is not permitted until
     * {@link #start()} is invoked.
     *
     * @param topic              the topic to treat as a log
     * @param producerConfigs    configuration options to use when creating the internal producer. At a minimum this must
     *                           contain compatible serializer settings for the generic types used on this class. Some
     *                           setting, such as the number of acks, will be overridden to ensure correct behavior of this
     *                           class.
     * @param consumerConfigs    configuration options to use when creating the internal consumer. At a minimum this must
     *                           contain compatible serializer settings for the generic types used on this class. Some
     *                           setting, such as the auto offset reset policy, will be overridden to ensure correct
     *                           behavior of this class.
     * @param topicAdminSupplier supplier function for an admin client, the lifecycle of which is expected to be controlled
     *                           by the calling component; may not be null
     * @param consumedCallback   callback to invoke for each {@link ConsumerRecord} consumed when tailing the log
     * @param time               Time interface
     * @param initializer        the function that should be run when this log is {@link #start() started}; may be null
     */
    public KafkaBasedLog(String topic,
                         Map<String, Object> producerConfigs,
                         Map<String, Object> consumerConfigs,
                         Supplier<TopicAdmin> topicAdminSupplier,
                         Callback<ConsumerRecord<K, V>> consumedCallback,
                         Time time,
                         java.util.function.Consumer<TopicAdmin> initializer) {
        this.topic = topic;
        this.producerConfigs = producerConfigs;
        this.consumerConfigs = consumerConfigs;
        this.topicAdminSupplier = Objects.requireNonNull(topicAdminSupplier);
        this.consumedCallback = consumedCallback;
        this.stopRequested = false;
        this.readLogEndOffsetCallbacks = new ArrayDeque<>();
        this.time = time;
        this.initializer = initializer != null ? initializer : admin -> { };
        // Initialize the producer Optional here to prevent NPEs later on
        this.producer = Optional.empty();

        // If the consumer is configured with isolation.level = read_committed, then its end offsets method cannot be relied on
        // as it will not take records from currently-open transactions into account. We want to err on the side of caution in that
        // case: when users request a read to the end of the log, we will read up to the point where the latest offsets visible to the
        // consumer are at least as high as the (possibly-part-of-a-transaction) end offsets of the topic.
        this.requireAdminForOffsets = IsolationLevel.READ_COMMITTED.toString()
                .equals(consumerConfigs.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
    }

    /**
     * Create a new KafkaBasedLog object using pre-existing Kafka clients. This does not start reading the log and writing
     * is not permitted until {@link #start()} is invoked. Note that the consumer and (if not null) producer given to this log
     * will be closed when this log is {@link #stop() stopped}.
     *
     * @param topic the topic to treat as a log
     * @param consumer the consumer to use for reading from the log; may not be null
     * @param producer the producer to use for writing to the log; may be null, which will create a read-only log
     * @param topicAdmin an admin client, the lifecycle of which is expected to be controlled by the calling component;
     *                   may not be null
     * @param consumedCallback   callback to invoke for each {@link ConsumerRecord} consumed when tailing the log
     * @param time               Time interface
     * @param initializer        the function that should be run when this log is {@link #start() started}; may be null
     * @param readTopicPartition A predicate which returns true for each {@link TopicPartition} that should be read
     * @return a {@link KafkaBasedLog} using the given clients
     */
    public static <K, V> KafkaBasedLog<K, V> withExistingClients(String topic,
                                                                 Consumer<K, V> consumer,
                                                                 Producer<K, V> producer,
                                                                 TopicAdmin topicAdmin,
                                                                 Callback<ConsumerRecord<K, V>> consumedCallback,
                                                                 Time time,
                                                                 java.util.function.Consumer<TopicAdmin> initializer,
                                                                 Predicate<TopicPartition> readTopicPartition
    ) {
        Objects.requireNonNull(topicAdmin);
        Objects.requireNonNull(readTopicPartition);
        return new KafkaBasedLog<>(topic,
                Collections.emptyMap(),
                Collections.emptyMap(),
                () -> topicAdmin,
                consumedCallback,
                time,
                initializer) {

            @Override
            protected Producer<K, V> createProducer() {
                return producer;
            }

            @Override
            protected Consumer<K, V> createConsumer() {
                return consumer;
            }

            @Override
            protected boolean readPartition(TopicPartition topicPartition) {
                return readTopicPartition.test(topicPartition);
            }

            @Override
            public void stop() {
                super.stop();
                // Close the clients here, if the thread that was responsible for closing them was never started.
                Utils.closeQuietly(producer, "producer");
                Utils.closeQuietly(consumer, "consumer");
            }
        };
    }

    public void start() {
        start(false);
    }

    public void start(boolean reportErrorsToCallback) {
        this.reportErrorsToCallback = reportErrorsToCallback;
        log.info("Starting KafkaBasedLog with topic {} reportErrorsToCallback={}", topic, reportErrorsToCallback);

        // Create the topic admin client and initialize the topic ...
        admin = topicAdminSupplier.get();   // may be null
        if (admin == null && requireAdminForOffsets) {
            throw new ConnectException(
                    "Must provide a TopicAdmin to KafkaBasedLog when consumer is configured with "
                            + ConsumerConfig.ISOLATION_LEVEL_CONFIG + " set to "
                            + IsolationLevel.READ_COMMITTED
            );
        }
        initializer.accept(admin);

        // Then create the producer and consumer
        producer = Optional.ofNullable(createProducer());
        if (producer.isEmpty())
            log.trace("Creating read-only KafkaBasedLog for topic {}", topic);
        consumer = createConsumer();

        List<TopicPartition> partitions = new ArrayList<>();

        // We expect that the topics will have been created either manually by the user or automatically by the herder
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        long started = time.nanoseconds();
        long sleepMs = 100;
        while (partitionInfos.isEmpty() && time.nanoseconds() - started < CREATE_TOPIC_TIMEOUT_NS) {
            time.sleep(sleepMs);
            sleepMs = Math.min(2 * sleepMs, MAX_SLEEP_MS);
            partitionInfos = consumer.partitionsFor(topic);
        }
        if (partitionInfos.isEmpty())
            throw new ConnectException("Could not look up partition metadata for topic '" + topic + "' in the" +
                    " allotted period. This could indicate a connectivity issue, unavailable topic partitions, or if" +
                    " this is your first use of the topic it may have taken too long to create.");

        for (PartitionInfo partition : partitionInfos) {
            TopicPartition topicPartition = new TopicPartition(partition.topic(), partition.partition());
            if (readPartition(topicPartition)) {
                partitions.add(topicPartition);
            }
        }
        if (partitions.isEmpty()) {
            throw new ConnectException("Some partitions for " + topic + " exist, but no partitions matched the " +
                    "required filter.");
        }
        partitionCount = partitions.size();
        consumer.assign(partitions);

        // Always consume from the beginning of all partitions. Necessary to ensure that we don't use committed offsets
        // when a 'group.id' is specified (if offsets happen to have been committed unexpectedly).
        consumer.seekToBeginning(partitions);

        readToLogEnd(true);

        thread = new WorkThread();
        thread.start();

        log.info("Finished reading KafkaBasedLog for topic {}", topic);

        log.info("Started KafkaBasedLog for topic {}", topic);
    }

    public void stop() {
        log.info("Stopping KafkaBasedLog for topic {}", topic);

        synchronized (this) {
            stopRequested = true;
        }
        if (consumer != null) {
            consumer.wakeup();
        }

        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new ConnectException("Failed to stop KafkaBasedLog. Exiting without cleanly shutting " +
                        "down it's producer and consumer.", e);
            }
        }

        producer.ifPresent(p -> Utils.closeQuietly(p, "KafkaBasedLog producer for topic " + topic));
        Utils.closeQuietly(consumer, "KafkaBasedLog consumer for topic " + topic);

        // do not close the admin client, since we don't own it
        admin = null;

        log.info("Stopped KafkaBasedLog for topic {}", topic);
    }

    /**
     * Flushes any outstanding writes and then reads to the current end of the log and invokes the specified callback.
     * Note that this checks the current offsets, reads to them, and invokes the callback regardless of whether
     * additional records have been written to the log. If the caller needs to ensure they have truly reached the end
     * of the log, they must ensure there are no other writers during this period.
     * <p>
     * This waits until the end of all partitions has been reached.
     * <p>
     * This method is asynchronous. If you need a synchronous version, pass an instance of
     * {@link org.apache.kafka.connect.util.FutureCallback} as the {@code callback} parameter and wait on it to block.
     *
     * @param callback the callback to invoke once the end of the log has been reached.
     */
    public void readToEnd(Callback<Void> callback) {
        log.trace("Starting read to end log for topic {}", topic);
        flush();
        synchronized (this) {
            readLogEndOffsetCallbacks.add(callback);
        }
        consumer.wakeup();
    }

    /**
     * Flush the underlying producer to ensure that all pending writes have been sent.
     */
    public void flush() {
        producer.ifPresent(Producer::flush);
    }

    /**
     * Same as {@link #readToEnd(Callback)} but provides a {@link Future} instead of using a callback.
     * @return the future associated with the operation
     */
    public Future<Void> readToEnd() {
        FutureCallback<Void> future = new FutureCallback<>(null);
        readToEnd(future);
        return future;
    }

    /**
     * Send a record asynchronously to the configured {@link #topic} without using a producer callback.
     * <p>
     * This method exists for backward compatibility reasons and delegates to the newer
     * {@link #sendWithReceipt(Object, Object)} method that returns a future.
     * @param key the key for the {@link ProducerRecord}
     * @param value the value for the {@link ProducerRecord}
     */
    public void send(K key, V value) {
        sendWithReceipt(key, value);
    }

    /**
     * Send a record asynchronously to the configured {@link #topic}.
     * <p>
     * This method exists for backward compatibility reasons and delegates to the newer
     * {@link #sendWithReceipt(Object, Object, org.apache.kafka.clients.producer.Callback)} method that returns a future.
     * @param key the key for the {@link ProducerRecord}
     * @param value the value for the {@link ProducerRecord}
     * @param callback the callback to invoke after completion; can be null if no callback is desired
     */
    public void send(K key, V value, org.apache.kafka.clients.producer.Callback callback) {
        sendWithReceipt(key, value, callback);
    }

    /**
     * Send a record asynchronously to the configured {@link #topic} without using a producer callback.
     * @param key the key for the {@link ProducerRecord}
     * @param value the value for the {@link ProducerRecord}
     *
     * @return the future from the call to {@link Producer#send}. {@link Future#get} can be called on this returned
     *         future if synchronous behavior is desired.
     */
    public Future<RecordMetadata> sendWithReceipt(K key, V value) {
        return sendWithReceipt(key, value, null);
    }

    /**
     * Send a record asynchronously to the configured {@link #topic}.
     * @param key the key for the {@link ProducerRecord}
     * @param value the value for the {@link ProducerRecord}
     * @param callback the callback to invoke after completion; can be null if no callback is desired
     *
     * @return the future from the call to {@link Producer#send}. {@link Future#get} can be called on this returned
     *         future if synchronous behavior is desired.
     */
    public Future<RecordMetadata> sendWithReceipt(K key, V value, org.apache.kafka.clients.producer.Callback callback) {
        return producer.orElseThrow(() ->
                new IllegalStateException("This KafkaBasedLog was created in read-only mode and does not support write operations")
        ).send(new ProducerRecord<>(topic, key, value), callback);
    }

    public int partitionCount() {
        return partitionCount;
    }

    protected Producer<K, V> createProducer() {
        // Always require producer acks to all to ensure durable writes
        producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all");

        // Don't allow more than one in-flight request to prevent reordering on retry (if enabled)
        producerConfigs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        return new KafkaProducer<>(producerConfigs);
    }

    protected Consumer<K, V> createConsumer() {
        // Always force reset to the beginning of the log since this class wants to consume all available log data
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Turn off autocommit since we always want to consume the full log
        consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<>(consumerConfigs);
    }

    /**
     * Signals whether a topic partition should be read by this log. Invoked on {@link #start() startup} once
     * for every partition found in the log's backing topic.
     * <p>This method can be overridden by subclasses when only a subset of the assigned partitions
     * should be read into memory. By default, all partitions are read.
     * @param topicPartition A topic partition which could be read by this log.
     * @return true if the partition should be read by this log, false if its contents should be ignored.
     */
    protected boolean readPartition(TopicPartition topicPartition) {
        return true;
    }

    private void poll() {
        try {
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(Integer.MAX_VALUE));
            for (ConsumerRecord<K, V> record : records)
                consumedCallback.onCompletion(null, record);
        } catch (WakeupException e) {
            // Expected on get() or stop(). The calling code should handle this
            throw e;
        } catch (KafkaException e) {
            log.error("Error polling: ", e);
            if (reportErrorsToCallback) {
                consumedCallback.onCompletion(e, null);
            }
        }
    }

    /**
     * This method finds the end offsets of the Kafka log's topic partitions, optionally retrying
     * if the {@code listOffsets()} method of the admin client throws a {@link RetriableException}.
     *
     * @param shouldRetry Boolean flag to enable retry for the admin client {@code listOffsets()} call.
     * @see TopicAdmin#retryEndOffsets
     */
    private void readToLogEnd(boolean shouldRetry) {
        Set<TopicPartition> assignment = consumer.assignment();
        Map<TopicPartition, Long> endOffsets = readEndOffsets(assignment, shouldRetry);
        log.trace("Reading to end of log offsets {}", endOffsets);

        while (!endOffsets.isEmpty()) {
            Iterator<Map.Entry<TopicPartition, Long>> it = endOffsets.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<TopicPartition, Long> entry = it.next();
                TopicPartition topicPartition = entry.getKey();
                long endOffset = entry.getValue();
                long lastConsumedOffset = consumer.position(topicPartition);
                if (lastConsumedOffset >= endOffset) {
                    log.trace("Read to end offset {} for {}", endOffset, topicPartition);
                    it.remove();
                } else {
                    log.trace("Behind end offset {} for {}; last-read offset is {}",
                            endOffset, topicPartition, lastConsumedOffset);
                    poll();
                    break;
                }
            }
        }
    }

    // Visible for testing
    /**
     * Read to the end of the given list of topic partitions
     * @param assignment the topic partitions to read to the end of
     * @param shouldRetry boolean flag to enable retry for the admin client {@code listOffsets()} call.
     * @throws UnsupportedVersionException if the log's consumer is using the "read_committed" isolation level (and
     * therefore a separate admin client is required to read end offsets for the topic), but the broker does not support
     * reading end offsets using an admin client
     */
    Map<TopicPartition, Long> readEndOffsets(Set<TopicPartition> assignment, boolean shouldRetry) throws UnsupportedVersionException {
        log.trace("Reading to end of offset log");

        // Note that we'd prefer to not use the consumer to find the end offsets for the assigned topic partitions.
        // That is because it's possible that the consumer is already blocked waiting for new records to appear, when
        // the consumer is already at the end. In such cases, using 'consumer.endOffsets(...)' will block until at least
        // one more record becomes available, meaning we can't even check whether we're at the end offset.
        // Since all we're trying to do here is get the end offset, we should use the supplied admin client
        // (if available) to obtain the end offsets for the given topic partitions.

        // Deprecated constructors do not provide an admin supplier, so the admin is potentially null.
        if (admin != null) {
            // Use the admin client to immediately find the end offsets for the assigned topic partitions.
            // Unlike using the consumer
            try {
                if (shouldRetry) {
                    return admin.retryEndOffsets(assignment,
                            ADMIN_CLIENT_RETRY_DURATION,
                            ADMIN_CLIENT_RETRY_BACKOFF_MS);
                }

                return admin.endOffsets(assignment);
            } catch (UnsupportedVersionException e) {
                // This may happen with really old brokers that don't support the auto topic creation
                // field in metadata requests
                if (requireAdminForOffsets) {
                    // Should be handled by the caller during log startup
                    throw e;
                }
                log.debug("Reading to end of log offsets with consumer since admin client is unsupported: {}", e.getMessage());
                // Forget the reference to the admin so that we won't even try to use the admin the next time this method is called
                admin = null;
                // continue and let the consumer handle the read
            }
            // Other errors, like timeouts and retriable exceptions are intentionally propagated
        }
        // The admin may be null if older deprecated constructor is used or if the admin client is using a broker that doesn't
        // support getting the end offsets (e.g., 0.10.x). In such cases, we should use the consumer, which is not ideal (see above).
        return consumer.endOffsets(assignment);
    }

    private class WorkThread extends Thread {
        public WorkThread() {
            super("KafkaBasedLog Work Thread - " + topic);
        }
        @Override
        public void run() {
            log.trace("{} started execution", this);
            while (true) {
                int numCallbacks = 0;
                try {
                    synchronized (KafkaBasedLog.this) {
                        if (stopRequested)
                            break;
                        numCallbacks = readLogEndOffsetCallbacks.size();
                    }

                    if (numCallbacks > 0) {
                        try {
                            readToLogEnd(false);
                            log.trace("Finished read to end log for topic {}", topic);
                        } catch (TimeoutException e) {
                            log.warn("Timeout while reading log to end for topic '{}'. Retrying automatically. " +
                                "This may occur when brokers are unavailable or unreachable. Reason: {}", topic, e.getMessage());
                            continue;
                        } catch (RetriableException | org.apache.kafka.connect.errors.RetriableException e) {
                            log.warn("Retriable error while reading log to end for topic '{}'. Retrying automatically. " +
                                "Reason: {}", topic, e.getMessage());
                            continue;
                        } catch (WakeupException e) {
                            // Either received another get() call and need to retry reading to end of log or stop() was
                            // called. Both are handled by restarting this loop.
                            continue;
                        }
                    }

                    synchronized (KafkaBasedLog.this) {
                        // Only invoke exactly the number of callbacks we found before triggering the read to log end
                        // since it is possible for another write + readToEnd to sneak in the meantime
                        for (int i = 0; i < numCallbacks; i++) {
                            Callback<Void> cb = readLogEndOffsetCallbacks.poll();
                            cb.onCompletion(null, null);
                        }
                    }

                    try {
                        poll();
                    } catch (WakeupException e) {
                        // See previous comment, both possible causes of this wakeup are handled by starting this loop again
                        continue;
                    }
                } catch (Throwable t) {
                    log.error("Unexpected exception in {}", this, t);
                    synchronized (KafkaBasedLog.this) {
                        // Only fail exactly the number of callbacks we found before triggering the read to log end
                        // since it is possible for another write + readToEnd to sneak in the meantime which we don't
                        // want to fail.
                        for (int i = 0; i < numCallbacks; i++) {
                            Callback<Void> cb = readLogEndOffsetCallbacks.poll();
                            cb.onCompletion(t, null);
                        }
                    }
                }
            }
        }
    }
}
