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
package org.apache.kafka.connect.storage;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.ConvertingFutureCallback;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.SharedTopicAdmin;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * <p>
 *     Implementation of OffsetBackingStore that uses a Kafka topic to store offset data.
 * </p>
 * <p>
 *     Internally, this implementation both produces to and consumes from a Kafka topic which stores the offsets.
 *     It accepts producer and consumer overrides via its configuration but forces some settings to specific values
 *     to ensure correct behavior (e.g. acks, auto.offset.reset).
 * </p>
 */
public class KafkaOffsetBackingStore implements OffsetBackingStore {
    private static final Logger log = LoggerFactory.getLogger(KafkaOffsetBackingStore.class);

    /**
     * Build a connector-specific offset store with read and write support. The producer will be {@link Producer#close(Duration) closed}
     * and the consumer will be {@link Consumer#close(Duration) closed} when this store is {@link #stop() stopped}, but the topic admin
     * must be {@link TopicAdmin#close(Duration) closed} by the caller.
     * @param topic the name of the offsets topic to use
     * @param producer the producer to use for writing to the offsets topic
     * @param consumer the consumer to use for reading from the offsets topic
     * @param topicAdmin the topic admin to use for creating and querying metadata for the offsets topic
     * @return an offset store backed by the given topic and Kafka clients
     */
    public static KafkaOffsetBackingStore forTask(
            String topic,
            Producer<byte[], byte[]> producer,
            Consumer<byte[], byte[]> consumer,
            TopicAdmin topicAdmin
    ) {
        return new KafkaOffsetBackingStore(() -> topicAdmin, KafkaOffsetBackingStore::noClientId) {
            @Override
            public void configure(final WorkerConfig config) {
                this.exactlyOnce = config.exactlyOnceSourceEnabled();
                this.offsetLog = KafkaBasedLog.withExistingClients(
                        topic,
                        consumer,
                        producer,
                        topicAdmin,
                        consumedCallback,
                        Time.SYSTEM,
                        initialize(topic, newTopicDescription(topic, config))
                );
            }
        };
    }

    /**
     * Build a connector-specific offset store with read-only support. The consumer will be {@link Consumer#close(Duration) closed}
     * when this store is {@link #stop() stopped}, but the topic admin must be {@link TopicAdmin#close(Duration) closed} by the caller.
     * @param topic the name of the offsets topic to use
     * @param consumer the consumer to use for reading from the offsets topic
     * @param topicAdmin the topic admin to use for creating and querying metadata for the offsets topic
     * @return a read-only offset store backed by the given topic and Kafka clients
     */
    public static KafkaOffsetBackingStore forConnector(
            String topic,
            Consumer<byte[], byte[]> consumer,
            TopicAdmin topicAdmin
    ) {
        return new KafkaOffsetBackingStore(() -> topicAdmin, KafkaOffsetBackingStore::noClientId) {
            @Override
            public void configure(final WorkerConfig config) {
                this.exactlyOnce = config.exactlyOnceSourceEnabled();
                this.offsetLog = KafkaBasedLog.withExistingClients(
                        topic,
                        consumer,
                        null,
                        topicAdmin,
                        consumedCallback,
                        Time.SYSTEM,
                        initialize(topic, newTopicDescription(topic, config))
                );
            }
        };
    }

    private static String noClientId() {
        throw new UnsupportedOperationException("This offset store should not instantiate any Kafka clients");
    }

    protected KafkaBasedLog<byte[], byte[]> offsetLog;
    // Visible for testing
    final HashMap<ByteBuffer, ByteBuffer> data = new HashMap<>();
    private final Supplier<TopicAdmin> topicAdminSupplier;
    private final Supplier<String> clientIdBase;
    private SharedTopicAdmin ownTopicAdmin;
    protected boolean exactlyOnce;

    /**
     * Create an {@link OffsetBackingStore} backed by a Kafka topic. This constructor will cause the
     * store to instantiate and close its own {@link TopicAdmin} during {@link #configure(WorkerConfig)}
     * and {@link #stop()}, respectively.
     *
     * @deprecated use {@link #KafkaOffsetBackingStore(Supplier, Supplier)} instead
     */
    @Deprecated
    public KafkaOffsetBackingStore() {
        this.topicAdminSupplier = null;
        this.clientIdBase = () -> "connect-distributed-";
    }

    /**
     * Create an {@link OffsetBackingStore} backed by a Kafka topic. This constructor will use the given
     * {@link Supplier} to acquire a {@link TopicAdmin} that will be used for interactions with the backing
     * Kafka topic. The caller is expected to manage the lifecycle of that object, including
     * {@link TopicAdmin#close(Duration) closing} it when it is no longer needed.
     * @param topicAdmin a {@link Supplier} for the {@link TopicAdmin} to use for this backing store;
     *                   may not be null, and may not return null
     * @param clientIdBase a {@link Supplier} that will be used to create a
     * {@link CommonClientConfigs#CLIENT_ID_DOC client ID} for Kafka clients instantiated by this store;
     *                     may not be null, and may not return null, but may throw {@link UnsupportedOperationException}
     *                     if this offset store should not create its own Kafka clients
     */
    public KafkaOffsetBackingStore(Supplier<TopicAdmin> topicAdmin, Supplier<String> clientIdBase) {
        this.topicAdminSupplier = Objects.requireNonNull(topicAdmin);
        this.clientIdBase = Objects.requireNonNull(clientIdBase);
    }


    @Override
    public void configure(final WorkerConfig config) {
        String topic = config.getString(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG);
        if (topic == null || topic.trim().length() == 0)
            throw new ConfigException("Offset storage topic must be specified");

        this.exactlyOnce = config.exactlyOnceSourceEnabled();

        String clusterId = config.kafkaClusterId();
        String clientId = Objects.requireNonNull(clientIdBase.get()) + "offsets";

        Map<String, Object> originals = config.originals();
        Map<String, Object> producerProps = new HashMap<>(originals);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE);
        // By default, Connect disables idempotent behavior for all producers, even though idempotence became
        // default for Kafka producers. This is to ensure Connect continues to work with many Kafka broker versions, including older brokers that do not support
        // idempotent producers or require explicit steps to enable them (e.g. adding the IDEMPOTENT_WRITE ACL to brokers older than 2.8).
        // These settings might change when https://cwiki.apache.org/confluence/display/KAFKA/KIP-318%3A+Make+Kafka+Connect+Source+idempotent
        // gets approved and scheduled for release.
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        producerProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
        ConnectUtils.addMetricsContextProperties(producerProps, config, clusterId);

        Map<String, Object> consumerProps = new HashMap<>(originals);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
        ConnectUtils.addMetricsContextProperties(consumerProps, config, clusterId);
        if (config.exactlyOnceSourceEnabled()) {
            ConnectUtils.ensureProperty(
                    consumerProps, ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT),
                    "for the worker offsets topic consumer when exactly-once source support is enabled",
                    false
            );
        }

        Map<String, Object> adminProps = new HashMap<>(originals);
        adminProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
        ConnectUtils.addMetricsContextProperties(adminProps, config, clusterId);
        Supplier<TopicAdmin> adminSupplier;
        if (topicAdminSupplier != null) {
            adminSupplier = topicAdminSupplier;
        } else {
            // Create our own topic admin supplier that we'll close when we're stopped
            this.ownTopicAdmin = new SharedTopicAdmin(adminProps);
            adminSupplier = ownTopicAdmin;
        }
        NewTopic topicDescription = newTopicDescription(topic, config);

        this.offsetLog = createKafkaBasedLog(topic, producerProps, consumerProps, consumedCallback, topicDescription, adminSupplier);
    }

    // Visible for testing
    KafkaBasedLog<byte[], byte[]> createKafkaBasedLog(String topic, Map<String, Object> producerProps,
                                                              Map<String, Object> consumerProps,
                                                              Callback<ConsumerRecord<byte[], byte[]>> consumedCallback,
                                                              final NewTopic topicDescription, Supplier<TopicAdmin> adminSupplier) {
        java.util.function.Consumer<TopicAdmin> createTopics = initialize(topic, topicDescription);
        return new KafkaBasedLog<>(topic, producerProps, consumerProps, adminSupplier, consumedCallback, Time.SYSTEM, createTopics);
    }

    protected NewTopic newTopicDescription(final String topic, final WorkerConfig config) {
        Map<String, Object> topicSettings = config instanceof DistributedConfig
                ? ((DistributedConfig) config).offsetStorageTopicSettings()
                : Collections.emptyMap();
        return TopicAdmin.defineTopic(topic)
                .config(topicSettings) // first so that we override user-supplied settings as needed
                .compacted()
                .partitions(config.getInt(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG))
                .replicationFactor(config.getShort(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG))
                .build();
    }

    protected java.util.function.Consumer<TopicAdmin> initialize(final String topic, final NewTopic topicDescription) {
        return admin -> {
            log.debug("Creating admin client to manage Connect internal offset topic");
            // Create the topic if it doesn't exist
            Set<String> newTopics = admin.createTopics(topicDescription);
            if (!newTopics.contains(topic)) {
                // It already existed, so check that the topic cleanup policy is compact only and not delete
                log.debug("Using admin client to check cleanup policy for '{}' topic is '{}'", topic, TopicConfig.CLEANUP_POLICY_COMPACT);
                admin.verifyTopicCleanupPolicyOnlyCompact(topic,
                        DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "source connector offsets");
            }
        };
    }

    @Override
    public void start() {
        log.info("Starting KafkaOffsetBackingStore");
        try {
            offsetLog.start();
        } catch (UnsupportedVersionException e) {
            String message;
            if (exactlyOnce) {
                message = "Enabling exactly-once support for source connectors requires a Kafka broker version that allows "
                        + "admin clients to read consumer offsets. Please either disable the worker's exactly-once "
                        + "support for source connectors, or upgrade to a newer Kafka broker version.";
            } else {
                message = "When " + ConsumerConfig.ISOLATION_LEVEL_CONFIG + "is set to "
                        + IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT)
                        + ", a Kafka broker version that allows admin clients to read consumer offsets is required. "
                        + "Please either reconfigure the worker or connector, or upgrade to a newer Kafka broker version.";
            }
            throw new ConnectException(message, e);
        }
        log.info("Finished reading offsets topic and starting KafkaOffsetBackingStore");
    }

    /**
     * Stop reading from and writing to the offsets topic, and relinquish resources allocated for interacting
     * with it, including Kafka clients.
     * <p>
     * <b>Note:</b> if the now-deprecated {@link #KafkaOffsetBackingStore()} constructor was used to create
     * this store, the underlying admin client allocated for interacting with the offsets topic will be closed.
     * On the other hand, if the recommended {@link #KafkaOffsetBackingStore(Supplier, Supplier)} constructor was
     * used to create this store, the admin client derived from the given {@link Supplier} will not be closed and it is the
     * caller's responsibility to manage its lifecycle accordingly.
     */
    @Override
    public void stop() {
        log.info("Stopping KafkaOffsetBackingStore");
        try {
            offsetLog.stop();
        } finally {
            if (ownTopicAdmin != null) {
                ownTopicAdmin.close();
            }
        }
        log.info("Stopped KafkaOffsetBackingStore");
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(final Collection<ByteBuffer> keys) {
        ConvertingFutureCallback<Void, Map<ByteBuffer, ByteBuffer>> future = new ConvertingFutureCallback<Void, Map<ByteBuffer, ByteBuffer>>() {
            @Override
            public Map<ByteBuffer, ByteBuffer> convert(Void result) {
                Map<ByteBuffer, ByteBuffer> values = new HashMap<>();
                for (ByteBuffer key : keys)
                    values.put(key, data.get(key));
                return values;
            }
        };
        // This operation may be relatively (but not too) expensive since it always requires checking end offsets, even
        // if we've already read up to the end. However, it also should not be common (offsets should only be read when
        // resetting a task). Always requiring that we read to the end is simpler than trying to differentiate when it
        // is safe not to (which should only be if we *know* we've maintained ownership since the last write).
        offsetLog.readToEnd(future);
        return future;
    }

    @Override
    public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values, final Callback<Void> callback) {
        SetCallbackFuture producerCallback = new SetCallbackFuture(values.size(), callback);

        for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
            ByteBuffer key = entry.getKey();
            ByteBuffer value = entry.getValue();
            offsetLog.send(key == null ? null : key.array(), value == null ? null : value.array(), producerCallback);
        }

        return producerCallback;
    }

    protected final Callback<ConsumerRecord<byte[], byte[]>> consumedCallback = (error, record) -> {
        if (error != null) {
            log.error("Failed to read from the offsets topic", error);
            return;
        }

        ByteBuffer key = record.key() != null ? ByteBuffer.wrap(record.key()) : null;

        if (record.value() == null) {
            data.remove(key);
        } else {
            data.put(key, ByteBuffer.wrap(record.value()));
        }
    };

    private static class SetCallbackFuture implements org.apache.kafka.clients.producer.Callback, Future<Void> {
        private int numLeft;
        private boolean completed = false;
        private Throwable exception = null;
        private final Callback<Void> callback;

        public SetCallbackFuture(int numRecords, Callback<Void> callback) {
            numLeft = numRecords;
            this.callback = callback;
        }

        @Override
        public synchronized void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                if (!completed) {
                    this.exception = exception;
                    callback.onCompletion(exception, null);
                    completed = true;
                    this.notify();
                }
                return;
            }

            numLeft -= 1;
            if (numLeft == 0) {
                callback.onCompletion(null, null);
                completed = true;
                this.notify();
            }
        }

        @Override
        public synchronized boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public synchronized boolean isCancelled() {
            return false;
        }

        @Override
        public synchronized boolean isDone() {
            return completed;
        }

        @Override
        public synchronized Void get() throws InterruptedException, ExecutionException {
            while (!completed) {
                this.wait();
            }
            if (exception != null)
                throw new ExecutionException(exception);
            return null;
        }

        @Override
        public synchronized Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            long started = System.currentTimeMillis();
            long limit = started + unit.toMillis(timeout);
            while (!completed) {
                long leftMs = limit - System.currentTimeMillis();
                if (leftMs < 0)
                    throw new TimeoutException("KafkaOffsetBackingStore Future timed out.");
                this.wait(leftMs);
            }
            if (exception != null)
                throw new ExecutionException(exception);
            return null;
        }
    }
}
