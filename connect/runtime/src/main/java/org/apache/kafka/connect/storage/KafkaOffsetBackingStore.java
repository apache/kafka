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

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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

    private KafkaBasedLog<byte[], byte[]> offsetLog;
    private HashMap<ByteBuffer, ByteBuffer> data;
    private final Supplier<TopicAdmin> topicAdminSupplier;
    private SharedTopicAdmin ownTopicAdmin;

    @Deprecated
    public KafkaOffsetBackingStore() {
        this.topicAdminSupplier = null;
    }

    public KafkaOffsetBackingStore(Supplier<TopicAdmin> topicAdmin) {
        this.topicAdminSupplier = Objects.requireNonNull(topicAdmin);
    }

    @Override
    public void configure(final WorkerConfig config) {
        String topic = config.getString(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG);
        if (topic == null || topic.trim().length() == 0)
            throw new ConfigException("Offset storage topic must be specified");

        String clusterId = ConnectUtils.lookupKafkaClusterId(config);
        data = new HashMap<>();

        Map<String, Object> originals = config.originals();
        Map<String, Object> producerProps = new HashMap<>(originals);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE);
        ConnectUtils.addMetricsContextProperties(producerProps, config, clusterId);

        Map<String, Object> consumerProps = new HashMap<>(originals);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        ConnectUtils.addMetricsContextProperties(consumerProps, config, clusterId);

        Map<String, Object> adminProps = new HashMap<>(originals);
        ConnectUtils.addMetricsContextProperties(adminProps, config, clusterId);
        Supplier<TopicAdmin> adminSupplier;
        if (topicAdminSupplier != null) {
            adminSupplier = topicAdminSupplier;
        } else {
            // Create our own topic admin supplier that we'll close when we're stopped
            ownTopicAdmin = new SharedTopicAdmin(adminProps);
            adminSupplier = ownTopicAdmin;
        }
        Map<String, Object> topicSettings = config instanceof DistributedConfig
                                            ? ((DistributedConfig) config).offsetStorageTopicSettings()
                                            : Collections.emptyMap();
        NewTopic topicDescription = TopicAdmin.defineTopic(topic)
                .config(topicSettings) // first so that we override user-supplied settings as needed
                .compacted()
                .partitions(config.getInt(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG))
                .replicationFactor(config.getShort(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG))
                .build();

        offsetLog = createKafkaBasedLog(topic, producerProps, consumerProps, consumedCallback, topicDescription, adminSupplier);
    }

    private KafkaBasedLog<byte[], byte[]> createKafkaBasedLog(String topic, Map<String, Object> producerProps,
                                                              Map<String, Object> consumerProps,
                                                              Callback<ConsumerRecord<byte[], byte[]>> consumedCallback,
                                                              final NewTopic topicDescription, Supplier<TopicAdmin> adminSupplier) {
        java.util.function.Consumer<TopicAdmin> createTopics = admin -> {
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
        return new KafkaBasedLog<>(topic, producerProps, consumerProps, adminSupplier, consumedCallback, Time.SYSTEM, createTopics);
    }

    @Override
    public void start() {
        log.info("Starting KafkaOffsetBackingStore");
        offsetLog.start();
        log.info("Finished reading offsets topic and starting KafkaOffsetBackingStore");
    }

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

    private final Callback<ConsumerRecord<byte[], byte[]>> consumedCallback = new Callback<ConsumerRecord<byte[], byte[]>>() {
        @Override
        public void onCompletion(Throwable error, ConsumerRecord<byte[], byte[]> record) {
            ByteBuffer key = record.key() != null ? ByteBuffer.wrap(record.key()) : null;
            ByteBuffer value = record.value() != null ? ByteBuffer.wrap(record.value()) : null;
            data.put(key, value);
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
