/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.storage;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.util.Callback;
import org.apache.kafka.copycat.util.ConvertingFutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implementation of OffsetBackingStore that uses a Kafka topic to store offset data.
 */
public class KafkaOffsetBackingStore implements OffsetBackingStore {
    private static final Logger log = LoggerFactory.getLogger(KafkaOffsetBackingStore.class);

    public final static String OFFSET_STORAGE_TOPIC_CONFIG = "offset.storage.topic";

    private final static long CREATE_TOPIC_TIMEOUT_MS = 30000;

    private Time time;
    private Map<String, ?> configs;
    private String topic;
    private Consumer<byte[], byte[]> consumer;
    private Producer<byte[], byte[]> producer;
    private HashMap<ByteBuffer, ByteBuffer> data;

    private Thread thread;
    private boolean stopRequested;
    private Queue<Callback<Void>> readLogEndOffsetCallbacks;

    public KafkaOffsetBackingStore() {
        this(new SystemTime());
    }

    public KafkaOffsetBackingStore(Time time) {
        this.time = time;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = configs;
        topic = (String) configs.get(OFFSET_STORAGE_TOPIC_CONFIG);
        if (topic == null)
            throw new CopycatException("Offset storage topic must be specified");

        data = new HashMap<>();
        stopRequested = false;
        readLogEndOffsetCallbacks = new ArrayDeque<>();
    }

    @Override
    public void start() {
        log.info("Starting KafkaOffsetBackingStore with topic " + topic);

        producer = createProducer();
        consumer = createConsumer();
        List<TopicPartition> partitions = new ArrayList<>();

        // Until we have admin utilities we can use to check for the existence of this topic and create it if it is missing,
        // we rely on topic auto-creation
        List<PartitionInfo> partitionInfos = null;
        long started = time.milliseconds();
        while (partitionInfos == null && time.milliseconds() - started < CREATE_TOPIC_TIMEOUT_MS) {
            partitionInfos = consumer.partitionsFor(topic);
            Utils.sleep(Math.min(time.milliseconds() - started, 1000));
        }
        if (partitionInfos == null)
            throw new CopycatException("Offset topic wasn't created within the allotted period");

        for (PartitionInfo partition : partitionInfos)
            partitions.add(new TopicPartition(partition.topic(), partition.partition()));
        consumer.assign(partitions);

        readToLogEnd();

        thread = new WorkThread();
        thread.start();

        log.info("Finished reading offsets topic and starting KafkaOffsetBackingStore");
    }

    @Override
    public void stop() {
        log.info("Stopping KafkaOffsetBackingStore");

        synchronized (this) {
            stopRequested = true;
            consumer.wakeup();
        }

        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new CopycatException("Failed to stop KafkaOffsetBackingStore. Exiting without cleanly shutting " +
                    "down it's producer and consumer.", e);
        }

        try {
            producer.close();
        } catch (KafkaException e) {
            log.error("Failed to close KafkaOffsetBackingStore producer", e);
        }

        try {
            consumer.close();
        } catch (KafkaException e) {
            log.error("Failed to close KafkaOffsetBackingStore consumer", e);
        }
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(final Collection<ByteBuffer> keys,
                                                   final Callback<Map<ByteBuffer, ByteBuffer>> callback) {
        ConvertingFutureCallback<Void, Map<ByteBuffer, ByteBuffer>> future = new ConvertingFutureCallback<Void, Map<ByteBuffer, ByteBuffer>>(callback) {
            @Override
            public Map<ByteBuffer, ByteBuffer> convert(Void result) {
                Map<ByteBuffer, ByteBuffer> values = new HashMap<>();
                synchronized (KafkaOffsetBackingStore.this) {
                    for (ByteBuffer key : keys)
                        values.put(key, data.get(key));
                }
                return values;
            }
        };
        readLogEndOffsetCallbacks.add(future);
        consumer.wakeup();
        return future;
    }

    @Override
    public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values, final Callback<Void> callback) {
        SetCallbackFuture producerCallback = new SetCallbackFuture(values.size(), callback);

        for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
            producer.send(new ProducerRecord<>(topic, entry.getKey().array(), entry.getValue().array()), producerCallback);
        }

        return producerCallback;
    }



    private Producer<byte[], byte[]> createProducer() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.putAll(configs);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(producerProps);
    }

    private Consumer<byte[], byte[]> createConsumer() {
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.putAll(configs);
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return new KafkaConsumer<>(consumerConfig);
    }

    private void poll(long timeoutMs) {
        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(timeoutMs);
            for (ConsumerRecord record : records) {
                ByteBuffer key = record.key() != null ? ByteBuffer.wrap((byte[]) record.key()) : null;
                ByteBuffer value = record.value() != null ? ByteBuffer.wrap((byte[]) record.value()) : null;
                data.put(key, value);
            }
        } catch (ConsumerWakeupException e) {
            // Expected on get() or stop(). The calling code should handle this
            throw e;
        } catch (KafkaException e) {
            log.error("Error polling: " + e);
        }
    }

    private void readToLogEnd() {
        log.trace("Reading to end of offset log");

        Set<TopicPartition> assignment = consumer.assignment();

        // This approach to getting the current end offset is hacky until we have an API for looking these up directly
        Map<TopicPartition, Long> offsets = new HashMap<>();
        for (TopicPartition tp : assignment) {
            long offset = consumer.position(tp);
            offsets.put(tp, offset);
            consumer.seekToEnd(tp);
        }

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        try {
            poll(0);
        } finally {
            // If there is an exception, even a possibly expected one like ConsumerWakeupException, we need to make sure
            // the consumers position is reset or it'll get into an inconsistent state.
            for (TopicPartition tp : assignment) {
                long startOffset = offsets.get(tp);
                long endOffset = consumer.position(tp);
                if (endOffset > startOffset) {
                    endOffsets.put(tp, endOffset);
                    consumer.seek(tp, startOffset);
                }
                log.trace("Reading to end of log for {}: starting offset {} to ending offset {}", tp, startOffset, endOffset);
            }
        }

        while (!endOffsets.isEmpty()) {
            poll(Integer.MAX_VALUE);

            Iterator<Map.Entry<TopicPartition, Long>> it = endOffsets.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<TopicPartition, Long> entry = it.next();
                if (consumer.position(entry.getKey()) >= entry.getValue())
                    it.remove();
                else
                    break;
            }
        }
    }


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

    private class WorkThread extends Thread {
        @Override
        public void run() {
            try {
                while (true) {
                    int numCallbacks;
                    synchronized (KafkaOffsetBackingStore.this) {
                        if (stopRequested)
                            break;
                        numCallbacks = readLogEndOffsetCallbacks.size();
                    }

                    if (numCallbacks > 0) {
                        try {
                            readToLogEnd();
                        } catch (ConsumerWakeupException e) {
                            // Either received another get() call and need to retry reading to end of log or stop() was
                            // called. Both are handled by restarting this loop.
                            continue;
                        }
                    }

                    synchronized (KafkaOffsetBackingStore.this) {
                        for (int i = 0; i < numCallbacks; i++) {
                            Callback<Void> cb = readLogEndOffsetCallbacks.poll();
                            cb.onCompletion(null, null);
                        }
                    }

                    try {
                        poll(Integer.MAX_VALUE);
                    } catch (ConsumerWakeupException e) {
                        // See previous comment, both possible causes of this wakeup are handled by starting this loop again
                        continue;
                    }
                }
            } catch (Throwable t) {
                log.error("Unexpected exception in KafkaOffsetBackingStore's work thread", t);
            }
        }
    }
}
